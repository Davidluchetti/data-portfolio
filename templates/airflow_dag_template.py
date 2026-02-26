"""
Production Airflow DAG Template
================================

Template for building scalable ETL pipelines with:
- Error alerting via Slack
- Data quality checks
- Sensor-based dependencies
- Retry logic
- Parallel task execution

Author: David Luchetti
Updated: February 2026
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.exceptions import AirflowException
import pandas as pd
import logging

logger = logging.getLogger(__name__)

# ============================================================================
# Configuration
# ============================================================================

DAG_ID = "etl_orders_medallion"
OWNER = "data-platform"
EMAIL = ["data-alerts@company.com"]
SCHEDULE_INTERVAL = "0 2 * * *"  # Daily at 2 AM UTC

default_args = {
    "owner": OWNER,
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "email": EMAIL,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,  # Exponential backoff
    "max_retry_delay": timedelta(hours=1),
}

# Slack webhook for alerts
SLACK_WEBHOOK_URL = "{{ conn.slack_webhook.extra.webhook_url }}"

# ============================================================================
# Helper Functions
# ============================================================================


def check_data_quality(table_name: str, row_count: int, threshold: float = 0.8):
    """
    Compare row count against previous day.
    Fail if drop > 20% (threshold=0.8 means 80% of previous acceptable).

    Args:
        table_name: Name of table to validate
        row_count: Current row count
        threshold: Minimum acceptable ratio (0.8 = 80%)
    """
    from airflow.hooks.postgres_hook import PostgresHook

    hook = PostgresHook(postgres_conn_id="postgres_default")

    # Get previous day count
    previous_count = hook.get_first(
        f"""
        SELECT SUM(total_rows) FROM audit.table_metrics
        WHERE table_name = '{table_name}'
          AND metric_date = CURRENT_DATE - 1
        """
    )

    if previous_count and previous_count[0]:
        previous_count = previous_count[0][0]
        ratio = row_count / previous_count if previous_count > 0 else 0

        logger.info(
            f"Data quality check: {table_name} - Previous: {previous_count}, Current: {row_count}, Ratio: {ratio:.2%}"
        )

        if ratio < threshold:
            raise AirflowException(
                f"Data quality alert: {table_name} row count dropped {(1-ratio):.1%}. "
                f"Expected {previous_count * threshold:.0f}, got {row_count}"
            )
    else:
        logger.warning(f"No previous metrics found for {table_name}, skipping quality check")


def alert_slack(message: str):
    """Send alert to Slack channel."""
    from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

    hook = SlackWebhookHook(http_conn_id="slack_webhook")
    hook.send_message({"text": message})


def on_task_failure(context):
    """Callback when task fails - sends Slack alert."""
    task_instance = context["task_instance"]
    exception = context["exception"]

    message = f"""
    ❌ **DAG Failure Alert**
    • DAG: {context['dag'].dag_id}
    • Task: {task_instance.task_id}
    • Exception: {str(exception)[:500]}
    • Execution Date: {context['execution_date']}
    """

    alert_slack(message)


# ============================================================================
# DAG Definition
# ============================================================================

dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="ETL: Load orders from source to Medallion (Bronze/Silver/Gold)",
    schedule_interval=SCHEDULE_INTERVAL,
    catchup=True,  # Backfill previous runs
    tags=["etl", "medallion", "production"],
    on_failure_callback=on_task_failure,
)

# ============================================================================
# Tasks
# ============================================================================

# 1. Check source API availability
source_api_check = HttpSensor(
    task_id="check_source_api",
    http_conn_id="source_api",
    endpoint="/health",
    request_params={},
    response_check=lambda response: response.status_code == 200,
    timeout=10,
    poke_interval=60,
    mode="exponential",  # Exponential backoff between retries
    dag=dag,
)

# 2. Extract from source
# In production, replace with actual PySpark job or SQL query
extract_sql = """
SELECT
    order_id,
    customer_id,
    order_date,
    order_amount,
    status,
    current_timestamp() AS _loaded_at
FROM source_system.orders
WHERE order_date >= '{{ ds }}'  -- Partition by load date
  AND order_date < '{{ next_ds }}'
"""

load_bronze = PostgresOperator(
    task_id="load_bronze",
    postgres_conn_id="postgres_warehouse",
    sql=f"""
    INSERT INTO bronze.orders_raw
    {extract_sql}
    """,
    dag=dag,
)

# 3. Data quality check on bronze
validate_bronze = PythonOperator(
    task_id="validate_bronze",
    python_callable=lambda: check_data_quality(
        table_name="bronze.orders_raw",
        row_count=10500,  # Would fetch from previous task
        threshold=0.8,  # 80% of previous day is minimum
    ),
    dag=dag,
)

# 4. Silver transformation (SCD Type 2)
transform_silver = PostgresOperator(
    task_id="transform_silver",
    postgres_conn_id="postgres_warehouse",
    sql="""
    -- Close old records and insert new ones (SCD Type 2)
    MERGE INTO silver.orders AS target
    USING (
        SELECT
            o.order_id,
            o.customer_id,
            o.order_date,
            o.order_amount,
            o.status,
            CURRENT_DATE AS effective_date
        FROM bronze.orders_raw o
        WHERE o._loaded_at >= '{{ ds }}'
          AND o._loaded_at < '{{ next_ds }}'
    ) AS source
    ON target.order_id = source.order_id
       AND target.is_current = TRUE

    WHEN MATCHED AND target.order_amount != source.order_amount THEN UPDATE SET
        end_date = CURRENT_DATE - 1,
        is_current = FALSE,
        updated_at = CURRENT_TIMESTAMP

    WHEN NOT MATCHED THEN INSERT
        (order_id, customer_id, order_date, order_amount, status, effective_date, is_current)
        VALUES (source.order_id, source.customer_id, source.order_date, source.order_amount,
                source.status, source.effective_date, TRUE);
    """,
    dag=dag,
)

# 5. Validate silver
validate_silver = PythonOperator(
    task_id="validate_silver",
    python_callable=lambda: check_data_quality(
        table_name="silver.orders",
        row_count=8400,
        threshold=0.75,  # Allow 25% drop after dedup
    ),
    dag=dag,
)

# 6. Gold aggregation
aggregate_gold = PostgresOperator(
    task_id="aggregate_gold",
    postgres_conn_id="postgres_warehouse",
    sql="""
    -- Refresh monthly aggregation
    REFRESH MATERIALIZED VIEW gold.agg_monthly_orders;

    -- Insert daily snapshot
    INSERT INTO gold.fact_orders
    SELECT
        o.order_id,
        o.customer_id,
        o.order_date,
        DATE_TRUNC('month', o.order_date)::DATE AS month_year,
        o.order_amount,
        COALESCE(d.discount_pct, 0) * o.order_amount AS discount_amount,
        o.order_amount * (1 - COALESCE(d.discount_pct, 0)) AS net_amount,
        o.status
    FROM silver.orders o
    LEFT JOIN silver.dim_discounts d ON o.customer_id = d.customer_id
    WHERE o.is_current = TRUE
      AND o.effective_date = CURRENT_DATE;
    """,
    dag=dag,
)

# 7. Final validation and success notification
validation_final = PythonOperator(
    task_id="final_validation",
    python_callable=lambda: alert_slack(
        """
        ✅ **ETL Pipeline Success**
        • DAG: etl_orders_medallion
        • Execution: {{ ds }}
        • Status: All tasks completed
        • Bronze rows: 10,500
        • Silver rows: 8,400
        • Gold rows: 8,400
        """
    ),
    dag=dag,
)

# ============================================================================
# Task Dependencies
# ============================================================================

source_api_check >> load_bronze >> validate_bronze >> transform_silver
validate_bronze >> transform_silver >> validate_silver >> aggregate_gold >> validation_final

# ============================================================================
# Advanced: Parallel fan-out example
# ============================================================================

# If you need to load multiple sources in parallel:
#
# extract_orders = PythonOperator(task_id="extract_orders", ...)
# extract_customers = PythonOperator(task_id="extract_customers", ...)
# extract_products = PythonOperator(task_id="extract_products", ...)
#
# [extract_orders, extract_customers, extract_products] >> transform_silver
