"""
Data Quality Testing Framework / Framework de Testes de Qualidade de Dados
===========================================================================

EN: Production-grade data quality tests using Pytest + SQL assertions.
    Validates schema, referential integrity, business logic, and performance.

PT: Testes de qualidade de dados para produção usando Pytest + SQL.
    Valida schema, integridade referencial, regras de negócio e performance.

Validates / Valida:
- Schema correctness / Tipos e nullability corretos
- Referential integrity / Integridade de chaves estrangeiras
- Business logic / Regras de negócio (ranges, deduplicação)
- Performance / Tempo de execução dentro do SLA

Author: David Luchetti
Updated: February 2026

Usage / Uso:
    pytest templates/pytest_data_quality.py -v
    pytest templates/pytest_data_quality.py::TestSilverOrdersSchema -v
    pytest templates/pytest_data_quality.py -m "not slow"  # Skip slow / Pula lentos
"""

import pytest
import pandas as pd
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

# ============================================================================
# Fixtures (Test Setup)
# ============================================================================


@pytest.fixture(scope="session")
def postgres_connection():
    """Fixture to establish PostgreSQL connection for all tests."""
    from airflow.hooks.postgres_hook import PostgresHook

    hook = PostgresHook(postgres_conn_id="postgres_warehouse")
    yield hook
    # Cleanup if needed


@pytest.fixture
def orders_df(postgres_connection):
    """Load current orders from silver layer."""
    sql = "SELECT * FROM silver.orders WHERE is_current = TRUE"
    return postgres_connection.get_pandas_df(sql)


@pytest.fixture
def customers_df(postgres_connection):
    """Load current customers from silver layer."""
    sql = "SELECT * FROM silver.customers WHERE is_current = TRUE"
    return postgres_connection.get_pandas_df(sql)


# ============================================================================
# Schema Validation Tests
# ============================================================================


class TestSilverOrdersSchema:
    """Validate schema of silver.orders table."""

    def test_required_columns_exist(self, orders_df):
        """Check that all required columns are present."""
        required_cols = [
            "order_id",
            "customer_id",
            "order_date",
            "order_amount",
            "status",
            "effective_date",
            "end_date",
            "is_current",
        ]
        assert all(col in orders_df.columns for col in required_cols), (
            f"Missing required columns. Expected {required_cols}, "
            f"got {list(orders_df.columns)}"
        )

    def test_column_data_types(self, orders_df):
        """Validate data types of critical columns."""
        expected_types = {
            "order_id": "int64",
            "order_amount": ["float64", "decimal"],
            "order_date": "object",  # datetime
            "is_current": "bool",
        }

        for col, expected_type in expected_types.items():
            actual_type = str(orders_df[col].dtype)
            if isinstance(expected_type, list):
                assert any(
                    exp in actual_type for exp in expected_type
                ), f"Column {col}: expected {expected_type}, got {actual_type}"
            else:
                assert (
                    expected_type in actual_type
                ), f"Column {col}: expected {expected_type}, got {actual_type}"

    def test_no_unexpected_nulls(self, orders_df):
        """Check that critical columns have no NULLs."""
        non_nullable = ["order_id", "customer_id", "order_date", "is_current"]

        for col in non_nullable:
            null_count = orders_df[col].isna().sum()
            assert (
                null_count == 0
            ), f"Column {col} should not have NULLs, but found {null_count}"


# ============================================================================
# Data Quality Tests (Deduplication, Integrity)
# ============================================================================


class TestSilverOrdersQuality:
    """Validate data quality of silver.orders."""

    def test_no_duplicate_current_records(self, orders_df):
        """Ensure no order_id appears twice with is_current=TRUE."""
        duplicates = orders_df[orders_df["is_current"] == True].groupby("order_id").size()
        max_duplicates = duplicates.max()

        assert (
            max_duplicates == 1
        ), f"Found {(duplicates > 1).sum()} orders with duplicate current records"

    def test_effective_date_logic(self, orders_df):
        """Validate effective/end date logic."""
        # Current records should have NULL end_date
        current_with_end = orders_df[
            (orders_df["is_current"] == True) & (orders_df["end_date"].notna())
        ]
        assert len(current_with_end) == 0, (
            f"Found {len(current_with_end)} current records with end_date != NULL"
        )

        # Historical records should have end_date > effective_date
        historical = orders_df[orders_df["is_current"] == False]
        if len(historical) > 0:
            invalid_dates = (
                historical[
                    pd.to_datetime(historical["end_date"])
                    <= pd.to_datetime(historical["effective_date"])
                ].shape[0]
            )
            assert (
                invalid_dates == 0
            ), f"Found {invalid_dates} historical records with end_date <= effective_date"

    def test_order_amounts_positive(self, orders_df):
        """All order amounts should be positive."""
        negative_amounts = (orders_df["order_amount"] < 0).sum()
        assert (
            negative_amounts == 0
        ), f"Found {negative_amounts} negative order amounts"

    def test_order_status_valid_values(self, orders_df):
        """Order status should be one of valid values."""
        valid_statuses = {"pending", "confirmed", "shipped", "delivered", "cancelled"}
        invalid_statuses = set(orders_df["status"].unique()) - valid_statuses

        assert len(invalid_statuses) == 0, (
            f"Found invalid status values: {invalid_statuses}. "
            f"Valid: {valid_statuses}"
        )


# ============================================================================
# Referential Integrity Tests
# ============================================================================


class TestReferentialIntegrity:
    """Validate foreign key relationships."""

    def test_customer_ids_exist(self, postgres_connection, orders_df):
        """All customer_ids in orders should exist in customers table."""
        order_customer_ids = set(orders_df["customer_id"].unique())

        customers_sql = "SELECT DISTINCT customer_id FROM silver.customers WHERE is_current = TRUE"
        customers_df = postgres_connection.get_pandas_df(customers_sql)
        valid_customer_ids = set(customers_df["customer_id"].unique())

        missing_customers = order_customer_ids - valid_customer_ids
        assert len(missing_customers) == 0, (
            f"Found {len(missing_customers)} orders with non-existent customer_ids: "
            f"{list(missing_customers)[:10]}"  # Show first 10
        )

    def test_no_orphaned_records(self, postgres_connection):
        """Check for orphaned records in any detail table."""
        orphan_check = postgres_connection.get_first(
            """
            SELECT COUNT(*) FROM silver.order_items oi
            WHERE NOT EXISTS (
                SELECT 1 FROM silver.orders o
                WHERE o.order_id = oi.order_id
                  AND o.is_current = TRUE
            )
            """
        )
        assert orphan_check[0] == 0, f"Found {orphan_check[0]} orphaned order items"


# ============================================================================
# Business Logic Tests
# ============================================================================


class TestBusinessLogic:
    """Validate business rules."""

    def test_recent_orders_exist(self, orders_df):
        """Check that we have recent orders (prevent missing data alerts)."""
        orders_df["order_date"] = pd.to_datetime(orders_df["order_date"])
        max_order_date = orders_df["order_date"].max()
        today = datetime.now()

        days_since_last_order = (today - max_order_date).days
        assert days_since_last_order <= 1, (
            f"No orders in the last 24 hours. Last order date: {max_order_date}"
        )

    def test_row_count_not_dropped_significantly(
        self, postgres_connection, orders_df
    ):
        """Alert if row count drops > 20% from previous day."""
        current_count = len(orders_df)

        # Get previous day count from audit table
        previous_count = postgres_connection.get_first(
            """
            SELECT SUM(total_rows) FROM audit.table_metrics
            WHERE table_name = 'silver.orders'
              AND metric_date = CURRENT_DATE - 1
            """
        )

        if previous_count and previous_count[0]:
            previous_count = previous_count[0][0]
            ratio = current_count / previous_count

            assert ratio >= 0.8, (
                f"Row count dropped {(1-ratio)*100:.1f}%. "
                f"Previous: {previous_count}, Current: {current_count}"
            )

    @pytest.mark.parametrize(
        "status",
        [
            "pending",
            "confirmed",
            "shipped",
            "delivered",
            "cancelled",
        ],
    )
    def test_all_status_categories_present(self, orders_df, status):
        """Ensure all expected order statuses have at least some data."""
        status_count = (orders_df["status"] == status).sum()
        assert status_count > 0, (
            f"No orders found with status='{status}'. "
            f"This might indicate incomplete data load."
        )


# ============================================================================
# Performance Tests
# ============================================================================


class TestPerformance:
    """Validate performance SLAs."""

    @pytest.mark.slow
    def test_query_execution_time(self, postgres_connection):
        """Ensure critical queries complete within SLA."""
        import time

        start = time.time()
        postgres_connection.get_pandas_df(
            "SELECT * FROM silver.orders WHERE is_current = TRUE LIMIT 1000"
        )
        elapsed = time.time() - start

        assert elapsed < 2.0, (
            f"Query took {elapsed:.2f}s, expected < 2s. "
            f"Consider adding indexes or partitioning."
        )

    @pytest.mark.slow
    def test_large_join_performance(self, postgres_connection):
        """Test performance of expensive joins."""
        import time

        start = time.time()
        postgres_connection.get_pandas_df(
            """
            SELECT COUNT(*)
            FROM silver.orders o
            JOIN silver.customers c ON o.customer_id = c.customer_id
            WHERE o.is_current = TRUE
              AND c.is_current = TRUE
            """
        )
        elapsed = time.time() - start

        assert elapsed < 5.0, (
            f"Large join took {elapsed:.2f}s, expected < 5s. "
            f"Check indexes on customer_id."
        )


# ============================================================================
# Test Markers (allow filtering)
# ============================================================================

# Usage: pytest -m "not slow" to skip slow tests


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line("markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')")
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests (require database)"
    )


# ============================================================================
# Example: Run with markers
# ============================================================================

#
# pytest templates/pytest_data_quality.py -v
#     → Runs all tests
#
# pytest templates/pytest_data_quality.py -m "not slow" -v
#     → Runs fast tests only (schema, integrity, business logic)
#
# pytest templates/pytest_data_quality.py::TestSilverOrdersSchema -v
#     → Runs only schema validation tests
#
# pytest templates/pytest_data_quality.py --tb=short
#     → Short traceback format
