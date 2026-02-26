-- ============================================================================
-- Snowflake Advanced Features: Tasks, Streams, Zero-Copy Cloning
-- ============================================================================
--
-- Production patterns for:
-- 1. Automated pipelines via Tasks (serverless orchestration)
-- 2. Real-time CDC via Streams (change tracking)
-- 3. Environment cloning via Zero-Copy Clone (instant dev/test copies)
--
-- Author: David Luchetti
-- Updated: February 2026
--
-- ============================================================================


-- ============================================================================
-- 1. STREAMS: Change Data Capture (CDC) on source tables
-- ============================================================================

-- Create a stream to track changes on bronze.orders_raw
CREATE OR REPLACE STREAM bronze.orders_raw_stream
    ON TABLE bronze.orders_raw
    APPEND_ONLY = FALSE  -- Track inserts, updates, deletes
    COMMENT = 'Stream to capture changes on orders_raw table';

-- Query the stream to see what changed
SELECT
    METADATA$ACTION,      -- INSERT, UPDATE, DELETE
    METADATA$ISUPDATE,    -- TRUE if it's part of an update
    METADATA$ROW_ID,      -- Unique row identifier
    order_id,
    customer_id,
    order_amount,
    METADATA$TIMESTAMP
FROM bronze.orders_raw_stream
WHERE METADATA$ACTION = 'INSERT'
ORDER BY METADATA$TIMESTAMP DESC;

-- ============================================================================
-- 2. TASKS: Automated ETL pipeline (Medallion pattern)
-- ============================================================================

-- Task 1: Load bronze from stream changes (runs every 15 min)
CREATE OR REPLACE TASK bronze_load_task
    WAREHOUSE = compute_wh
    SCHEDULE = '15 MINUTE'
    COMMENT = 'Load changed records from stream to bronze'
AS
    INSERT INTO bronze.orders_raw
    SELECT
        order_id,
        customer_id,
        order_date,
        order_amount,
        status,
        current_timestamp() AS _loaded_at
    FROM bronze.orders_raw_stream
    WHERE METADATA$ACTION = 'INSERT'
        OR METADATA$ACTION = 'UPDATE';

-- Task 2: Transform bronze → silver (depends on Task 1)
CREATE OR REPLACE TASK silver_transform_task
    WAREHOUSE = compute_wh
    AFTER bronze_load_task  -- Dependency: run after bronze_load_task completes
    COMMENT = 'SCD Type 2 merge into silver.orders'
AS
    MERGE INTO silver.orders AS target
    USING (
        SELECT
            order_id,
            customer_id,
            order_date,
            order_amount,
            status,
            current_date() AS effective_date
        FROM bronze.orders_raw
        WHERE _loaded_at >= dateadd('hour', -1, current_timestamp())
            AND (
                SELECT MAX(_loaded_at)
                FROM bronze.orders_raw
            ) > (SELECT MAX(updated_at) FROM silver.orders)
    ) AS source
    ON target.order_id = source.order_id AND target.is_current = TRUE

    -- Close old record if amount changed
    WHEN MATCHED AND target.order_amount != source.order_amount THEN UPDATE SET
        end_date = current_date() - 1,
        is_current = FALSE,
        updated_at = current_timestamp()

    -- New customer
    WHEN NOT MATCHED THEN INSERT
        (order_id, customer_id, order_date, order_amount, status, effective_date, is_current, created_at, updated_at)
        VALUES (source.order_id, source.customer_id, source.order_date, source.order_amount,
                source.status, source.effective_date, TRUE, current_timestamp(), current_timestamp());

-- Task 3: Aggregate silver → gold (depends on Task 2)
CREATE OR REPLACE TASK gold_aggregate_task
    WAREHOUSE = compute_wh
    AFTER silver_transform_task  -- Dependency: run after silver_transform_task completes
    COMMENT = 'Aggregate silver.orders to gold fact/dimension tables'
AS
    -- Refresh materialized view
    ALTER MATERIALIZED VIEW gold.agg_monthly_orders REFRESH;

    -- Insert or update fact table
    INSERT OVERWRITE INTO gold.fact_orders
    SELECT
        o.order_id,
        o.customer_id,
        o.order_date,
        date_trunc('month', o.order_date)::DATE AS month_year,
        o.order_amount,
        coalesce(d.discount_pct, 0) * o.order_amount AS discount_amount,
        o.order_amount * (1 - coalesce(d.discount_pct, 0)) AS net_amount,
        o.status,
        current_timestamp() AS updated_at
    FROM silver.orders o
    LEFT JOIN silver.dim_discounts d ON o.customer_id = d.customer_id
    WHERE o.is_current = TRUE
        AND o.effective_date >= current_date() - 1;

-- Enable the task tree (parent task starts chain)
ALTER TASK bronze_load_task RESUME;
ALTER TASK silver_transform_task RESUME;
ALTER TASK gold_aggregate_task RESUME;

-- Monitor task execution
SELECT
    name,
    state,
    last_successful_run,
    next_scheduled_time,
    last_suspended_on,
    suspended_count,
    error_code,
    comment
FROM information_schema.tasks
WHERE database_name = 'analytics'
ORDER BY name;

-- ============================================================================
-- 3. ZERO-COPY CLONING: Instant dev/test environment copies
-- ============================================================================

-- Clone entire schema (perfect for dev/test)
CREATE SCHEMA dev.analytics_clone CLONE prod.analytics;

-- Clone individual table
CREATE TABLE dev.orders_test CLONE prod.silver.orders;

-- Clone with timestamp (restore to specific point in time)
CREATE TABLE dev.orders_backup_20260226 CLONE prod.silver.orders
    AT (TIMESTAMP => '2026-02-26 10:00:00'::TIMESTAMP_NTZ);

-- Query cloned data (no data movement!)
SELECT COUNT(*) FROM dev.orders_test;

-- Drop clone when done (instant, no cleanup needed)
DROP SCHEMA dev.analytics_clone;
DROP TABLE dev.orders_test;

-- ============================================================================
-- 4. ADVANCED: Dynamic SQL for metadata-driven pipelines
-- ============================================================================

-- Create a configuration table
CREATE TABLE config.table_mappings (
    source_table STRING,
    target_table STRING,
    column_mappings OBJECT,
    is_active BOOLEAN,
    created_at TIMESTAMP DEFAULT current_timestamp()
);

-- Insert metadata
INSERT INTO config.table_mappings VALUES
    ('bronze.orders_raw', 'silver.orders', object_construct('order_id', 'order_id', 'customer_id', 'customer_id'), TRUE, current_timestamp()),
    ('bronze.customers_raw', 'silver.customers', object_construct('cust_id', 'customer_id', 'name', 'name'), TRUE, current_timestamp());

-- Dynamic transformation task
CREATE OR REPLACE TASK generic_transform_task
    WAREHOUSE = compute_wh
    SCHEDULE = '1 HOUR'
    COMMENT = 'Metadata-driven transformation'
AS
    DECLARE
        mapping OBJECT;
    BEGIN
        FOR mapping IN
            SELECT column_mappings FROM config.table_mappings WHERE is_active = TRUE
        DO
            -- Use EXECUTE IMMEDIATE for dynamic SQL
            LET sql = 'INSERT INTO silver.' || mapping['target_table'] || ' SELECT * FROM bronze.' || mapping['source_table'];
            EXECUTE IMMEDIATE :sql;
        END FOR;
        RETURN 'Transformation completed';
    END;

-- ============================================================================
-- 5. ROW-LEVEL SECURITY (Masking): Protect sensitive data
-- ============================================================================

-- Create masking function for email addresses
CREATE OR REPLACE MASKING POLICY email_mask
    AS (email VARCHAR) RETURNS VARCHAR ->
        CASE
            WHEN current_role() IN ('ANALYST', 'BI_USER') THEN
                'MASKED: ' || substring(email, 1, 2) || '****@' || split_part(email, '@', 2)
            ELSE
                email  -- Full email for data engineers
        END;

-- Apply masking policy to email column
ALTER TABLE silver.customers
    MODIFY COLUMN email
        SET MASKING POLICY email_mask;

-- Test masking (result depends on role)
SET ROLE 'BI_USER';
SELECT customer_id, email FROM silver.customers LIMIT 5;
-- Output: 'MASKED: jo****@example.com'

SET ROLE 'SYSADMIN';
SELECT customer_id, email FROM silver.customers LIMIT 5;
-- Output: 'john@example.com'

-- ============================================================================
-- 6. MONITORING & DEBUGGING
-- ============================================================================

-- View task execution history
SELECT
    name,
    scheduled_time,
    query_start_time,
    completed_time,
    state,
    return_value,
    query_text
FROM table(information_schema.task_history(
    task_name => 'silver_transform_task',
    result_limit => 10
))
ORDER BY scheduled_time DESC;

-- Get current task state
SELECT
    name,
    database_name,
    schema_name,
    state,
    last_successful_run,
    next_scheduled_time,
    comment
FROM information_schema.tasks
WHERE name = 'silver_transform_task';

-- Monitor stream state
SELECT
    name,
    table_name,
    stale,
    mode,
    comment
FROM information_schema.streams
WHERE database_name = 'analytics'
ORDER BY name;

-- ============================================================================
-- 7. PERFORMANCE OPTIMIZATION
-- ============================================================================

-- Add clustering to improve query performance
ALTER TABLE silver.orders CLUSTER BY (customer_id, effective_date);

-- Check cluster health
SELECT
    cluster_name,
    round(avg(clustering_depth), 2) AS avg_depth,
    round(average_overlap, 2) AS avg_overlap
FROM table(information_schema.clustering_information('silver.orders'));

-- Drop clustering if no longer needed
ALTER TABLE silver.orders DROP CLUSTERING KEY;

-- ============================================================================
-- 8. ALERTING: Fail task if quality check fails
-- ============================================================================

-- Task with built-in validation
CREATE OR REPLACE TASK silver_transform_with_validation
    WAREHOUSE = compute_wh
    AFTER bronze_load_task
AS
    BEGIN
        -- Transform
        MERGE INTO silver.orders ...;

        -- Validate row count didn't drop > 20%
        LET prev_count = (SELECT SUM(total_rows) FROM audit.table_metrics
                          WHERE table_name = 'silver.orders'
                            AND metric_date = current_date() - 1);
        LET current_count = (SELECT COUNT(*) FROM silver.orders WHERE is_current = TRUE);

        IF (current_count / NVL(prev_count, current_count)) < 0.8 THEN
            RAISE EXCEPTION 'Quality check failed: row count dropped > 20%';
        END IF;

        RETURN 'Transformation and validation successful';
    END;

-- ============================================================================
-- Best Practices Summary
-- ============================================================================
--
-- ✅ Use STREAMS for real-time CDC (no extra tables needed)
-- ✅ Use TASKS for serverless orchestration (no Airflow needed)
-- ✅ Use ZERO-COPY CLONE for instant dev environments
-- ✅ Use MASKING POLICIES for PII protection
-- ✅ Use CLUSTERING for query optimization
-- ✅ Monitor task_history() regularly
-- ✅ Test with small data first, then scale
--
-- Real-world example: Medallion pipeline completes in 15-60 minutes
-- depending on data volume, all without external orchestration tool.
