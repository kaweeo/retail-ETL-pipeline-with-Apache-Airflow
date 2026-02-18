-- =============================================================================
-- SNOWFLAKE SETUP & DDL STATEMENTS
-- =============================================================================
-- This file contains the DDL (Data Definition Language) statements needed to
-- set up the Snowflake data warehouse for the Retail ETL Pipeline.
--
-- Execute these statements manually or use the ensure_snowflake_infrastructure()
-- function in Python for automated idempotent setup.
--
-- =============================================================================

-- ============================================================================
-- 1. CREATE WAREHOUSE
-- ============================================================================

CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WAREHOUSE_SIZE = XSMALL
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

-- Scale up for analytics (adjust SIZE based on needs)
-- ALTER WAREHOUSE COMPUTE_WH SET WAREHOUSE_SIZE = 'LARGE';

-- ============================================================================
-- 2. CREATE DATABASE
-- ============================================================================

CREATE DATABASE IF NOT EXISTS RETAIL_DW;

-- ============================================================================
-- 3. CREATE SCHEMAS
-- ============================================================================

-- Raw/Staging Schema
CREATE SCHEMA IF NOT EXISTS RETAIL_DW.RAW;

-- Analytics/Processed Schema
CREATE SCHEMA IF NOT EXISTS RETAIL_DW.ANALYTICS;

-- ============================================================================
-- 4. CREATE ROLES & GRANTS (Optional - for multi-user environments)
-- ============================================================================

-- Create transformer role (for Airflow/ETL)
CREATE ROLE IF NOT EXISTS TRANSFORMER;

-- Create analyst role (for business users)
CREATE ROLE IF NOT EXISTS ANALYST;

-- Grant warehouse access
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE TRANSFORMER;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE ANALYST;

-- Grant database access
GRANT USAGE ON DATABASE RETAIL_DW TO ROLE TRANSFORMER;
GRANT USAGE ON DATABASE RETAIL_DW TO ROLE ANALYST;

-- Grant schema access
GRANT USAGE ON SCHEMA RETAIL_DW.RAW TO ROLE TRANSFORMER;
GRANT USAGE ON SCHEMA RETAIL_DW.ANALYTICS TO ROLE TRANSFORMER;
GRANT USAGE ON SCHEMA RETAIL_DW.ANALYTICS TO ROLE ANALYST;

-- Grant table permissions
GRANT CREATE TABLE ON SCHEMA RETAIL_DW.RAW TO ROLE TRANSFORMER;
GRANT CREATE TABLE ON SCHEMA RETAIL_DW.ANALYTICS TO ROLE TRANSFORMER;
GRANT SELECT ON ALL TABLES IN SCHEMA RETAIL_DW.ANALYTICS TO ROLE ANALYST;

-- ============================================================================
-- 5. CREATE FILE FORMAT
-- ============================================================================

CREATE FILE FORMAT IF NOT EXISTS RETAIL_DW.RAW.CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null', '')
    COMPRESSION = 'GZIP';

-- ============================================================================
-- 6. CREATE S3 STAGE (requires Storage Integration)
-- ============================================================================

-- Option A: With Storage Integration (Recommended)
-- Prerequisites:
--   1. Create AWS IAM policy for Snowflake
--   2. Create Storage Integration in Snowflake
--   3. Grant Storage Integration privileges

CREATE STORAGE INTEGRATION IF NOT EXISTS s3_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::YOUR_AWS_ACCOUNT_ID:role/snowflake-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://retail-data-warehouse/cleansed-data/');

CREATE STAGE IF NOT EXISTS RETAIL_DW.RAW.S3_PROCESSED_STAGE
    URL = 's3://retail-data-warehouse/cleansed-data/'
    STORAGE_INTEGRATION = s3_integration
    FILE_FORMAT = RETAIL_DW.RAW.CSV_FORMAT;

-- Option B: Without Storage Integration (for testing only)
-- CREATE STAGE IF NOT EXISTS RETAIL_DW.RAW.S3_PROCESSED_STAGE
--     URL = 's3://retail-data-warehouse/cleansed-data/'
--     CREDENTIALS = (AWS_KEY_ID = 'xxx', AWS_SECRET_KEY = 'xxx')
--     FILE_FORMAT = RETAIL_DW.RAW.CSV_FORMAT;

-- ============================================================================
-- 7. CREATE MAIN FACT TABLE: sales_clean
-- ============================================================================

CREATE TABLE IF NOT EXISTS RETAIL_DW.ANALYTICS.sales_clean (
    sales_id INTEGER PRIMARY KEY,
    product_id INTEGER NOT NULL,
    
    -- Dimensions (foreign keys to implicit dimensions)
    category VARCHAR NOT NULL,
    brand VARCHAR NOT NULL,
    region VARCHAR NOT NULL,
    
    -- Measures
    qty INTEGER NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    discount DECIMAL(5, 2) NOT NULL,
    revenue DECIMAL(12, 2) NOT NULL,
    
    -- Product Enrichment
    rating DECIMAL(3, 1),
    is_in_stock BOOLEAN NOT NULL,
    is_discounted BOOLEAN NOT NULL,
    
    -- Time Dimensions
    sale_date DATE NOT NULL,
    sale_hour INTEGER NOT NULL,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (sale_date, region, category);

-- Create clustered index for performance
CREATE INDEX idx_sales_date ON RETAIL_DW.ANALYTICS.sales_clean(sale_date);

-- ============================================================================
-- 8. CREATE OPTIONAL DIMENSION TABLES
-- ============================================================================

-- Product Dimension
CREATE TABLE IF NOT EXISTS RETAIL_DW.ANALYTICS.dim_product (
    product_id INTEGER PRIMARY KEY,
    category VARCHAR NOT NULL,
    brand VARCHAR NOT NULL,
    rating DECIMAL(3, 1),
    in_stock BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Region Dimension
CREATE TABLE IF NOT EXISTS RETAIL_DW.ANALYTICS.dim_region (
    region_id INTEGER PRIMARY KEY AUTOINCREMENT,
    region_name VARCHAR UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Date Dimension (for enhanced time analysis)
CREATE TABLE IF NOT EXISTS RETAIL_DW.ANALYTICS.dim_date (
    date_key INTEGER PRIMARY KEY,
    date_value DATE UNIQUE NOT NULL,
    day_of_week INTEGER,
    week_of_year INTEGER,
    month INTEGER,
    quarter INTEGER,
    year INTEGER,
    is_weekend BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- 9. CREATE AGGREGATE/SUMMARY TABLES
-- ============================================================================

-- Daily Summary (for faster reporting)
CREATE TABLE IF NOT EXISTS RETAIL_DW.ANALYTICS.daily_summary (
    summary_date DATE NOT NULL,
    region VARCHAR NOT NULL,
    category VARCHAR NOT NULL,
    transaction_count INTEGER,
    units_sold INTEGER,
    revenue DECIMAL(12, 2),
    avg_price DECIMAL(10, 2),
    discounted_sales INTEGER,
    pct_in_stock DECIMAL(5, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (summary_date, region, category)
);

-- ============================================================================
-- 10. CREATE VIEWS (Optional)
-- ============================================================================

-- High-level Sales Dashboard View
CREATE OR REPLACE VIEW RETAIL_DW.ANALYTICS.v_sales_dashboard AS
SELECT
    sale_date,
    region,
    category,
    COUNT(*) as transactions,
    SUM(qty) as units,
    SUM(revenue) as revenue,
    ROUND(AVG(revenue), 2) as avg_transaction,
    ROUND(100.0 * COUNT(CASE WHEN is_discounted THEN 1 END) / COUNT(*), 1) as discount_pct
FROM RETAIL_DW.ANALYTICS.sales_clean
GROUP BY sale_date, region, category;

-- Product Performance View
CREATE OR REPLACE VIEW RETAIL_DW.ANALYTICS.v_product_performance AS
SELECT
    product_id,
    category,
    brand,
    COUNT(*) as sales_count,
    SUM(qty) as units,
    SUM(revenue) as revenue,
    ROUND(AVG(rating), 2) as avg_rating,
    ROUND(100.0 * COUNT(CASE WHEN is_in_stock THEN 1 END) / COUNT(*), 1) as pct_in_stock
FROM RETAIL_DW.ANALYTICS.sales_clean
GROUP BY product_id, category, brand;

-- ============================================================================
-- 11. GRANT PERMISSIONS FOR PIPELINE EXECUTION
-- ============================================================================

-- Grant on all future tables (for TRANSFORMER role)
GRANT USAGE ON SCHEMA RETAIL_DW.ANALYTICS TO ROLE TRANSFORMER;
GRANT CREATE TABLE ON SCHEMA RETAIL_DW.ANALYTICS TO ROLE TRANSFORMER;
GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA RETAIL_DW.ANALYTICS TO ROLE TRANSFORMER;

-- Grant SELECT on all future tables (for ANALYST role)
GRANT SELECT ON ALL TABLES IN SCHEMA RETAIL_DW.ANALYTICS TO ROLE ANALYST;
GRANT SELECT ON ALL VIEWS IN SCHEMA RETAIL_DW.ANALYTICS TO ROLE ANALYST;

-- ============================================================================
-- 12. CREATE MONITORING TABLES (Optional)
-- ============================================================================

-- Pipeline Run History
CREATE TABLE IF NOT EXISTS RETAIL_DW.RAW.pipeline_runs (
    run_id VARCHAR PRIMARY KEY,
    dag_id VARCHAR,
    execution_date TIMESTAMP,
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    status VARCHAR,
    rows_extracted INTEGER,
    rows_transformed INTEGER,
    rows_loaded INTEGER,
    duration_seconds INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Data Quality Metrics
CREATE TABLE IF NOT EXISTS RETAIL_DW.RAW.data_quality_metrics (
    metric_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id VARCHAR,
    table_name VARCHAR,
    metric_name VARCHAR,
    metric_value DECIMAL(12, 2),
    threshold DECIMAL(12, 2),
    status VARCHAR,
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- 13. VERIFY SETUP (Run These Queries to Check)
-- ============================================================================

-- Verify warehouse
SHOW WAREHOUSES LIKE 'COMPUTE_WH';

-- Verify database
USE RETAIL_DW;

-- Verify schemas
SHOW SCHEMAS IN DATABASE RETAIL_DW;

-- Verify tables
SHOW TABLES IN SCHEMA RETAIL_DW.ANALYTICS;

-- Verify stage
SHOW STAGES IN SCHEMA RETAIL_DW.RAW;

-- Verify file format
SHOW FILE FORMATS IN SCHEMA RETAIL_DW.RAW;

-- Check grants
SHOW GRANTS TO ROLE TRANSFORMER;
SHOW GRANTS TO ROLE ANALYST;

-- ============================================================================
-- 14. CLEANUP (If needed - drops all objects)
-- ============================================================================

-- DROP DATABASE RETAIL_DW CASCADE;
-- DROP WAREHOUSE COMPUTE_WH;
-- DROP ROLE TRANSFORMER;
-- DROP ROLE ANALYST;

-- =============================================================================
-- END OF DDL STATEMENTS
-- =============================================================================
