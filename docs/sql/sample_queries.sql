-- =============================================================================
-- RETAIL ETL DATA WAREHOUSE - SAMPLE ANALYTICS QUERIES
-- =============================================================================
-- These queries demonstrate common analytical patterns using the sales_clean
-- data loaded by the ETL pipeline.
--
-- Database: RETAIL_DW
-- Schema: ANALYTICS
-- Main Table: sales_clean
--
-- =============================================================================

-- ============================================================================
-- 1. BASIC AGGREGATIONS
-- ============================================================================

-- Total Sales Revenue by Date
SELECT
    sale_date,
    COUNT(*) as transaction_count,
    SUM(qty) as total_quantity,
    SUM(revenue) as total_revenue,
    AVG(revenue) as avg_transaction_value,
    MIN(revenue) as min_transaction,
    MAX(revenue) as max_transaction
FROM RETAIL_DW.ANALYTICS.sales_clean
GROUP BY sale_date
ORDER BY sale_date DESC
LIMIT 30;

-- Sales by Region (Last 30 Days)
SELECT
    region,
    COUNT(*) as transactions,
    SUM(qty) as units_sold,
    SUM(revenue) as revenue,
    AVG(price) as avg_price,
    COUNT(CASE WHEN is_discounted THEN 1 END) as discounted_transactions
FROM RETAIL_DW.ANALYTICS.sales_clean
WHERE sale_date >= CURRENT_DATE - 30
GROUP BY region
ORDER BY revenue DESC;

-- ============================================================================
-- 2. PRODUCT PERFORMANCE
-- ============================================================================

-- Top 10 Products by Revenue
SELECT
    product_id,
    category,
    brand,
    COUNT(*) as sales_count,
    SUM(qty) as total_units,
    SUM(revenue) as total_revenue,
    ROUND(AVG(rating), 2) as avg_rating,
    ROUND(100.0 * COUNT(CASE WHEN is_in_stock THEN 1 END) / COUNT(*), 1) as pct_in_stock
FROM RETAIL_DW.ANALYTICS.sales_clean
GROUP BY product_id, category, brand
ORDER BY total_revenue DESC
LIMIT 10;

-- Category Performance Comparison
SELECT
    category,
    COUNT(*) as transactions,
    SUM(qty) as units_sold,
    SUM(revenue) as revenue,
    ROUND(SUM(revenue) / COUNT(*), 2) as revenue_per_transaction,
    ROUND(100.0 * COUNT(CASE WHEN is_discounted THEN 1 END) / COUNT(*), 1) as discount_rate_pct
FROM RETAIL_DW.ANALYTICS.sales_clean
GROUP BY category
ORDER BY revenue DESC;

-- ============================================================================
-- 3. DISCOUNT ANALYSIS
-- ============================================================================

-- Impact of Discounts on Revenue
SELECT
    CASE
        WHEN discount = 0 THEN 'No Discount'
        WHEN discount <= 0.1 THEN '1-10% Discount'
        WHEN discount <= 0.2 THEN '11-20% Discount'
        ELSE '>20% Discount'
    END as discount_bracket,
    COUNT(*) as transaction_count,
    SUM(qty) as units_sold,
    SUM(revenue) as total_revenue,
    ROUND(AVG(revenue), 2) as avg_transaction_value,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 1) as pct_of_transactions
FROM RETAIL_DW.ANALYTICS.sales_clean
GROUP BY discount_bracket
ORDER BY discount DESC;

-- Discount Effectiveness by Category
SELECT
    category,
    is_discounted,
    COUNT(*) as transactions,
    SUM(qty) as units,
    SUM(revenue) as revenue,
    ROUND(AVG(revenue), 2) as avg_transaction
FROM RETAIL_DW.ANALYTICS.sales_clean
GROUP BY category, is_discounted
ORDER BY category, is_discounted DESC;

-- ============================================================================
-- 4. TIME-BASED ANALYSIS
-- ============================================================================

-- Hourly Sales Distribution
SELECT
    sale_hour,
    COUNT(*) as transactions,
    SUM(qty) as units,
    SUM(revenue) as revenue,
    ROUND(AVG(revenue), 2) as avg_revenue
FROM RETAIL_DW.ANALYTICS.sales_clean
GROUP BY sale_hour
ORDER BY sale_hour;

-- Daily Trends (Last 7 Days)
SELECT
    sale_date,
    DAYNAME(sale_date) as day_of_week,
    COUNT(*) as transactions,
    SUM(qty) as units_sold,
    SUM(revenue) as revenue,
    ROUND(100.0 * COUNT(CASE WHEN is_discounted THEN 1 END) / COUNT(*), 1) as discount_pct
FROM RETAIL_DW.ANALYTICS.sales_clean
WHERE sale_date >= CURRENT_DATE - 7
GROUP BY sale_date, day_of_week
ORDER BY sale_date DESC;

-- ============================================================================
-- 5. INVENTORY INSIGHTS
-- ============================================================================

-- Stock Status Impact
SELECT
    is_in_stock,
    COUNT(*) as transactions,
    SUM(qty) as units_sold,
    SUM(revenue) as revenue,
    ROUND(AVG(price), 2) as avg_price,
    ROUND(AVG(rating), 2) as avg_rating
FROM RETAIL_DW.ANALYTICS.sales_clean
GROUP BY is_in_stock;

-- Out-of-Stock Product Sales (if any)
SELECT
    product_id,
    category,
    brand,
    COUNT(*) as sales_while_out_of_stock,
    SUM(qty) as units_sold,
    SUM(revenue) as revenue
FROM RETAIL_DW.ANALYTICS.sales_clean
WHERE is_in_stock = false
GROUP BY product_id, category, brand
ORDER BY revenue DESC;

-- ============================================================================
-- 6. RATING CORRELATION
-- ============================================================================

-- Rating vs. Sales Performance
SELECT
    CASE
        WHEN rating IS NULL THEN 'No Rating'
        WHEN rating < 2 THEN '< 2.0 stars'
        WHEN rating < 3 THEN '2.0-2.9 stars'
        WHEN rating < 4 THEN '3.0-3.9 stars'
        WHEN rating < 5 THEN '4.0-4.9 stars'
        ELSE '5.0 stars'
    END as rating_group,
    COUNT(*) as sales_count,
    SUM(qty) as units,
    SUM(revenue) as revenue,
    ROUND(AVG(revenue), 2) as avg_transaction_value
FROM RETAIL_DW.ANALYTICS.sales_clean
GROUP BY rating_group
ORDER BY CASE WHEN rating IS NULL THEN 6 ELSE FLOOR(rating) END DESC;

-- ============================================================================
-- 7. DATA QUALITY METRICS
-- ============================================================================

-- Data Freshness Check
SELECT
    MAX(sale_date) as latest_data_date,
    MIN(sale_date) as earliest_data_date,
    DATEDIFF(day, MIN(sale_date), MAX(sale_date)) as days_of_data,
    COUNT(*) as total_records,
    COUNT(DISTINCT product_id) as unique_products,
    COUNT(DISTINCT region) as unique_regions
FROM RETAIL_DW.ANALYTICS.sales_clean;

-- NULL Value Survey
SELECT
    COUNT(CASE WHEN sales_id IS NULL THEN 1 END) as null_sales_id,
    COUNT(CASE WHEN product_id IS NULL THEN 1 END) as null_product_id,
    COUNT(CASE WHEN rating IS NULL THEN 1 END) as null_rating,
    COUNT(CASE WHEN category IS NULL THEN 1 END) as null_category,
    COUNT(CASE WHEN region IS NULL THEN 1 END) as null_region
FROM RETAIL_DW.ANALYTICS.sales_clean;

-- Revenue Sanity Check
SELECT
    COUNT(*) as row_count,
    SUM(revenue) as total_revenue,
    MIN(revenue) as min_revenue,
    MAX(revenue) as max_revenue,
    ROUND(AVG(revenue), 2) as avg_revenue,
    ROUND(STDDEV(revenue), 2) as stddev_revenue
FROM RETAIL_DW.ANALYTICS.sales_clean;

-- ============================================================================
-- 8. ADVANCED: WINDOW FUNCTIONS
-- ============================================================================

-- Running Total of Revenue by Date
SELECT
    sale_date,
    SUM(revenue) as daily_revenue,
    SUM(SUM(revenue)) OVER (ORDER BY sale_date) as cumulative_revenue,
    ROUND(100.0 * SUM(revenue) / SUM(SUM(revenue)) OVER() * 100, 2) as pct_of_total
FROM RETAIL_DW.ANALYTICS.sales_clean
GROUP BY sale_date
ORDER BY sale_date;

-- Rank Products by Revenue within Category
SELECT
    category,
    product_id,
    brand,
    SUM(revenue) as product_revenue,
    ROW_NUMBER() OVER (PARTITION BY category ORDER BY SUM(revenue) DESC) as rank_in_category
FROM RETAIL_DW.ANALYTICS.sales_clean
GROUP BY category, product_id, brand
QUALIFY rank_in_category <= 5;

-- Month-over-Month Growth
WITH monthly_sales AS (
    SELECT
        DATE_TRUNC('month', sale_date) as month,
        SUM(revenue) as monthly_revenue
    FROM RETAIL_DW.ANALYTICS.sales_clean
    GROUP BY DATE_TRUNC('month', sale_date)
)
SELECT
    month,
    monthly_revenue,
    LAG(monthly_revenue) OVER (ORDER BY month) as prev_month_revenue,
    ROUND(100.0 * (monthly_revenue - LAG(monthly_revenue) OVER (ORDER BY month)) 
          / LAG(monthly_revenue) OVER (ORDER BY month), 2) as growth_pct
FROM monthly_sales
ORDER BY month;

-- ============================================================================
-- 9. CTAS: CREATE ANALYTICAL TABLES
-- ============================================================================

-- Daily Summary Table
CREATE OR REPLACE TABLE RETAIL_DW.ANALYTICS.daily_summary AS
SELECT
    sale_date,
    region,
    category,
    COUNT(*) as transaction_count,
    SUM(qty) as units_sold,
    SUM(revenue) as revenue,
    ROUND(AVG(price), 2) as avg_price,
    COUNT(CASE WHEN is_discounted THEN 1 END) as discounted_sales,
    ROUND(100.0 * COUNT(CASE WHEN is_in_stock THEN 1 END) / COUNT(*), 1) as pct_in_stock
FROM RETAIL_DW.ANALYTICS.sales_clean
GROUP BY sale_date, region, category;

-- Top Products Materialized View
CREATE OR REPLACE TABLE RETAIL_DW.ANALYTICS.top_products AS
SELECT
    product_id,
    category,
    brand,
    COUNT(*) as sales_count,
    SUM(qty) as units,
    SUM(revenue) as revenue,
    ROUND(AVG(rating), 2) as avg_rating
FROM RETAIL_DW.ANALYTICS.sales_clean
GROUP BY product_id, category, brand
ORDER BY revenue DESC;

-- ============================================================================
-- 10. PERFORMANCE OPTIMIZATION TIPS
-- ============================================================================

-- Create Cluster Key for better query performance
ALTER TABLE RETAIL_DW.ANALYTICS.sales_clean
CLUSTER BY (sale_date, region, category);

-- Create Secondary Index on commonly filtered columns
CREATE INDEX idx_sales_date ON RETAIL_DW.ANALYTICS.sales_clean(sale_date);
CREATE INDEX idx_region ON RETAIL_DW.ANALYTICS.sales_clean(region);
CREATE INDEX idx_category ON RETAIL_DW.ANALYTICS.sales_clean(category);

-- =============================================================================
-- END OF SAMPLE QUERIES
-- =============================================================================
