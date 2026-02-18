import pandas as pd
from include.logger import setup_logger

logger = setup_logger("etl.transform")


def transform_sales_and_products(
    sales_df: pd.DataFrame,
    products_df: pd.DataFrame
) -> pd.DataFrame:

    logger.info("Starting transformation and enrichment")

    # --------------------------------------------------
    # 0. Normalize column names (safety)
    # --------------------------------------------------
    sales_df.columns = sales_df.columns.str.strip().str.lower().str.replace(" ", "_")

    # --------------------------------------------------
    # 1. Filter only completed orders
    # --------------------------------------------------
    sales_df = sales_df[sales_df["order_status"] == "Completed"]
    logger.info(f"Filtered: {len(sales_df)} completed orders retained")

    # --------------------------------------------------
    # 2. Normalize region to uppercase and fill nulls
    # --------------------------------------------------
    sales_df["region"] = (
        sales_df["region"]
        .fillna("UNKNOWN")  # Unspecified regions
        .str.upper()         # Standardize case
    )
    logger.info(f"Region normalization: {sales_df['region'].nunique()} unique regions")

    # --------------------------------------------------
    # 3. Parse timestamps with flexible format support
    # --------------------------------------------------
    # Handles mixed date formats from source data (common in data integration).
    # Examples of supported formats:
    #   - "2026-01-01" (ISO)
    #   - "2026-01-01 10:30" (ISO with time)
    #   - "01/04/2026" (US format)
    #   - Mixed: coerce=True converts unparseable dates to NaT
    # Extract temporal dimensions needed for time-based analysis (by hour, by date).
    sales_df["timestamp"] = pd.to_datetime(
        sales_df["time_stamp"],
        errors="coerce",  # Invalid dates → NaT (will be filtered later)
        format="mixed"    # Auto-detect format
    )

    sales_df["sale_date"] = sales_df["timestamp"].dt.date
    sales_df["sale_hour"] = sales_df["timestamp"].dt.hour

    # --------------------------------------------------
    # 4. Revenue calculation (with discount)
    # --------------------------------------------------
    # Business metric: Calculate final revenue after discount.
    # Formula: revenue = quantity × unit_price × (1 - discount_rate)
    # Example: qty=5, price=100, discount=0.1 → revenue = 5 * 100 * 0.9 = 450
    # Fill null discounts with 0 (no discount applied).
    sales_df["discount"] = sales_df["discount"].fillna(0)

    sales_df["revenue"] = (
        sales_df["qty"]
        * sales_df["price"]
        * (1 - sales_df["discount"])
    )
    logger.info(f"Revenue calculated: ${sales_df['revenue'].sum():,.2f} total")

    # --------------------------------------------------
    # 4.5 Filter out negative prices and revenues
    # --------------------------------------------------
    # Data quality check: Remove invalid transactions.
    # Negative prices/revenues can occur due to:
    # - Data entry errors (wrong signs)
    # - Incorrect discount values (e.g., discount > 1)
    # - Refunds or credits (not part of this analysis)
    # These would skew financial reports, so must be excluded.
    initial_count = len(sales_df)
    sales_df = sales_df[(sales_df["price"] > 0) & (sales_df["revenue"] > 0)]
    filtered_count = initial_count - len(sales_df)
    if filtered_count > 0:
        logger.warning(f"Quality filter: Removed {filtered_count} rows with negative values")

    # --------------------------------------------------
    # 5. Enrich with product metadata
    # --------------------------------------------------
    # Join dimension data: product_id links to product attributes.
    # LEFT JOIN strategy:
    #   - Retains all sales (left) even if product not found (rare)
    #   - Adds category, brand, rating, stock status to each transaction
    # This creates a denormalized fact table ready for analytics.
    # Example result: sales_id=1234 → joined with product_id=567
    #   → gets category="Electronics", brand="Apple", rating=4.8
    enriched_df = sales_df.merge(
        products_df,
        on="product_id",
        how="left"
    )
    logger.info(f"Enrichment: {len(enriched_df)} sales enriched with product data")

    # --------------------------------------------------
    # 6. Create business flags for analytics
    # --------------------------------------------------
    # Binary indicators for segmented reporting.
    # is_discounted: TRUE if any discount applied (discount > 0)
    #   → Helps analyze effectiveness of promotions
    # is_in_stock: TRUE if product available (in_stock = True)
    #   → Identifies out-of-stock orders (potential fulfillment issues)
    enriched_df["is_discounted"] = enriched_df["discount"] > 0
    enriched_df["is_in_stock"] = enriched_df["in_stock"].fillna(False)

    # --------------------------------------------------
    # 7. Type casting for schema compliance
    # --------------------------------------------------
    # Ensure data types match the target schema in Snowflake.
    # sale_hour should be integer (0-23) for efficient storage
    # and aggregation in data warehouse.
    enriched_df["sale_hour"] = enriched_df["sale_hour"].astype("int64")

    # --------------------------------------------------
    # 8. Final column selection and ordering
    # --------------------------------------------------
    # Select only required columns in business-logical order:
    # Identifiers → Dimensions → Measures → Flags → Timestamps
    # This ensures:
    # - Dropped rows never reach warehouse (intermediate columns removed)
    # - Consistent column order for reporting systems
    # - Performance optimization (unnecessary columns not loaded)
    final_columns = [
        # Keys for joining
        "sales_id",
        "product_id",
        
        # Dimensions (attributes of the transaction)
        "category",      # Product category
        "brand",         # Product brand
        "region",        # Geographic location
        
        # Measures (quantitative metrics)
        "qty",           # Units sold
        "price",         # Unit price
        "discount",      # Discount rate (0.0-1.0)
        "revenue",       # Final amount (qty × price × (1-discount))
        
        # Enrichment attributes
        "rating",        # Product rating (0.0-5.0)
        
        # Status flags
        "is_in_stock",   # TRUE if product available
        "is_discounted", # TRUE if discount applied
        
        # Temporal dimensions
        "sale_date",     # Date of transaction (for daily aggregations)
        "sale_hour",     # Hour of transaction (for hourly analysis)
    ]

    final_df = enriched_df[final_columns]
    logger.info(f"Transformation completed: {len(final_df)} records ready for warehouse")

    return final_df
