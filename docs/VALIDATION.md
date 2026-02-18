# Data Validation Strategy

This document describes the multi-stage data quality validation framework used throughout the Retail ETL Pipeline.

## Overview

The pipeline implements a **3-stage validation architecture**:

```
RAW DATA (S3)
    ↓
[VALIDATE INPUT] → Check schema contracts, remove invalid rows
    ↓
[TRANSFORM] → Enrich with business logic
    ↓
[VALIDATE OUTPUT] → Final quality gate before warehouse
    ↓
WAREHOUSE (Snowflake) → STAR schema dimensions & facts
```

This approach ensures:

-   **Data quality** at every stage
-   **Early detection** of upstream issues
-   **Automatic remediation** (row removal with logging)
-   **Schema enforcement** using Pandera

---

## Stage 1: Input Validation

**Module**: `include/validations/validate_inputs.py`

### Sales Input Schema

| Column         | Type  | Nullable | Constraints | Purpose                                      |
| -------------- | ----- | -------- | ----------- | -------------------------------------------- |
| `sales_id`     | int   | ✗        | Primary key | Unique transaction ID                        |
| `product_id`   | int   | ✗        | Foreign key | Links to products table                      |
| `order_status` | str   | ✗        | -           | Order state (pending, completed, etc.)       |
| `qty`          | int   | ✗        | > 0         | Quantity ordered (must be positive)          |
| `price`        | float | ✗        | -           | Unit price (allows negative for adjustments) |
| `discount`     | float | ✓        | [0, 1]      | Discount rate as decimal (0.1 = 10%)         |
| `region`       | str   | ✓        | -           | Sales region (nullable, filled as 'UNKNOWN') |
| `time_stamp`   | str   | ✗        | -           | Mixed date formats accepted                  |

**Validation Logic**:

```python
# Input validation automatically:
1. Removes null values in required fields (sales_id, product_id, qty, time_stamp)
2. Enforces integer types for IDs and quantities
3. Validates qty > 0
4. Validates discount ∈ [0, 1]
5. Fills null regions with 'UNKNOWN'
6. Logs all dropped rows with reasons
```

**Output**: Clean sales DataFrame + count of dropped rows

### Products Input Schema

| Column       | Type  | Nullable | Constraints | Purpose                                    |
| ------------ | ----- | -------- | ----------- | ------------------------------------------ |
| `product_id` | int   | ✗        | Primary key | Unique product identifier                  |
| `category`   | str   | ✗        | -           | Product category (electronics, home, etc.) |
| `brand`      | str   | ✗        | -           | Manufacturer/brand name                    |
| `rating`     | float | ✓        | [0, 5]      | Customer rating out of 5                   |
| `in_stock`   | bool  | ✓        | -           | Stock availability flag                    |

**Validation Logic**:

```python
# Product validation automatically:
1. Removes null product_ids
2. Requires category and brand
3. Validates ratings ∈ [0, 5] if provided
4. Accepts boolean in_stock values
5. Logs rejected products
```

---

## Stage 2: Transformation

**Module**: `include/etl/transform.py`

This stage enriches and calculates metrics from validated raw data:

### Key Transformations

1. **Filter Orders**: Keep only `order_status = 'completed'`
2. **Parse Timestamps**: Convert multiple date formats to datetime
3. **Join Dimensions**: Merge sales with product metadata
4. **Calculate Revenue**: `revenue = qty × price × (1 - discount)`
5. **Add Business Flags**:
    - `is_discounted`: discount > 0
    - `is_in_stock`: product in_stock = True
6. **Extract Time Dimensions**:
    - `sale_date`: Date portion of timestamp
    - `sale_hour`: Hour portion (0-23)

---

## Stage 3: Output Validation

**Module**: `include/validations/validate_outputs.py`

### Sales Clean Output Schema (Data Warehouse Format)

| Column          | Type  | Nullable | Constraints | Purpose                           |
| --------------- | ----- | -------- | ----------- | --------------------------------- |
| `sales_id`      | int   | ✗        | -           | Transaction ID                    |
| `product_id`    | int   | ✗        | -           | Product ID                        |
| `category`      | str   | ✗        | -           | Product category (STAR dimension) |
| `brand`         | str   | ✗        | -           | Product brand (STAR dimension)    |
| `region`        | str   | ✗        | -           | Sales region (STAR dimension)     |
| `qty`           | int   | ✗        | > 0         | Quantity purchased                |
| `price`         | float | ✗        | -           | Unit price                        |
| `discount`      | float | ✗        | [0, 1]      | Discount rate                     |
| `revenue`       | float | ✗        | -           | Total revenue (calculated)        |
| `rating`        | float | ✓        | [0, 5]      | Product rating                    |
| `is_in_stock`   | bool  | ✗        | -           | Stock availability                |
| `is_discounted` | bool  | ✗        | -           | Whether discount applied          |
| `sale_date`     | date  | ✗        | -           | Date of sale                      |
| `sale_hour`     | int   | ✗        | [0, 23]     | Hour of sale (0-23)               |

**Validation Logic**:

```python
# Output validation ensures:
1. Strict schema compliance (strict=True)
2. No null values in fact table columns
3. Valid time dimensions (hour ∈ [0, 23])
4. All dimension attributes populated
5. Revenue calculated correctly
```

**Rejection Criteria**:

-   Missing dimension attributes (category, brand, region)
-   Invalid time values (hour > 23)
-   Negative or zero revenue (indicators of calculation errors)
-   Null values in required fact table columns

---

## Error Handling & Logging

When validation fails:

1. **Invalid rows are automatically removed** (not errored out)
2. **Detailed logging** records:
    - Number of rows dropped
    - Column and check that failed
    - Summary of violation types
3. **Pipeline continues** with cleaned data
4. **If all data becomes invalid**, pipeline fails with alert

### Example Log Output

```
INFO Starting sales validation on 2500 rows
WARNING Sales validation failed: 45 invalid rows
WARNING Errors:
  column        check            count
  qty           greater_than     23
  region        nullable         22
INFO Cleaned sales: 2455 rows remaining
```

---

## Data Quality Metrics

The pipeline produces these quality indicators:

```python
# Input Stage
- Sales input rows: 2,500
- Valid sales rows: 2,455
- Dropped rate: 1.8%

# Transform Stage
- Pre-transform rows: 2,455
- Post-transform rows: 2,300 (after filtering incomplete orders)
- Enrichment success: 100%

# Output Stage
- Clean rows ready for warehouse: 2,300
- End-to-end drop rate: 8%
```

---

## STAR Schema Compliance

Output validation ensures the data warehouse follows **STAR schema principles**:

### Fact Table: `sales_clean`

-   **Grain**: One row per transaction
-   **Keys**: sales_id (primary), product_id, region (foreign)
-   **Measures**: qty, price, revenue, discount
-   **Flags**: is_discounted, is_in_stock

### Time Dimension: Implicit

-   sale_date (DATE type for aggregation)
-   sale_hour (INT 0-23 for intraday analysis)

### Product Dimension: Implicit

-   category, brand (from merge with products table)
-   rating, in_stock (denormalized from products)

### Region Dimension: Implicit

-   region (required, no nulls)

---

## Configuration & Customization

To modify validation rules, edit:

**Input Schemas** → `include/validations/input_schemas.py`

```python
sales_schema = DataFrameSchema({
    "qty": Column(int, Check.gt(0), nullable=False),  # Modify here
})
```

**Output Schemas** → `include/validations/output_schemas.py`

```python
sales_clean_schema = DataFrameSchema({
    "revenue": Column(float, nullable=False),  # Modify here
})
```

---

## Testing Validation Rules

Run unit tests for validation logic:

```bash
pytest tests/test_validations.py -v
```

Test coverage includes:

-   Valid data passes all checks
-   Invalid data is properly removed
-   Dimension merges complete correctly
-   Time dimension extraction works for mixed formats
-   Revenue calculations are accurate

---

## Production Monitoring

In production, monitor these metrics:

-   **Data Quality Rate**: (Valid Rows / Input Rows) \* 100
-   **Drop Rate by Column**: Identify upstream data quality issues
-   **Dimension Coverage**: Ensure dimension joins don't drop rows
-   **Revenue Accuracy**: Validate calculation consistency

Alerts trigger if:

-   Drop rate > 10%
-   Required dimensions have more than 5% nulls
-   Data becomes empty after validation
-   Unexpected schema violations appear

---

## References

-   [Pandera Schema Validation](https://pandera.readthedocs.io/)
-   [STAR Schema Design](https://en.wikipedia.org/wiki/Star_schema)
-   [Data Quality Framework](ARCHITECTURE.md#validation-layer)
