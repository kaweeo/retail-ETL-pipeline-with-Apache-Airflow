# API & Configuration Reference

This document describes the configuration options, API endpoints, and module interfaces for the Retail ETL Pipeline.

## Table of Contents

-   [Configuration Files](#configuration-files)
-   [Environment Variables](#environment-variables)
-   [Module APIs](#module-apis)
-   [Airflow Connections](#airflow-connections)
-   [Data Contracts](#data-contracts)

---

## Configuration Files

### `include/config.yaml`

Main pipeline configuration file. See [config.example.yaml](../include/config.example.yaml) for reference.

```yaml
# AWS Configuration
aws_conn_id: "aws_default" # Airflow connection ID

s3:
    bucket: "retail-data-warehouse"
    raw_folder: "retail-data/"
    cleansed_folder: "cleansed-data/"
    sales_key: "sales.csv"
    products_key: "product_data.json"
    processed_sales_key: "sales_clean.csv"

# Snowflake Configuration
snowflake:
    conn_id: "snowflake_default"
    database: "RETAIL_DW"
    schema: "ANALYTICS"
    warehouse: "COMPUTE_WH"
    role: "TRANSFORMER"

    # Optional: Storage integration for S3 stage
    storage_integration: "s3_integration"
    stage_schema: "RAW"
    stage_name: "S3_PROCESSED_STAGE"
    file_format: "CSV_FORMAT"

# Email Configuration (optional)
email:
    enabled: false
    from: "airflow@company.com"
    recipients:
        - "data-alerts@company.com"
```

---

## Environment Variables

All required environment variables are documented in [.env.example](.env.example).

**Critical Variables** (must be set before running):

```bash
export AWS_ACCESS_KEY_ID=xxxxx
export AWS_SECRET_ACCESS_KEY=xxxxx
export SNOWFLAKE_USER=xxxxx
export SNOWFLAKE_PASSWORD=xxxxx
export SNOWFLAKE_ACCOUNT=xxxxx
```

**Optional Variables**:

```bash
export LOG_LEVEL="INFO"
export ENABLE_SNOWFLAKE_LOAD="true"
export ENABLE_EMAIL_ALERTS="false"
```

Load all variables:

```bash
source .env
```

---

## Module APIs

### Extract Module: `include/etl/extract_s3.py`

**Function**: `extract_sales_and_products()`

```python
def extract_sales_and_products(
    aws_conn_id: str,
    bucket: str,
    sales_key: str,
    products_key: str,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Extract sales and product data from S3.

    Args:
        aws_conn_id: Airflow AWS connection ID
        bucket: S3 bucket name
        sales_key: S3 path to sales CSV file
        products_key: S3 path to products JSON file

    Returns:
        Tuple of (sales_df, products_df) with normalized column names

    Raises:
        AirflowException: If S3 access fails or file not found

    Example:
        sales_df, products_df = extract_sales_and_products(
            aws_conn_id="aws_default",
            bucket="retail-data-warehouse",
            sales_key="retail-data/sales.csv",
            products_key="retail-data/product_data.json"
        )
    """
```

---

### Transform Module: `include/etl/transform.py`

**Function**: `transform_sales_and_products()`

```python
def transform_sales_and_products(
    sales_clean: pd.DataFrame,
    products_clean: pd.DataFrame,
) -> pd.DataFrame:
    """
    Transform and enrich sales data with product metadata.

    Transformations Applied:
    1. Filter completed orders only
    2. Parse mixed date formats to datetime
    3. Merge product dimensions (category, brand, rating)
    4. Calculate revenue: qty × price × (1 - discount)
    5. Add business flags (is_discounted, is_in_stock)
    6. Extract time dimensions (sale_date, sale_hour)

    Args:
        sales_clean: Validated sales DataFrame
        products_clean: Validated products DataFrame

    Returns:
        Transformed DataFrame ready for warehouse loading

    Example:
        sales_transformed = transform_sales_and_products(
            sales_clean=validated_sales_df,
            products_clean=validated_products_df
        )

    Notes:
        - Requires index reset after merge operations
        - Handle null regions before merge
    """
```

---

### Load Module: `include/etl/load_s3_csv.py`

**Function**: `write_sales_clean_csv_to_s3()`

```python
def write_sales_clean_csv_to_s3(
    sales_clean: pd.DataFrame,
    aws_conn_id: str,
    bucket: str,
    s3_key: str,
) -> str:
    """
    Write cleaned sales data to S3 in CSV format.

    Args:
        sales_clean: Transformed DataFrame
        aws_conn_id: Airflow AWS connection ID
        bucket: S3 bucket name
        s3_key: S3 path for output file (e.g., "cleansed-data/sales_clean.csv")

    Returns:
        S3 URI of written file (e.g., "s3://bucket/path/sales_clean.csv")

    Example:
        s3_uri = write_sales_clean_csv_to_s3(
            sales_clean=transformed_df,
            aws_conn_id="aws_default",
            bucket="retail-data-warehouse",
            s3_key="cleansed-data/sales_clean.csv"
        )
        # Returns: "s3://retail-data-warehouse/cleansed-data/sales_clean.csv"
    """
```

---

### Load Module: `include/etl/load_snowflake.py`

**Function**: `ensure_snowflake_infrastructure()`

```python
def ensure_snowflake_infrastructure(
    *,
    snowflake_conn_id: str,
    database: str,
    schema: str,
    warehouse: str,
    role: Optional[str] = None,
    stage_schema: str = "RAW",
    stage_name: str = "S3_PROCESSED_STAGE",
    file_format_name: str = "CSV_FORMAT",
    storage_integration: Optional[str] = None,
    s3_bucket: Optional[str] = None,
    s3_key: Optional[str] = None,
    create_stage: bool = True,
) -> None:
    """
    Idempotently create Snowflake warehouse, database, schemas, and stage.

    Args:
        snowflake_conn_id: Airflow Snowflake connection ID
        database: Database name
        schema: Schema name for tables
        warehouse: Warehouse name
        role: Snowflake role (optional)
        stage_schema: Schema for staging files
        stage_name: Stage name for S3 integration
        storage_integration: Snowflake storage integration name (optional)
        s3_bucket: S3 bucket name if creating stage
        s3_key: S3 path to extract folder structure from
        create_stage: Whether to create S3 stage (default: True)

    Raises:
        AirflowException: If required parameters missing or Snowflake access fails

    Example:
        ensure_snowflake_infrastructure(
            snowflake_conn_id="snowflake_default",
            database="RETAIL_DW",
            schema="ANALYTICS",
            warehouse="COMPUTE_WH",
            s3_bucket="retail-data-warehouse",
            storage_integration="s3_integration"
        )
    """
```

**Function**: `load_sales_clean_to_snowflake()`

```python
def load_sales_clean_to_snowflake(
    *,
    snowflake_conn_id: str,
    database: str,
    schema: str,
    sales_clean: pd.DataFrame,
    table_name: str = "sales_clean",
    if_exists: str = "replace",
) -> int:
    """
    Load transformed sales DataFrame to Snowflake.

    Args:
        snowflake_conn_id: Airflow Snowflake connection ID
        database: Target database
        schema: Target schema
        sales_clean: Validated and transformed DataFrame
        table_name: Snowflake table name
        if_exists: Action if table exists ("replace", "append", "fail")

    Returns:
        Number of rows loaded

    Example:
        rows_loaded = load_sales_clean_to_snowflake(
            snowflake_conn_id="snowflake_default",
            database="RETAIL_DW",
            schema="ANALYTICS",
            sales_clean=cleaned_df,
            if_exists="replace"
        )
        print(f"Loaded {rows_loaded} rows")
    """
```

---

### Validation Schemas

#### Input Validation: `include/validations/input_schemas.py`

```python
from pandera.pandas import DataFrameSchema, Column, Check

# Sales Input Schema
sales_schema = DataFrameSchema({
    "sales_id": Column(int, nullable=False),
    "product_id": Column(int, nullable=False),
    "order_status": Column(str, nullable=False),
    "qty": Column(int, Check.gt(0), nullable=False),
    "price": Column(float, nullable=False),
    "discount": Column(float, Check.between(0, 1), nullable=True),
    "region": Column(str, nullable=True),
    "time_stamp": Column(str, nullable=False),
})

# Products Input Schema
products_schema = DataFrameSchema({
    "product_id": Column(int, nullable=False),
    "category": Column(str, nullable=False),
    "brand": Column(str, nullable=False),
    "rating": Column(float, Check.between(0, 5), nullable=True),
    "in_stock": Column(bool, nullable=True),
})
```

#### Output Validation: `include/validations/output_schemas.py`

```python
# Sales Clean Output Schema (warehouse format)
sales_clean_schema = DataFrameSchema({
    "sales_id": Column(int, nullable=False),
    "product_id": Column(int, nullable=False),
    "category": Column(str, nullable=False),
    "brand": Column(str, nullable=False),
    "region": Column(str, nullable=False),
    "qty": Column(int, Check.gt(0), nullable=False),
    "price": Column(float, nullable=False),
    "discount": Column(float, Check.between(0, 1), nullable=False),
    "revenue": Column(float, nullable=False),
    "rating": Column(float, Check.between(0, 5), nullable=True),
    "is_in_stock": Column(bool, nullable=False),
    "is_discounted": Column(bool, nullable=False),
    "sale_date": Column(pa.Date, nullable=False),
    "sale_hour": Column(int, Check.between(0, 23), nullable=False),
})
```

---

## Airflow Connections

### Required Connections

Configure in Airflow Web UI: **Admin > Connections**

#### 1. AWS Connection

```
Connection ID: aws_default
Conn Type: Amazon Web Services
Login: YOUR_AWS_ACCESS_KEY_ID
Password: YOUR_AWS_SECRET_ACCESS_KEY
Extra JSON: {
  "region_name": "us-east-1"
}
```

#### 2. Snowflake Connection

```
Connection ID: snowflake_default
Conn Type: Snowflake
Host: YOUR_ACCOUNT.us-east-1.snowflakecomputing.com
Schema: YOUR_SCHEMA
Login: YOUR_SNOWFLAKE_USER
Password: YOUR_SNOWFLAKE_PASSWORD
Port: 443
Extra JSON: {
  "account": "YOUR_ACCOUNT",
  "database": "RETAIL_DW",
  "warehouse": "COMPUTE_WH",
  "role": "TRANSFORMER"
}
```

#### 3. SMTP Connection (optional, for email alerts)

```
Connection ID: email_default
Conn Type: SMTP
Host: smtp.gmail.com
Port: 587
Login: your-email@gmail.com
Password: your-app-password
Extra JSON: {
  "tls": true
}
```

---

## Data Contracts

### Input Data Contract (Raw from S3)

**Sales CSV Format**:

```csv
sales_id,product_id,order_status,qty,price,discount,region,time_stamp
1,101,completed,2,29.99,0.1,West,"2024-01-15 10:30:00"
2,102,completed,1,49.99,0.0,East,"2024-01-15 11:00:00"
```

**Products JSON Format**:

```json
{
	"product_id": 101,
	"category": "Electronics",
	"brand": "TechBrand",
	"rating": 4.5,
	"in_stock": true
}
```

### Output Data Contract (Warehouse Format)

**sales_clean Table**:

```sql
CREATE TABLE sales_clean (
    sales_id INTEGER PRIMARY KEY,
    product_id INTEGER NOT NULL,
    category VARCHAR NOT NULL,
    brand VARCHAR NOT NULL,
    region VARCHAR NOT NULL,
    qty INTEGER NOT NULL,
    price DECIMAL NOT NULL,
    discount DECIMAL NOT NULL,
    revenue DECIMAL NOT NULL,
    rating DECIMAL,
    is_in_stock BOOLEAN NOT NULL,
    is_discounted BOOLEAN NOT NULL,
    sale_date DATE NOT NULL,
    sale_hour INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
```

---

## Error Codes

| Code                      | Meaning                        | Action                             |
| ------------------------- | ------------------------------ | ---------------------------------- |
| `S3_KEY_NOT_FOUND`        | File not found in S3           | Verify file path in config         |
| `INVALID_CREDENTIALS`     | AWS/Snowflake auth failed      | Check connection credentials       |
| `SCHEMA_VALIDATION_ERROR` | Data doesn't match schema      | Review validation logs for details |
| `SNOWFLAKE_STAGE_ERROR`   | Cannot create S3 stage         | Verify storage integration exists  |
| `DATA_EMPTY_ERROR`        | All data dropped by validation | Investigate upstream data quality  |

---

## References

-   [Pandera API Documentation](https://pandera.readthedocs.io/api_reference.html)
-   [Airflow Operators Reference](https://airflow.apache.org/docs/apache-airflow/stable/howto/operator-index.html)
-   [Snowflake Python Connector](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector)
-   [AWS Boto3 S3 Documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html)
