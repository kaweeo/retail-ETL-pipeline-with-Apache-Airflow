# Retail ETL Data Warehouse - Architecture Documentation

## System Overview

This document describes the technical architecture of the Retail ETL Data Warehouse pipeline, including components, data flow, design patterns, and deployment considerations.

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      ORCHESTRATION LAYER                        │
│                    (Apache Airflow + Astronomer)                │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │        Retail ETL Pipeline DAG (retail_etl_dag.py)       │   │
│  │                                                          │   │
│  │  Extract → Validate Input → Transform → Validate Output  │   │
│  │       ↓                                                  │   │
│  │  [Task Dependencies + Error Handling + Retries]          │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
         ↓                                              ↓
    ┌────────────┐                                ┌──────────┐
    │   AWS S3   │                                │ Snowflake│
    │ (RAW DATA) │                                │   (EDW)  │
    └────────────┘                                └──────────┘
         ↑                                              ↑
    [Extract]                                     [Load]
```

## Component Architecture

### 1. **Orchestration Layer: Apache Airflow**

**Purpose**: Schedule, monitor, and orchestrate ETL tasks

**Components**:

-   **Scheduler**: Triggers DAGs on schedule (@daily)
-   **DAG Processor**: Parses Python DAG definitions
-   **Executor**: Runs task instances
-   **Metadata DB**: PostgreSQL stores run history and state
-   **Web UI**: Monitoring and debugging interface

**Containerization**: Astronomer Runtime 3.0

-   Provides production-grade Airflow with managed dependencies
-   Dockerfile specifies runtime version and custom Python packages

### 2. **Data Extraction Layer**

**Module**: `include/etl/extract_s3.py`

**Functionality**:

-   Connects to AWS S3 using Airflow S3Hook
-   Reads sales data (CSV format)
-   Reads product data (JSON format)
-   Normalizes column names (lowercase + underscores)

**Key Functions**:

```python
extract_sales_and_products(aws_conn_id, bucket, sales_key, products_key)
→ pd.DataFrame, pd.DataFrame
```

**Error Handling**:

-   S3Hook credential validation
-   File read error logging
-   Connection retry via Airflow provider

### 3. **Validation Layer** (Input & Output)

**Modules**:

-   `include/validations/input_schemas.py` - Pandera schemas
-   `include/validations/validate_inputs.py` - Validation logic
-   `include/validations/output_schemas.py` - Output schemas
-   `include/validations/validate_outputs.py` - Final validation

**Design Pattern**: Schema-First Validation

-   Define data contracts as Pandera schemas
-   Validate lazily to capture all errors
-   Handle schema violations gracefully (drop invalid rows)

**Validation Stages**:

| Stage  | Schema             | Checks                                         |
| ------ | ------------------ | ---------------------------------------------- |
| Input  | sales_schema       | sales_id (non-null), qty > 0, discount ∈ [0,1] |
| Input  | products_schema    | product_id (non-null), rating ∈ [0,5]          |
| Output | sales_clean_schema | All above + sale_hour ∈ [0,23], price > 0      |

**Error Handling Strategy**:

```
Schema Validation
    ↓
[FAIL] → Extract failed rows → Log details → Drop invalid rows → Re-validate
    ↓
[OK] → Continue to next stage
```

### 4. **Transformation Layer**

**Module**: `include/etl/transform.py`

**Purpose**: Enrich and prepare data for analytics

**Transformations** (in order):

1. Filter to "Completed" orders only
2. Normalize region values (uppercase, fill nulls with "UNKNOWN")
3. Parse timestamps (flexible format support)
4. Calculate revenue: `qty × price × (1 - discount)`
5. Filter negative prices/revenues
6. Enrich with product metadata (LEFT JOIN)
7. Create business flags (`is_discounted`, `is_in_stock`)
8. Extract date/hour dimensions
9. Type casting for schema compliance

**Data Model Output**:

```
STAR Schema Fact Table (sales_clean)
├── Dimensions
│   ├── product_id → products
│   ├── region → geographic dimension
│   ├── sale_date → date dimension
│   └── sale_hour → time dimension
└── Measures
    ├── qty (quantity)
    ├── price (unit price)
    ├── discount (discount %)
    ├── revenue (final amount)
    └── rating (product rating)
```

### 5. **Loading Layer**

**Module**: `include/etl/load_s3_csv.py`

**Functionality**:

-   Writes validated DataFrame to S3 as CSV
-   Location: `s3://bucket/cleansed/sales_clean.csv`
-   Snowflake ingestion ready (compatible format)

**Error Handling**:

-   Empty DataFrame validation
-   S3 connection error handling
-   Type conversion for compatibility
-   Detailed logging for debugging

## Data Flow Diagram

```
┌──────────────────────────────────────────────────────────────┐
│                        RAW DATA ZONE                         │
│                                                              │
│  S3: sales_data.csv              S3: product_data.json      │
│  ├─ sales_id (id)                ├─ product_id (id)        │
│  ├─ product_id (fk)              ├─ category               │
│  ├─ order_status                 ├─ brand                  │
│  ├─ qty                          ├─ rating                 │
│  ├─ price                        └─ in_stock               │
│  ├─ discount                                                │
│  ├─ region                                                  │
│  └─ time_stamp                                              │
└──────────────────────────────────────────────────────────────┘
                            ↓
                    [EXTRACT TASK]
                            ↓
        ┌────────────────────┴────────────────────┐
        ↓                                          ↓
  sales_df (2500+ rows)               products_df (100+ rows)
  [Columns normalized]                [Columns normalized]
        ↓                                          ↓
                    [VALIDATE INPUT]
                            ↓
        ┌────────────────────┴────────────────────┐
        ↓                                          ↓
  clean_sales (~2450 rows)            clean_products
  [Invalid rows removed]              [Invalid rows removed]
        ↓                                          ↓
                    [TRANSFORM ENRICH]
                            ↓
              sales_enriched (after filters)
┌─────────────────────────────────────────────────┐
│ Transformations Applied:                         │
│ ✓ Completed orders only                         │
│ ✓ Region normalized to UPPERCASE               │
│ ✓ Timestamps parsed (mixed formats)            │
│ ✓ Revenue calculated (qty×price×(1-discount)) │
│ ✓ Negative prices/revenues filtered            │
│ ✓ Product metadata merged (LEFT JOIN)          │
│ ✓ Business flags created                       │
│ ✓ Date/hour extracted                          │
│ ✓ Type casting applied                         │
└─────────────────────────────────────────────────┘
                            ↓
                 [VALIDATE OUTPUT]
                            ↓
            sales_clean (ready for warehouse)
                            ↓
        ┌───────────────────┴───────────────────┐
        ↓                                         ↓
  S3 CLEANSED ZONE            SNOWFLAKE EDW
  (cleansed/sales_clean.csv)  (STAR.sales_clean table)
```

## Module Dependencies

```
retail_etl_dag.py (orchestrator)
  ├── include/etl/extract_s3.py
  │   └── include/logger.py
  ├── include/etl/transform.py
  │   └── include/logger.py
  ├── include/etl/load_s3_csv.py
  │   └── include/logger.py
  ├── include/validations/validate_inputs.py
  │   ├── include/validations/input_schemas.py
  │   └── include/logger.py
  ├── include/validations/validate_outputs.py
  │   ├── include/validations/output_schemas.py
  │   └── include/logger.py
  └── include/config.yaml (configuration)
```

## Design Patterns Used

### 1. **Schema-First Validation**

Define data contracts before processing. Fail early with detailed error messages.

### 2. **Lazy Validation**

Use Pandera's lazy mode to capture all errors in one pass, not fail on first error.

### 3. **Modular Architecture**

Separate concerns: extract, validate, transform, load are independent functions.

### 4. **Configuration Externalization**

Use YAML config file instead of hardcoding values (S3 paths, credentials).

### 5. **Custom Logging**

Centralized logger utility ensures consistent log format across modules.

### 6. **Error Handling & Retry**

Airflow provides:

-   Automatic retries with exponential backoff
-   Email alerts on task failure
-   Execution timeout enforcement

## Airflow DAG Structure

```python
@dag(
    dag_id="retail_etl_pipeline",
    schedule="@daily",               # Run daily
    start_date=datetime(2026, 1, 1),
    catchup=False,                   # Don't catch up missed runs
    max_active_runs=1,               # Single concurrent run
    default_args={
        "retries": 2,                # Retry twice
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(hours=1),
        "email_on_failure": True,
    }
)
def retail_etl_pipeline():
    extracted = extract()
    validated = validate_inputs(extracted)
    transformed = transform(validated)
    result = validate_and_load(transformed)
```

**Task Dependencies**:

```
extract → validate_inputs → transform → validate_and_load
   ↑          ↑               ↑              ↑
   └──────────┴───────────────┴──────────────┘
        (Linear dependency chain)
```

## Configuration Management

**File**: `include/config.yaml`

```yaml
aws_conn_id: aws_conn_id # Airflow connection ID
s3:
    bucket: amzn-softuni-demo-bucket
    sales_key: data-wh-etl/sales_data.csv
    products_key: data-wh-etl/product_data.json
snowflake:
    conn_id: my_snowflake_conn
    account: your-account
    warehouse: ETL_AIRFLOW_WH
    database: RETAIL_DB
```

**Loaded at DAG startup** → Available to all tasks

## Deployment Architecture

### Local Development

```
Docker Compose (Astronomer CLI)
├── PostgreSQL (Metadata DB)
├── Airflow Scheduler
├── Airflow DAG Processor
├── Airflow Web Server (UI)
└── Airflow Triggerer
```

### Production Deployment

```
Astronomer Cloud (or self-hosted)
├── Kubernetes cluster
├── Airflow Deployment
├── PostgreSQL Cloud DB
├── S3 bucket access (IAM roles)
└── Snowflake connectivity
```

## Performance Considerations

| Aspect          | Strategy                                                 |
| --------------- | -------------------------------------------------------- |
| Data Volume     | Optimized for ~2500 records/run (typical daily batch)    |
| Processing Time | 2-5 minutes (S3 network dependent)                       |
| Memory          | Single-threaded execution (~500MB)                       |
| Parallelism     | Single DAG run for consistency (set `max_active_runs=1`) |
| Scaling         | Modify concurrency settings for larger datasets          |

## Monitoring & Observability

**Metrics Collected**:

-   Task execution time
-   Row counts at each stage
-   Validation drop counts
-   Error rates and types

**Logging Strategy**:

-   INFO: Business metrics (rows extracted, transformed)
-   WARNING: Data quality issues (rows dropped)
-   ERROR: Task failures and exceptions

**Airflow UI Visualization**:

-   DAG graph shows task dependencies
-   Task logs available for debugging
-   Gantt chart shows execution timeline
-   Task duration analytics

## Security Considerations

### Credential Management

-   AWS credentials via Airflow Connection (encrypted)
-   Snowflake credentials via Airflow Connection
-   No hardcoded secrets in code

### Data Access

-   S3 bucket policies restrict data access
-   Snowflake roles enforce data governance
-   Row-level security (RLS) implemented

### Code Quality

-   Type hints for easier debugging
-   Comprehensive error handling
-   Logging for audit trails

## Testing Strategy

**Unit Tests** (`tests/`):

-   Test validation functions
-   Test transform logic
-   Mock external dependencies (S3, Snowflake)
-   Edge case coverage

**Integration Tests**:

-   Full DAG execution in Docker
-   End-to-end data flow validation
-   Error scenario handling

## Future Enhancements

1. **dbt Integration**: Move transformations to dbt for version control
2. **Great Expectations**: Advanced data quality frameworks
3. **Real-time Processing**: Kafka integration for streaming
4. **ML Features**: Anomaly detection on incoming data
5. **Data Lineage**: Apache Atlas for metadata tracking
6. **CI/CD Pipeline**: Automated testing and deployment

## Documentation References

-   [Apache Airflow Documentation](https://airflow.apache.org)
-   [Astronomer Platform Docs](https://docs.astronomer.io)
-   [Pandera Schema Validation](https://pandera.readthedocs.io)
-   [AWS S3 Documentation](https://docs.aws.amazon.com/s3/)
-   [Snowflake Documentation](https://docs.snowflake.com)
