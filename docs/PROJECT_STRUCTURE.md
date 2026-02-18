# Project Structure Guide

## Directory Layout

```
data_wh_etl/
├── dags/                           # Apache Airflow DAG definitions
│   └── retail_etl_dag.py          # Main ETL pipeline (daily @2026-01-01)
│
├── include/                         # Reusable modules (Airflow best practice)
│   ├── config.yaml                # Pipeline configuration (S3 bucket, Snowflake conn)
│   ├── logger.py                  # Logging setup
│   ├── etl/                       # Data extraction, transformation, loading
│   │   ├── extract_s3.py         # Read sales.csv & product_data.json from S3
│   │   ├── transform.py          # Enrich data with business logic
│   │   └── load_s3_csv.py        # Write clean data to S3 processed zone
│   └── validations/              # Data quality enforcement
│       ├── input_schemas.py      # Pandera schemas for raw data contracts
│       ├── output_schemas.py     # Pandera schemas for warehouse data contracts
│       ├── validate_inputs.py    # Validate & clean raw data (Stage 1)
│       └── validate_outputs.py   # Validate & clean transformed data (Stage 3)
│
├── tests/                          # Test suite (29 tests, all passing)
│   ├── test_etl_functions.py      # Unit tests for transform logic
│   ├── test_validations.py        # Unit tests for validation schemas
│   └── conftest.py                # Pytest fixtures
│
├── sql/                            # Database setup scripts
│   └── setup_snowflake.txt        # Snowflake DDL for STAR schema
│
├── support_validation_assets/      # Sample data + screenshots
│   ├── data_wh_etl.sql            # Sample warehouse queries
│   ├── sales_clean.csv            # Example clean output
│   └── *.png                       # Architecture & dashboard screenshots
│
├── plugins/                        # Custom Airflow plugins (empty)
│
├── .env                           # Environment variables (⚠️ NEVER commit secrets)
├── airflow_settings.yaml          # Airflow configuration
├── Dockerfile                     # Astronomer Runtime 3.0
├── packages.txt                   # OS-level dependencies
├── requirements.txt               # Python dependencies
├── pytest.ini                     # Pytest configuration
│
├── README.md                      # Project overview & quick start
├── ARCHITECTURE.md                # System design & component details
├── VALIDATION.md                  # Data validation strategy & schemas
├── CONTRIBUTING.md                # Development setup & guidelines
├── DEPLOYMENT.md                  # Docker, Astronomer deployment
└── LICENSE                        # MIT License

```

---

## What Each Module Does

### 1. ETL Pipeline (`include/etl/`)

| Module                  | Input            | Process                          | Output             |
| ----------------------- | ---------------- | -------------------------------- | ------------------ |
| **extract_s3.py**       | S3 bucket        | Read CSV/JSON, normalize columns | 2 DataFrames       |
| **validate_inputs.py**  | Raw DataFrames   | Drop invalid rows                | Clean DataFrames   |
| **transform.py**        | Clean DataFrames | Aggregate, enrich, calculate     | Transformed DF     |
| **validate_outputs.py** | Transformed DF   | Final quality checks             | Warehouse-ready DF |
| **load_s3_csv.py**      | Clean DF         | Write to S3 processed zone       | CSV file           |

### 2. Validation System (`include/validations/`)

Enforces **3-stage validation**:

```
RAW DATA (S3)
    ↓
[VALIDATE INPUT] — Define data contracts, drop invalid rows
    ↓
[TRANSFORM] — Enrich with business logic
    ↓
[VALIDATE OUTPUT] — Final quality gate before warehouse
    ↓
WAREHOUSE (Snowflake)
```

See [VALIDATION.md](VALIDATION.md) for detailed validation rules.

### 3. Configuration

-   **config.yaml**: S3 bucket names, Snowflake connection ID
-   **.env**: Actual credentials (AWS, Snowflake) — never commit!

---

## Data Flow

### Extract Stage

```python
extract_s3.py:
  S3("sales_data.csv") → normalize columns → sales_df
  S3("product_data.json") → normalize columns → products_df
```

### Validate Input Stage

```python
validate_inputs.py:
  sales_df → [check non-null IDs, qty > 0, discount ∈ [0,1]] → clean_sales_df
  products_df → [check non-null category/brand] → clean_products_df
```

### Transform Stage

```python
transform.py:
  clean_sales_df + clean_products_df →
    [filter completed orders]
    [normalize region to uppercase]
    [parse mixed timestamp formats]
    [calculate revenue = qty × price × (1-discount)]
    [merge product dimensions]
    [create is_discounted & is_in_stock flags]
    → enriched_df
```

### Validate Output Stage

```python
validate_outputs.py:
  enriched_df → [strict schema validation] → final_clean_df
```

### Load Stage

```python
load_s3_csv.py:
  final_clean_df → S3("cleansed/sales_clean.csv")
```

---

## Configuration Reference

### `include/config.yaml`

```yaml
aws_conn_id: aws_conn_id # Airflow connection ID for AWS
s3:
    bucket: amzn-s3-retail-data # S3 bucket name
    sales_key: sales_data.csv # Raw sales file path
    products_key: product_data.json # Raw products file path

snowflake:
    conn_id: my_snowflake_conn # Airflow connection ID for Snowflake
    account: OFQFQYC-QO56332 # Snowflake account (from URL)
    warehouse: RETAIL_DATA_WH # Warehouse name
    database: RETAIL_DATA # Database name
```

### `.env` (Never Commit!)

```bash
AWS_ACCESS_KEY_ID=your_key              # Generated from AWS console
AWS_SECRET_ACCESS_KEY=your_secret       # Keep secret!
SNOWFLAKE_ACCOUNT=your_account_id       # From Snowflake URL
SNOWFLAKE_USER=your_user                # Snowflake login
SNOWFLAKE_PASSWORD=your_password        # Keep secret!
```

---

## Key Dependencies

| Package               | Version | Purpose                |
| --------------------- | ------- | ---------------------- |
| `apache-airflow`      | 2.x     | Orchestration          |
| `pandas`              | 2.1.4   | Data manipulation      |
| `pandera`             | 0.24.0  | Schema validation      |
| `boto3`               | 1.38.44 | AWS S3 access          |
| `snowflake-connector` | 3.15.0  | Snowflake connection   |
| `pydantic`            | —       | (implicit via pandera) |

Install all: `pip install -r requirements.txt`

---

## Testing

### Run All Tests

```bash
pytest tests/ -v --tb=short
# Output: 29 passed ✅
```

### Run Specific Test

```bash
pytest tests/test_validations.py::TestInputSalesValidation::test_negative_qty_dropped -v
```

### View Coverage

```bash
pytest tests/ --cov=include --cov-report=html
# Opens htmlcov/index.html
```

---

## Common Tasks

### Adding a New Transformation

1. Edit `include/etl/transform.py` (add your logic)
2. Add test in `tests/test_etl_functions.py`
3. Run `pytest tests/test_etl_functions.py -v`

### Adding a New Validation Rule

1. Update schema in `include/validations/input_schemas.py` or `output_schemas.py`
2. Add test in `tests/test_validations.py`
3. Run `pytest tests/test_validations.py -v`

### Running the DAG Locally

```bash
# Option 1: Airflow CLI (requires Airflow setup)
airflow dags test retail_etl_pipeline

# Option 2: Docker (requires docker-compose)
astro dev start
```

### Debugging a Failed Task

```bash
# Check DAG logs
cat logs/dags/retail_etl_pipeline/extract_raw_data/2026-01-01T00:00:00+00:00/attempt-1.log

# Or use Airflow Web UI
# http://localhost:8080
```

---

## Best Practices Applied

✅ **Clear separation of concerns**: Extract → Validate → Transform → Validate → Load  
✅ **DRY principle**: Reusable validation schemas  
✅ **Error handling**: Graceful degradation with detailed logging  
✅ **Testing**: >80% coverage with unit tests  
✅ **Documentation**: Inline comments + markdown guides  
✅ **Configuration**: External config files (no hardcoding)  
✅ **Secrets management**: Use `.env` + `.gitignore`, never commit credentials

---

## Next Steps

1. **Clone repo** → Set up local environment
2. **Update .env** → Add your AWS & Snowflake credentials
3. **Run tests** → `pytest tests/`
4. **Deploy** → Use Dockerfile with Astronomer
5. **Monitor** → Check Airflow Web UI for DAG runs

Need help? See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup.
