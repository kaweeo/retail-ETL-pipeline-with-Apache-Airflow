# Quick Start Guide

Fast reference for running the Retail ETL Data Warehouse project.

## ⚡ One-Time Setup

```bash
# 1. Navigate to project
cd data_wh_etl

# 2. Create virtual environment
python3 -m venv venv
source venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt
pip install pytest pytest-cov black flake8

# 4. Configure AWS (optional for local testing)
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret

# 5. Update config.yaml with your S3 bucket
nano include/config.yaml
```

## 🚀 Running Airflow Locally

```bash
# Start Airflow (runs in Docker)
astro dev start

# Open browser to http://localhost:8080 (admin/admin)

# Stop Airflow
astro dev stop

# View logs
astro dev logs -f
```

## 🧪 Running Tests

```bash
# All tests
pytest tests/ -v

# Specific test file
pytest tests/test_validations.py -v
pytest tests/test_etl_functions.py -v

# With coverage report
pytest tests/ --cov=include/ --cov-report=term
pytest tests/ --cov=include/ --cov-report=html
# View: open htmlcov/index.html

# Only tests matching a pattern
pytest tests/ -k "revenue" -v
```

## 📝 Code Quality

```bash
# Format code
black include/ dags/ tests/

# Check formatting
black --check include/ dags/ tests/

# Lint code
flake8 include/ dags/ tests/ --max-line-length=120

# Run all checks
black include/ dags/ tests/ && \
flake8 include/ dags/ tests/ --max-line-length=120 && \
pytest tests/ -v --cov=include/
```

## 📂 Project Structure

```
data_wh_etl/
├── dags/
│   └── retail_etl_dag.py          # Main DAG orchestration
├── include/
│   ├── config.yaml                # Configuration
│   ├── etl/
│   │   ├── extract_s3.py          # Extract data
│   │   ├── transform.py           # Transform logic
│   │   └── load_s3_csv.py         # Load to S3
│   └── validations/
│       ├── input_schemas.py       # Input validation rules
│       ├── output_schemas.py      # Output validation rules
│       ├── validate_inputs.py     # Validation functions
│       └── validate_outputs.py    # Final validation
├── tests/
│   ├── test_validations.py        # Validation unit tests
│   ├── test_etl_functions.py      # ETL unit tests
│   └── conftest.py                # Test configuration
├── sql/
│   └── setup_snowflake.txt        # Snowflake DDL
├── README.md                       # Project overview
├── ARCHITECTURE.md                 # System design
├── CONTRIBUTING.md                 # Contributing guide
└── pytest.ini                      # Pytest configuration
```

## 🔑 Key Files Explained

| File                    | Purpose                                                                        |
| ----------------------- | ------------------------------------------------------------------------------ |
| `retail_etl_dag.py`     | Defines the DAG with 5 tasks: extract → validate → transform → validate → load |
| `extract_s3.py`         | Reads sales.csv and product_data.json from S3                                  |
| `transform.py`          | Enriches data: merges products, calculates revenue, creates flags              |
| `input_schemas.py`      | Pandera schemas for raw data validation                                        |
| `output_schemas.py`     | Pandera schemas for clean data validation                                      |
| `test_validations.py`   | 25+ tests for validation functions                                             |
| `test_etl_functions.py` | 20+ tests for transform and ETL functions                                      |

## 🐛 Debugging

```bash
# View DAG logs
astro dev logs -f

# Test DAG syntax
astro dev run dags test retail_etl_pipeline 2026-01-01

# Check available DAGs
astro dev run dags list

# Run single task
astro dev run tasks test retail_etl_pipeline extract_raw_data 2026-01-01
```

## 📊 Data Flow

```
RAW S3 → EXTRACT → VALIDATE INPUT → TRANSFORM → VALIDATE OUTPUT → LOAD S3
(CSV)                               (enrich)       (quality check)
& JSON    ↓                          ↓                                ↓
         2500+ rows    → clean rows → calculate revenue      → sales_clean.csv
                       → merge products with sales
                       → create business flags (is_discounted, is_in_stock)
                       → extract date/hour dimensions
```

## 🎯 Next Steps

1. **Run Tests**: `pytest tests/ -v` to verify everything works
2. **Read Architecture**: Check `ARCHITECTURE.md` for system design
3. **Set Up Locally**: Follow CONTRIBUTING.md for full setup
4. **Customize**: Update config.yaml for your AWS S3 bucket
5. **Deploy**: Use `astro dev start` to run Airflow locally

## 📖 Additional Resources

-   [README.md](README.md) - Complete project overview
-   [ARCHITECTURE.md](ARCHITECTURE.md) - Technical architecture details
-   [CONTRIBUTING.md](CONTRIBUTING.md) - Dev setup and contribution guidelines
-   [include/config.yaml](include/config.yaml) - Configuration reference

## 💡 Common Tasks

**Add a new validation rule:**

1. Update schema in `input_schemas.py` or `output_schemas.py`
2. Add test in `tests/test_validations.py`
3. Run: `pytest tests/test_validations.py -v`

**Add a new transformation:**

1. Add function in `include/etl/transform.py`
2. Add test in `tests/test_etl_functions.py`
3. Run: `pytest tests/test_etl_functions.py::TestTransformFunction::test_new_feature -v`

**Update documentation:**

1. Edit README.md (project overview)
2. Edit ARCHITECTURE.md (system design)
3. Edit function docstrings (code documentation)
