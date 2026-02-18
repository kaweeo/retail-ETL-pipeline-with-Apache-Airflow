# Contributing Guide

This document provides guidelines for setting up the development environment, running tests, and contributing to the Retail ETL Data Warehouse project.

## Development Environment Setup

### Prerequisites

-   Python 3.10+
-   Docker & Docker Compose
-   Git
-   Astronomer CLI

### Step 1: Clone the Repository

```bash
git clone https://github.com/YOUR-USERNAME/retail-etl-pipeline-airflow.git
cd data_wh_etl
```

**Note**: Replace `YOUR-USERNAME` with your GitHub username or organization name.

### Step 2: Create Python Virtual Environment

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### Step 3: Install Dependencies

```bash
# Install project dependencies
pip install -r requirements.txt

# Install development dependencies
pip install pytest pytest-cov black flake8 mypy
```

### Step 4: Configure AWS Credentials

For local testing with S3:

```bash
# Option 1: Environment variables
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret

# Option 2: AWS credentials file (~/.aws/credentials)
[default]
aws_access_key_id = your_key
aws_secret_access_key = your_secret
```

### Step 5: Update Configuration

Edit `include/config.yaml` with your actual AWS and Snowflake settings:

```yaml
aws_conn_id: aws_conn_id
s3:
    bucket: your-bucket-name
    sales_key: path/to/sales.csv
    products_key: path/to/products.json

snowflake:
    conn_id: my_snowflake_conn
    account: your-account
```

## Running Airflow Locally

### Start Airflow Development Environment

```bash
astro dev start
```

This starts:

-   PostgreSQL (metadata database)
-   Airflow Scheduler
-   Airflow DAG Processor
-   Airflow Web UI (http://localhost:8080)
-   Triggerer

### Access the Web UI

-   URL: http://localhost:8080
-   Default credentials: admin / admin

### View Logs

```bash
# Follow scheduler logs
astro dev logs -f

# View logs for specific task
astro dev logs dag_id task_id
```

### Stop Airflow

```bash
astro dev stop
```

### Reset Development Environment

```bash
astro dev kill  # Stop all containers
astro dev start # Start fresh
```

## Running Tests

### Run All Tests

```bash
pytest tests/ -v
```

### Run Specific Test File

```bash
pytest tests/test_validations.py -v
pytest tests/test_etl_functions.py -v
```

### Run with Coverage Report

```bash
pytest tests/ --cov=include/ --cov-report=html --cov-report=term
# View HTML report: open htmlcov/index.html
```

### Run Only Unit Tests

```bash
pytest tests/ -m unit -v
```

### Run Integration Tests (with external services)

```bash
# Requires S3 and Snowflake credentials
pytest tests/ --integration -v
```

## Code Quality

### Format Code with Black

```bash
# Format all Python files
black include/ dags/ tests/

# Check formatting without changes
black --check include/ dags/ tests/
```

### Lint Code with Flake8

```bash
# Check code style
flake8 include/ dags/ tests/ --max-line-length=120
```

### Type Checking with MyPy

```bash
# Check type hints
mypy include/ dags/ --ignore-missing-imports
```

### Run All Quality Checks

```bash
# Format
black include/ dags/ tests/

# Lint
flake8 include/ dags/ tests/ --max-line-length=120

# Type check
mypy include/ dags/ --ignore-missing-imports

# Tests
pytest tests/ -v --cov=include/
```

## Testing the DAG

### Validate DAG Syntax

```bash
# Check if DAG loads without errors
astro dev run dags list
astro dev run dags test retail_etl_pipeline 2026-01-01
```

### Test with Sample Data

```bash
# Create sample CSV in S3
aws s3 cp include/sample_data/sales.csv s3://your-bucket/sales.csv

# Run DAG
astro dev run dags test retail_etl_pipeline 2026-01-01
```

### Check Task Logs

```bash
astro dev logs -f
# Or in Airflow UI: click on task → Logs tab
```

## Making Changes

### Creating a Feature Branch

```bash
git checkout -b feature/your-feature-name
```

### Code Style Guidelines

1. **Python Style**: Follow PEP 8

    - Max line length: 120 characters
    - Use type hints for function arguments and returns
    - Use meaningful variable names

2. **Documentation**:

    - Add docstrings to all functions and classes
    - Include examples in docstrings when helpful
    - Add inline comments for complex logic

3. **Testing**:
    - Add unit tests for new functions
    - Aim for minimum 80% code coverage
    - Test edge cases and error scenarios

### Example: Adding a New Transformation

1. Create transformation function in `include/etl/transform.py`
2. Add unit tests in `tests/test_etl_functions.py`
3. Update documentation if needed
4. Run tests locally: `pytest tests/ -v`
5. Format and lint: `black`, `flake8`
6. Submit pull request

### Example: Adding Validation Rule

1. Update schema in `include/validations/input_schemas.py` or `output_schemas.py`
2. Add validation tests in `tests/test_validations.py`
3. Document the validation rule in schema file
4. Run tests: `pytest tests/test_validations.py -v`
5. Submit pull request

## Debugging

### Enable Debug Logging

In DAG file, set logging level:

```python
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
```

### Use Python Debugger

```python
import pdb

# In your code
pdb.set_trace()

# Then run tests
pytest tests/test_file.py -s  # -s captures output
```

### Inspect Dataframes

```python
# In transform.py or validation.py
print(df.head())
print(df.dtypes)
print(df.describe())
```

## Common Issues & Solutions

| Issue                    | Solution                                                   |
| ------------------------ | ---------------------------------------------------------- |
| "ModuleNotFoundError"    | Ensure virtual env is activated and dependencies installed |
| S3 connection fails      | Check AWS credentials and bucket permissions               |
| Pandera validation fails | Review test data against schema definitions                |
| DAG won't load           | Check syntax with `astro dev run dags list`                |
| Port 8080 in use         | Change Airflow port in docker-compose override             |

## Submitting Changes

1. **Push to feature branch**

    ```bash
    git push origin feature/your-feature-name
    ```

2. **Create Pull Request**

    - Provide clear description of changes
    - Link to related issues
    - Ensure all tests pass

3. **Code Review**

    - Address review comments
    - Keep commits clean and focused
    - Update documentation as needed

4. **Merge**
    - Ensure CI/CD pipeline passes
    - Delete feature branch after merge

## Documentation

### Updating README

Edit [README.md](README.md) to document:

-   New features
-   Configuration changes
-   New dependencies

### Updating Architecture

Edit [ARCHITECTURE.md](ARCHITECTURE.md) if:

-   Adding new data flow
-   Changing system design
-   Adding components

### Code Docstrings

Every function should have:

```python
def my_function(param1: str, param2: int) -> bool:
    """
    Brief description of what function does.

    Longer explanation of behavior, edge cases, and examples.

    Args:
        param1: Description of param1
        param2: Description of param2

    Returns:
        Description of return value

    Raises:
        ValueError: When X condition occurs
    """
```

## Performance Optimization

When improving performance:

1. Profile the code

    ```python
    import cProfile
    cProfile.run('function_call()')
    ```

2. Benchmark improvements

    ```bash
    # Before and after timing
    time python -m pytest tests/test_transforms.py
    ```

3. Document performance impact in PR

## Security Considerations

-   Never commit AWS credentials or tokens
-   Use Airflow Connections for sensitive data
-   Review environment variables in CI/CD
-   Don't log sensitive information

## Questions or Issues?

-   Check [ARCHITECTURE.md](ARCHITECTURE.md) for system design questions
-   Review existing tests for usage examples
-   Check Airflow and Pandas documentation for library questions

## Resources

-   [Apache Airflow Docs](https://airflow.apache.org/docs/)
-   [Pandas Documentation](https://pandas.pydata.org/docs/)
-   [Pandera Validation](https://pandera.readthedocs.io/)
-   [Pytest Documentation](https://docs.pytest.org/)
