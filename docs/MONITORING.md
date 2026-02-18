# Production Monitoring & Operations

This guide covers monitoring, debugging, and operational best practices for the Retail ETL Pipeline in production environments.

## Table of Contents

-   [Monitoring Strategy](#monitoring-strategy)
-   [Key Metrics](#key-metrics)
-   [Alerting & SLAs](#alerting--slas)
-   [Troubleshooting](#troubleshooting)
-   [Performance Tuning](#performance-tuning)
-   [Runbook](#runbook)

---

## Monitoring Strategy

### 1. **Airflow Web UI Monitoring**

**URL**: `https://your-airflow-instance.com/home`

**Monitor These Views**:

| View             | What to Check                   | Alert Trigger                                    |
| ---------------- | ------------------------------- | ------------------------------------------------ |
| **DAG Overview** | Execution frequency, last run   | DAG doesn't run within 2 hours of scheduled time |
| **Task Log**     | Individual task success/failure | Red tasks or timeout errors                      |
| **Tree View**    | Run history across dates        | Consecutive failures                             |
| **Gantt Chart**  | Task duration trends            | Tasks exceed baseline by >50%                    |

### 2. **Metrics & Observability**

**Recommended Tools**:

-   **Datadog** (AWS-native monitoring)
-   **New Relic** (Application performance)
-   **CloudWatch** (AWS infrastructure)
-   **Snowflake Query History** (Warehouse performance)

### 3. **Log Aggregation**

Pipeline logs are written to:

```
Airflow Logs: /opt/airflow/logs/retail_etl_pipeline/
S3 Logs:      s3://your-bucket/logs/retail_etl/{date}/
Snowflake:    QUERY_HISTORY table
```

**Log Levels**:

-   `INFO`: Normal operation progress
-   `WARNING`: Data quality issues (dropped rows, validation failures)
-   `ERROR`: Task failure (retry will occur)
-   `CRITICAL`: Pipeline halt (manual intervention needed)

---

## Key Metrics

### 1. **Pipeline Execution Metrics**

```yaml
Metric: DAG_Run_Duration
Target: < 30 minutes (95th percentile)
Warning: > 45 minutes
Alert: > 60 minutes (matches execution timeout)

Metric: Task_Success_Rate
Target: > 99%
Warning: < 98%
Alert: < 95%

Metric: Task_Retry_Rate
Target: < 1% of all task runs
Warning: > 2%
Alert: > 5%
```

### 2. **Data Quality Metrics**

```yaml
Metric: Input_Drop_Rate
Definition: (Invalid Rows / Input Rows) * 100
Target: < 5%
Warning: 5-10%
Alert: > 10%
Action: Investigate upstream data quality

Metric: Output_Validation_Success_Rate
Definition: (Valid Output Rows / Pre-Validation Rows) * 100
Target: > 95%
Warning: 90-95%
Alert: < 90%

Metric: Revenue_Calculation_Variance
Definition: |Calculated Revenue - Expected Range| / Expected
Target: 0% (exact match)
Warning: > 0.1%
Alert: > 1%
```

### 3. **Performance Metrics**

```yaml
Metric: S3_Read_Latency
Target: < 5 seconds per 10MB
Warning: 5-10 seconds
Alert: > 10 seconds

Metric: Snowflake_Load_Throughput
Target: > 10,000 rows/second
Warning: 5,000-10,000 rows/sec
Alert: < 5,000 rows/sec

Metric: Transform_Speed
Target: < 2 seconds per 1000 rows
Warning: 2-5 seconds
Alert: > 5 seconds per 1000 rows
```

### 4. **Warehouse Metrics**

```yaml
Metric: Snowflake_Query_Time
Definition: Time to execute sample analytics query
Target: < 2 seconds
Warning: 2-5 seconds
Alert: > 5 seconds (indicates scan inefficiency)

Metric: Data_Freshness
Definition: Time since last successful load
Target: < 24 hours (daily pipeline)
Warning: 24-48 hours (1-2 days late)
Alert: > 48 hours (more than 2 days late)
```

---

## Alerting & SLAs

### Email Alert Configuration

Alerts configured in [retail_etl_dag.py](dags/retail_etl_dag.py#L28):

```python
DEFAULT_ARGS = {
    "email": ["data-alerts@company.com"],
    "email_on_failure": True,
    "email_on_retry": False,
}
```

**To Enable**:

1. Configure SMTP in `airflow_settings.yaml`:

    ```yaml
    smtp_host: smtp.gmail.com
    smtp_port: 587
    smtp_user: your-email@gmail.com
    smtp_password: ${AIRFLOW_SMTP_PASSWORD} # Use Airflow Variable
    ```

2. Test email:
    ```bash
    airflow dags test retail_etl_pipeline 2026-01-01
    ```

### SLA Configuration

**Current SLA**: Daily @ 2:00 AM execution, must complete by 3:00 AM (1-hour window)

To modify, update in DAG:

```python
@dag(
    dag_id="retail_etl_pipeline",
    schedule="@daily",  # 00:00 UTC by default
    sla=timedelta(hours=1),  # Add SLA here
)
```

### Alert Conditions

| Condition                       | Severity     | Action                                                   |
| ------------------------------- | ------------ | -------------------------------------------------------- |
| DAG fails to start              | **CRITICAL** | Check Airflow scheduler status, review logs              |
| Extract task fails (timeout S3) | **HIGH**     | Check S3 connectivity, verify file locations             |
| Validation drops >10% of rows   | **HIGH**     | Review data quality upstream, may indicate schema change |
| Transform errors                | **MEDIUM**   | Check business logic, verify dimension joins             |
| Load fails (Snowflake issue)    | **HIGH**     | Check Snowflake warehouse, verify credentials            |
| DAG runs late (>1 hour)         | **MEDIUM**   | Performance degradation, increase executor resources     |

---

## Troubleshooting

### Issue 1: "Extract Task Timeout"

**Symptoms**: Extract task fails after 1 hour

**Diagnosis**:

```bash
# Check S3 connectivity
aws s3 ls s3://your-bucket/retail-data/ --profile default

# Check file sizes
aws s3 ls s3://your-bucket/retail-data/ --recursive --human-readable --summarize

# Check network latency
time aws s3 cp s3://your-bucket/retail-data/sales.csv . --no-progress
```

**Solutions**:

1. **Larger files**: Increase task execution timeout

    ```python
    @task(execution_timeout=timedelta(hours=2))
    def extract():
        ...
    ```

2. **Network bottleneck**: Use S3 Transfer Acceleration or VPC endpoint
3. **Missing credentials**: Verify IAM permissions
    ```bash
    aws s3api get-bucket-versioning --bucket your-bucket
    ```

---

### Issue 2: "High Data Quality Drop Rate (>10%)"

**Symptoms**: Warning logs show >10% of rows dropped

**Diagnosis**:

```python
# Check validation logs for column-specific failures
grep "validation failed" /opt/airflow/logs/retail_etl_pipeline/

# Sample query to understand drop patterns
SELECT column, check, COUNT(*)
FROM validation_logs
WHERE date = CURRENT_DATE
GROUP BY column, check;
```

**Solutions**:

1. **Understand upstream data changes**: Talk to data provider
2. **Update schema if intentional**:
    ```python
    # In include/validations/input_schemas.py
    "qty": Column(int, Check.gt(0), nullable=False)  # Adjust Check
    ```
3. **Add data imputation logic** in transform if acceptable:
    ```python
    df["region"] = df["region"].fillna("UNKNOWN")
    ```

---

### Issue 3: "Snowflake Load Fails"

**Symptoms**: Load task fails with permission or connection error

**Diagnosis**:

```sql
-- Check Snowflake warehouse status
SELECT * FROM INFORMATION_SCHEMA.WAREHOUSES WHERE WAREHOUSE_NAME = 'COMPUTE_WH';

-- Check role permissions
SHOW GRANTS TO ROLE <YOUR_ROLE>;

-- Check table existence
SHOW TABLES LIKE 'SALES_CLEAN' IN SCHEMA <SCHEMA>;
```

**Solutions**:

1. **Warehouse suspended**: Resume warehouse

    ```sql
    ALTER WAREHOUSE COMPUTE_WH RESUME;
    ```

2. **Missing stage**: Recreate S3 stage

    ```bash
    python include/etl/load_snowflake.py ensure_snowflake_infrastructure
    ```

3. **Permission denied**: Grant role permissions
    ```sql
    GRANT ALL ON SCHEMA <SCHEMA> TO ROLE <YOUR_ROLE>;
    ```

---

### Issue 4: "DAG Still Running After 2 Hours"

**Symptoms**: DAG execution hangs or takes too long

**Diagnosis**:

```bash
# Check task status
airflow tasks list retail_etl_pipeline --detail

# Check executor resources
kubectl top pods -n airflow  # If using Kubernetes

# Check if data is larger than expected
aws s3 ls s3://your-bucket/retail-data/sales.csv --summarize
```

**Solutions**:

1. **Larger dataset**: Increase executor parallelism

    ```yaml
    # In airflow_settings.yaml
    parallelism: 8
    max_active_tasks_per_dag: 4
    ```

2. **Resource constraints**: Scale Airflow workers/executors

3. **Database bottleneck**: Check Airflow metadata database performance
    ```bash
    # For PostgreSQL backend
    SELECT * FROM pg_stat_activity;
    ```

---

## Performance Tuning

### 1. **Optimize Extract Performance**

```python
# Use pandas read_csv with optimized dtypes
df = pd.read_csv(
    file,
    dtype={
        "sales_id": "int32",
        "product_id": "int32",
        "qty": "int16",
    },
    usecols=["sales_id", "product_id", "qty"],  # Only needed columns
)
```

### 2. **Optimize Transform Performance**

```python
# Use pandas groupby with observed=True to reduce memory
df.groupby("category", observed=True).agg(...)

# Or use numpy/polars for large datasets
import polars as pl
df_polars = pl.from_pandas(df)
result = df_polars.groupby("category").agg(...)
```

### 3. **Optimize Load Performance**

```sql
-- In Snowflake, create cluster key for common filters
CREATE OR REPLACE TABLE sales_clean (
    sales_id INTEGER,
    region VARCHAR,
    sale_date DATE,
    ...
)
CLUSTER BY (region, sale_date);  -- Optimize for time-series queries
```

### 4. **Optimize S3 Access**

```yaml
# In airflow_settings.yaml
aws_conn_maximum_attempts: 10 # Retry connection
aws_conn_read_timeout: 60 # Increase timeout

# Use S3 Transfer Acceleration (if bucket enabled)
s3_address_style: virtual # Path-style vs virtual-hosted
```

---

## Runbook

### Daily Operations Checklist

**Morning (Before 8 AM)**:

-   [ ] Verify DAG ran successfully (check Airflow Web UI)
-   [ ] Check data freshness (last successful load timestamp)
-   [ ] Review data quality metrics (drop rate, validation failures)
-   [ ] Spot-check output data in Snowflake:
    ```sql
    SELECT COUNT(*), MAX(sale_date) FROM sales_clean;
    ```

**Weekly**:

-   [ ] Review performance metrics and trends
-   [ ] Check for any warnings in logs
-   [ ] Validate business logic accuracy with sample queries
-   [ ] Update documentation with any schema changes

**Monthly**:

-   [ ] Assess and optimize slow-running tasks
-   [ ] Review and tune Snowflake queries
-   [ ] Validate forecast vs. actual data volumes
-   [ ] Plan capacity for next quarter

### Incident Response

**Step 1: Isolate**

```bash
# Stop the DAG immediately
airflow dags pause retail_etl_pipeline

# Save logs for analysis
airflow logs retail_etl_pipeline $(date +%Y-%m-%dT%H:%M:%S) > incident.log
```

**Step 2: Diagnose**

```bash
# Check system health
df -h  # Disk space
free -h  # Memory
top  # CPU

# Check connectivity
aws s3 ls s3://your-bucket/
snowflake_sql_driver.connect(...)  # Test Snowflake
```

**Step 3: Remediate**

-   Fix the identified issue (see Troubleshooting section)
-   Test fix with a single DAG run:
    ```bash
    airflow dags test retail_etl_pipeline 2026-01-01
    ```

**Step 4: Resume**

```bash
# Re-enable the DAG
airflow dags unpause retail_etl_pipeline
```

**Step 5: Document**

-   Log incident in issue tracker
-   Update runbook with new learnings
-   Schedule post-mortem if production impact

---

## References

-   [Airflow Monitoring](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#monitoring)
-   [Snowflake Query History](https://docs.snowflake.com/en/sql-reference/account-usage/query_history.html)
-   [AWS S3 Performance Optimization](https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html)
-   [Data Quality Best Practices](https://www.sre.google/sre-book/monitoring-distributed-systems/)
