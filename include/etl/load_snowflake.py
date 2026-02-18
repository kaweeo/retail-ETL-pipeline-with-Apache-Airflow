from typing import Optional, Tuple

from airflow.exceptions import AirflowException
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from include.logger import setup_logger

logger = setup_logger("etl.load_snowflake")


def _split_s3_key(key: str) -> Tuple[str, str]:
    clean_key = key.lstrip("/")
    if "/" in clean_key:
        prefix, filename = clean_key.rsplit("/", 1)
        return prefix, filename
    return "", clean_key


def _qualify(name: str, database: Optional[str], schema: Optional[str]) -> str:
    if "." in name:
        return name
    if database and schema:
        return f"{database}.{schema}.{name}"
    if schema:
        return f"{schema}.{name}"
    return name


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
    s3_stage_url: Optional[str] = None,
    aws_key_id: Optional[str] = None,
    aws_secret_key: Optional[str] = None,
    aws_session_token: Optional[str] = None,
    create_stage: bool = True,
    warehouse_size: str = "XSMALL",
    auto_suspend_seconds: int = 300,
    auto_resume: bool = True,
    initially_suspended: bool = True,
) -> None:
    """
    Ensure Snowflake warehouse, database, schemas, file format, and stage exist.
    Intended for idempotent bootstrap in Airflow.
    """
    if not snowflake_conn_id:
        raise AirflowException("Snowflake connection ID is required")
    if not database or not schema or not warehouse:
        raise AirflowException("Snowflake database, schema, and warehouse are required")

    stage_url = s3_stage_url
    if create_stage and not stage_url:
        if not s3_bucket:
            raise AirflowException("S3 bucket is required to create Snowflake stage")
        # Extract folder path from S3 key (e.g., "retail-data/cleansed/sales/" -> "retail-data/cleansed/")
        stage_path = ""
        if s3_key:
            s3_prefix, _ = _split_s3_key(s3_key)
            stage_path = f"{s3_prefix}/" if s3_prefix else ""
        stage_url = f"s3://{s3_bucket}/{stage_path}"
        logger.info("Constructed Snowflake stage URL: %s", stage_url)

    # Previously we hard-failed if create_stage=True but no storage integration
    # or AWS credentials were provided. This is very strict and makes it hard
    # to work with manually managed stages. Instead, we now log a warning and
    # skip stage creation, assuming the stage already exists.
    if create_stage and not storage_integration and not (aws_key_id and aws_secret_key):
        logger.warning(
            "Snowflake stage creation requested but no storage_integration or "
            "AWS credentials provided. Skipping stage creation and assuming "
            "the stage already exists."
        )
        create_stage = False

    stage_qualified = _qualify(stage_name, database, stage_schema)
    file_format_qualified = _qualify(file_format_name, database, stage_schema)

    hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)

    try:
        if role:
            hook.run(f"USE ROLE {role}")

        create_warehouse_sql = f"""
        CREATE WAREHOUSE IF NOT EXISTS {warehouse}
            WAREHOUSE_SIZE = {warehouse_size}
            AUTO_SUSPEND = {auto_suspend_seconds}
            AUTO_RESUME = {str(auto_resume).upper()}
            INITIALLY_SUSPENDED = {str(initially_suspended).upper()}
        """
        hook.run(create_warehouse_sql)

        hook.run(f"CREATE DATABASE IF NOT EXISTS {database}")
        hook.run(f"CREATE SCHEMA IF NOT EXISTS {database}.{schema}")
        hook.run(f"CREATE SCHEMA IF NOT EXISTS {database}.{stage_schema}")

        create_file_format_sql = f"""
        CREATE FILE FORMAT IF NOT EXISTS {file_format_qualified}
            TYPE = 'CSV'
            FIELD_DELIMITER = ','
            SKIP_HEADER = 1
            NULL_IF = ('NULL', 'null', '')
            EMPTY_FIELD_AS_NULL = TRUE
            COMPRESSION = 'NONE'
        """
        hook.run(create_file_format_sql)

        if create_stage:
            stage_parts = [
                f"CREATE STAGE IF NOT EXISTS {stage_qualified}",
                f"URL = '{stage_url}'",
                f"FILE_FORMAT = {file_format_qualified}",
            ]
            if storage_integration:
                stage_parts.append(f"STORAGE_INTEGRATION = {storage_integration}")
            else:
                creds = [
                    f"AWS_KEY_ID = '{aws_key_id}'",
                    f"AWS_SECRET_KEY = '{aws_secret_key}'",
                ]
                if aws_session_token:
                    creds.append(f"AWS_TOKEN = '{aws_session_token}'")
                stage_parts.append(f"CREDENTIALS = ({' '.join(creds)})")

            hook.run(" ".join(stage_parts))

        logger.info("Snowflake infrastructure ensured for %s.%s", database, schema)

    except Exception as exc:
        logger.error("Snowflake bootstrap failed: %s", str(exc), exc_info=True)
        raise AirflowException(f"Snowflake bootstrap failed: {str(exc)}") from exc


def load_sales_clean_to_snowflake(
    *,
    snowflake_conn_id: str,
    s3_bucket: str,
    s3_key: str,
    database: str,
    schema: str,
    warehouse: str,
    role: Optional[str] = None,
    stage_schema: str = "RAW",
    stage_name: str = "S3_PROCESSED_STAGE",
    file_format_name: str = "CSV_FORMAT",
    table_name: str = "SALES_CLEAN",
    storage_integration: Optional[str] = None,
    s3_stage_url: Optional[str] = None,
    aws_key_id: Optional[str] = None,
    aws_secret_key: Optional[str] = None,
    aws_session_token: Optional[str] = None,
    create_stage: bool = True,
    truncate_before_load: bool = True,
    on_error: str = "ABORT_STATEMENT",
) -> int:
    """
    Load cleansed sales data from S3 into Snowflake using an external stage and COPY INTO.
    Returns the row count after load.
    """
    if not snowflake_conn_id:
        raise AirflowException("Snowflake connection ID is required")
    if not s3_bucket or not s3_key:
        raise AirflowException("S3 bucket and key are required for Snowflake load")
    if not database or not schema or not warehouse:
        raise AirflowException("Snowflake database, schema, and warehouse are required")

    s3_prefix, filename = _split_s3_key(s3_key)
    if not filename:
        raise AirflowException(f"Invalid S3 key: {s3_key}")

    stage_url = s3_stage_url
    if not stage_url:
        # Extract folder path from S3 key (e.g., "retail-data/cleansed/sales/" -> "retail-data/cleansed/")
        stage_path = f"{s3_prefix}/" if s3_prefix else ""
        stage_url = f"s3://{s3_bucket}/{stage_path}"
        logger.info("Constructed Snowflake stage URL for load: %s", stage_url)

    stage_qualified = _qualify(stage_name, database, stage_schema)
    file_format_qualified = _qualify(file_format_name, database, stage_schema)
    table_qualified = _qualify(table_name, database, schema)

    logger.info(
        "Preparing Snowflake load from s3://%s/%s into %s",
        s3_bucket,
        s3_key,
        table_qualified,
    )

    # Same soft-fail behaviour during load: if create_stage=True but we lack
    # integration/credentials, log and continue instead of throwing, assuming
    # the stage is managed outside this pipeline.
    if create_stage and not storage_integration and not (aws_key_id and aws_secret_key):
        logger.warning(
            "Snowflake stage creation requested during load but no "
            "storage_integration or AWS credentials provided. "
            "Proceeding without creating/updating the stage and assuming it exists."
        )
        create_stage = False

    hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)

    try:
        if role:
            hook.run(f"USE ROLE {role}")
        hook.run(f"USE WAREHOUSE {warehouse}")
        hook.run(f"USE DATABASE {database}")
        hook.run(f"USE SCHEMA {schema}")

        create_file_format_sql = f"""
        CREATE FILE FORMAT IF NOT EXISTS {file_format_qualified}
            TYPE = 'CSV'
            FIELD_DELIMITER = ','
            SKIP_HEADER = 1
            NULL_IF = ('NULL', 'null', '')
            EMPTY_FIELD_AS_NULL = TRUE
            COMPRESSION = 'NONE'
        """
        hook.run(create_file_format_sql)

        if create_stage:
            stage_parts = [
                f"CREATE STAGE IF NOT EXISTS {stage_qualified}",
                f"URL = '{stage_url}'",
                f"FILE_FORMAT = {file_format_qualified}",
            ]
            if storage_integration:
                stage_parts.append(f"STORAGE_INTEGRATION = {storage_integration}")
            else:
                creds = [
                    f"AWS_KEY_ID = '{aws_key_id}'",
                    f"AWS_SECRET_KEY = '{aws_secret_key}'",
                ]
                if aws_session_token:
                    creds.append(f"AWS_TOKEN = '{aws_session_token}'")
                stage_parts.append(f"CREDENTIALS = ({' '.join(creds)})")

            hook.run(" ".join(stage_parts))

        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_qualified} (
            sales_id INTEGER,
            product_id INTEGER,
            category STRING,
            brand STRING,
            region STRING,
            qty FLOAT,
            price FLOAT,
            discount FLOAT,
            revenue FLOAT,
            rating FLOAT,
            is_in_stock BOOLEAN,
            is_discounted BOOLEAN,
            sale_date DATE,
            sale_hour INTEGER
        )
        """
        hook.run(create_table_sql)

        if truncate_before_load:
            hook.run(f"TRUNCATE TABLE {table_qualified}")

        copy_sql = f"""
        COPY INTO {table_qualified}
        FROM @{stage_qualified}/{filename}
        FILE_FORMAT = (FORMAT_NAME = {file_format_qualified})
        ON_ERROR = '{on_error}'
        FORCE = TRUE
        """
        hook.run(copy_sql)

        count = hook.get_first(f"SELECT COUNT(*) FROM {table_qualified}")
        row_count = int(count[0]) if count else 0
        logger.info("Snowflake load complete: %s rows in %s", row_count, table_qualified)
        return row_count

    except Exception as exc:
        logger.error("Snowflake load failed: %s", str(exc), exc_info=True)
        raise AirflowException(f"Snowflake load failed: {str(exc)}") from exc
