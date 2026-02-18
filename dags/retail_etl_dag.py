from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
import yaml
from pathlib import Path
from typing import Any

from include.logger import setup_logger
from include.utils.s3_paths import build_cleansed_s3_key, build_raw_s3_key

# Load config
CONFIG_PATH = Path(__file__).parent.parent / "include" / "config.yaml"
with open(CONFIG_PATH) as f:
    config = yaml.safe_load(f)

AWS_CONN_ID = config["aws_conn_id"]
S3_CONFIG = config["s3"]
BUCKET = S3_CONFIG["bucket"]
RAW_FOLDER = S3_CONFIG.get("raw_folder", "retail-data/")
CLEANSED_FOLDER = S3_CONFIG.get("cleansed_folder", "cleansed-data/")
# Build full S3 keys using shared path helpers
SALES_KEY = build_raw_s3_key(RAW_FOLDER, S3_CONFIG["sales_key"])
PRODUCTS_KEY = build_raw_s3_key(RAW_FOLDER, S3_CONFIG["products_key"])
PROCESSED_KEY = build_cleansed_s3_key(
    CLEANSED_FOLDER, S3_CONFIG.get("processed_sales_key", "sales_clean.csv")
)

# Default arguments for DAG
DEFAULT_ARGS = {
    "owner": "data-engineering",
    "email": ["data-alerts@company.com"],
    "email_on_failure": False,  # Disabled until SMTP is configured
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}


@dag(
    dag_id="retail_etl_pipeline",
    description="""
    Retail ETL Pipeline - Extracts sales & product data from S3,
    validates, transforms, and loads clean data back to S3.
    
    Data Flow:
    1. Extract: Load sales.csv & product_data.json from S3
    2. Validate: Check data quality with Pandera schemas
    3. Transform: Enrich with business logic, handle timestamps, calculate metrics
    4. Validate: Final quality checks before loading
    5. Load: Write clean data to S3 processed zone
    """,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["retail", "etl", "production"],
    doc_md=__doc__,
)
def retail_etl_pipeline():
    """
    Main Retail ETL Pipeline DAG
    
    Processes retail sales data through validation and transformation stages.
    Implements data quality checks at each stage with comprehensive error handling.
    """
    from include.etl.extract_s3 import extract_sales_and_products
    from include.validations.validate_inputs import validate_sales, validate_products
    from include.etl.transform import transform_sales_and_products
    from include.validations.validate_outputs import validate_sales_clean
    from include.etl.load_s3_csv import write_sales_clean_csv_to_s3
    from include.etl.load_snowflake import (
        ensure_snowflake_infrastructure,
        load_sales_clean_to_snowflake,
    )

    logger = setup_logger("dags.retail_etl_pipeline")

    @task(
        task_id="extract_raw_data",
        doc_md="""
        Extracts raw sales and product data from S3.
        
        **Outputs:**
        - sales_df: DataFrame with 2500+ sales records
        - products_df: DataFrame with product metadata
        
        **Column Normalization:**
        - Converts all column names to lowercase
        - Replaces spaces with underscores
        """,
    )
    def extract():
        """Extract sales and products data from S3"""
        try:
            sales_df, products_df = extract_sales_and_products(
                aws_conn_id=AWS_CONN_ID,
                bucket=BUCKET,
                sales_key=SALES_KEY,
                products_key=PRODUCTS_KEY
            )
            logger.info(f"✓ Extracted {len(sales_df)} sales records")
            logger.info(f"✓ Extracted {len(products_df)} product records")
            return {"sales": sales_df, "products": products_df}
        except Exception as e:
            logger.error(f"✗ Extraction failed: {str(e)}")
            raise AirflowException(f"Data extraction failed: {str(e)}")

    @task(
        task_id="validate_input_data",
        doc_md="""
        Validates raw data against input schemas.
        
        **Quality Checks:**
        - sales_id, product_id: Non-null, integer type
        - qty: Positive integers
        - region: Non-null or filled with 'UNKNOWN'
        - timestamp: Mixed date formats accepted
        
        **Action:**
        - Removes invalid rows
        - Returns cleaned dataset + count of dropped rows
        """,
    )
    def validate_inputs(data: Any):
        """Validate input data quality"""
        try:
            clean_sales, sales_dropped = validate_sales(data["sales"])
            clean_products, prod_dropped = validate_products(data["products"])
            
            logger.info(f"✓ Input validation passed")
            logger.info(f"  - Sales: {len(clean_sales)} valid ({sales_dropped} dropped)")
            logger.info(f"  - Products: {len(clean_products)} valid ({prod_dropped} dropped)")
            
            if clean_sales.empty or clean_products.empty:
                raise AirflowException("Validation resulted in empty dataset")
            
            return {"sales": clean_sales, "products": clean_products}
        except Exception as e:
            logger.error(f"✗ Input validation failed: {str(e)}")
            raise

    @task(
        task_id="transform_enrich_data",
        doc_md="""
        Transforms and enriches raw data.
        
        **Transformations:**
        1. Filter completed orders only
        2. Normalize region values (uppercase)
        3. Parse mixed-format timestamps
        4. Calculate revenue with discount
        5. Merge product metadata
        6. Create business flags (is_discounted, is_in_stock)
        7. Type casting for schema compliance
        8. Filter negative prices/revenues
        
        **Output:**
        - Enriched DataFrame ready for final validation
        """,
    )
    def transform(data: Any):
        """Transform and enrich data"""
        try:
            enriched_df = transform_sales_and_products(
                sales_df=data["sales"],
                products_df=data["products"]
            )
            logger.info(f"✓ Transformation completed: {len(enriched_df)} records")
            return enriched_df
        except Exception as e:
            logger.error(f"✗ Transformation failed: {str(e)}")
            raise AirflowException(f"Data transformation failed: {str(e)}")

    @task(
        task_id="validate_and_load_clean_data",
        doc_md="""
        Final validation and loads to S3 processed zone.
        
        **Final Quality Checks:**
        - All required columns present
        - Consistent data types (sales_id: int, price: float, etc.)
        - Positive values for price & revenue
        - sale_hour in range [0, 23]
        - No null values in critical fields
        
        **Output Location:**
        - s3://{bucket}/{cleansed_folder}sales/sales_clean.csv
        
        """.format(
            bucket=BUCKET,
            cleansed_folder=CLEANSED_FOLDER
        ),
    )
    def validate_and_load(df):
        """Validate output and load to S3"""
        try:
            clean_df, dropped = validate_sales_clean(df)
            
            if clean_df.empty:
                raise AirflowException("No valid records to load")
            
            logger.info(f"✓ Output validation passed: {len(clean_df)} records")
            if dropped > 0:
                logger.warning(f"  ⚠ {dropped} rows failed validation and were excluded")
            
            write_sales_clean_csv_to_s3(
                df=clean_df,
                aws_conn_id=AWS_CONN_ID,
                bucket=BUCKET,
                key=PROCESSED_KEY
            )
            
            success_msg = f"✓ Pipeline SUCCESS: {len(clean_df)} records loaded"
            logger.info(success_msg)
            return success_msg
            
        except Exception as e:
            logger.error(f"✗ Validation/Load failed: {str(e)}")
            raise AirflowException(f"Pipeline failed at final stage: {str(e)}")

    @task(
        task_id="prepare_snowflake",
        doc_md="""
        Creates Snowflake warehouse/database/schemas/file format and stage (idempotent).
        Use storage integration for production-grade access to S3.
        """,
    )
    def prepare_snowflake():
        """Ensure Snowflake infrastructure exists before loading."""
        snowflake_cfg = config.get("snowflake")
        if not snowflake_cfg:
            raise AirflowException("Snowflake configuration missing in include/config.yaml")

        enabled = snowflake_cfg.get("enabled", True)
        if not enabled:
            logger.info("Snowflake bootstrap skipped (snowflake.enabled = false)")
            return "Snowflake bootstrap skipped"

        bootstrap = snowflake_cfg.get("bootstrap", True)
        if not bootstrap:
            logger.info("Snowflake bootstrap skipped (snowflake.bootstrap = false)")
            return "Snowflake bootstrap skipped"

        ensure_snowflake_infrastructure(
            snowflake_conn_id=snowflake_cfg.get("conn_id"),
            database=snowflake_cfg.get("database"),
            schema=snowflake_cfg.get("schema", "CLEANSED"),
            warehouse=snowflake_cfg.get("warehouse"),
            role=snowflake_cfg.get("role"),
            stage_schema=snowflake_cfg.get("stage_schema", "RAW"),
            stage_name=snowflake_cfg.get("stage_name", "S3_PROCESSED_STAGE"),
            file_format_name=snowflake_cfg.get("file_format_name", "CSV_FORMAT"),
            storage_integration=snowflake_cfg.get("storage_integration"),
            s3_bucket=BUCKET,
            s3_key=PROCESSED_KEY,
            s3_stage_url=snowflake_cfg.get("s3_stage_url"),
            aws_key_id=snowflake_cfg.get("aws_key_id"),
            aws_secret_key=snowflake_cfg.get("aws_secret_key"),
            aws_session_token=snowflake_cfg.get("aws_session_token"),
            create_stage=snowflake_cfg.get("create_stage", True),
            warehouse_size=snowflake_cfg.get("warehouse_size", "XSMALL"),
            auto_suspend_seconds=snowflake_cfg.get("auto_suspend_seconds", 300),
            auto_resume=snowflake_cfg.get("auto_resume", True),
            initially_suspended=snowflake_cfg.get("initially_suspended", True),
        )
        return "Snowflake bootstrap complete"

    @task(
        task_id="load_to_snowflake",
        doc_md="""
        Loads cleansed sales data from S3 into Snowflake.
        
        **Requirements:**
        - Snowflake connection configured in Airflow
        - External stage configured (or stage creation enabled with integration/creds)
        
        **Result:**
        - CLEANSED.SALES_CLEAN populated from s3://{bucket}/{processed_key}
        """.format(
            bucket=BUCKET,
            processed_key=PROCESSED_KEY
        ),
    )
    def load_to_snowflake():
        """Load cleansed data into Snowflake using COPY INTO."""
        snowflake_cfg = config.get("snowflake")
        if not snowflake_cfg:
            raise AirflowException("Snowflake configuration missing in include/config.yaml")

        enabled = snowflake_cfg.get("enabled", True)
        if not enabled:
            logger.warning("Snowflake load skipped (snowflake.enabled = false)")
            return "Snowflake load skipped"

        bootstrap = snowflake_cfg.get("bootstrap", True)
        create_stage = snowflake_cfg.get("create_stage", True)
        load_create_stage = create_stage and not bootstrap

        row_count = load_sales_clean_to_snowflake(
            snowflake_conn_id=snowflake_cfg.get("conn_id"),
            s3_bucket=BUCKET,
            s3_key=PROCESSED_KEY,
            database=snowflake_cfg.get("database"),
            schema=snowflake_cfg.get("schema", "CLEANSED"),
            warehouse=snowflake_cfg.get("warehouse"),
            role=snowflake_cfg.get("role"),
            stage_schema=snowflake_cfg.get("stage_schema", "RAW"),
            stage_name=snowflake_cfg.get("stage_name", "S3_PROCESSED_STAGE"),
            file_format_name=snowflake_cfg.get("file_format_name", "CSV_FORMAT"),
            table_name=snowflake_cfg.get("table_name", "SALES_CLEAN"),
            storage_integration=snowflake_cfg.get("storage_integration"),
            s3_stage_url=snowflake_cfg.get("s3_stage_url"),
            aws_key_id=snowflake_cfg.get("aws_key_id"),
            aws_secret_key=snowflake_cfg.get("aws_secret_key"),
            aws_session_token=snowflake_cfg.get("aws_session_token"),
            create_stage=load_create_stage,
            truncate_before_load=snowflake_cfg.get("truncate_before_load", True),
            on_error=snowflake_cfg.get("on_error", "ABORT_STATEMENT"),
        )
        return f"Snowflake load complete: {row_count} rows"

    # Define task dependencies
    extracted = extract()
    validated = validate_inputs(extracted)
    transformed = transform(validated)
    result = validate_and_load(transformed)
    prepared = prepare_snowflake()
    snowflake_load = load_to_snowflake()
    result >> prepared >> snowflake_load


# Instantiate DAG
retail_etl_pipeline()
