import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from io import StringIO
from include.logger import setup_logger

logger = setup_logger('etl.extract_s3')

def extract_sales_and_products(aws_conn_id: str, bucket: str, sales_key: str, products_key: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Extract sales.csv and product_data.json from S3 and return DataFrames.
    Normalizes column names to lowercase with underscores.
    """
    hook = S3Hook(aws_conn_id=aws_conn_id)

    # Extract sales (CSV)
    logger.info(f"Extracting sales from s3://{bucket}/{sales_key}")
    sales_content = hook.read_key(key=sales_key, bucket_name=bucket)
    sales_df = pd.read_csv(StringIO(sales_content))
    logger.info(f"Successfully extracted {len(sales_df)} rows from sales")

    # Normalize column names: lowercase and replace spaces with underscores
    sales_df.columns = sales_df.columns.str.lower().str.replace(' ', '_')
    logger.info(f"Normalized sales columns: {list(sales_df.columns)}")

    # Extract products (JSON)
    logger.info(f"Extracting products from s3://{bucket}/{products_key}")
    products_content = hook.read_key(key=products_key, bucket_name=bucket)
    products_df = pd.read_json(StringIO(products_content))  # assumes JSON array
    logger.info(f"Successfully extracted {len(products_df)} products")

    # Normalize product column names
    products_df.columns = products_df.columns.str.lower().str.replace(' ', '_')
    logger.info(f"Normalized product columns: {list(products_df.columns)}")

    return sales_df, products_df


# Data Lake Structure (implemented):
# amzn-s3-retail-data-eucent2
# │
# ├── retail-data/
# │   ├── product_data.json
# │   └── sales_data.csv
# │
# └── cleansed-data/
#     └── sales_clean.csv