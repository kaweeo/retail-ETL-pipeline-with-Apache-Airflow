import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from botocore.exceptions import ClientError, NoCredentialsError
from io import StringIO
from include.logger import setup_logger

logger = setup_logger("etl.load_s3")


def write_sales_clean_csv_to_s3(
    df: pd.DataFrame,
    aws_conn_id: str,
    bucket: str,
    key: str
) -> None:
    """
    Write validated sales dataframe to S3 as CSV.
    Includes error handling for authentication and S3 operations.
    """

    logger.info(f"Writing {len(df)} records to s3://{bucket}/{key}")

    try:
        # Validate inputs
        if df.empty:
            raise ValueError("DataFrame is empty - cannot write to S3")
        
        if not bucket or not key:
            raise ValueError("Bucket and key must not be empty")

        df = df.copy()

        # Type conversions for S3 compatibility
        df["sale_date"] = df["sale_date"].astype(str)
        df["is_in_stock"] = df["is_in_stock"].astype(bool)
        df["is_discounted"] = df["is_discounted"].astype(bool)

        # Convert DataFrame to CSV string
        buffer = StringIO()
        df.to_csv(buffer, index=False)
        csv_data = buffer.getvalue()

        if not csv_data:
            raise ValueError("CSV conversion resulted in empty data")

        logger.info(f"CSV prepared: {len(csv_data)} bytes")

        # Initialize S3 hook with credentials
        try:
            hook = S3Hook(aws_conn_id=aws_conn_id)
            logger.info(f"AWS credentials validated for connection: {aws_conn_id}")
        except NoCredentialsError as e:
            logger.error(f"AWS credentials not found for connection '{aws_conn_id}'")
            raise ValueError(
                f"Invalid AWS connection '{aws_conn_id}'. "
                "Ensure AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are set in .env"
            ) from e

        # Write to S3
        try:
            hook.load_string(
                string_data=csv_data,
                key=key,
                bucket_name=bucket,
                replace=True
            )
            logger.info(f"Successfully written to S3: s3://{bucket}/{key}")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchBucket':
                logger.error(f"S3 bucket '{bucket}' does not exist")
                raise ValueError(f"S3 bucket '{bucket}' not found") from e
            elif error_code == 'AccessDenied':
                logger.error(f"Access denied to bucket '{bucket}'")
                raise PermissionError(
                    f"Access denied to S3 bucket '{bucket}'. "
                    "Check AWS credentials and bucket permissions."
                ) from e
            else:
                logger.error(f"S3 operation failed: {error_code} - {e}")
                raise

        logger.info("Processed CSV successfully written to S3")

    except (ValueError, PermissionError) as e:
        logger.error(f"Validation/Permission error: {str(e)}")
        raise

    except Exception as e:
        logger.error(f"Unexpected error writing to S3: {str(e)}", exc_info=True)
        raise RuntimeError(
            f"Failed to write data to S3: {str(e)}"
        ) from e


# Future enhancement: write Parquet to S3 (faster + columnar).
# See README.md -> "Future enhancement: Parquet output" for a concrete plan.