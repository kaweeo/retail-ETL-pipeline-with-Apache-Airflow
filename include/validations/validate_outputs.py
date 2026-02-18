from pandera.errors import SchemaErrors
from .output_schemas import sales_clean_schema
from include.logger import setup_logger

logger = setup_logger("validation.output")


def validate_sales_clean(df):
    """
    Validate transformed sales dataset before writing to processed S3.
    """
    logger.info(f"Starting output validation on {len(df)} records")

    try:
        validated_df = sales_clean_schema.validate(df, lazy=True)
        logger.info("Output validation passed")
        return validated_df, 0

    except SchemaErrors as err:
        failed = err.failure_cases
        invalid_count = len(failed)

        logger.error(
            f"Output validation failed with {invalid_count} issues"
        )
        logger.error(
            f"Failure summary:\n{failed.groupby(['column', 'check']).size()}"
        )

        # Drop invalid rows by filtering out failed indices
        if len(failed) > 0:
            failed_indices = failed["index"].dropna().unique()
            if len(failed_indices) > 0:
                clean_df = df.drop(index=failed_indices)
            else:
                clean_df = df.copy()
        else:
            clean_df = df.copy()

        if clean_df.empty:
            raise ValueError(
                "All rows failed output validation — aborting pipeline"
            )

        # Re-validate cleaned dataset
        try:
            clean_df = sales_clean_schema.validate(clean_df)
            logger.info(
                f"Cleaned output dataset: {len(clean_df)} valid rows"
            )
        except SchemaErrors:
            logger.warning("Could not clean all invalid rows. Returning best effort.")

        return clean_df, invalid_count
