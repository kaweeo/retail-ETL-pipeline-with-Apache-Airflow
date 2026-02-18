from pandera.errors import SchemaErrors
from .input_schemas import sales_schema, products_schema
from include.logger import setup_logger

logger = setup_logger('validation.input')

def validate_sales(df):
    logger.info(f"Starting sales validation on {len(df)} rows")
    try:
        validated_df = sales_schema.validate(df, lazy=True)
        logger.info("Sales validation passed")
        return validated_df, 0
    
    except SchemaErrors as err:
        failed = err.failure_cases
        invalid_count = len(failed)
        logger.warning(f"Sales validation failed: {invalid_count} invalid rows")
        logger.warning(f"Errors summary:\n{failed.groupby(['column', 'check']).size()}")

        # Drop invalid rows by filtering out failed indices
        if len(failed) > 0:
            failed_indices = failed["index"].dropna().unique()
            if len(failed_indices) > 0:
                clean_df = df.drop(index=failed_indices)
            else:
                clean_df = df.copy()
        else:
            clean_df = df.copy()

        try:
            clean_df = sales_schema.validate(clean_df)  # re-validate clean data
            logger.info(f"Cleaned sales: {len(clean_df)} rows remaining")
        except SchemaErrors:
            logger.warning("Could not clean all invalid rows. Returning best effort.")
        
        return clean_df, invalid_count


def validate_products(df):
    logger.info(f"Starting products validation on {len(df)} rows")
    try:
        validated_df = products_schema.validate(df, lazy=True)
        logger.info("Products validation passed")
        return validated_df, 0
    
    except SchemaErrors as err:
        failed = err.failure_cases
        invalid_count = len(failed)
        logger.warning(f"Products validation failed: {invalid_count} issues")
        logger.warning(f"Errors:\n{failed}")

        # Drop invalid rows by filtering out failed indices
        if len(failed) > 0:
            failed_indices = failed["index"].dropna().unique()
            if len(failed_indices) > 0:
                clean_df = df.drop(index=failed_indices)
            else:
                clean_df = df.copy()
        else:
            clean_df = df.copy()

        try:
            clean_df = products_schema.validate(clean_df)
            logger.info(f"Cleaned products: {len(clean_df)} rows")
        except SchemaErrors:
            logger.warning("Could not clean all invalid rows. Returning best effort.")
        
        return clean_df, invalid_count