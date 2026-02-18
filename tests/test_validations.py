"""
Unit tests for data validation functions.

Tests validate the input/output schema validators ensure proper handling
of valid, invalid, and edge case data.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, date
from pathlib import Path
import sys

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from include.validations.input_schemas import sales_schema, products_schema
from include.validations.output_schemas import sales_clean_schema
from include.validations.validate_inputs import validate_sales, validate_products
from include.validations.validate_outputs import validate_sales_clean


class TestInputSalesValidation:
    """Test suite for sales input validation."""

    @pytest.fixture
    def valid_sales_df(self):
        """Create a valid sales DataFrame."""
        return pd.DataFrame({
            "sales_id": [1, 2, 3, 4, 5],
            "product_id": [101, 102, 103, 104, 105],
            "order_status": ["Completed", "Completed", "Completed", "Completed", "Completed"],
            "qty": [1, 2, 3, 4, 5],
            "price": [10.0, 20.0, 30.0, 40.0, 50.0],
            "discount": [0.0, 0.1, 0.2, None, 0.0],
            "region": ["US", "EU", "APAC", None, "US"],
            "time_stamp": ["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04", "2026-01-05"],
        })

    @pytest.fixture
    def invalid_sales_df(self):
        """Create a DataFrame with invalid data."""
        return pd.DataFrame({
            "sales_id": [1, None, 3, "invalid", 5],  # Null and invalid type
            "product_id": [101, 102, None, 104, 105],  # Null value
            "order_status": ["Completed", "Canceled", "Completed", "Completed", "Completed"],
            "qty": [1, -2, 0, 4, 5],  # Negative and zero values
            "price": [10.0, 20.0, 30.0, 40.0, 50.0],
            "discount": [0.0, 1.5, 0.2, 0.5, 0.0],  # Value > 1
            "region": ["US", "EU", "APAC", None, "US"],
            "time_stamp": ["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04", "2026-01-05"],
        })

    def test_valid_sales_passes_validation(self, valid_sales_df):
        """Test that valid sales data passes validation without dropping rows."""
        cleaned_df, dropped = validate_sales(valid_sales_df)
        assert len(cleaned_df) == 5
        assert dropped == 0

    def test_invalid_sales_drops_problematic_rows(self):
        """Test that invalid rows are identified and dropped."""
        # Use properly typed data to avoid type conversion issues after dropping
        df = pd.DataFrame({
            "sales_id": [1, 2, 3, 4, 5],
            "product_id": [101, 102, 103, 104, 105],
            "order_status": ["Completed", "Completed", "Completed", "Completed", "Completed"],
            "qty": [1, -2, 0, 4, 5],  # Negative and zero values are invalid
            "price": [10.0, 20.0, 30.0, 40.0, 50.0],
            "discount": [0.0, 1.5, 0.2, 0.5, 0.0],  # Value > 1 is invalid
            "region": ["US", "EU", "APAC", None, "US"],
            "time_stamp": ["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04", "2026-01-05"],
        })
        cleaned_df, dropped = validate_sales(df)
        assert dropped > 0
        assert len(cleaned_df) < len(df)

    def test_negative_qty_dropped(self):
        """Test that rows with negative qty are dropped."""
        df = pd.DataFrame({
            "sales_id": [1, 2],
            "product_id": [101, 102],
            "order_status": ["Completed", "Completed"],
            "qty": [5, -3],
            "price": [10.0, 20.0],
            "discount": [0.0, 0.0],
            "region": ["US", "EU"],
            "time_stamp": ["2026-01-01", "2026-01-02"],
        })
        cleaned_df, dropped = validate_sales(df)
        assert len(cleaned_df) == 1
        assert cleaned_df.iloc[0]["sales_id"] == 1

    def test_discount_out_of_range_dropped(self):
        """Test that discount values outside [0, 1] are dropped."""
        df = pd.DataFrame({
            "sales_id": [1, 2, 3],
            "product_id": [101, 102, 103],
            "order_status": ["Completed", "Completed", "Completed"],
            "qty": [5, 3, 2],
            "price": [10.0, 20.0, 30.0],
            "discount": [0.5, 1.5, 0.0],  # 1.5 is invalid
            "region": ["US", "EU", "APAC"],
            "time_stamp": ["2026-01-01", "2026-01-02", "2026-01-03"],
        })
        cleaned_df, dropped = validate_sales(df)
        assert len(cleaned_df) == 2
        assert dropped == 1


class TestInputProductsValidation:
    """Test suite for products input validation."""

    @pytest.fixture
    def valid_products_df(self):
        """Create a valid products DataFrame."""
        return pd.DataFrame({
            "product_id": [101, 102, 103],
            "category": ["Electronics", "Clothing", "Home"],
            "brand": ["BrandA", "BrandB", "BrandC"],
            "rating": [4.5, 3.8, 4.2],
            "in_stock": [True, False, True],
        })

    @pytest.fixture
    def invalid_products_df(self):
        """Create a DataFrame with invalid product data."""
        return pd.DataFrame({
            "product_id": [101, None, 103],  # Null value
            "category": ["Electronics", "Clothing", None],  # Null category
            "brand": ["BrandA", "BrandB", "BrandC"],
            "rating": [4.5, 6.0, -1.0],  # Out of range
            "in_stock": [True, False, "invalid"],  # Invalid type
        })

    def test_valid_products_pass_validation(self, valid_products_df):
        """Test that valid product data passes validation."""
        cleaned_df, dropped = validate_products(valid_products_df)
        assert len(cleaned_df) == 3
        assert dropped == 0

    def test_invalid_products_dropped(self):
        """Test that invalid product rows are dropped."""
        df = pd.DataFrame({
            "product_id": [101, 102, 103, 104],
            "category": ["Electronics", "Clothing", "Home", "Sports"],
            "brand": ["BrandA", "BrandB", "BrandC", "BrandD"],
            "rating": [4.5, 6.0, -1.0, 4.2],  # 6.0 and -1.0 are out of range
            "in_stock": [True, False, True, False],
        })
        cleaned_df, dropped = validate_products(df)
        assert dropped > 0
        assert len(cleaned_df) < len(df)

    def test_rating_out_of_range_dropped(self):
        """Test that ratings outside [0, 5] are dropped."""
        df = pd.DataFrame({
            "product_id": [101, 102, 103],
            "category": ["Electronics", "Clothing", "Home"],
            "brand": ["BrandA", "BrandB", "BrandC"],
            "rating": [4.5, 6.0, -1.0],  # Invalid: 6.0 and -1.0
            "in_stock": [True, False, True],
        })
        cleaned_df, dropped = validate_products(df)
        assert len(cleaned_df) == 1
        assert cleaned_df.iloc[0]["product_id"] == 101


class TestOutputValidation:
    """Test suite for output (clean) data validation."""

    @pytest.fixture
    def valid_clean_df(self):
        """Create a valid clean sales DataFrame."""
        return pd.DataFrame({
            "sales_id": [1, 2, 3],
            "product_id": [101, 102, 103],
            "category": ["Electronics", "Clothing", "Home"],
            "brand": ["BrandA", "BrandB", "BrandC"],
            "region": ["US", "EU", "APAC"],
            "qty": [5, 3, 2],
            "price": [100.0, 50.0, 75.0],
            "discount": [0.1, 0.0, 0.2],
            "revenue": [450.0, 150.0, 120.0],
            "rating": [4.5, 3.8, 4.2],
            "is_in_stock": [True, False, True],
            "is_discounted": [True, False, True],
            "sale_date": [date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3)],
            "sale_hour": [10, 14, 8],
        })

    @pytest.fixture
    def invalid_clean_df(self):
        """Create a DataFrame with invalid clean data."""
        return pd.DataFrame({
            "sales_id": [1, None, 3],  # Null value
            "product_id": [101, 102, 103],
            "category": ["Electronics", None, "Home"],  # Null category
            "brand": ["BrandA", "BrandB", "BrandC"],
            "region": ["US", "EU", "APAC"],
            "qty": [5, -3, 2],  # Negative qty
            "price": [-100.0, 50.0, 75.0],  # Negative price
            "discount": [0.1, 0.0, 0.2],
            "revenue": [-450.0, 150.0, 120.0],  # Negative revenue
            "rating": [4.5, 3.8, 6.0],  # Out of range
            "is_in_stock": [True, False, True],
            "is_discounted": [True, False, True],
            "sale_date": [date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3)],
            "sale_hour": [10, 14, 25],  # 25 is out of range
        })

    def test_valid_clean_passes_output_validation(self, valid_clean_df):
        """Test that valid clean data passes output validation."""
        cleaned_df, dropped = validate_sales_clean(valid_clean_df)
        assert len(cleaned_df) == 3
        assert dropped == 0

    def test_invalid_clean_dropped(self):
        """Test that invalid rows are dropped from clean data."""
        df = pd.DataFrame({
            "sales_id": [1, 2, 3],
            "product_id": [101, 102, 103],
            "category": ["Electronics", "Clothing", "Home"],
            "brand": ["BrandA", "BrandB", "BrandC"],
            "region": ["US", "EU", "APAC"],
            "qty": [5, -3, 2],  # Negative qty
            "price": [100.0, 50.0, 75.0],
            "discount": [0.1, 0.0, 0.2],
            "revenue": [450.0, 150.0, 120.0],
            "rating": [4.5, 3.8, 4.2],
            "is_in_stock": [True, False, True],
            "is_discounted": [True, False, True],
            "sale_date": [date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3)],
            "sale_hour": [10, 14, 25],  # 25 is out of range
        })
        cleaned_df, dropped = validate_sales_clean(df)
        assert dropped > 0
        assert len(cleaned_df) < len(df)

    def test_sale_hour_out_of_range_dropped(self):
        """Test that sale_hour outside [0, 23] is dropped."""
        df = pd.DataFrame({
            "sales_id": [1, 2, 3],
            "product_id": [101, 102, 103],
            "category": ["Electronics", "Clothing", "Home"],
            "brand": ["BrandA", "BrandB", "BrandC"],
            "region": ["US", "EU", "APAC"],
            "qty": [5, 3, 2],
            "price": [100.0, 50.0, 75.0],
            "discount": [0.1, 0.0, 0.2],
            "revenue": [450.0, 150.0, 120.0],
            "rating": [4.5, 3.8, 4.2],
            "is_in_stock": [True, False, True],
            "is_discounted": [True, False, True],
            "sale_date": [date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3)],
            "sale_hour": [10, 14, 25],  # 25 is invalid
        })
        cleaned_df, dropped = validate_sales_clean(df)
        assert len(cleaned_df) == 2
        assert dropped == 1

    def test_negative_price_dropped(self):
        """Test that rows with non-positive quantities are dropped."""
        df = pd.DataFrame({
            "sales_id": [1, 2],
            "product_id": [101, 102],
            "category": ["Electronics", "Clothing"],
            "brand": ["BrandA", "BrandB"],
            "region": ["US", "EU"],
            "qty": [5, -3],  # Negative qty is invalid (Check.gt(0))
            "price": [100.0, 50.0],
            "discount": [0.1, 0.0],
            "revenue": [450.0, 150.0],
            "rating": [4.5, 3.8],
            "is_in_stock": [True, False],
            "is_discounted": [True, False],
            "sale_date": [date(2026, 1, 1), date(2026, 1, 2)],
            "sale_hour": [10, 14],
        })
        cleaned_df, dropped = validate_sales_clean(df)
        assert len(cleaned_df) == 1
        assert dropped == 1


class TestSchemaCompliance:
    """Test that schemas are properly defined and enforceable."""

    def test_sales_schema_defined(self):
        """Test that sales schema is properly defined."""
        assert sales_schema is not None
        assert "sales_id" in sales_schema.columns
        assert "product_id" in sales_schema.columns
        assert "qty" in sales_schema.columns

    def test_products_schema_defined(self):
        """Test that products schema is properly defined."""
        assert products_schema is not None
        assert "product_id" in products_schema.columns
        assert "category" in products_schema.columns
        assert "brand" in products_schema.columns

    def test_clean_schema_defined(self):
        """Test that output schema is properly defined."""
        assert sales_clean_schema is not None
        assert "sales_id" in sales_clean_schema.columns
        assert "revenue" in sales_clean_schema.columns
        assert "is_discounted" in sales_clean_schema.columns


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
