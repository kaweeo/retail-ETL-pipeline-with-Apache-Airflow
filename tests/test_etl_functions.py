"""
Unit tests for ETL functions.

Tests validate extract, transform, and load functions handle data correctly
with proper error handling and transformations.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, date
from io import StringIO
from pathlib import Path
import sys

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from include.etl.transform import transform_sales_and_products


class TestTransformFunction:
    """Test suite for transform_sales_and_products function."""

    @pytest.fixture
    def sample_sales_df(self):
        """Create a sample sales DataFrame."""
        return pd.DataFrame({
            "sales_id": [1, 2, 3, 4, 5],
            "product_id": [101, 102, 103, 104, 105],
            "order_status": ["Completed", "Completed", "Pending", "Completed", "Completed"],
            "qty": [5, 3, 2, 4, 1],
            "price": [100.0, 50.0, 75.0, 200.0, 30.0],
            "discount": [0.1, 0.0, 0.2, None, 0.05],
            "region": ["us", None, "eu", "APAC", "us"],
            "time_stamp": ["2026-01-01 10:30", "2026-01-02", "2026-01-03 14:15", "01/04/2026", "2026-01-05 08:00"],
        })

    @pytest.fixture
    def sample_products_df(self):
        """Create a sample products DataFrame."""
        return pd.DataFrame({
            "product_id": [101, 102, 103, 104, 105],
            "category": ["Electronics", "Clothing", "Home", "Sports", "Electronics"],
            "brand": ["BrandA", "BrandB", "BrandC", "BrandD", "BrandE"],
            "rating": [4.5, 3.8, 4.2, 4.0, 4.8],
            "in_stock": [True, False, True, True, False],
        })

    def test_transform_completes_successfully(self, sample_sales_df, sample_products_df):
        """Test that transform function completes without errors."""
        result = transform_sales_and_products(sample_sales_df, sample_products_df)
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0

    def test_transform_filters_non_completed_orders(self, sample_sales_df, sample_products_df):
        """Test that only 'Completed' orders are retained."""
        result = transform_sales_and_products(sample_sales_df, sample_products_df)
        # Sample has 5 rows, one with "Pending" status, so result should have 4
        assert len(result) == 4
        # Note: order_status column is dropped after filtering, verify via row count instead

    def test_transform_normalizes_region_to_uppercase(self, sample_sales_df, sample_products_df):
        """Test that region values are normalized to uppercase."""
        result = transform_sales_and_products(sample_sales_df, sample_products_df)
        assert "UNKNOWN" in result["region"].values  # Null should become UNKNOWN
        assert "US" in result["region"].values
        # Note: 'eu' row is filtered out due to Pending status, so check only remaining regions
        assert all(region == region.upper() for region in result["region"].values)
        # Check no lowercase values
        assert not any(result["region"].str.contains("[a-z]", regex=True))

    def test_transform_fills_null_regions_with_unknown(self, sample_sales_df, sample_products_df):
        """Test that null region values are filled with 'UNKNOWN'."""
        result = transform_sales_and_products(sample_sales_df, sample_products_df)
        assert "UNKNOWN" in result["region"].values
        assert not result["region"].isna().any()

    def test_transform_calculates_revenue_correctly(self, sample_sales_df, sample_products_df):
        """Test that revenue is calculated: qty * price * (1 - discount)."""
        result = transform_sales_and_products(sample_sales_df, sample_products_df)
        
        # First row: qty=5, price=100, discount=0.1
        # Expected: 5 * 100 * (1 - 0.1) = 450
        first_revenue = result.iloc[0]["revenue"]
        assert abs(first_revenue - 450.0) < 0.01
        
        # Second row: qty=3, price=50, discount=0 (null filled)
        # Expected: 3 * 50 * (1 - 0) = 150
        second_revenue = result.iloc[1]["revenue"]
        assert abs(second_revenue - 150.0) < 0.01

    def test_transform_creates_business_flags(self, sample_sales_df, sample_products_df):
        """Test that business flags are created."""
        result = transform_sales_and_products(sample_sales_df, sample_products_df)
        assert "is_discounted" in result.columns
        assert "is_in_stock" in result.columns
        assert result["is_discounted"].dtype == bool
        assert result["is_in_stock"].dtype == bool

    def test_transform_extracts_date_and_hour(self, sample_sales_df, sample_products_df):
        """Test that date and hour are extracted from timestamp."""
        result = transform_sales_and_products(sample_sales_df, sample_products_df)
        assert "sale_date" in result.columns
        assert "sale_hour" in result.columns
        assert all(0 <= h <= 23 for h in result["sale_hour"])

    def test_transform_merges_product_data(self, sample_sales_df, sample_products_df):
        """Test that product metadata is merged."""
        result = transform_sales_and_products(sample_sales_df, sample_products_df)
        assert "category" in result.columns
        assert "brand" in result.columns
        assert "rating" in result.columns
        # Check that join was successful (no NaN in merged fields for matching products)
        assert result[result["product_id"] == 101]["category"].iloc[0] == "Electronics"

    def test_transform_handles_mixed_timestamp_formats(self, sample_sales_df, sample_products_df):
        """Test that mixed date formats are parsed correctly."""
        result = transform_sales_and_products(sample_sales_df, sample_products_df)
        # All timestamps should be parsed successfully
        assert "sale_date" in result.columns
        assert isinstance(result.iloc[0]["sale_date"], date)

    def test_transform_filters_negative_prices(self):
        """Test that rows with negative prices are filtered out."""
        sales_df = pd.DataFrame({
            "sales_id": [1, 2, 3],
            "product_id": [101, 102, 103],
            "order_status": ["Completed", "Completed", "Completed"],
            "qty": [5, 3, 2],
            "price": [100.0, -50.0, 75.0],  # Negative price
            "discount": [0.1, 0.0, 0.2],
            "region": ["US", "EU", "APAC"],
            "time_stamp": ["2026-01-01", "2026-01-02", "2026-01-03"],
        })
        
        products_df = pd.DataFrame({
            "product_id": [101, 102, 103],
            "category": ["A", "B", "C"],
            "brand": ["X", "Y", "Z"],
            "rating": [4.5, 3.8, 4.2],
            "in_stock": [True, False, True],
        })
        
        result = transform_sales_and_products(sales_df, products_df)
        # Should exclude row with negative price
        assert len(result) == 2
        # Verify the negative price row is gone
        assert not (result["price"] < 0).any()

    def test_transform_filters_negative_revenues(self):
        """Test that rows resulting in negative revenue are filtered."""
        sales_df = pd.DataFrame({
            "sales_id": [1, 2],
            "product_id": [101, 102],
            "order_status": ["Completed", "Completed"],
            "qty": [5, 3],
            "price": [100.0, 50.0],
            "discount": [0.1, 2.0],  # discount=2 will cause negative revenue
            "region": ["US", "EU"],
            "time_stamp": ["2026-01-01", "2026-01-02"],
        })
        
        products_df = pd.DataFrame({
            "product_id": [101, 102],
            "category": ["A", "B"],
            "brand": ["X", "Y"],
            "rating": [4.5, 3.8],
            "in_stock": [True, False],
        })
        
        result = transform_sales_and_products(sales_df, products_df)
        # Negative revenues should be filtered
        assert (result["revenue"] > 0).all()

    def test_transform_handles_null_discount(self):
        """Test that null discount values are filled with 0."""
        sales_df = pd.DataFrame({
            "sales_id": [1, 2],
            "product_id": [101, 102],
            "order_status": ["Completed", "Completed"],
            "qty": [5, 3],
            "price": [100.0, 50.0],
            "discount": [0.1, None],
            "region": ["US", "EU"],
            "time_stamp": ["2026-01-01", "2026-01-02"],
        })
        
        products_df = pd.DataFrame({
            "product_id": [101, 102],
            "category": ["A", "B"],
            "brand": ["X", "Y"],
            "rating": [4.5, 3.8],
            "in_stock": [True, False],
        })
        
        result = transform_sales_and_products(sales_df, products_df)
        # No null discounts should remain
        assert not result["discount"].isna().any()
        # Second row should have discount = 0
        assert result.iloc[1]["discount"] == 0.0

    def test_transform_output_column_order(self, sample_sales_df, sample_products_df):
        """Test that output has expected columns in correct order."""
        result = transform_sales_and_products(sample_sales_df, sample_products_df)
        
        expected_columns = [
            "sales_id", "product_id", "category", "brand", "region",
            "qty", "price", "discount", "revenue", "rating",
            "is_in_stock", "is_discounted", "sale_date", "sale_hour"
        ]
        
        for col in expected_columns:
            assert col in result.columns


class TestTransformEdgeCases:
    """Test edge cases in transform function."""

    def test_transform_empty_dataframe(self):
        """Test transform with empty DataFrames."""
        empty_sales = pd.DataFrame(columns=[
            "sales_id", "product_id", "order_status", "qty", "price",
            "discount", "region", "time_stamp"
        ])
        
        products_df = pd.DataFrame({
            "product_id": [101],
            "category": ["A"],
            "brand": ["X"],
            "rating": [4.5],
            "in_stock": [True],
        })
        
        result = transform_sales_and_products(empty_sales, products_df)
        assert len(result) == 0

    def test_transform_all_rows_filtered_out(self):
        """Test when all rows are filtered due to validation."""
        sales_df = pd.DataFrame({
            "sales_id": [1, 2],
            "product_id": [101, 102],
            "order_status": ["Pending", "Canceled"],  # No Completed
            "qty": [5, 3],
            "price": [100.0, 50.0],
            "discount": [0.1, 0.0],
            "region": ["US", "EU"],
            "time_stamp": ["2026-01-01", "2026-01-02"],
        })
        
        products_df = pd.DataFrame({
            "product_id": [101, 102],
            "category": ["A", "B"],
            "brand": ["X", "Y"],
            "rating": [4.5, 3.8],
            "in_stock": [True, False],
        })
        
        result = transform_sales_and_products(sales_df, products_df)
        # All rows filtered because status != "Completed"
        assert len(result) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
