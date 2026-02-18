import pandera.pandas as pa
from pandera.pandas import Check, Column, DataFrameSchema


sales_clean_schema = DataFrameSchema(
    {
        # Identifiers
        "sales_id": Column(int, nullable=False),
        "product_id": Column(int, nullable=False),

        # Dimensions (required for STAR schema)
        "category": Column(str, nullable=False),
        "brand": Column(str, nullable=False),
        "region": Column(str, nullable=False),

        # Measures (allow negative for flexible validation)
        "qty": Column(int, Check.gt(0), nullable=False),
        "price": Column(float, nullable=False),
        "discount": Column(float, Check.between(0, 1), nullable=False),
        "revenue": Column(float, nullable=False),

        # Enrichment attributes
        "rating": Column(float, nullable=True),
        "is_in_stock": Column(bool, nullable=False),
        "is_discounted": Column(bool, nullable=False),

        # Date dimensions
        "sale_date": Column(pa.Date, nullable=False),
        "sale_hour": Column(int, Check.between(0, 23), nullable=False),
    },
    strict=True
)
