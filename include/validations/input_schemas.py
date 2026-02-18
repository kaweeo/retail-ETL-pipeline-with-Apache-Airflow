from pandera.pandas import Check, Column, DataFrameSchema


sales_schema = DataFrameSchema(
    {
        # Identifiers (raw from source)
        "sales_id": Column(int, nullable=False),
        "product_id": Column(int, nullable=False),

        # Order info
        "order_status": Column(str, nullable=False),
        "qty": Column(int, Check.gt(0), nullable=False),
        "price": Column(float, nullable=False),  # Allow negative/zero for data cleaning

        # Discount (can be null or valid range)
        "discount": Column(float, Check.between(0, 1), nullable=True),

        # Location
        "region": Column(str, nullable=True),

        # Timestamp
        "time_stamp": Column(str, nullable=False),
    },
    strict=False  # Allow extra columns (will be dropped during transform)
)


products_schema = DataFrameSchema(
    {
        # Identifiers
        "product_id": Column(int, nullable=False),

        # Product attributes
        "category": Column(str, nullable=False),
        "brand": Column(str, nullable=False),
        "rating": Column(float, Check.between(0, 5), nullable=True),
        "in_stock": Column(bool, nullable=True),
    },
    strict=False  # Allow extra columns
)
