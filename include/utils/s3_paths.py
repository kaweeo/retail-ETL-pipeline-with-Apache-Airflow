"""
S3 path helpers.

We keep S3 key construction in one place to avoid subtle bugs caused by missing
or double slashes across the DAG and operators.
"""


def _ensure_trailing_slash(prefix: str) -> str:
    if not prefix:
        return ""
    return prefix if prefix.endswith("/") else f"{prefix}/"


def _strip_leading_slash(path: str) -> str:
    return path.lstrip("/") if path else ""


def build_raw_s3_key(raw_folder: str, relative_key: str) -> str:
    """
    Build a full S3 key for raw/source files.

    Example:
        build_raw_s3_key("retail-data", "sales_data.csv")
        -> "retail-data/sales_data.csv"
    """

    return f"{_ensure_trailing_slash(raw_folder)}{_strip_leading_slash(relative_key)}"


def build_cleansed_s3_key(cleansed_folder: str, relative_key: str) -> str:
    """
    Build a full S3 key for processed/cleansed files.

    Example:
        build_cleansed_s3_key("cleansed-data/", "sales_clean.csv")
        -> "cleansed-data/sales_clean.csv"
    """

    return f"{_ensure_trailing_slash(cleansed_folder)}{_strip_leading_slash(relative_key)}"

