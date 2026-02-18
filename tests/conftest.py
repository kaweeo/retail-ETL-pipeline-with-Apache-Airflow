"""
Pytest configuration and fixtures for ETL tests.

This file is automatically discovered by pytest and provides
shared fixtures and configuration for all test modules.
"""

import pytest
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))


@pytest.fixture(scope="session")
def test_output_dir(tmp_path_factory):
    """
    Create a temporary directory for test outputs.
    Useful for saving test data or logs.
    """
    return tmp_path_factory.mktemp("test_output")


@pytest.fixture
def sample_config():
    """
    Provide sample configuration for tests that need AWS/Snowflake settings.
    """
    return {
        "aws_conn_id": "test_aws_conn",
        "s3_bucket": "test-bucket",
        "snowflake_conn_id": "test_snowflake_conn",
    }


# Add pytest CLI options
def pytest_addoption(parser):
    """Add custom pytest options."""
    parser.addoption(
        "--integration",
        action="store_true",
        default=False,
        help="Run integration tests (requires S3/Snowflake credentials)"
    )


def pytest_collection_modifyitems(config, items):
    """Mark integration tests for conditional execution."""
    if config.getoption("--integration"):
        # Run all tests
        return
    
    # Skip integration tests if flag not provided
    skip_integration = pytest.mark.skip(reason="need --integration option to run")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip_integration)
