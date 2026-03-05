"""Pytest configuration and shared fixtures."""

import pytest

from tests.spark_session import generate_spark_session


@pytest.fixture(scope="session")
def spark():
    """Session-scoped SparkSession with Delta Lake for converter tests."""
    return generate_spark_session()
