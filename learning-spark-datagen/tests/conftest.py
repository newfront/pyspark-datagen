"""Pytest configuration and shared fixtures."""

import pytest

from learning_spark_datagen.utils.spark_session import generate_spark_session


@pytest.fixture(scope="session")
def spark():
    """Session-scoped SparkSession with Delta Lake for converter tests."""
    return generate_spark_session(app_name="learning-spark-datagen-test")
