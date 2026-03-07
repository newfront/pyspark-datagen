"""SparkSession fixture for tests: delegates to package utils."""

from learning_spark_datagen.utils.spark_session import generate_spark_session

__all__ = ["generate_spark_session"]
