"""Test SparkSession — delegates to the shared utility with a test app name."""

from learning_spark_datagen.utils.spark_session import generate_spark_session as _gen


def generate_spark_session():
    return _gen(app_name="learning-spark-datagen-test")
