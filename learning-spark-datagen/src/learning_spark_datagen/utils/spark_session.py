"""SparkSession builder for local and CLI runs (Delta Lake)."""

import os
import sys

from pyspark.sql import SparkSession


def generate_spark_session(app_name: str = "learning-spark-datagen") -> SparkSession:
    """Build a local SparkSession with Delta Lake for CLI and local runs.

    Uses local[*] and Delta 4.x. For production or notebooks, use
    SparkSession.builder.getOrCreate() with your cluster config.

    If the ``MAVEN_PROXY`` environment variable is set, its value is applied as
    ``spark.jars.repositories`` so Ivy resolves packages through that proxy.
    When the variable is absent the config key is omitted entirely.
    """
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    builder = (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.13:4.1.0,org.apache.spark:spark-protobuf_2.13:4.1.1",
        )
        .config(
            "spark.driver.extraJavaOptions",
            "-Divy.cache.dir=/tmp -Divy.home=/tmp -Dio.netty.tryReflectionSetAccessible=true",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.session.timeZone", "UTC")
    )

    maven_proxy = os.environ.get("MAVEN_PROXY")
    if maven_proxy:
        builder = builder.config("spark.jars.repositories", maven_proxy)

    return builder.getOrCreate()
