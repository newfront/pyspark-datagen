"""SparkSession builder for local and CLI runs (Delta Lake)."""

import os

from pyspark.sql import SparkSession


def generate_spark_session(app_name: str = "learning-spark-datagen") -> SparkSession:
    """Build a local SparkSession with Delta Lake for CLI and local runs.

    Uses local[*] and Delta 4.x. For production or notebooks, use
    SparkSession.builder.getOrCreate() with your cluster config.

    Maven proxy: if the ``MAVEN_PROXY`` environment variable is set (e.g. an
    internal mirror URL), its value is passed to Spark via
    ``spark.jars.repositories`` so that ``--packages`` / ``spark.jars.packages``
    resolution goes through that proxy. The env var is the only source — no
    vendor URLs are hardcoded in this repo. The setting must be applied at
    SparkSession builder time; ``spark.conf.set(...)`` after the session is
    started is too late because package resolution happens during launch.
    """
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
