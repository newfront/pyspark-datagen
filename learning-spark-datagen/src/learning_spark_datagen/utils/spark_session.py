"""SparkSession builder for local and CLI runs (Delta Lake)."""

from pyspark.sql import SparkSession


def generate_spark_session(app_name: str = "learning-spark-datagen") -> SparkSession:
    """Build a local SparkSession with Delta Lake for CLI and local runs.

    Uses local[*] and Delta 4.x. For production or notebooks, use
    SparkSession.builder.getOrCreate() with your cluster config.
    """
    return (
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
        .getOrCreate()
    )
