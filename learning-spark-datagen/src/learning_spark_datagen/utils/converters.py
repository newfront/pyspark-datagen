"""Convert protobuf binary data to Apache Spark DataFrames (e.g. for Delta Lake)."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.protobuf.functions import from_protobuf
from pyspark.sql.types import BinaryType, StructField, StructType


def _read_descriptor_bytes(path: Path | str) -> bytes:
    """Read a binary descriptor file into memory."""
    return Path(path).read_bytes()


class Converters:
    """Convert generated protobuf messages to Spark DataFrames for ingestion (e.g. Delta Lake)."""

    @staticmethod
    def protobuf_to_df(
        data: list[bytes],
        spark: SparkSession,
        descriptor_path: Path | str,
        message_name: str,
        column_name: str = "wrapper",
    ) -> DataFrame:
        """Build a DataFrame from a list of serialized protobuf bytes.

        The descriptor is loaded from the given path (e.g. from `make descriptor`).
        The resulting DataFrame has one row per message with columns from the
        protobuf schema (nested structs preserved).

        Args:
            data: List of serialized protobuf bytes (e.g. [msg.SerializeToString() for msg in messages]).
            spark: Spark session (e.g. from App.generate_spark_session() or builder).
            descriptor_path: Path to the binary FileDescriptorSet (e.g. gen/descriptors/descriptor.bin).
            message_name: Fully qualified protobuf message name (e.g. "user.v1.User", "order.v1.Order").
            column_name: Name of the temporary binary column; default "wrapper" since we expand to .*.

        Returns:
            DataFrame with one row per record and columns matching the protobuf message fields.
        """
        if not data:
            schema = StructType([StructField(column_name, BinaryType(), True)])
            return spark.createDataFrame([], schema)

        descriptor_bytes = _read_descriptor_bytes(descriptor_path)
        schema = StructType([StructField(column_name, BinaryType(), True)])
        df = spark.createDataFrame([(bytes(b),) for b in data], schema)

        return df.withColumn(
            column_name,
            from_protobuf(
                col(column_name),
                messageName=message_name,
                options={"mode": "FAILFAST"},
                binaryDescriptorSet=descriptor_bytes,
            ),
        ).selectExpr(f"{column_name}.*")
