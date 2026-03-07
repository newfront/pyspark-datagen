"""Read NDJSON files and convert to protobuf messages or Delta tables."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from google.protobuf import json_format
from pyspark.sql import DataFrame, SparkSession

from learning_spark_datagen.utils.converters import Converters

# Lazy registry: message_name -> message class. Populated on first use so gen/python
# can be on sys.path (e.g. when run from project root or pytest with pythonpath).
_MESSAGE_REGISTRY: dict[str, type[Any]] = {}


def _get_message_class(message_name: str) -> type[Any]:
    """Resolve a fully qualified message name to its protobuf Message class."""
    if not _MESSAGE_REGISTRY:
        try:
            from user.v1 import user_pb2  # noqa: F401

            _MESSAGE_REGISTRY["user.v1.User"] = user_pb2.User
        except ImportError:
            pass
        try:
            from order.v1 import order_pb2  # noqa: F401

            _MESSAGE_REGISTRY["order.v1.Order"] = order_pb2.Order
        except ImportError:
            pass
    if message_name not in _MESSAGE_REGISTRY:
        raise ValueError(
            f"Unknown message_name {message_name!r}. "
            f"Supported: {list(_MESSAGE_REGISTRY.keys())}"
        )
    return _MESSAGE_REGISTRY[message_name]


def ndjson_to_protobuf_bytes(path: Path | str, message_name: str) -> list[bytes]:
    """Read an NDJSON file and convert each line to a protobuf message, then serialize to bytes.

    Uses the Google protobuf JSON utility to parse each line into the message type
    identified by message_name (e.g. "user.v1.User", "order.v1.Order"). The result
    can be passed to Converters.protobuf_to_df for writing to Delta.

    Args:
        path: Path to the NDJSON file (one JSON object per line).
        message_name: Fully qualified protobuf message name (e.g. "user.v1.User").

    Returns:
        List of serialized protobuf bytes, one per line.

    Raises:
        ValueError: If message_name is not in the registry of known message types.
    """
    path = Path(path)
    message_class = _get_message_class(message_name)
    result: list[bytes] = []
    with path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            msg = message_class()
            json_format.ParseDict(obj, msg)
            result.append(msg.SerializeToString())
    return result


def ndjson_file_to_dataframe(
    path: Path | str,
    message_name: str,
    spark: SparkSession,
    descriptor_path: Path | str,
    column_name: str = "wrapper",
) -> DataFrame:
    """Read an NDJSON file and convert it to a Spark DataFrame (e.g. for Delta write).

    Equivalent to: ndjson_to_protobuf_bytes(path, message_name) then
    Converters.protobuf_to_df(...).

    Args:
        path: Path to the NDJSON file.
        message_name: Fully qualified protobuf message name (e.g. "user.v1.User").
        spark: Spark session (with Delta/Protobuf support).
        descriptor_path: Path to the binary FileDescriptorSet (e.g. gen/descriptors/descriptor.bin).
        column_name: Temporary binary column name for protobuf decoding; default "wrapper".

    Returns:
        DataFrame with one row per NDJSON record and columns from the protobuf schema.
    """
    data = ndjson_to_protobuf_bytes(path, message_name)
    return Converters.protobuf_to_df(
        data=data,
        spark=spark,
        descriptor_path=descriptor_path,
        message_name=message_name,
        column_name=column_name,
    )


def ndjson_file_to_delta(
    ndjson_path: Path | str,
    message_name: str,
    spark: SparkSession,
    descriptor_path: Path | str,
    delta_path: Path | str,
    *,
    coalesce: int = 1,
    mode: str = "overwrite",
) -> None:
    """Read an NDJSON file and write it as a Delta table.

    Converts each line to the given protobuf message type, builds a DataFrame,
    and writes it to the specified Delta table path. Use this to turn static
    NDJSON (e.g. users.ndjson, orders.ndjson) into Delta tables.

    Args:
        ndjson_path: Path to the NDJSON file.
        message_name: Fully qualified protobuf message name (e.g. "user.v1.User").
        spark: Spark session (with Delta/Protobuf support).
        descriptor_path: Path to the binary FileDescriptorSet.
        delta_path: Output path for the Delta table (local or cloud).
        coalesce: Number of output files; default 1 for small datasets.
        mode: Write mode for Delta; default "overwrite".
    """
    df = ndjson_file_to_dataframe(
        ndjson_path, message_name, spark, descriptor_path
    )
    out = str(Path(delta_path).resolve())
    df.coalesce(coalesce).write.format("delta").mode(mode).save(out)
