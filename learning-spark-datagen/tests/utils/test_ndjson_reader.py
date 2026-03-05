"""Tests for NDJSON reader utilities (static JSON -> protobuf -> Delta)."""

from pathlib import Path

import pytest

from learning_spark_datagen.utils.ndjson_reader import (
    ndjson_file_to_dataframe,
    ndjson_file_to_delta,
    ndjson_to_protobuf_bytes,
)

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_DESCRIPTOR_PATH = _PROJECT_ROOT / "gen" / "descriptors" / "descriptor.bin"
_RESOURCES = _PROJECT_ROOT / "tests" / "resources"
_DELTA_BASE = _RESOURCES / "delta"


def test_ndjson_to_protobuf_bytes_users():
    """Reading users.ndjson with message_name user.v1.User returns serialized User bytes."""
    users_path = _RESOURCES / "users.ndjson"
    assert users_path.exists()

    data = ndjson_to_protobuf_bytes(users_path, "user.v1.User")
    assert len(data) == 3  # 3 lines in static users.ndjson
    assert all(isinstance(b, bytes) and len(b) > 0 for b in data)


def test_ndjson_to_protobuf_bytes_orders():
    """Reading orders.ndjson with message_name order.v1.Order returns serialized Order bytes."""
    orders_path = _RESOURCES / "orders.ndjson"
    assert orders_path.exists()

    data = ndjson_to_protobuf_bytes(orders_path, "order.v1.Order")
    assert len(data) >= 1
    assert all(isinstance(b, bytes) and len(b) > 0 for b in data)


def test_ndjson_to_protobuf_bytes_unknown_message_raises():
    """Unknown message_name raises ValueError with supported list."""
    with pytest.raises(ValueError, match="Unknown message_name.*Supported:"):
        ndjson_to_protobuf_bytes(_RESOURCES / "users.ndjson", "unknown.v1.Message")


def test_ndjson_file_to_dataframe_users(spark):
    """NDJSON file can be converted to DataFrame and matches GenUser.read_ndjson + protobuf_to_df."""
    from learning_spark_datagen.datagen import GenUser
    from learning_spark_datagen.utils import Converters

    users_path = _RESOURCES / "users.ndjson"
    assert users_path.exists()
    assert _DESCRIPTOR_PATH.exists()

    # Our new API
    df = ndjson_file_to_dataframe(
        users_path, "user.v1.User", spark, _DESCRIPTOR_PATH
    )
    assert df.count() == 3
    assert "uuid" in df.columns
    assert "first_name" in df.columns

    # Same as legacy path: read_ndjson -> serialize -> protobuf_to_df
    users = GenUser.read_ndjson(users_path)
    legacy_data = [u.SerializeToString() for u in users]
    df_legacy = Converters.protobuf_to_df(
        legacy_data, spark, _DESCRIPTOR_PATH, "user.v1.User"
    )
    assert df_legacy.count() == df.count()
    uuids_new = {row["uuid"] for row in df.collect()}
    uuids_legacy = {row["uuid"] for row in df_legacy.collect()}
    assert uuids_new == uuids_legacy


def test_ndjson_file_to_delta_users(spark):
    """ndjson_file_to_delta writes a Delta table that can be read back."""
    users_path = _RESOURCES / "users.ndjson"
    delta_path = _DELTA_BASE / "users_from_ndjson"
    assert users_path.exists()
    assert _DESCRIPTOR_PATH.exists()

    delta_path.parent.mkdir(parents=True, exist_ok=True)
    ndjson_file_to_delta(
        users_path,
        "user.v1.User",
        spark,
        _DESCRIPTOR_PATH,
        delta_path,
        coalesce=1,
        mode="overwrite",
    )

    df = spark.read.format("delta").load(str(delta_path))
    assert df.count() == 3
    assert "uuid" in df.columns
