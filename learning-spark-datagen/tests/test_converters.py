"""Tests for Converters.protobuf_to_df and Delta write using static test resources."""

from pathlib import Path

from learning_spark_datagen.datagen import GenOrder, GenUser
from learning_spark_datagen.utils import Converters


# Project root when running pytest from learning-spark-datagen/ (pyproject pythonpath = src, gen/python)
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_DESCRIPTOR_PATH = _PROJECT_ROOT / "gen" / "descriptors" / "descriptor.bin"
_RESOURCES = _PROJECT_ROOT / "tests" / "resources"
_DELTA_BASE = _RESOURCES / "delta"


def test_protobuf_to_df_users(spark):
    """Convert static users from test resources to DataFrame and write to Delta."""
    users_path = _RESOURCES / "users.ndjson"
    assert users_path.exists(), "Run from project root; static users.ndjson must exist"
    assert _DESCRIPTOR_PATH.exists(), "Run 'make descriptor' to create gen/descriptors/descriptor.bin"

    users = GenUser.read_ndjson(users_path)
    data = [u.SerializeToString() for u in users]

    df = Converters.protobuf_to_df(
        data=data,
        spark=spark,
        descriptor_path=_DESCRIPTOR_PATH,
        message_name="user.v1.User",
        column_name="wrapper",
    )

    assert df.count() == len(users)
    # Check schema has expected user fields
    assert "uuid" in df.columns
    assert "first_name" in df.columns
    assert "email_address" in df.columns
    row = df.first()
    assert row is not None
    assert row["uuid"] in {u.uuid for u in users}

    # Write to Delta for downstream use
    delta_path = _DELTA_BASE / "users"
    delta_path.parent.mkdir(parents=True, exist_ok=True)
    df.write.format("delta").mode("overwrite").save(str(delta_path))


def test_protobuf_to_df_orders(spark):
    """Convert static orders from test resources to DataFrame and write to Delta."""
    orders_path = _RESOURCES / "orders.ndjson"
    assert orders_path.exists(), "Run from project root; static orders.ndjson must exist"
    assert _DESCRIPTOR_PATH.exists(), "Run 'make descriptor' to create gen/descriptors/descriptor.bin"

    orders = GenOrder.read_ndjson(orders_path)
    data = [o.SerializeToString() for o in orders]

    df = Converters.protobuf_to_df(
        data=data,
        spark=spark,
        descriptor_path=_DESCRIPTOR_PATH,
        message_name="order.v1.Order",
        column_name="wrapper",
    )

    assert df.count() == len(orders)
    assert "created_at" in df.columns
    assert "user_id" in df.columns
    assert "products" in df.columns
    assert "total" in df.columns
    row = df.first()
    assert row is not None
    assert row["user_id"] in {o.user_id for o in orders}

    # Write to Delta for downstream use
    delta_path = _DELTA_BASE / "orders"
    delta_path.parent.mkdir(parents=True, exist_ok=True)
    df.write.format("delta").mode("overwrite").save(str(delta_path))
