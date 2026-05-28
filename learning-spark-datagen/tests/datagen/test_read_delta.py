"""Delta round-trip tests for GenUser.read_delta and GenProduct.read_delta.

Each test:
  1. Generates a small batch via the existing generator.
  2. Writes it to Delta via `Converters.protobuf_to_df` + `write_df_to_delta`.
  3. Reads it back via the new `read_delta` static method.
  4. Asserts the round-trip preserves identity for the fields exercised at
     runtime by `--users-table` / `--products-table` (uuid + key columns).

These tests share the session-scoped `spark` fixture from `tests/conftest.py`.
"""

import shutil
import tempfile
from pathlib import Path

import protovalidate
from learning_spark_datagen.datagen import GenProduct, GenUser
from learning_spark_datagen.utils import Converters

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_DESCRIPTOR_PATH = _PROJECT_ROOT / "gen" / "descriptors" / "descriptor.bin"


def _write_delta(spark, records, message_name: str, tmp_dir: Path) -> Path:
    """Serialize `records` to a Delta table under `tmp_dir` and return the path."""
    data = [r.SerializeToString() for r in records]
    df = Converters.protobuf_to_df(
        data=data,
        spark=spark,
        descriptor_path=_DESCRIPTOR_PATH,
        message_name=message_name,
    )
    delta_path = tmp_dir / "delta_table"
    Converters.write_df_to_delta(df, delta_path, mode="overwrite")
    return delta_path


def test_gen_user_read_delta_round_trip(spark):
    """Write 10 users to Delta and read them back via GenUser.read_delta."""
    assert _DESCRIPTOR_PATH.exists(), (
        "Run 'make descriptor' to create gen/descriptors/descriptor.bin"
    )
    users_in = GenUser(seed=42).generate(10)
    for u in users_in:
        protovalidate.validate(u)

    tmp = Path(tempfile.mkdtemp(prefix="ut_users_delta_"))
    try:
        delta_path = _write_delta(spark, users_in, "user.v1.User", tmp)
        users_out = GenUser.read_delta(spark, delta_path)
        assert len(users_out) == len(users_in)
        # Round-trip preserves UUIDs (the field --users-table consumes downstream).
        assert {u.uuid for u in users_out} == {u.uuid for u in users_in}
        # And other key fields per-row (matched on uuid since Delta does not preserve order).
        by_uuid_in = {u.uuid: u for u in users_in}
        for u in users_out:
            protovalidate.validate(u)
            ref = by_uuid_in[u.uuid]
            assert u.first_name == ref.first_name
            assert u.last_name == ref.last_name
            assert u.email_address == ref.email_address
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def test_gen_product_read_delta_round_trip(spark):
    """Write 12 products to Delta and read them back via GenProduct.read_delta."""
    assert _DESCRIPTOR_PATH.exists(), (
        "Run 'make descriptor' to create gen/descriptors/descriptor.bin"
    )
    products_in = GenProduct(seed=7).generate(12)
    for p in products_in:
        protovalidate.validate(p)

    tmp = Path(tempfile.mkdtemp(prefix="ut_products_delta_"))
    try:
        delta_path = _write_delta(spark, products_in, "product.v1.Product", tmp)
        products_out = GenProduct.read_delta(spark, delta_path)
        assert len(products_out) == len(products_in)
        by_id_in = {p.product_id: p for p in products_in}
        for p in products_out:
            protovalidate.validate(p)
            ref = by_id_in[p.product_id]
            assert p.sku == ref.sku
            assert p.name == ref.name
            assert p.brand == ref.brand
            assert p.category == ref.category
            assert p.price.currency == ref.price.currency
            assert p.price.units == ref.price.units
            assert p.price.nanos == ref.price.nanos
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
