"""Tests for GenProduct (Tidewell athleisure catalog) and NDJSON round-trip."""

import os
import tempfile
from pathlib import Path

import protovalidate
from learning_spark_datagen.datagen import GenProduct


def test_generate_one_deterministic():
    gen = GenProduct(seed=42)
    p1 = gen.generate_one(index=0)
    p2 = gen.generate_one(index=0)
    protovalidate.validate(p1)
    protovalidate.validate(p2)
    assert p1.product_id == p2.product_id
    assert p1.sku == p2.sku
    assert p1.name == p2.name
    assert p1.category == p2.category
    assert p1.variant == p2.variant
    assert p1.size == p2.size
    assert p1.price.units == p2.price.units
    assert p1.price.nanos == p2.price.nanos


def test_generate_count_validates():
    gen = GenProduct(seed=42)
    products = gen.generate(50)
    assert len(products) == 50
    for p in products:
        protovalidate.validate(p)
        assert p.brand == "Tidewell"
        assert p.price.currency == "USD"
        assert 0 <= p.percent_discount <= 100


def test_unique_product_ids_and_skus():
    gen = GenProduct(seed=42)
    products = gen.generate(300)
    ids = {p.product_id for p in products}
    skus = {p.sku for p in products}
    assert len(ids) == 300, "product_id must be unique across the catalog"
    assert len(skus) == 300, "SKU must be unique across the catalog"


def test_price_within_category_band():
    """Prices land in the per-category band defined in gen_product._PRICE_BANDS."""
    from learning_spark_datagen.datagen.gen_product import _PRICE_BANDS

    gen = GenProduct(seed=42)
    products = gen.generate(200)
    for p in products:
        low, high = _PRICE_BANDS[p.category]
        assert low <= p.price.units <= high, (
            f"price {p.price.units} for {p.category} should be in [{low}, {high}]"
        )


def test_generate_range_matches_generate():
    gen = GenProduct(seed=7)
    a = gen.generate_range(10, 25)
    b = gen.generate(25)[10:25]
    assert len(a) == len(b) == 15
    for x, y in zip(a, b):
        assert x.product_id == y.product_id
        assert x.sku == y.sku


def test_brand_override():
    gen = GenProduct(seed=42, brand="Sundial")
    p = gen.generate_one(0)
    protovalidate.validate(p)
    assert p.brand == "Sundial"


def test_ndjson_round_trip():
    gen = GenProduct(seed=42)
    products = gen.generate(10)
    fd, name = tempfile.mkstemp(suffix=".ndjson")
    os.close(fd)
    path = Path(name)
    try:
        GenProduct.write_ndjson(path, products)
        read_back = GenProduct.read_ndjson(path)
        assert len(read_back) == 10
        for a, b in zip(products, read_back):
            protovalidate.validate(a)
            protovalidate.validate(b)
            assert a.product_id == b.product_id
            assert a.sku == b.sku
            assert a.name == b.name
            assert a.price.units == b.price.units
            assert a.price.nanos == b.price.nanos
            assert a.created.seconds == b.created.seconds
    finally:
        path.unlink(missing_ok=True)
