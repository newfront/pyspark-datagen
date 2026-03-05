"""Tests for GenOrder and NDJSON round-trip."""

import os
import tempfile
from pathlib import Path

import protovalidate
from learning_spark_datagen.datagen import GenOrder


def test_generate_one_deterministic():
    gen = GenOrder(seed=42)
    o1 = gen.generate_one(index=0)
    o2 = gen.generate_one(index=0)
    protovalidate.validate(o1)
    protovalidate.validate(o2)
    assert o1.created_at == o2.created_at
    assert o1.user_id == o2.user_id
    assert len(o1.products) == len(o2.products)
    for p1, p2 in zip(o1.products, o2.products):
        assert p1.product_id == p2.product_id
        assert p1.num_items == p2.num_items
        assert p1.percent_discount == p2.percent_discount


def test_generate_one_has_products():
    gen = GenOrder(seed=42)
    order = gen.generate_one(index=0)
    protovalidate.validate(order)
    assert len(order.products) >= 1
    for p in order.products:
        assert p.product_id
        assert p.num_items >= 1
        assert p.unit_cost.currency == "USD"
        assert p.unit_cost.units >= 0


def test_generate_one_total_matches_products():
    gen = GenOrder(seed=42)
    order = gen.generate_one(index=0)
    protovalidate.validate(order)
    total_nanos = 0
    for p in order.products:
        unit_nanos = p.unit_cost.units * 1_000_000_000 + p.unit_cost.nanos
        line_nanos = int(p.num_items * unit_nanos * (100 - p.percent_discount) / 100)
        total_nanos += line_nanos
    expected_units = total_nanos // 1_000_000_000
    expected_nanos = int(total_nanos % 1_000_000_000)
    assert order.total.units == expected_units
    assert order.total.nanos == expected_nanos


def test_generate_count():
    gen = GenOrder(seed=42)
    orders = gen.generate(10)
    assert len(orders) == 10
    for order in orders:
        protovalidate.validate(order)


def test_generate_with_user_ids():
    # user_id must be valid UUID (order.proto string.uuid validation).
    user_ids = [
        "550e8400-e29b-41d4-a716-446655440001",
        "550e8400-e29b-41d4-a716-446655440002",
        "550e8400-e29b-41d4-a716-446655440003",
    ]
    gen = GenOrder(seed=42, user_ids=user_ids)
    orders = gen.generate(9)
    seen = []
    for i, o in enumerate(orders):
        protovalidate.validate(o)
        expected = user_ids[i % len(user_ids)]
        assert o.user_id == expected
        seen.append(o.user_id)
    assert user_ids[0] in seen and user_ids[1] in seen and user_ids[2] in seen


def test_ndjson_round_trip():
    gen = GenOrder(seed=42)
    orders = gen.generate(5)
    fd, name = tempfile.mkstemp(suffix=".ndjson")
    os.close(fd)
    path = Path(name)
    try:
        GenOrder.write_ndjson(path, orders)
        read_back = GenOrder.read_ndjson(path)
        assert len(read_back) == 5
        for a, b in zip(orders, read_back):
            protovalidate.validate(a)
            protovalidate.validate(b)
            assert a.created_at == b.created_at
            assert a.user_id == b.user_id
            assert len(a.products) == len(b.products)
            for pa, pb in zip(a.products, b.products):
                assert pa.product_id == pb.product_id
                assert pa.num_items == pb.num_items
    finally:
        path.unlink(missing_ok=True)
