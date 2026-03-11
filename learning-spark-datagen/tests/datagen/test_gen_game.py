"""Tests for GenGame and NDJSON round-trip."""

import os
import tempfile
from pathlib import Path

import protovalidate
from learning_spark_datagen.datagen import GenGame


def test_generate_one_deterministic():
    gen = GenGame(seed=42)
    g1 = gen.generate_one(index=0)
    g2 = gen.generate_one(index=0)
    protovalidate.validate(g1)
    protovalidate.validate(g2)
    assert g1.uuid == g2.uuid
    assert g1.name == g2.name
    assert g1.genre == g2.genre
    assert g1.released_at == g2.released_at


def test_generate_one_has_required_fields():
    gen = GenGame(seed=42)
    game = gen.generate_one(index=0)
    protovalidate.validate(game)
    assert game.uuid
    assert len(game.name) >= 1
    assert game.released_at > 0


def test_generate_range():
    gen = GenGame(seed=42)
    games = gen.generate_range(0, 10)
    assert len(games) == 10
    for g in games:
        protovalidate.validate(g)
    uuids = {g.uuid for g in games}
    assert len(uuids) == 10


def test_generate_count():
    gen = GenGame(seed=42)
    games = gen.generate(10)
    assert len(games) == 10
    for g in games:
        protovalidate.validate(g)


def test_ndjson_round_trip():
    gen = GenGame(seed=42)
    games = gen.generate(5)
    fd, name = tempfile.mkstemp(suffix=".ndjson")
    os.close(fd)
    path = Path(name)
    try:
        GenGame.write_ndjson(path, games)
        read_back = GenGame.read_ndjson(path)
        assert len(read_back) == 5
        for a, b in zip(games, read_back):
            protovalidate.validate(a)
            protovalidate.validate(b)
            assert a.uuid == b.uuid
            assert a.name == b.name
            assert a.released_at == b.released_at
    finally:
        path.unlink(missing_ok=True)
