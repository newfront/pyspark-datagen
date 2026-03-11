"""Tests for GenLeaderboardEntry and NDJSON round-trip."""

import os
import tempfile
from pathlib import Path

import protovalidate
from learning_spark_datagen.datagen import GenLeaderboardEntry


def test_generate_one_deterministic():
    gen = GenLeaderboardEntry(seed=42)
    e1 = gen.generate_one(index=0)
    e2 = gen.generate_one(index=0)
    protovalidate.validate(e1)
    protovalidate.validate(e2)
    assert e1.user_id == e2.user_id
    assert e1.game_id == e2.game_id
    assert e1.score == e2.score
    assert e1.rank == e2.rank
    assert e1.played_at == e2.played_at


def test_generate_one_has_required_fields():
    gen = GenLeaderboardEntry(seed=42)
    entry = gen.generate_one(index=0)
    protovalidate.validate(entry)
    assert entry.user_id
    assert entry.game_id
    assert entry.rank >= 1
    assert entry.played_at > 0


def test_generate_range():
    gen = GenLeaderboardEntry(seed=42)
    entries = gen.generate_range(0, 10)
    assert len(entries) == 10
    for e in entries:
        protovalidate.validate(e)


def test_generate_with_user_ids_and_game_ids():
    user_ids = [
        "550e8400-e29b-41d4-a716-446655440001",
        "550e8400-e29b-41d4-a716-446655440002",
    ]
    game_ids = [
        "660e8400-e29b-41d4-a716-446655440001",
        "660e8400-e29b-41d4-a716-446655440002",
        "660e8400-e29b-41d4-a716-446655440003",
    ]
    gen = GenLeaderboardEntry(seed=42, user_ids=user_ids, game_ids=game_ids)
    entries = gen.generate(6)
    for i, e in enumerate(entries):
        protovalidate.validate(e)
        assert e.user_id == user_ids[i % len(user_ids)]
        assert e.game_id == game_ids[i % len(game_ids)]


def test_ndjson_round_trip():
    gen = GenLeaderboardEntry(seed=42)
    entries = gen.generate(5)
    fd, name = tempfile.mkstemp(suffix=".ndjson")
    os.close(fd)
    path = Path(name)
    try:
        GenLeaderboardEntry.write_ndjson(path, entries)
        read_back = GenLeaderboardEntry.read_ndjson(path)
        assert len(read_back) == 5
        for a, b in zip(entries, read_back):
            protovalidate.validate(a)
            protovalidate.validate(b)
            assert a.user_id == b.user_id
            assert a.game_id == b.game_id
            assert a.score == b.score
            assert a.rank == b.rank
            assert a.played_at == b.played_at
    finally:
        path.unlink(missing_ok=True)
