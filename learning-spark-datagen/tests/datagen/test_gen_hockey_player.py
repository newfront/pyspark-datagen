"""Tests for GenHockeyPlayer and NDJSON round-trip.

Covers:
- Determinism (same seed + index always yields the same player)
- Proto validation on every generated player
- Field constraints (jersey number 1-99, nationality 3 chars, non-empty names)
- Enum validity (position, status, handedness)
- generate / generate_range batch counts
- NDJSON write → read round-trip fidelity
"""

import tempfile
from pathlib import Path

import protovalidate
import pytest

from learning_spark_datagen.datagen import GenHockeyPlayer


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


def test_generate_one_deterministic():
    gen = GenHockeyPlayer(seed=42)
    p1 = gen.generate_one(0)
    p2 = gen.generate_one(0)
    protovalidate.validate(p1)
    protovalidate.validate(p2)
    assert p1.player_id == p2.player_id
    assert p1.first_name == p2.first_name
    assert p1.last_name == p2.last_name
    assert p1.team_abbreviation == p2.team_abbreviation


def test_different_seeds_differ():
    gen1 = GenHockeyPlayer(seed=1)
    gen2 = GenHockeyPlayer(seed=2)
    p1 = gen1.generate_one(0)
    p2 = gen2.generate_one(0)
    assert p1.player_id != p2.player_id


def test_different_indices_differ():
    gen = GenHockeyPlayer(seed=42)
    p0 = gen.generate_one(0)
    p1 = gen.generate_one(1)
    assert p0.player_id != p1.player_id


# ---------------------------------------------------------------------------
# Proto validation
# ---------------------------------------------------------------------------


def test_single_player_validates():
    gen = GenHockeyPlayer(seed=99)
    player = gen.generate_one(index=5)
    protovalidate.validate(player)


def test_batch_validates():
    gen = GenHockeyPlayer(seed=7)
    players = gen.generate(20)
    for p in players:
        protovalidate.validate(p)


# ---------------------------------------------------------------------------
# Field constraints
# ---------------------------------------------------------------------------


def test_jersey_number_in_range():
    gen = GenHockeyPlayer(seed=42)
    for i in range(50):
        p = gen.generate_one(i)
        assert 1 <= p.jersey_number <= 99, f"jersey_number={p.jersey_number} out of range"


def test_nationality_is_three_chars():
    gen = GenHockeyPlayer(seed=42)
    for i in range(50):
        p = gen.generate_one(i)
        assert len(p.nationality) == 3, f"nationality={p.nationality!r} is not 3 chars"


def test_names_non_empty():
    gen = GenHockeyPlayer(seed=42)
    for i in range(20):
        p = gen.generate_one(i)
        assert len(p.first_name) >= 1
        assert len(p.last_name) >= 1


def test_team_abbreviation_length():
    gen = GenHockeyPlayer(seed=42)
    for i in range(50):
        p = gen.generate_one(i)
        assert 2 <= len(p.team_abbreviation) <= 3


def test_enum_values_defined():
    """position, status, and shoots_catches must be non-UNSPECIFIED."""
    from hockey.v1 import player_pb2
    gen = GenHockeyPlayer(seed=42)
    for i in range(30):
        p = gen.generate_one(i)
        assert p.position != player_pb2.POSITION_UNSPECIFIED
        assert p.status != player_pb2.PLAYER_STATUS_UNSPECIFIED
        assert p.shoots_catches != player_pb2.HANDEDNESS_UNSPECIFIED


def test_created_at_set():
    gen = GenHockeyPlayer(seed=42)
    for i in range(10):
        p = gen.generate_one(i)
        assert p.created_at.seconds > 0


# ---------------------------------------------------------------------------
# Batch generation
# ---------------------------------------------------------------------------


def test_generate_count():
    gen = GenHockeyPlayer(seed=42)
    players = gen.generate(100)
    assert len(players) == 100


def test_generate_zero():
    gen = GenHockeyPlayer(seed=42)
    assert gen.generate(0) == []


def test_generate_range():
    gen = GenHockeyPlayer(seed=42)
    batch = gen.generate_range(10, 20)
    assert len(batch) == 10
    for p in batch:
        protovalidate.validate(p)


def test_generate_range_empty():
    gen = GenHockeyPlayer(seed=42)
    assert gen.generate_range(5, 5) == []


def test_generate_range_matches_generate_one():
    gen = GenHockeyPlayer(seed=42)
    batch = gen.generate_range(0, 5)
    for i, p in enumerate(batch):
        p_one = gen.generate_one(i)
        assert p.player_id == p_one.player_id


# ---------------------------------------------------------------------------
# NDJSON round-trip
# ---------------------------------------------------------------------------


def test_ndjson_round_trip():
    gen = GenHockeyPlayer(seed=42)
    players = gen.generate(10)
    with tempfile.NamedTemporaryFile(suffix=".ndjson", delete=False) as f:
        path = Path(f.name)
    try:
        GenHockeyPlayer.write_ndjson(path, players)
        loaded = GenHockeyPlayer.read_ndjson(path)
        assert len(loaded) == len(players)
        for orig, rt in zip(players, loaded):
            protovalidate.validate(rt)
            assert orig.player_id == rt.player_id
            assert orig.first_name == rt.first_name
            assert orig.last_name == rt.last_name
            assert orig.team_name == rt.team_name
            assert orig.position == rt.position
    finally:
        path.unlink(missing_ok=True)


def test_player_to_dict_has_expected_keys():
    gen = GenHockeyPlayer(seed=42)
    p = gen.generate_one(0)
    d = GenHockeyPlayer.player_to_dict(p)
    assert "player_id" in d
    assert "first_name" in d
    assert "last_name" in d
    assert "team_name" in d
    assert "team_abbreviation" in d
    assert "position" in d
