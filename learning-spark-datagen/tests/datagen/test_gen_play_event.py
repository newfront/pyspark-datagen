"""Tests for GenPlayEvent and NDJSON round-trip.

Covers:
- Determinism (same seed + index always yields the same event)
- Proto validation on every generated event
- Field constraints (period 1-4, game_clock format, non-empty strings)
- Enum validity (event_type, strength_state)
- player_id linking (from supplied player_ids list)
- secondary_player_id presence only for applicable event types
- generate / generate_range batch counts
- NDJSON write → read round-trip fidelity
"""

import tempfile
from pathlib import Path

import protovalidate
import pytest

from learning_spark_datagen.datagen import GenPlayEvent

# Valid UUIDs to supply as player_ids.
_PLAYER_IDS = [
    "550e8400-e29b-41d4-a716-446655440001",
    "550e8400-e29b-41d4-a716-446655440002",
    "550e8400-e29b-41d4-a716-446655440003",
    "550e8400-e29b-41d4-a716-446655440004",
    "550e8400-e29b-41d4-a716-446655440005",
]


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


def test_generate_one_deterministic():
    gen = GenPlayEvent(seed=42)
    e1 = gen.generate_one(0)
    e2 = gen.generate_one(0)
    protovalidate.validate(e1)
    protovalidate.validate(e2)
    assert e1.event_id == e2.event_id
    assert e1.game_id == e2.game_id
    assert e1.player_id == e2.player_id
    assert e1.event_type == e2.event_type


def test_different_seeds_differ():
    e1 = GenPlayEvent(seed=1).generate_one(0)
    e2 = GenPlayEvent(seed=2).generate_one(0)
    assert e1.event_id != e2.event_id


def test_different_indices_differ():
    gen = GenPlayEvent(seed=42)
    e0 = gen.generate_one(0)
    e1 = gen.generate_one(1)
    assert e0.event_id != e1.event_id


# ---------------------------------------------------------------------------
# Proto validation
# ---------------------------------------------------------------------------


def test_single_event_validates():
    gen = GenPlayEvent(seed=99)
    event = gen.generate_one(index=3)
    protovalidate.validate(event)


def test_batch_validates():
    gen = GenPlayEvent(seed=7)
    for ev in gen.generate(30):
        protovalidate.validate(ev)


def test_with_player_ids_validates():
    gen = GenPlayEvent(seed=42, player_ids=_PLAYER_IDS)
    for ev in gen.generate(20):
        protovalidate.validate(ev)


# ---------------------------------------------------------------------------
# Field constraints
# ---------------------------------------------------------------------------


def test_period_in_range():
    gen = GenPlayEvent(seed=42)
    for i in range(100):
        ev = gen.generate_one(i)
        assert 1 <= ev.period <= 4, f"period={ev.period} out of range"


def test_game_clock_format():
    """game_clock must be mm:ss with valid minute/second ranges."""
    gen = GenPlayEvent(seed=42)
    for i in range(50):
        ev = gen.generate_one(i)
        parts = ev.game_clock.split(":")
        assert len(parts) == 2, f"game_clock={ev.game_clock!r} has wrong format"
        mm, ss = int(parts[0]), int(parts[1])
        assert 0 <= mm <= 19, f"minutes={mm} out of range"
        assert 0 <= ss <= 59, f"seconds={ss} out of range"


def test_season_non_empty():
    gen = GenPlayEvent(seed=42)
    for i in range(10):
        ev = gen.generate_one(i)
        assert len(ev.season) >= 1


def test_team_abbreviations_non_empty():
    gen = GenPlayEvent(seed=42)
    for i in range(20):
        ev = gen.generate_one(i)
        assert len(ev.home_team) >= 1
        assert len(ev.away_team) >= 1
        assert ev.home_team != ev.away_team, "home and away team must differ"


def test_enum_values_defined():
    from hockey.v1 import play_event_pb2
    gen = GenPlayEvent(seed=42)
    for i in range(50):
        ev = gen.generate_one(i)
        assert ev.event_type != play_event_pb2.EVENT_TYPE_UNSPECIFIED
        assert ev.strength_state != play_event_pb2.STRENGTH_STATE_UNSPECIFIED


def test_custom_season():
    gen = GenPlayEvent(seed=42, season="2023-24")
    for i in range(5):
        ev = gen.generate_one(i)
        assert ev.season == "2023-24"


# ---------------------------------------------------------------------------
# player_id linking
# ---------------------------------------------------------------------------


def test_player_ids_link_to_pool():
    gen = GenPlayEvent(seed=42, player_ids=_PLAYER_IDS)
    for i in range(20):
        ev = gen.generate_one(i)
        assert ev.player_id in _PLAYER_IDS, f"player_id={ev.player_id!r} not in supplied pool"


def test_all_supplied_player_ids_used():
    """With enough events, every supplied player_id should appear."""
    gen = GenPlayEvent(seed=42, player_ids=_PLAYER_IDS)
    events = gen.generate(50)
    seen = {ev.player_id for ev in events}
    for pid in _PLAYER_IDS:
        assert pid in seen, f"player_id {pid} never appeared"


def test_secondary_player_id_is_valid_uuid_when_set():
    """When secondary_player_id is non-empty it must be a valid UUID."""
    import re
    uuid_re = re.compile(
        r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
        re.IGNORECASE,
    )
    gen = GenPlayEvent(seed=42, player_ids=_PLAYER_IDS)
    for ev in gen.generate(100):
        if ev.secondary_player_id:
            assert uuid_re.match(ev.secondary_player_id), (
                f"secondary_player_id={ev.secondary_player_id!r} is not a valid UUID"
            )


# ---------------------------------------------------------------------------
# Batch generation
# ---------------------------------------------------------------------------


def test_generate_count():
    gen = GenPlayEvent(seed=42)
    events = gen.generate(200)
    assert len(events) == 200


def test_generate_zero():
    gen = GenPlayEvent(seed=42)
    assert gen.generate(0) == []


def test_generate_range():
    gen = GenPlayEvent(seed=42)
    batch = gen.generate_range(5, 15)
    assert len(batch) == 10
    for ev in batch:
        protovalidate.validate(ev)


def test_generate_range_empty():
    gen = GenPlayEvent(seed=42)
    assert gen.generate_range(10, 10) == []


def test_generate_range_matches_generate_one():
    gen = GenPlayEvent(seed=42)
    batch = gen.generate_range(0, 8)
    for i, ev in enumerate(batch):
        ev_one = gen.generate_one(i)
        assert ev.event_id == ev_one.event_id


# ---------------------------------------------------------------------------
# Event type distribution
# ---------------------------------------------------------------------------


def test_shot_on_goal_most_common():
    """SHOT_ON_GOAL should be the most common event type in a large batch."""
    from hockey.v1 import play_event_pb2
    from collections import Counter
    gen = GenPlayEvent(seed=42)
    events = gen.generate(1000)
    counts = Counter(ev.event_type for ev in events)
    most_common_type, _ = counts.most_common(1)[0]
    assert most_common_type == play_event_pb2.EVENT_TYPE_SHOT_ON_GOAL


def test_goals_less_frequent_than_shots():
    from hockey.v1 import play_event_pb2
    from collections import Counter
    gen = GenPlayEvent(seed=42)
    events = gen.generate(1000)
    counts = Counter(ev.event_type for ev in events)
    assert counts[play_event_pb2.EVENT_TYPE_GOAL] < counts[play_event_pb2.EVENT_TYPE_SHOT_ON_GOAL]


# ---------------------------------------------------------------------------
# NDJSON round-trip
# ---------------------------------------------------------------------------


def test_ndjson_round_trip():
    gen = GenPlayEvent(seed=42, player_ids=_PLAYER_IDS)
    events = gen.generate(15)
    with tempfile.NamedTemporaryFile(suffix=".ndjson", delete=False) as f:
        path = Path(f.name)
    try:
        GenPlayEvent.write_ndjson(path, events)
        loaded = GenPlayEvent.read_ndjson(path)
        assert len(loaded) == len(events)
        for orig, rt in zip(events, loaded):
            protovalidate.validate(rt)
            assert orig.event_id == rt.event_id
            assert orig.game_id == rt.game_id
            assert orig.player_id == rt.player_id
            assert orig.event_type == rt.event_type
    finally:
        path.unlink(missing_ok=True)


def test_event_to_dict_has_expected_keys():
    gen = GenPlayEvent(seed=42)
    ev = gen.generate_one(0)
    d = GenPlayEvent.event_to_dict(ev)
    assert "event_id" in d
    assert "game_id" in d
    assert "player_id" in d
    assert "event_type" in d
    assert "period" in d
    assert "game_clock" in d
    assert "season" in d
