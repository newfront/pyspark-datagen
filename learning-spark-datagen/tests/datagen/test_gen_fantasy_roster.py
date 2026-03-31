"""Tests for GenFantasyRoster and NDJSON round-trip.

Covers:
- Determinism (same seed + snapshot_index always yields the same snapshot)
- Proto validation on every generated snapshot
- Roster structure (18 slots per team, correct slot types present)
- Ranking correctness (ranks 1..n_teams, rank_delta sensible)
- Fantasy point computation (active vs bench, non-negative totals)
- player_id linking (from supplied player_ids list)
- generate / generate_range batch counts
- NDJSON write → read round-trip fidelity
"""

import tempfile
from pathlib import Path

import protovalidate
import pytest

from learning_spark_datagen.datagen import GenFantasyRoster
from learning_spark_datagen.datagen.gen_fantasy_roster import _N_TEAMS, _ROSTER_SIZE

# Valid UUIDs to supply as player_ids (enough for n_teams × roster_size).
_PLAYER_IDS = [
    f"550e8400-e29b-41d4-a716-{str(i).zfill(12)}"
    for i in range(1, _N_TEAMS * _ROSTER_SIZE + 1)
]


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


def test_generate_one_deterministic():
    gen = GenFantasyRoster(seed=42, weeks=4)
    s1 = gen.generate_one(0)
    s2 = gen.generate_one(0)
    protovalidate.validate(s1)
    protovalidate.validate(s2)
    assert s1.snapshot_id == s2.snapshot_id
    assert s1.fantasy_team_id == s2.fantasy_team_id
    assert s1.total_fantasy_points == s2.total_fantasy_points


def test_different_seeds_differ():
    s1 = GenFantasyRoster(seed=1, weeks=2).generate_one(0)
    s2 = GenFantasyRoster(seed=2, weeks=2).generate_one(0)
    assert s1.snapshot_id != s2.snapshot_id


def test_different_indices_differ():
    gen = GenFantasyRoster(seed=42, weeks=4)
    s0 = gen.generate_one(0)
    s1 = gen.generate_one(1)
    assert s0.snapshot_id != s1.snapshot_id


# ---------------------------------------------------------------------------
# Proto validation
# ---------------------------------------------------------------------------


def test_single_snapshot_validates():
    gen = GenFantasyRoster(seed=99, weeks=3)
    snap = gen.generate_one(0)
    protovalidate.validate(snap)


def test_batch_validates():
    gen = GenFantasyRoster(seed=7, weeks=2)
    for snap in gen.generate():
        protovalidate.validate(snap)


def test_with_player_ids_validates():
    gen = GenFantasyRoster(seed=42, player_ids=_PLAYER_IDS, weeks=2)
    for snap in gen.generate():
        protovalidate.validate(snap)


# ---------------------------------------------------------------------------
# Roster structure
# ---------------------------------------------------------------------------


def test_roster_size_is_18():
    gen = GenFantasyRoster(seed=42, weeks=2)
    snap = gen.generate_one(0)
    assert len(snap.roster) == _ROSTER_SIZE


def test_roster_contains_expected_slot_types():
    """Each snapshot should have 2 C, 2 LW, 2 RW, 4 D, 2 G, 2 UTIL, 4 BN."""
    from collections import Counter
    from hockey.v1 import fantasy_roster_pb2
    expected = {
        fantasy_roster_pb2.SLOT_TYPE_CENTER:     2,
        fantasy_roster_pb2.SLOT_TYPE_LEFT_WING:  2,
        fantasy_roster_pb2.SLOT_TYPE_RIGHT_WING: 2,
        fantasy_roster_pb2.SLOT_TYPE_DEFENSE:    4,
        fantasy_roster_pb2.SLOT_TYPE_GOALIE:     2,
        fantasy_roster_pb2.SLOT_TYPE_UTILITY:    2,
        fantasy_roster_pb2.SLOT_TYPE_BENCH:      4,
    }
    gen = GenFantasyRoster(seed=42, weeks=2)
    snap = gen.generate_one(0)
    counts = Counter(e.slot_type for e in snap.roster)
    for slot_type, expected_count in expected.items():
        assert counts[slot_type] == expected_count, (
            f"slot_type={slot_type} count={counts[slot_type]} expected={expected_count}"
        )


def test_active_slots_have_is_active_true():
    from hockey.v1 import fantasy_roster_pb2
    gen = GenFantasyRoster(seed=42, weeks=2)
    snap = gen.generate_one(0)
    for entry in snap.roster:
        if entry.slot_type == fantasy_roster_pb2.SLOT_TYPE_BENCH:
            assert not entry.is_active
        else:
            assert entry.is_active


def test_bench_slots_are_inactive():
    from hockey.v1 import fantasy_roster_pb2
    gen = GenFantasyRoster(seed=42, weeks=2)
    snap = gen.generate_one(0)
    bench = [e for e in snap.roster if e.slot_type == fantasy_roster_pb2.SLOT_TYPE_BENCH]
    assert all(not e.is_active for e in bench)


def test_all_roster_players_have_valid_names():
    gen = GenFantasyRoster(seed=42, weeks=2)
    snap = gen.generate_one(0)
    for entry in snap.roster:
        assert len(entry.player_name) >= 1


# ---------------------------------------------------------------------------
# Ranking and fantasy points
# ---------------------------------------------------------------------------


def test_rank_in_range():
    """Rank must be between 1 and n_teams inclusive."""
    gen = GenFantasyRoster(seed=42, weeks=3)
    snaps = gen.generate()
    for snap in snaps:
        assert 1 <= snap.rank <= _N_TEAMS, f"rank={snap.rank} out of range"


def test_all_ranks_covered_per_week():
    """For each scoring week, all ranks 1..n_teams should appear exactly once."""
    from collections import defaultdict, Counter
    gen = GenFantasyRoster(seed=42, weeks=3)
    snaps = gen.generate()
    by_week: defaultdict[str, list[int]] = defaultdict(list)
    for snap in snaps:
        by_week[snap.scoring_week].append(snap.rank)
    for week, ranks in by_week.items():
        counter = Counter(ranks)
        for r in range(1, _N_TEAMS + 1):
            assert counter[r] == 1, f"week={week} rank={r} appeared {counter[r]} times"


def test_total_fantasy_points_non_negative():
    gen = GenFantasyRoster(seed=42, weeks=3)
    for snap in gen.generate():
        assert snap.total_fantasy_points >= 0.0


def test_active_fp_sum_matches_total():
    """total_fantasy_points should equal sum of active roster entry fantasy_points."""
    gen = GenFantasyRoster(seed=42, weeks=2)
    for snap in gen.generate():
        active_sum = sum(e.fantasy_points for e in snap.roster if e.is_active)
        assert abs(active_sum - snap.total_fantasy_points) < 0.05, (
            f"active_sum={active_sum} != total={snap.total_fantasy_points}"
        )


def test_rank_delta_first_week_is_zero():
    """Week 0 (snapshot_index < n_teams) should always have rank_delta == 0."""
    gen = GenFantasyRoster(seed=42, weeks=4)
    for team_idx in range(_N_TEAMS):
        snap = gen.generate_one(team_idx)  # week 0
        assert snap.rank_delta == 0, f"team={team_idx} week=0 rank_delta={snap.rank_delta}"


def test_scoring_week_format():
    """scoring_week must match 'YYYY-WNN' pattern."""
    import re
    week_re = re.compile(r"^\d{4}-W\d{2}$")
    gen = GenFantasyRoster(seed=42, weeks=4)
    for snap in gen.generate():
        assert week_re.match(snap.scoring_week), (
            f"scoring_week={snap.scoring_week!r} has wrong format"
        )


# ---------------------------------------------------------------------------
# player_id linking
# ---------------------------------------------------------------------------


def test_player_ids_link_to_pool():
    gen = GenFantasyRoster(seed=42, player_ids=_PLAYER_IDS, weeks=2)
    for snap in gen.generate():
        for entry in snap.roster:
            assert entry.player_id in _PLAYER_IDS, (
                f"player_id={entry.player_id!r} not in supplied pool"
            )


# ---------------------------------------------------------------------------
# Batch generation
# ---------------------------------------------------------------------------


def test_generate_count():
    weeks = 4
    gen = GenFantasyRoster(seed=42, weeks=weeks)
    snaps = gen.generate()
    assert len(snaps) == weeks * _N_TEAMS


def test_generate_with_weeks_override():
    gen = GenFantasyRoster(seed=42, weeks=5)
    snaps = gen.generate(weeks=3)
    assert len(snaps) == 3 * _N_TEAMS


def test_generate_range():
    gen = GenFantasyRoster(seed=42, weeks=4)
    batch = gen.generate_range(0, 10)
    assert len(batch) == 10
    for snap in batch:
        protovalidate.validate(snap)


def test_generate_range_empty():
    gen = GenFantasyRoster(seed=42, weeks=2)
    assert gen.generate_range(5, 5) == []


def test_generate_range_matches_generate_one():
    gen = GenFantasyRoster(seed=42, weeks=4)
    batch = gen.generate_range(0, 5)
    for i, snap in enumerate(batch):
        snap_one = gen.generate_one(i)
        assert snap.snapshot_id == snap_one.snapshot_id


# ---------------------------------------------------------------------------
# NDJSON round-trip
# ---------------------------------------------------------------------------


def test_ndjson_round_trip():
    gen = GenFantasyRoster(seed=42, player_ids=_PLAYER_IDS, weeks=2)
    snaps = gen.generate()
    with tempfile.NamedTemporaryFile(suffix=".ndjson", delete=False) as f:
        path = Path(f.name)
    try:
        GenFantasyRoster.write_ndjson(path, snaps)
        loaded = GenFantasyRoster.read_ndjson(path)
        assert len(loaded) == len(snaps)
        for orig, rt in zip(snaps, loaded):
            protovalidate.validate(rt)
            assert orig.snapshot_id == rt.snapshot_id
            assert orig.fantasy_team_id == rt.fantasy_team_id
            assert orig.scoring_week == rt.scoring_week
            assert orig.rank == rt.rank
            assert len(orig.roster) == len(rt.roster)
    finally:
        path.unlink(missing_ok=True)


def test_snapshot_to_dict_has_expected_keys():
    gen = GenFantasyRoster(seed=42, weeks=1)
    snap = gen.generate_one(0)
    d = GenFantasyRoster.snapshot_to_dict(snap)
    assert "snapshot_id" in d
    assert "fantasy_team_id" in d
    assert "fantasy_team_name" in d
    assert "league_id" in d
    assert "scoring_week" in d
    assert "rank" in d
    assert "total_fantasy_points" in d
    assert "roster" in d
