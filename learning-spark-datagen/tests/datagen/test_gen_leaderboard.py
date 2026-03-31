"""Tests for GenLeaderboard and NDJSON round-trip.

Covers:
- Determinism (same seed + snapshot_index always yields the same snapshot)
- Proto validation on every generated snapshot
- Structural invariants (ranks contiguous from 1, entry count ≤ 1000)
- Score progression (scores are non-decreasing across hourly snapshots for each player)
- rank_delta / score_delta correctness between consecutive snapshots
- User-ID linking (player_ids match supplied UUIDs)
- NDJSON round-trip fidelity
- generate_range partial batch
"""

import os
import tempfile
from pathlib import Path

import protovalidate
import pytest

from learning_spark_datagen.datagen import GenLeaderboard

# Valid UUIDs to use as player_ids (must pass string.uuid proto validation).
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
    gen = GenLeaderboard(seed=42, days=3)
    s1 = gen.generate_one(0)
    s2 = gen.generate_one(0)
    protovalidate.validate(s1)
    protovalidate.validate(s2)
    assert s1.snapshot_id == s2.snapshot_id
    assert s1.leaderboard_id == s2.leaderboard_id
    assert len(s1.entries) == len(s2.entries)
    for e1, e2 in zip(s1.entries, s2.entries):
        assert e1.player_id == e2.player_id
        assert e1.score == e2.score
        assert e1.rank == e2.rank


def test_different_snapshot_indices_differ():
    gen = GenLeaderboard(seed=42, days=5)
    s0 = gen.generate_one(0)
    s1 = gen.generate_one(1)
    # Different snapshot IDs.
    assert s0.snapshot_id != s1.snapshot_id
    # captured_at advances by exactly one hour.
    assert s1.captured_at.seconds - s0.captured_at.seconds == 3600


def test_different_seeds_differ():
    g1 = GenLeaderboard(seed=1, days=3)
    g2 = GenLeaderboard(seed=2, days=3)
    assert g1.generate_one(0).leaderboard_id != g2.generate_one(0).leaderboard_id


# ---------------------------------------------------------------------------
# Proto validation
# ---------------------------------------------------------------------------


def test_protovalidate_single_snapshot():
    gen = GenLeaderboard(seed=42, days=2)
    snap = gen.generate_one(0)
    protovalidate.validate(snap)


def test_protovalidate_all_snapshots_small():
    gen = GenLeaderboard(seed=7, days=2)
    for snap in gen.generate():
        protovalidate.validate(snap)


# ---------------------------------------------------------------------------
# Structural invariants
# ---------------------------------------------------------------------------


def test_ranks_are_contiguous_from_one():
    gen = GenLeaderboard(seed=42, days=1)
    snap = gen.generate_one(0)
    ranks = [e.rank for e in snap.entries]
    assert ranks == list(range(1, len(ranks) + 1))


def test_entry_count_at_most_1000():
    gen = GenLeaderboard(seed=42, days=3)
    for snap in gen.generate():
        assert len(snap.entries) <= 1000


def test_total_entries_matches_entry_list():
    gen = GenLeaderboard(seed=42, days=2)
    for snap in gen.generate():
        assert snap.total_entries == len(snap.entries)


def test_leaderboard_id_stable_across_snapshots():
    gen = GenLeaderboard(seed=42, days=3)
    snaps = gen.generate()
    lid = snaps[0].leaderboard_id
    for snap in snaps:
        assert snap.leaderboard_id == lid


def test_snapshot_ids_unique():
    gen = GenLeaderboard(seed=42, days=3)
    snaps = gen.generate()
    ids = [s.snapshot_id for s in snaps]
    assert len(ids) == len(set(ids))


# ---------------------------------------------------------------------------
# Score progression (temporal dynamics)
# ---------------------------------------------------------------------------


def test_scores_non_decreasing_over_time():
    """Each player's score should never decrease between consecutive hourly snapshots."""
    gen = GenLeaderboard(seed=42, days=3)
    snaps = gen.generate()
    prev_scores: dict[str, int] = {}
    for snap in snaps:
        for entry in snap.entries:
            pid = entry.player_id
            if pid in prev_scores:
                assert entry.score >= prev_scores[pid], (
                    f"Player {pid} score decreased: {prev_scores[pid]} → {entry.score}"
                )
            prev_scores[pid] = entry.score


def test_score_delta_matches_actual_change():
    """score_delta for each entry should equal current score − previous snapshot's score."""
    gen = GenLeaderboard(seed=42, days=2)
    snaps = gen.generate()
    prev_scores: dict[str, int] = {}
    for snap_idx, snap in enumerate(snaps):
        for entry in snap.entries:
            pid = entry.player_id
            if snap_idx == 0 or pid not in prev_scores:
                # New entrant or first snapshot: score_delta should be 0.
                assert entry.score_delta == 0 or entry.previous_rank == 0
            else:
                expected_delta = entry.score - prev_scores[pid]
                assert entry.score_delta == expected_delta, (
                    f"snapshot {snap_idx}, player {pid}: "
                    f"score_delta={entry.score_delta} but expected {expected_delta}"
                )
            prev_scores[pid] = entry.score


def test_rank_delta_matches_actual_rank_change():
    """rank_delta should equal previous_rank − current rank for non-new-entrants."""
    gen = GenLeaderboard(seed=42, days=2)
    snaps = gen.generate()
    prev_ranks: dict[str, int] = {}
    for snap_idx, snap in enumerate(snaps):
        for entry in snap.entries:
            pid = entry.player_id
            if entry.previous_rank > 0 and pid in prev_ranks:
                expected_delta = prev_ranks[pid] - entry.rank
                assert entry.rank_delta == expected_delta
            prev_ranks[pid] = entry.rank


def test_new_entrants_have_zero_previous_rank():
    """Players appearing for the first time should have previous_rank == 0."""
    gen = GenLeaderboard(seed=42, days=5)
    snaps = gen.generate()
    seen: set[str] = set()
    for snap in snaps:
        for entry in snap.entries:
            pid = entry.player_id
            if pid not in seen:
                assert entry.previous_rank == 0, (
                    f"New entrant {pid} has previous_rank={entry.previous_rank}"
                )
            seen.add(pid)


# ---------------------------------------------------------------------------
# User-ID linking
# ---------------------------------------------------------------------------


def test_player_ids_linked_to_user_ids():
    gen = GenLeaderboard(seed=42, user_ids=_PLAYER_IDS * 200, days=1)
    snap = gen.generate_one(0)
    protovalidate.validate(snap)
    player_ids_in_snap = {e.player_id for e in snap.entries}
    # All player_ids in the snapshot must come from the supplied pool.
    assert player_ids_in_snap.issubset(set(_PLAYER_IDS * 200))


# ---------------------------------------------------------------------------
# generate() and generate_range()
# ---------------------------------------------------------------------------


def test_generate_total_snapshots():
    gen = GenLeaderboard(seed=42, days=3)
    snaps = gen.generate()
    assert len(snaps) == 3 * 24


def test_generate_days_override():
    gen = GenLeaderboard(seed=42, days=5)
    snaps = gen.generate(days=2)
    assert len(snaps) == 2 * 24


def test_generate_range():
    gen = GenLeaderboard(seed=42, days=5)
    full = gen.generate()
    partial = gen.generate_range(10, 20)
    assert len(partial) == 10
    for i, snap in enumerate(partial):
        assert snap.snapshot_id == full[10 + i].snapshot_id


def test_generate_range_empty():
    gen = GenLeaderboard(seed=42, days=2)
    assert gen.generate_range(5, 5) == []
    assert gen.generate_range(10, 5) == []


# ---------------------------------------------------------------------------
# NDJSON round-trip
# ---------------------------------------------------------------------------


def test_ndjson_round_trip():
    gen = GenLeaderboard(seed=42, days=1)
    snaps = gen.generate()
    fd, name = tempfile.mkstemp(suffix=".ndjson")
    os.close(fd)
    path = Path(name)
    try:
        GenLeaderboard.write_ndjson(path, snaps)
        read_back = GenLeaderboard.read_ndjson(path)
        assert len(read_back) == len(snaps)
        for orig, rb in zip(snaps, read_back):
            protovalidate.validate(rb)
            assert orig.snapshot_id == rb.snapshot_id
            assert orig.leaderboard_id == rb.leaderboard_id
            assert len(orig.entries) == len(rb.entries)
            for eo, er in zip(orig.entries, rb.entries):
                assert eo.player_id == er.player_id
                assert eo.score == er.score
                assert eo.rank == er.rank
                assert eo.rank_delta == er.rank_delta
                assert eo.score_delta == er.score_delta
    finally:
        path.unlink(missing_ok=True)
