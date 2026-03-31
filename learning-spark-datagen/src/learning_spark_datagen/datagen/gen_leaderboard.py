"""Generate deterministic fake LeaderboardSnapshot protobufs modeling hourly leaderboard state.

Each call to generate() produces `days` * 24 snapshots (one per hour) for a single leaderboard.
Player scores grow deterministically over time — each player has an individual score velocity and
active-hour profile — so the resulting time series captures realistic gameplay dynamics:

  - Grinders: high velocity, active most hours.
  - Burst players: high velocity but only active in short windows.
  - Casual players: low velocity, sparse activity.
  - New entrants: arrive after the board opens, may climb quickly.
  - Stalled players: stop gaining after a while (churned).

Usage
-----
gen = GenLeaderboard(seed=42, user_ids=[...], days=30)
snapshots = gen.generate()          # list[LeaderboardSnapshot], len == 30*24

# Or generate one snapshot at a time (snapshot_index = hours elapsed since epoch):
snap = gen.generate_one(snapshot_index=0)   # first hour
snap = gen.generate_one(snapshot_index=71)  # last hour of day 3
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from random import Random
from uuid import UUID

_here = Path(__file__).resolve().parent
_root = _here.parent.parent.parent.parent
_gen = _root / "gen" / "python"
if _gen.exists():
    sys.path.insert(0, str(_gen))

from google.protobuf import json_format, timestamp_pb2  # noqa: E402

from leaderboard.v1 import leaderboard_pb2  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_PLAYERS_PER_SNAPSHOT = 1000

# Player archetypes control score-velocity distribution.
_ARCHETYPE_GRINDER = "grinder"       # high velocity, always active
_ARCHETYPE_BURST = "burst"           # very high velocity but only a few hours/day
_ARCHETYPE_CASUAL = "casual"         # low velocity, sparse
_ARCHETYPE_NEWCOMER = "newcomer"     # joins late, climbs fast initially then slows
_ARCHETYPE_STALLED = "stalled"       # active early, stops gaining mid-way

# Base score for a top-100 player at hour 0 (gives realistic starting spread).
_STARTING_SCORE_TOP = 500_000
# Points per active hour for each archetype.
_VELOCITY = {
    _ARCHETYPE_GRINDER:  1_800,
    _ARCHETYPE_BURST:    4_500,
    _ARCHETYPE_CASUAL:     350,
    _ARCHETYPE_NEWCOMER: 2_200,
    _ARCHETYPE_STALLED:  1_500,
}
# Fraction of hours each archetype is actively scoring.
_ACTIVITY_RATE = {
    _ARCHETYPE_GRINDER:  0.70,
    _ARCHETYPE_BURST:    0.12,
    _ARCHETYPE_CASUAL:   0.08,
    _ARCHETYPE_NEWCOMER: 0.45,
    _ARCHETYPE_STALLED:  0.40,
}
_ARCHETYPES = [
    _ARCHETYPE_GRINDER,
    _ARCHETYPE_BURST,
    _ARCHETYPE_CASUAL,
    _ARCHETYPE_NEWCOMER,
    _ARCHETYPE_STALLED,
]
_ARCHETYPE_WEIGHTS = [0.15, 0.10, 0.45, 0.15, 0.15]

# Steam-style game IDs used when no game_id is supplied.
_GAME_IDS = ("730", "570", "440", "252490", "1172470", "271590")
_LEADERBOARD_NAMES = (
    "All-Time High Scores",
    "Weekly Challenge",
    "Ranked Competitive",
    "Season 1 Finals",
    "Speed Run - Any%",
    "Hardcore Mode",
)
_PLAYER_NAME_PREFIXES = (
    "xX", "Xx", "Pro", "Dark", "Neo", "Ultra", "Shadow", "Elite", "Iron", "Neon",
)
_PLAYER_NAME_SUFFIXES = (
    "Xx", "420", "999", "7", "GG", "PWR", "ACE", "MVP", "HD", "OP",
)

# ---------------------------------------------------------------------------
# Internal player state (not serialized directly — used for generation)
# ---------------------------------------------------------------------------


@dataclass
class _PlayerState:
    player_id: str
    player_name: str
    archetype: str
    # Hour offset within the simulation at which this player first appears.
    entry_hour: int
    # Per-player velocity multiplier (1.0 ± some noise).
    velocity_mult: float
    # For stalled players: hour at which they stop gaining.
    stall_hour: int
    # Cumulative hours played at hour 0 (players already in-game before the board opens).
    base_hours: float


# ---------------------------------------------------------------------------
# Generator
# ---------------------------------------------------------------------------


class GenLeaderboard:
    """Generate a time series of LeaderboardSnapshot protobufs (one per hour).

    Parameters
    ----------
    seed:
        Random seed for full reproducibility. Same seed + user_ids always yields
        identical snapshots.
    user_ids:
        List of UUIDs (str) from the users table. player_id values will be drawn
        from this pool (cycling if the pool is smaller than _PLAYERS_PER_SNAPSHOT).
        If None, random UUIDs are generated deterministically from the seed.
    days:
        Number of days to simulate. Total snapshots = days * 24.
    leaderboard_name:
        Human-readable name for the board. Picked deterministically if None.
    game_id:
        Steam App ID string. Picked deterministically if None.
    epoch:
        UTC datetime of the first snapshot (hour 0). Defaults to 2024-01-01 00:00 UTC.
    """

    def __init__(
        self,
        seed: int = 42,
        user_ids: list[str] | None = None,
        days: int = 30,
        leaderboard_name: str | None = None,
        game_id: str | None = None,
        epoch: datetime | None = None,
    ) -> None:
        self._seed = seed
        self._days = days
        self._total_hours = days * 24
        self._epoch = epoch or datetime(2024, 1, 1, tzinfo=timezone.utc)

        rng = Random(seed)

        # Stable leaderboard identity.
        self._leaderboard_id = str(UUID(int=rng.getrandbits(128)))
        self._leaderboard_name = leaderboard_name or rng.choice(_LEADERBOARD_NAMES)
        self._game_id = game_id or rng.choice(_GAME_IDS)
        self._sort_method = leaderboard_pb2.SORT_METHOD_DESCENDING
        self._display_type = leaderboard_pb2.DISPLAY_TYPE_NUMERIC

        # Build the player pool.
        self._players = self._build_players(rng, user_ids)

    # ------------------------------------------------------------------
    # Player pool construction
    # ------------------------------------------------------------------

    def _build_players(
        self, rng: Random, user_ids: list[str] | None
    ) -> list[_PlayerState]:
        """Construct a deterministic pool of player states for the whole simulation."""
        players: list[_PlayerState] = []
        for i in range(_PLAYERS_PER_SNAPSHOT):
            p_rng = Random(self._seed + 10_000 + i)

            if user_ids:
                player_id = user_ids[i % len(user_ids)]
            else:
                player_id = str(UUID(int=p_rng.getrandbits(128)))

            prefix = p_rng.choice(_PLAYER_NAME_PREFIXES)
            suffix = p_rng.choice(_PLAYER_NAME_SUFFIXES)
            player_name = f"{prefix}Player{i}{suffix}"

            archetype = p_rng.choices(_ARCHETYPES, weights=_ARCHETYPE_WEIGHTS, k=1)[0]

            # Newcomers arrive later (random hour in first half of the simulation).
            if archetype == _ARCHETYPE_NEWCOMER:
                entry_hour = p_rng.randint(self._total_hours // 4, self._total_hours // 2)
            else:
                entry_hour = p_rng.randint(0, max(1, self._total_hours // 10))

            # Stalled players stop gaining at some point.
            if archetype == _ARCHETYPE_STALLED:
                stall_hour = p_rng.randint(
                    self._total_hours // 4, (self._total_hours * 3) // 4
                )
            else:
                stall_hour = self._total_hours + 1  # Never stalls.

            velocity_mult = p_rng.uniform(0.7, 1.4)
            # Give earlier entrants a head-start so the leaderboard is realistic at hour 0.
            base_hours = p_rng.uniform(0.0, 200.0) if entry_hour == 0 else 0.0

            players.append(
                _PlayerState(
                    player_id=player_id,
                    player_name=player_name,
                    archetype=archetype,
                    entry_hour=entry_hour,
                    velocity_mult=velocity_mult,
                    stall_hour=stall_hour,
                    base_hours=base_hours,
                )
            )
        return players

    # ------------------------------------------------------------------
    # Score simulation helpers
    # ------------------------------------------------------------------

    def _score_at_hour(self, player: _PlayerState, hour: int) -> int:
        """Return the cumulative score for a player at the given hour offset."""
        if hour < player.entry_hour:
            return 0

        active_hours = min(hour, player.stall_hour) - player.entry_hour
        if active_hours <= 0:
            return 0

        base_velocity = _VELOCITY[player.archetype] * player.velocity_mult
        activity_rate = _ACTIVITY_RATE[player.archetype]

        # Each hour is independently "active" based on a per-player-per-hour coin flip.
        # We derive this deterministically from a hash of (seed, player_id, hour).
        total_score = 0
        for h in range(player.entry_hour, player.entry_hour + active_hours):
            h_rng = Random(self._seed + hash(player.player_id) + h * 7919)
            if h_rng.random() < activity_rate:
                # Score gained this hour: velocity ± 30% noise.
                noise = h_rng.uniform(0.7, 1.3)
                total_score += int(base_velocity * noise)

        return total_score

    def _hours_played_at(self, player: _PlayerState, hour: int) -> float:
        """Return cumulative hours played at a given simulation hour."""
        if hour < player.entry_hour:
            return 0.0
        active_hours = hour - player.entry_hour
        activity_rate = _ACTIVITY_RATE[player.archetype]
        return round(player.base_hours + active_hours * activity_rate, 2)

    # ------------------------------------------------------------------
    # Public generation interface
    # ------------------------------------------------------------------

    def generate_one(self, snapshot_index: int) -> leaderboard_pb2.LeaderboardSnapshot:
        """Generate one LeaderboardSnapshot for the given hour offset (0-based).

        snapshot_index=0 is the first hour; snapshot_index=(days*24 - 1) is the last.
        """
        hour = snapshot_index

        # Compute scores for all eligible players.
        scored: list[tuple[int, int, _PlayerState]] = []
        for i, player in enumerate(self._players):
            if hour < player.entry_hour:
                continue
            score = self._score_at_hour(player, hour)
            scored.append((score, i, player))

        # Sort descending by score (higher = better rank for this leaderboard).
        scored.sort(key=lambda t: (-t[0], t[1]))

        # Compute previous-hour scores for delta fields.
        prev_scores: dict[str, int] = {}
        prev_ranks: dict[str, int] = {}
        if hour > 0:
            prev_scored: list[tuple[int, int, _PlayerState]] = []
            for i, player in enumerate(self._players):
                if (hour - 1) < player.entry_hour:
                    continue
                prev_score = self._score_at_hour(player, hour - 1)
                prev_scored.append((prev_score, i, player))
            prev_scored.sort(key=lambda t: (-t[0], t[1]))
            for rank_idx, (sc, _, pl) in enumerate(prev_scored):
                prev_scores[pl.player_id] = sc
                prev_ranks[pl.player_id] = rank_idx + 1

        # Build entries.
        entries: list[leaderboard_pb2.LeaderboardEntry] = []
        for rank_idx, (score, _, player) in enumerate(scored):
            rank = rank_idx + 1
            prev_rank = prev_ranks.get(player.player_id, 0)
            rank_delta = (prev_rank - rank) if prev_rank > 0 else 0
            score_delta = score - prev_scores.get(player.player_id, 0)
            hours_played = self._hours_played_at(player, hour)

            entry = leaderboard_pb2.LeaderboardEntry(
                rank=rank,
                player_id=player.player_id,
                player_name=player.player_name,
                score=score,
                previous_rank=prev_rank,
                rank_delta=rank_delta,
                score_delta=score_delta,
                hours_played=hours_played,
            )
            entries.append(entry)

        captured_dt = self._epoch + timedelta(hours=hour)
        captured_ts = timestamp_pb2.Timestamp()
        captured_ts.FromDatetime(captured_dt)

        snap_rng = Random(self._seed + snapshot_index * 31337)
        snapshot_id = str(UUID(int=snap_rng.getrandbits(128)))

        snapshot = leaderboard_pb2.LeaderboardSnapshot(
            snapshot_id=snapshot_id,
            leaderboard_id=self._leaderboard_id,
            leaderboard_name=self._leaderboard_name,
            game_id=self._game_id,
            captured_at=captured_ts,
            total_entries=len(entries),
            sort_method=self._sort_method,
            display_type=self._display_type,
            entries=entries,
        )
        return snapshot

    def generate(self, days: int | None = None) -> list[leaderboard_pb2.LeaderboardSnapshot]:
        """Generate all hourly snapshots for the simulation period.

        Parameters
        ----------
        days:
            Override the number of days for this call (defaults to self._days).
        """
        total = (days or self._days) * 24
        return [self.generate_one(i) for i in range(total)]

    def generate_range(
        self, start: int, end: int
    ) -> list[leaderboard_pb2.LeaderboardSnapshot]:
        """Generate snapshots for snapshot indices [start, end). Useful for batched Delta writes."""
        if start >= end:
            return []
        return [self.generate_one(i) for i in range(start, end)]

    # ------------------------------------------------------------------
    # Serialization helpers
    # ------------------------------------------------------------------

    @staticmethod
    def snapshot_to_dict(
        snapshot: leaderboard_pb2.LeaderboardSnapshot,
    ) -> dict:
        """Convert a LeaderboardSnapshot proto to a JSON-serializable dict."""
        return json_format.MessageToDict(
            snapshot,
            always_print_fields_with_no_presence=False,
            preserving_proto_field_name=True,
        )

    @staticmethod
    def write_ndjson(
        path: str | Path,
        snapshots: list[leaderboard_pb2.LeaderboardSnapshot],
    ) -> None:
        """Write snapshots to newline-delimited JSON (one JSON object per line)."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w") as f:
            for snap in snapshots:
                f.write(json.dumps(GenLeaderboard.snapshot_to_dict(snap)) + "\n")

    @staticmethod
    def read_ndjson(
        path: str | Path,
    ) -> list[leaderboard_pb2.LeaderboardSnapshot]:
        """Read snapshots from a newline-delimited JSON file."""
        path = Path(path)
        snapshots = []
        with path.open() as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                d = json.loads(line)
                snap = leaderboard_pb2.LeaderboardSnapshot()
                json_format.ParseDict(d, snap)
                snapshots.append(snap)
        return snapshots
