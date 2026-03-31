"""Generate deterministic fake FantasyRosterSnapshot protobufs and NDJSON I/O.

Each FantasyRosterSnapshot captures one fantasy team's full 18-slot weekly
roster and accumulated NHL statistics for a single scoring week.  A series
of snapshots with the same fantasy_team_id traces that team's season-long
trajectory (rank changes, roster construction, player performance).

The generator models a 10-team Yahoo-style H2H-points league using the
default scoring weights:
  Skaters:  G=6, A=4, +/-=2, SOG=0.9, HIT=1, BLK=1
  Goalies:  W=5, GA=-3, SV=0.6

Roster composition per team (18 slots, Yahoo defaults):
  2 C, 2 LW, 2 RW, 4 D, 2 G, 2 UTIL (any skater), 4 BN (bench)

Usage
-----
gen = GenFantasyRoster(seed=42, player_ids=[...], weeks=25)
snapshots = gen.generate()          # list[FantasyRosterSnapshot], len == weeks * n_teams
snap = gen.generate_one(0)          # first team's first week
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

from hockey.v1 import fantasy_roster_pb2  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_N_TEAMS = 10

# Roster slot layout: (SlotType, is_active, is_goalie)
_ROSTER_LAYOUT: list[tuple[int, bool, bool]] = [
    (fantasy_roster_pb2.SLOT_TYPE_CENTER,      True,  False),
    (fantasy_roster_pb2.SLOT_TYPE_CENTER,      True,  False),
    (fantasy_roster_pb2.SLOT_TYPE_LEFT_WING,   True,  False),
    (fantasy_roster_pb2.SLOT_TYPE_LEFT_WING,   True,  False),
    (fantasy_roster_pb2.SLOT_TYPE_RIGHT_WING,  True,  False),
    (fantasy_roster_pb2.SLOT_TYPE_RIGHT_WING,  True,  False),
    (fantasy_roster_pb2.SLOT_TYPE_DEFENSE,     True,  False),
    (fantasy_roster_pb2.SLOT_TYPE_DEFENSE,     True,  False),
    (fantasy_roster_pb2.SLOT_TYPE_DEFENSE,     True,  False),
    (fantasy_roster_pb2.SLOT_TYPE_DEFENSE,     True,  False),
    (fantasy_roster_pb2.SLOT_TYPE_GOALIE,      True,  True),
    (fantasy_roster_pb2.SLOT_TYPE_GOALIE,      True,  True),
    (fantasy_roster_pb2.SLOT_TYPE_UTILITY,     True,  False),
    (fantasy_roster_pb2.SLOT_TYPE_UTILITY,     True,  False),
    (fantasy_roster_pb2.SLOT_TYPE_BENCH,       False, False),
    (fantasy_roster_pb2.SLOT_TYPE_BENCH,       False, False),
    (fantasy_roster_pb2.SLOT_TYPE_BENCH,       False, False),
    (fantasy_roster_pb2.SLOT_TYPE_BENCH,       False, False),
]
_ROSTER_SIZE = len(_ROSTER_LAYOUT)

# Player quality tiers that control stat magnitudes.
_TIER_ELITE = "elite"
_TIER_GOOD = "good"
_TIER_AVERAGE = "average"
_TIER_BELOW = "below"
_TIERS = [_TIER_ELITE, _TIER_GOOD, _TIER_AVERAGE, _TIER_BELOW]
_TIER_WEIGHTS = [0.10, 0.25, 0.40, 0.25]

# Base stats per game played (used to compute weekly totals).
# Format: (goals, assists, shots, hits, blocks, pim, plus_minus)
_SKATER_BASE: dict[str, tuple[float, ...]] = {
    _TIER_ELITE:   (0.40, 0.65, 3.50, 1.50, 0.80, 0.70, +0.35),
    _TIER_GOOD:    (0.25, 0.45, 2.80, 1.80, 1.10, 0.80, +0.20),
    _TIER_AVERAGE: (0.15, 0.28, 2.00, 2.00, 1.30, 0.90,  0.00),
    _TIER_BELOW:   (0.08, 0.15, 1.40, 2.20, 1.50, 1.10, -0.10),
}
# Goalie base stats per game: (wins_prob, saves, goals_against)
_GOALIE_BASE: dict[str, tuple[float, ...]] = {
    _TIER_ELITE:   (0.58, 26.0, 2.0),
    _TIER_GOOD:    (0.52, 24.5, 2.5),
    _TIER_AVERAGE: (0.47, 23.0, 2.9),
    _TIER_BELOW:   (0.42, 21.5, 3.4),
}

# Fantasy scoring weights (Yahoo H2H-points defaults).
_FP_GOAL     = 6.0
_FP_ASSIST   = 4.0
_FP_PLUS     = 2.0    # per +/- point
_FP_SOG      = 0.9
_FP_HIT      = 1.0
_FP_BLOCK    = 1.0
_FP_WIN      = 5.0
_FP_GA       = -3.0
_FP_SAVE     = 0.6

# Season start for weekly timestamp anchoring (2024-25 season).
_SEASON_START = datetime(2024, 10, 8, tzinfo=timezone.utc)

# Fantasy team name templates.
_TEAM_NAMES = [
    "The Puck Stops Here",
    "Ice Cold Killers",
    "Slapshot Squad",
    "Penalty Box Heroes",
    "Icing Machines",
    "Five-Hole Fillers",
    "Goalie Interference",
    "Hat Trick Hunters",
    "Power Play Pros",
    "Overtime Outlaws",
]

_LEAGUE_NAMES = [
    "USA Hockey Fantasy Premier",
    "Rink Rats Fantasy League",
    "Frozen Four Fantasy",
    "Stanley's Cup Chasers",
    "The Blue Line League",
]


# ---------------------------------------------------------------------------
# Internal player profile (not serialized)
# ---------------------------------------------------------------------------


@dataclass
class _PlayerProfile:
    player_id: str
    player_name: str
    tier: str
    is_goalie: bool
    # Average number of games played per week (2–4 for most players).
    games_per_week: float


# ---------------------------------------------------------------------------
# Generator
# ---------------------------------------------------------------------------


class GenFantasyRoster:
    """Generate a time series of FantasyRosterSnapshot protobufs (one per team per week).

    Parameters
    ----------
    seed:
        Random seed; same seed + player_ids always produces identical snapshots.
    player_ids:
        Optional list of UUIDs from a HockeyPlayer NDJSON file.  Roster
        player_id values are drawn from this pool.  If None, random UUIDs
        are generated deterministically from the seed.
    weeks:
        Number of scoring weeks to simulate.  Total snapshots = weeks × n_teams.
    n_teams:
        Number of fantasy teams in the league (default 10).
    league_name:
        Human-readable league name; picked deterministically if None.
    """

    def __init__(
        self,
        seed: int = 42,
        player_ids: list[str] | None = None,
        weeks: int = 25,
        n_teams: int = _N_TEAMS,
        league_name: str | None = None,
    ) -> None:
        self._seed = seed
        self._weeks = weeks
        self._n_teams = n_teams

        rng = Random(seed)

        self._league_id = str(UUID(int=rng.getrandbits(128)))
        self._league_name = league_name or rng.choice(_LEAGUE_NAMES)

        # Build deterministic player pool.
        self._all_player_ids: list[str] = list(player_ids) if player_ids else []
        if not self._all_player_ids:
            id_rng = Random(seed + 55_555)
            self._all_player_ids = [
                str(UUID(int=id_rng.getrandbits(128)))
                for _ in range(n_teams * _ROSTER_SIZE)
            ]

        # Build team structures: each team has a stable roster of _ROSTER_SIZE players.
        self._teams = self._build_teams(rng)

    # ------------------------------------------------------------------
    # Team / player construction
    # ------------------------------------------------------------------

    def _build_teams(self, rng: Random) -> list[dict]:
        """Construct deterministic team metadata and player rosters."""
        teams = []
        # Shuffle team names deterministically.
        name_pool = list(_TEAM_NAMES)
        rng.shuffle(name_pool)

        for t in range(self._n_teams):
            t_rng = Random(self._seed + 20_000 + t)
            team_id = str(UUID(int=t_rng.getrandbits(128)))
            team_name = name_pool[t % len(name_pool)]

            # Each team owns a contiguous slice of the player pool (cycling as needed).
            profiles: list[_PlayerProfile] = []
            for slot_idx, (_, _, is_goalie) in enumerate(_ROSTER_LAYOUT):
                pool_idx = (t * _ROSTER_SIZE + slot_idx) % len(self._all_player_ids)
                pid = self._all_player_ids[pool_idx]

                p_rng = Random(self._seed + 30_000 + t * 100 + slot_idx)
                tier = p_rng.choices(_TIERS, weights=_TIER_WEIGHTS, k=1)[0]
                games_per_week = p_rng.uniform(2.5, 4.0)
                # Bench players miss more games.
                _, is_active, _ = _ROSTER_LAYOUT[slot_idx]
                if not is_active:
                    games_per_week = p_rng.uniform(1.5, 3.0)

                first = p_rng.choice(
                    ["Alex", "Ryan", "Tyler", "Connor", "Nathan", "Jack",
                     "Drew", "Auston", "Sidney", "Patrick", "Nikita", "Leon"]
                )
                last = p_rng.choice(
                    ["Smith", "Johnson", "Williams", "Brown", "Jones",
                     "Garcia", "Miller", "Davis", "Wilson", "Moore",
                     "Taylor", "Anderson", "Thomas", "Jackson", "White"]
                )
                player_name = f"{first} {last}"

                profiles.append(_PlayerProfile(
                    player_id=pid,
                    player_name=player_name,
                    tier=tier,
                    is_goalie=is_goalie,
                    games_per_week=games_per_week,
                ))

            teams.append({
                "team_id": team_id,
                "team_name": team_name,
                "profiles": profiles,
            })

        return teams

    # ------------------------------------------------------------------
    # Stat / fantasy point simulation
    # ------------------------------------------------------------------

    def _weekly_skater_stats(
        self, profile: _PlayerProfile, week: int, team_idx: int, slot_idx: int
    ) -> tuple[int, int, int, int, int, int, int, float]:
        """Return (goals, assists, sog, hits, blocks, pim, plus_minus, fp) for a skater."""
        rng = Random(self._seed + week * 100_000 + team_idx * 1_000 + slot_idx)
        games = max(0, round(profile.games_per_week * rng.uniform(0.6, 1.3)))
        if games == 0:
            return 0, 0, 0, 0, 0, 0, 0, 0.0

        base = _SKATER_BASE[profile.tier]
        noise = lambda: rng.uniform(0.7, 1.3)  # noqa: E731

        goals       = max(0, round(base[0] * games * noise()))
        assists     = max(0, round(base[1] * games * noise()))
        shots       = max(0, round(base[2] * games * noise()))
        hits        = max(0, round(base[3] * games * noise()))
        blocks      = max(0, round(base[4] * games * noise()))
        pim         = max(0, round(base[5] * games * noise()))
        plus_minus  = round(base[6] * games * rng.uniform(0.5, 1.5))

        fp = (
            goals * _FP_GOAL
            + assists * _FP_ASSIST
            + plus_minus * _FP_PLUS
            + shots * _FP_SOG
            + hits * _FP_HIT
            + blocks * _FP_BLOCK
        )
        return goals, assists, shots, hits, blocks, pim, plus_minus, round(fp, 2)

    def _weekly_goalie_stats(
        self, profile: _PlayerProfile, week: int, team_idx: int, slot_idx: int
    ) -> tuple[int, int, int, float]:
        """Return (wins, saves, goals_against, fp) for a goalie."""
        rng = Random(self._seed + week * 100_000 + team_idx * 1_000 + slot_idx)
        games = max(0, round(profile.games_per_week * rng.uniform(0.6, 1.3)))
        if games == 0:
            return 0, 0, 0, 0.0

        base = _GOALIE_BASE[profile.tier]
        noise = lambda: rng.uniform(0.8, 1.2)  # noqa: E731

        wins  = sum(1 for _ in range(games) if rng.random() < base[0])
        saves = max(0, round(base[1] * games * noise()))
        ga    = max(0, round(base[2] * games * noise()))

        fp = wins * _FP_WIN + saves * _FP_SAVE + ga * _FP_GA
        return wins, saves, ga, round(fp, 2)

    def _team_total_fp(self, team_idx: int, week: int) -> float:
        """Compute total active-roster fantasy points for a team in a given week."""
        total = 0.0
        team = self._teams[team_idx]
        for slot_idx, (_, is_active, is_goalie) in enumerate(_ROSTER_LAYOUT):
            if not is_active:
                continue
            profile = team["profiles"][slot_idx]
            if is_goalie:
                _, _, _, fp = self._weekly_goalie_stats(profile, week, team_idx, slot_idx)
            else:
                _, _, _, _, _, _, _, fp = self._weekly_skater_stats(
                    profile, week, team_idx, slot_idx
                )
            total += fp
        return round(total, 2)

    # ------------------------------------------------------------------
    # Public generation interface
    # ------------------------------------------------------------------

    def generate_one(self, snapshot_index: int) -> fantasy_roster_pb2.FantasyRosterSnapshot:
        """Generate one FantasyRosterSnapshot.

        snapshot_index encodes (week, team): snapshot_index = week * n_teams + team_idx.
        Same (seed, snapshot_index) always yields the same snapshot.
        """
        team_idx = snapshot_index % self._n_teams
        week = snapshot_index // self._n_teams
        team = self._teams[team_idx]

        # Compute all teams' fantasy points for ranking.
        all_fps = [self._team_total_fp(t, week) for t in range(self._n_teams)]
        sorted_fps = sorted(enumerate(all_fps), key=lambda x: -x[1])
        rank_this_week = next(i + 1 for i, (t, _) in enumerate(sorted_fps) if t == team_idx)

        # Previous week rank for rank_delta.
        rank_delta = 0
        if week > 0:
            prev_fps = [self._team_total_fp(t, week - 1) for t in range(self._n_teams)]
            prev_sorted = sorted(enumerate(prev_fps), key=lambda x: -x[1])
            prev_rank = next(i + 1 for i, (t, _) in enumerate(prev_sorted) if t == team_idx)
            rank_delta = prev_rank - rank_this_week  # positive = moved up

        snap_rng = Random(self._seed + snapshot_index * 13_337)
        snapshot_id = str(UUID(int=snap_rng.getrandbits(128)))

        # Week timestamp: end of scoring week (Sunday midnight UTC).
        captured_dt = _SEASON_START + timedelta(weeks=week + 1)
        captured_ts = timestamp_pb2.Timestamp()
        captured_ts.FromDatetime(captured_dt)

        # ISO week label: 2024-W01 … 2024-W25.
        iso_year, iso_week, _ = captured_dt.isocalendar()
        scoring_week = f"{iso_year}-W{iso_week:02d}"

        # Build roster entries.
        roster: list[fantasy_roster_pb2.RosterEntry] = []
        total_fp = 0.0
        for slot_idx, (slot_type, is_active, is_goalie) in enumerate(_ROSTER_LAYOUT):
            profile = team["profiles"][slot_idx]
            if is_goalie:
                wins, saves, ga, fp = self._weekly_goalie_stats(
                    profile, week, team_idx, slot_idx
                )
                entry = fantasy_roster_pb2.RosterEntry(
                    player_id=profile.player_id,
                    player_name=profile.player_name,
                    slot_type=slot_type,
                    fantasy_points=fp,
                    wins=wins,
                    saves=saves,
                    goals_against=ga,
                    is_active=is_active,
                )
            else:
                g, a, sog, hits, blk, pim, pm, fp = self._weekly_skater_stats(
                    profile, week, team_idx, slot_idx
                )
                entry = fantasy_roster_pb2.RosterEntry(
                    player_id=profile.player_id,
                    player_name=profile.player_name,
                    slot_type=slot_type,
                    fantasy_points=fp,
                    goals=g,
                    assists=a,
                    shots_on_goal=sog,
                    hits=hits,
                    blocked_shots=blk,
                    penalty_minutes=pim,
                    plus_minus=pm,
                    is_active=is_active,
                )
            if is_active:
                total_fp += fp
            roster.append(entry)

        return fantasy_roster_pb2.FantasyRosterSnapshot(
            snapshot_id=snapshot_id,
            fantasy_team_id=team["team_id"],
            fantasy_team_name=team["team_name"],
            league_id=self._league_id,
            scoring_week=scoring_week,
            rank=rank_this_week,
            total_fantasy_points=round(total_fp, 2),
            rank_delta=rank_delta,
            roster=roster,
            captured_at=captured_ts,
        )

    def generate(self, weeks: int | None = None) -> list[fantasy_roster_pb2.FantasyRosterSnapshot]:
        """Generate all weekly snapshots for every team.

        Total snapshots = weeks × n_teams.

        Parameters
        ----------
        weeks:
            Override number of weeks (defaults to self._weeks).
        """
        total_weeks = weeks or self._weeks
        total = total_weeks * self._n_teams
        return [self.generate_one(i) for i in range(total)]

    def generate_range(
        self, start: int, end: int
    ) -> list[fantasy_roster_pb2.FantasyRosterSnapshot]:
        """Generate snapshots for indices [start, end). Useful for batched Delta writes."""
        if start >= end:
            return []
        return [self.generate_one(i) for i in range(start, end)]

    # ------------------------------------------------------------------
    # Serialization helpers
    # ------------------------------------------------------------------

    @staticmethod
    def snapshot_to_dict(snapshot: fantasy_roster_pb2.FantasyRosterSnapshot) -> dict:
        """Convert a FantasyRosterSnapshot proto to a JSON-serializable dict."""
        return json_format.MessageToDict(
            snapshot,
            always_print_fields_with_no_presence=False,
            preserving_proto_field_name=True,
        )

    @staticmethod
    def write_ndjson(
        path: str | Path,
        snapshots: list[fantasy_roster_pb2.FantasyRosterSnapshot],
    ) -> None:
        """Write snapshots to a newline-delimited JSON file."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w") as f:
            for snap in snapshots:
                f.write(json.dumps(GenFantasyRoster.snapshot_to_dict(snap)) + "\n")

    @staticmethod
    def read_ndjson(
        path: str | Path,
    ) -> list[fantasy_roster_pb2.FantasyRosterSnapshot]:
        """Read snapshots from a newline-delimited JSON file."""
        path = Path(path)
        snapshots = []
        with path.open() as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                d = json.loads(line)
                snap = fantasy_roster_pb2.FantasyRosterSnapshot()
                json_format.ParseDict(d, snap)
                snapshots.append(snap)
        return snapshots
