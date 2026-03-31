"""Generate deterministic fake PlayEvent protobufs and NDJSON I/O.

Each PlayEvent represents a single tracked in-game action (goal, shot, save,
hit, etc.) by one player during an NHL-style game.  player_id links to
HockeyPlayer.player_id; game_id groups events into individual games.

Event type frequencies are calibrated to NHL play-by-play statistics:
shots on goal dominate, goals are rare, faceoffs occur frequently, etc.

Usage
-----
gen = GenPlayEvent(seed=42, player_ids=[...])
events = gen.generate(count=10_000)
"""

from __future__ import annotations

import json
import sys
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

from hockey.v1 import play_event_pb2  # noqa: E402

# ---------------------------------------------------------------------------
# Reference data
# ---------------------------------------------------------------------------

# NHL season identifier used when none is supplied.
_DEFAULT_SEASON = "2024-25"

# Season epoch: first game of the 2024-25 regular season.
_SEASON_EPOCH = datetime(2024, 10, 8, tzinfo=timezone.utc)

# Team abbreviations for generating realistic matchups.
_TEAM_ABBRS = [
    "ANA", "BOS", "BUF", "CGY", "CAR", "CHI", "COL", "CBJ", "DAL", "DET",
    "EDM", "FLA", "LAK", "MIN", "MTL", "NSH", "NJD", "NYI", "NYR", "OTT",
    "PHI", "PIT", "SJS", "SEA", "STL", "TBL", "TOR", "UTA", "VAN", "VGK",
    "WSH", "WPG",
]

# Event type distribution calibrated to NHL play-by-play averages.
# A typical NHL game has ~65 SOG, ~50 faceoffs, ~20 hits, ~10 blocks, ~6 goals/assists.
_EVENT_TYPES = [
    play_event_pb2.EVENT_TYPE_SHOT_ON_GOAL,
    play_event_pb2.EVENT_TYPE_FACEOFF_WIN,
    play_event_pb2.EVENT_TYPE_FACEOFF_LOSS,
    play_event_pb2.EVENT_TYPE_HIT,
    play_event_pb2.EVENT_TYPE_BLOCKED_SHOT,
    play_event_pb2.EVENT_TYPE_SAVE,
    play_event_pb2.EVENT_TYPE_PENALTY,
    play_event_pb2.EVENT_TYPE_GOAL,
    play_event_pb2.EVENT_TYPE_ASSIST,
    play_event_pb2.EVENT_TYPE_TAKEAWAY,
    play_event_pb2.EVENT_TYPE_GIVEAWAY,
]
_EVENT_WEIGHTS = [32, 14, 14, 12, 8, 7, 5, 3, 3, 2, 2]

# Event types where a secondary_player_id is meaningful.
_SECONDARY_PLAYER_EVENTS = {
    play_event_pb2.EVENT_TYPE_GOAL,    # scorer → secondary assist player
    play_event_pb2.EVENT_TYPE_ASSIST,  # primary assist → secondary assist player
}

# Strength-state distribution: most play is 5-on-5 even strength.
_STRENGTH_STATES = [
    play_event_pb2.STRENGTH_STATE_EVEN,
    play_event_pb2.STRENGTH_STATE_POWER_PLAY,
    play_event_pb2.STRENGTH_STATE_PENALTY_KILL,
    play_event_pb2.STRENGTH_STATE_EMPTY_NET,
]
_STRENGTH_WEIGHTS = [0.75, 0.17, 0.06, 0.02]

# Period distribution: play is roughly even across three periods;
# ~5 % of events occur in overtime.
_PERIODS = [1, 2, 3, 4]
_PERIOD_WEIGHTS = [0.32, 0.34, 0.29, 0.05]

# Number of unique games in the season pool.
_GAMES_IN_POOL = 500


class GenPlayEvent:
    """Generate one or more fake PlayEvent protobufs with a deterministic seed.

    Parameters
    ----------
    seed:
        Random seed; same seed always yields the same event sequence.
    player_ids:
        Optional list of UUIDs from a HockeyPlayer NDJSON file.  player_id
        values are drawn from this pool (cycling if smaller than needed).
        When None, random UUIDs are generated deterministically from the seed.
    season:
        NHL season string, e.g. "2024-25".
    """

    def __init__(
        self,
        seed: int = 42,
        player_ids: list[str] | None = None,
        season: str = _DEFAULT_SEASON,
    ) -> None:
        self._seed = seed
        self._season = season

        # Build a fixed game-ID pool so events group naturally into games.
        pool_rng = Random(seed + 99_999)
        self._game_ids = [
            str(UUID(int=pool_rng.getrandbits(128))) for _ in range(_GAMES_IN_POOL)
        ]
        self._player_ids = player_ids or []

        # Build a fallback UUID pool for when no player_ids are supplied.
        if not self._player_ids:
            id_rng = Random(seed + 77_777)
            self._player_ids = [
                str(UUID(int=id_rng.getrandbits(128))) for _ in range(200)
            ]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _game_teams(self, game_id: str) -> tuple[str, str]:
        """Return (home_abbr, away_abbr) deterministically for a game UUID."""
        h = hash(game_id) & 0xFFFFFFFF
        home_idx = h % len(_TEAM_ABBRS)
        away_idx = (h // len(_TEAM_ABBRS) + 1) % len(_TEAM_ABBRS)
        if away_idx == home_idx:
            away_idx = (away_idx + 1) % len(_TEAM_ABBRS)
        return _TEAM_ABBRS[home_idx], _TEAM_ABBRS[away_idx]

    # ------------------------------------------------------------------
    # Public generation interface
    # ------------------------------------------------------------------

    def generate_one(self, index: int = 0) -> play_event_pb2.PlayEvent:
        """Generate a single PlayEvent.  Same (seed, index) → same event."""
        rng = Random(self._seed + index)

        event_id = str(UUID(int=rng.getrandbits(128)))
        game_id = rng.choice(self._game_ids)
        player_id = self._player_ids[index % len(self._player_ids)]

        event_type = rng.choices(_EVENT_TYPES, weights=_EVENT_WEIGHTS, k=1)[0]
        period = rng.choices(_PERIODS, weights=_PERIOD_WEIGHTS, k=1)[0]
        strength_state = rng.choices(_STRENGTH_STATES, weights=_STRENGTH_WEIGHTS, k=1)[0]

        # Game clock: 0:00–19:59 within the period.
        minutes = rng.randint(0, 19)
        seconds = rng.randint(0, 59)
        game_clock = f"{minutes:02d}:{seconds:02d}"

        # Secondary player only for goals and assists (50 % chance when applicable).
        secondary_player_id = ""
        if event_type in _SECONDARY_PLAYER_EVENTS and rng.random() < 0.50:
            # Pick a different player as the secondary.
            alt_idx = (index + 1) % len(self._player_ids)
            secondary_player_id = self._player_ids[alt_idx]

        # Spread events across the NHL season (82 games per team ≈ 4 months).
        days_offset = rng.randint(0, 120)
        hours_offset = rng.randint(0, 23)
        occurred_at_dt = _SEASON_EPOCH + timedelta(days=days_offset, hours=hours_offset)
        occurred_ts = timestamp_pb2.Timestamp()
        occurred_ts.FromDatetime(occurred_at_dt)

        home_team, away_team = self._game_teams(game_id)

        # Simulate a partially played game score (0–6 goals per side).
        home_score = rng.randint(0, 6)
        away_score = rng.randint(0, 6)

        return play_event_pb2.PlayEvent(
            event_id=event_id,
            game_id=game_id,
            player_id=player_id,
            event_type=event_type,
            period=period,
            game_clock=game_clock,
            strength_state=strength_state,
            secondary_player_id=secondary_player_id,
            occurred_at=occurred_ts,
            season=self._season,
            home_team=home_team,
            away_team=away_team,
            home_score=home_score,
            away_score=away_score,
        )

    def generate(self, count: int) -> list[play_event_pb2.PlayEvent]:
        """Generate `count` deterministic play events."""
        if count <= 0:
            return []
        return [self.generate_one(index=i) for i in range(count)]

    def generate_range(self, start: int, end: int) -> list[play_event_pb2.PlayEvent]:
        """Generate events for indices [start, end). Useful for batched Delta writes."""
        if start >= end:
            return []
        return [self.generate_one(index=i) for i in range(start, end)]

    # ------------------------------------------------------------------
    # Serialization helpers
    # ------------------------------------------------------------------

    @staticmethod
    def event_to_dict(event: play_event_pb2.PlayEvent) -> dict:
        """Convert a PlayEvent proto to a JSON-serializable dict."""
        return json_format.MessageToDict(
            event,
            always_print_fields_with_no_presence=False,
            preserving_proto_field_name=True,
        )

    @staticmethod
    def write_ndjson(path: str | Path, events: list[play_event_pb2.PlayEvent]) -> None:
        """Write events to a newline-delimited JSON file."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w") as f:
            for ev in events:
                f.write(json.dumps(GenPlayEvent.event_to_dict(ev)) + "\n")

    @staticmethod
    def read_ndjson(path: str | Path) -> list[play_event_pb2.PlayEvent]:
        """Read events from a newline-delimited JSON file."""
        path = Path(path)
        events = []
        with path.open() as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                d = json.loads(line)
                ev = play_event_pb2.PlayEvent()
                json_format.ParseDict(d, ev)
                events.append(ev)
        return events
