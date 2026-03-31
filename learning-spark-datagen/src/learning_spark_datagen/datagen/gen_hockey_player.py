"""Generate deterministic fake HockeyPlayer protobufs and NDJSON I/O.

Players model NHL-style roster records with realistic position distribution,
team assignments, nationality mix, and handedness drawn from known hockey
demographics.  The same (seed, index) always produces the same player.
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

from faker import Faker  # noqa: E402
from google.protobuf import json_format, timestamp_pb2  # noqa: E402

from hockey.v1 import player_pb2  # noqa: E402

# ---------------------------------------------------------------------------
# Reference data
# ---------------------------------------------------------------------------

# 32-team NHL roster as of 2024-25 season.
_NHL_TEAMS: list[tuple[str, str]] = [
    ("Anaheim Ducks", "ANA"),
    ("Boston Bruins", "BOS"),
    ("Buffalo Sabres", "BUF"),
    ("Calgary Flames", "CGY"),
    ("Carolina Hurricanes", "CAR"),
    ("Chicago Blackhawks", "CHI"),
    ("Colorado Avalanche", "COL"),
    ("Columbus Blue Jackets", "CBJ"),
    ("Dallas Stars", "DAL"),
    ("Detroit Red Wings", "DET"),
    ("Edmonton Oilers", "EDM"),
    ("Florida Panthers", "FLA"),
    ("Los Angeles Kings", "LAK"),
    ("Minnesota Wild", "MIN"),
    ("Montreal Canadiens", "MTL"),
    ("Nashville Predators", "NSH"),
    ("New Jersey Devils", "NJD"),
    ("New York Islanders", "NYI"),
    ("New York Rangers", "NYR"),
    ("Ottawa Senators", "OTT"),
    ("Philadelphia Flyers", "PHI"),
    ("Pittsburgh Penguins", "PIT"),
    ("San Jose Sharks", "SJS"),
    ("Seattle Kraken", "SEA"),
    ("St. Louis Blues", "STL"),
    ("Tampa Bay Lightning", "TBL"),
    ("Toronto Maple Leafs", "TOR"),
    ("Utah Hockey Club", "UTA"),
    ("Vancouver Canucks", "VAN"),
    ("Vegas Golden Knights", "VGK"),
    ("Washington Capitals", "WSH"),
    ("Winnipeg Jets", "WPG"),
]

# Position distribution based on typical NHL team roster construction:
# ~12 forwards, 6 D, 2 G per team.
_POSITIONS = [
    player_pb2.POSITION_CENTER,
    player_pb2.POSITION_LEFT_WING,
    player_pb2.POSITION_RIGHT_WING,
    player_pb2.POSITION_DEFENSE,
    player_pb2.POSITION_GOALIE,
]
_POSITION_WEIGHTS = [0.20, 0.20, 0.20, 0.30, 0.10]

# Nationality distribution approximates actual NHL player origins.
_NATIONALITIES = ["CAN", "USA", "SWE", "FIN", "RUS", "CZE", "SVK", "DEU", "CHE", "NOR"]
_NATIONALITY_WEIGHTS = [0.40, 0.20, 0.13, 0.10, 0.07, 0.05, 0.03, 0.01, 0.005, 0.005]

# ~60 % of NHL skaters shoot/catch left.
_HANDEDNESS = [player_pb2.HANDEDNESS_LEFT, player_pb2.HANDEDNESS_RIGHT]
_HANDEDNESS_WEIGHTS = [0.60, 0.40]

_STATUSES = [
    player_pb2.PLAYER_STATUS_ACTIVE,
    player_pb2.PLAYER_STATUS_INJURED,
    player_pb2.PLAYER_STATUS_SUSPENDED,
]
_STATUS_WEIGHTS = [0.85, 0.12, 0.03]


class GenHockeyPlayer:
    """Generate one or more fake HockeyPlayer protobufs with a deterministic seed.

    Parameters
    ----------
    seed:
        Random seed; same seed always yields the same player sequence.
    """

    def __init__(self, seed: int = 42) -> None:
        self._seed = seed
        self._base_time = datetime(2020, 1, 1, tzinfo=timezone.utc)

    def generate_one(self, index: int = 0) -> player_pb2.HockeyPlayer:
        """Generate a single HockeyPlayer.  Same (seed, index) → same player."""
        rng = Random(self._seed + index)
        faker = Faker()
        faker.seed_instance(self._seed + index + 1)

        player_id = str(UUID(int=rng.getrandbits(128)))

        first_name = faker.first_name()[:42]
        last_name = faker.last_name()[:42]

        team_name, team_abbr = rng.choice(_NHL_TEAMS)

        position = rng.choices(_POSITIONS, weights=_POSITION_WEIGHTS, k=1)[0]
        jersey_number = rng.randint(1, 99)
        nationality = rng.choices(_NATIONALITIES, weights=_NATIONALITY_WEIGHTS, k=1)[0]
        shoots_catches = rng.choices(_HANDEDNESS, weights=_HANDEDNESS_WEIGHTS, k=1)[0]
        status = rng.choices(_STATUSES, weights=_STATUS_WEIGHTS, k=1)[0]

        days_ago = rng.randint(30, 1800)
        created_dt = self._base_time - timedelta(days=days_ago)
        created_ts = timestamp_pb2.Timestamp()
        created_ts.FromDatetime(created_dt)

        return player_pb2.HockeyPlayer(
            player_id=player_id,
            first_name=first_name,
            last_name=last_name,
            team_name=team_name,
            team_abbreviation=team_abbr,
            position=position,
            jersey_number=jersey_number,
            nationality=nationality,
            shoots_catches=shoots_catches,
            status=status,
            created_at=created_ts,
        )

    def generate(self, count: int) -> list[player_pb2.HockeyPlayer]:
        """Generate `count` deterministic players."""
        if count <= 0:
            return []
        return [self.generate_one(index=i) for i in range(count)]

    def generate_range(self, start: int, end: int) -> list[player_pb2.HockeyPlayer]:
        """Generate players for indices [start, end). Useful for batched Delta writes."""
        if start >= end:
            return []
        return [self.generate_one(index=i) for i in range(start, end)]

    @staticmethod
    def player_to_dict(player: player_pb2.HockeyPlayer) -> dict:
        """Convert a HockeyPlayer proto to a JSON-serializable dict."""
        return json_format.MessageToDict(
            player,
            always_print_fields_with_no_presence=False,
            preserving_proto_field_name=True,
        )

    @staticmethod
    def write_ndjson(path: str | Path, players: list[player_pb2.HockeyPlayer]) -> None:
        """Write players to a newline-delimited JSON file."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w") as f:
            for p in players:
                f.write(json.dumps(GenHockeyPlayer.player_to_dict(p)) + "\n")

    @staticmethod
    def read_ndjson(path: str | Path) -> list[player_pb2.HockeyPlayer]:
        """Read players from a newline-delimited JSON file."""
        path = Path(path)
        players = []
        with path.open() as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                d = json.loads(line)
                p = player_pb2.HockeyPlayer()
                json_format.ParseDict(d, p)
                players.append(p)
        return players
