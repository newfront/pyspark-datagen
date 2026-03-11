"""Generate deterministic fake Game protobufs and NDJSON I/O."""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from random import Random
from uuid import UUID

_here = Path(__file__).resolve().parent
_root = _here.parent.parent.parent.parent
_gen = _root / "gen" / "python"
if _gen.exists():
    sys.path.insert(0, str(_gen))

from google.protobuf import json_format  # noqa: E402

from game.v1 import game_pb2  # noqa: E402

# Deterministic pool of game names and genres.
_GAME_NAME_STEMS = (
    "Shadow", "Dragon", "Cyber", "Star", "Dark", "Eternal", "Mega", "Ultra",
    "Battle", "Quest", "Legend", "Hero", "Storm", "Blade", "Chaos",
)
_GAME_NAME_SUFFIXES = (
    "Runner", "Strike", "Craft", "World", "Realm", "Arena", "Rise", "Fall",
    "Legacy", "Empire", "Odyssey", "Chronicles", "Online", "Unlimited",
)
_GENRES = ("Action", "RPG", "FPS", "Strategy", "Adventure", "Simulation", "Sports", "Puzzle")


class GenGame:
    """Generate one or more fake Game protobufs with a deterministic seed."""

    def __init__(self, seed: int = 42) -> None:
        self._seed = seed
        self._rng = Random(seed)

    def generate_one(self, index: int = 0) -> game_pb2.Game:
        """Generate a single Game. Same (seed, index) always yields the same game (including UUID)."""
        rng = Random(self._seed + index)
        uuid_val = str(UUID(int=rng.getrandbits(128)))
        name_stem = rng.choice(_GAME_NAME_STEMS)
        name_suffix = rng.choice(_GAME_NAME_SUFFIXES)
        name = f"{name_stem} {name_suffix}"
        if len(name) > 256:
            name = name[:256]
        genre = rng.choice(_GENRES)
        year = rng.randint(2010, 2024)
        month = rng.randint(1, 12)
        day = rng.randint(1, 28)
        released_dt = datetime(year, month, day, tzinfo=timezone.utc)
        released_at = int(released_dt.timestamp())

        return game_pb2.Game(
            uuid=uuid_val,
            name=name,
            genre=genre,
            released_at=released_at,
        )

    def generate(self, count: int) -> list[game_pb2.Game]:
        """Generate `count` deterministic games (same seed => same sequence)."""
        if count <= 0:
            return []
        return [self.generate_one(index=i) for i in range(count)]

    def generate_range(self, start: int, end: int) -> list[game_pb2.Game]:
        """Generate games for indices [start, end). Useful for batched Delta writes."""
        if start >= end:
            return []
        return [self.generate_one(index=i) for i in range(start, end)]

    @staticmethod
    def game_to_dict(game: game_pb2.Game) -> dict:
        """Convert a Game proto to a JSON-serializable dict (NDJSON format)."""
        return json_format.MessageToDict(
            game,
            always_print_fields_with_no_presence=False,
            preserving_proto_field_name=True,
        )

    @staticmethod
    def write_ndjson(path: str | Path, games: list[game_pb2.Game]) -> None:
        """Write games to newline-delimited JSON (one JSON object per line)."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w") as f:
            for game in games:
                f.write(json.dumps(GenGame.game_to_dict(game)) + "\n")

    @staticmethod
    def read_ndjson(path: str | Path) -> list[game_pb2.Game]:
        """Read games from a newline-delimited JSON file."""
        path = Path(path)
        games = []
        with path.open() as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                d = json.loads(line)
                game = game_pb2.Game()
                json_format.ParseDict(d, game)
                games.append(game)
        return games
