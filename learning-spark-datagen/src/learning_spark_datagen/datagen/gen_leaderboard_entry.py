"""Generate deterministic fake LeaderboardEntry protobufs and NDJSON I/O."""

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

from google.protobuf import json_format  # noqa: E402

from leaderboard.v1 import leaderboard_pb2  # noqa: E402

_BASE_TIME = datetime(2020, 1, 1, tzinfo=timezone.utc)


class GenLeaderboardEntry:
    """Generate one or more fake LeaderboardEntry protobufs (user_id, game_id, score, rank)."""

    def __init__(
        self,
        seed: int = 42,
        user_ids: list[str] | None = None,
        game_ids: list[str] | None = None,
    ) -> None:
        self._seed = seed
        self._rng = Random(seed)
        self._user_ids = user_ids
        self._game_ids = game_ids

    def generate_one(self, index: int = 0) -> leaderboard_pb2.LeaderboardEntry:
        """Generate a single LeaderboardEntry. Same (seed, index) yields the same entry."""
        rng = Random(self._seed + index)

        if self._user_ids:
            user_id = self._user_ids[index % len(self._user_ids)]
        else:
            user_id = str(UUID(int=rng.getrandbits(128)))

        if self._game_ids:
            game_id = self._game_ids[index % len(self._game_ids)]
        else:
            game_id = str(UUID(int=rng.getrandbits(128)))

        score = rng.randint(0, 10_000_000)
        rank = rng.randint(1, 10000)

        days_ago = rng.randint(0, 1800)
        played_dt = _BASE_TIME - timedelta(days=days_ago)
        played_at = int(played_dt.timestamp())

        return leaderboard_pb2.LeaderboardEntry(
            user_id=user_id,
            game_id=game_id,
            score=score,
            rank=rank,
            played_at=played_at,
        )

    def generate(self, count: int) -> list[leaderboard_pb2.LeaderboardEntry]:
        """Generate `count` deterministic leaderboard entries (same seed => same sequence)."""
        if count <= 0:
            return []
        return [self.generate_one(index=i) for i in range(count)]

    def generate_range(
        self, start: int, end: int
    ) -> list[leaderboard_pb2.LeaderboardEntry]:
        """Generate entries for indices [start, end). Useful for batched Delta writes."""
        if start >= end:
            return []
        return [self.generate_one(index=i) for i in range(start, end)]

    @staticmethod
    def entry_to_dict(entry: leaderboard_pb2.LeaderboardEntry) -> dict:
        """Convert a LeaderboardEntry proto to a JSON-serializable dict (NDJSON format)."""
        return json_format.MessageToDict(
            entry,
            always_print_fields_with_no_presence=False,
            preserving_proto_field_name=True,
        )

    @staticmethod
    def write_ndjson(
        path: str | Path,
        entries: list[leaderboard_pb2.LeaderboardEntry],
    ) -> None:
        """Write leaderboard entries to newline-delimited JSON (one JSON object per line)."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w") as f:
            for entry in entries:
                f.write(json.dumps(GenLeaderboardEntry.entry_to_dict(entry)) + "\n")

    @staticmethod
    def read_ndjson(path: str | Path) -> list[leaderboard_pb2.LeaderboardEntry]:
        """Read leaderboard entries from a newline-delimited JSON file."""
        path = Path(path)
        entries = []
        with path.open() as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                d = json.loads(line)
                entry = leaderboard_pb2.LeaderboardEntry()
                json_format.ParseDict(d, entry)
                entries.append(entry)
        return entries
