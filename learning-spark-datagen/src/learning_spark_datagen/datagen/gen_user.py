"""Generate deterministic fake User protobufs and NDJSON I/O."""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from random import Random
from uuid import UUID

# Ensure generated protos are importable when run from project root (gen/python).
_here = Path(__file__).resolve().parent
# datagen -> learning_spark_datagen -> src -> project root
_root = _here.parent.parent.parent.parent
_gen = _root / "gen" / "python"
if _gen.exists():
    sys.path.insert(0, str(_gen))

from google.protobuf import json_format  # noqa: E402
from google.protobuf import timestamp_pb2  # noqa: E402
from faker import Faker  # noqa: E402

from user.v1 import user_pb2  # noqa: E402

# Common IANA time zones and BCP-47 locales for variety.
_TIMEZONES = (
    "America/Los_Angeles",
    "America/New_York",
    "America/Chicago",
    "Europe/London",
    "Europe/Paris",
    "Asia/Tokyo",
    "Australia/Sydney",
    "UTC",
)
_LOCALES = ("en_US", "en_GB", "fr_FR", "de_DE", "ja_JP", "es_ES")


class GenUser:
    """Generate one or more fake User protobufs with a deterministic seed."""

    def __init__(self, seed: int = 42) -> None:
        self._seed = seed
        self._rng = Random(seed)
        # Base time in the past so created/updated are not in the future (validation).
        self._base_time = datetime(2020, 1, 1, tzinfo=timezone.utc)

    def generate_one(self, index: int = 0) -> user_pb2.User:
        """Generate a single User. Same (seed, index) always yields the same user (including UUID)."""
        rng = Random(self._seed + index)
        faker = Faker()
        faker.seed_instance(self._seed + index + 1)

        # Deterministic UUID from the seeded RNG.
        uuid_val = str(UUID(int=rng.getrandbits(128)))

        first_name = faker.first_name()[:42]
        last_name = faker.last_name()[:42]
        # Unique, deterministic email (avoids Faker.unique exhaustion and is reproducible).
        email = f"user{index}@example.com"

        # Created/updated in the past, deterministic per index.
        days_ago_created = rng.randint(1, 1800)
        days_ago_updated = rng.randint(0, days_ago_created)
        created_dt = self._base_time - timedelta(days=days_ago_created)
        updated_dt = self._base_time - timedelta(days=days_ago_updated)
        created_ts = timestamp_pb2.Timestamp()
        created_ts.FromDatetime(created_dt)
        updated_ts = timestamp_pb2.Timestamp()
        updated_ts.FromDatetime(updated_dt)

        user = user_pb2.User(
            uuid=uuid_val,
            first_name=first_name,
            last_name=last_name,
            email_address=email,
            created=created_ts,
            updated=updated_ts,
            time_zone=rng.choice(_TIMEZONES),
            status=rng.choice(
                (user_pb2.USER_STATUS_ACTIVE, user_pb2.USER_STATUS_INACTIVE)
            ),
            locale=rng.choice(_LOCALES),
        )
        return user

    def generate(self, count: int) -> list[user_pb2.User]:
        """Generate `count` unique, deterministic users (same seed => same sequence)."""
        if count <= 0:
            return []
        # Use a dedicated Faker sequence so unique.email() doesn't collide across indices.
        users = []
        for i in range(count):
            users.append(self.generate_one(index=i))
        return users

    @staticmethod
    def user_to_dict(user: user_pb2.User) -> dict:
        """Convert a User proto to a JSON-serializable dict (same format as NDJSON)."""
        return json_format.MessageToDict(
            user,
            always_print_fields_with_no_presence=False,
            preserving_proto_field_name=True,
        )

    @staticmethod
    def write_ndjson(path: str | Path, users: list[user_pb2.User]) -> None:
        """Write users to a newline-delimited JSON file (one JSON object per line)."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w") as f:
            for user in users:
                d = GenUser.user_to_dict(user)
                f.write(json.dumps(d) + "\n")

    @staticmethod
    def read_ndjson(path: str | Path) -> list[user_pb2.User]:
        """Read users from a newline-delimited JSON file (stable UUIDs and timestamps)."""
        path = Path(path)
        users = []
        with path.open() as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                d = json.loads(line)
                user = user_pb2.User()
                json_format.ParseDict(d, user)
                users.append(user)
        return users
