"""Tests for GenUser and NDJSON round-trip."""

import os
import tempfile
from pathlib import Path

from learning_spark_datagen.datagen import GenUser


def test_generate_one_deterministic():
    gen = GenUser(seed=42)
    u1 = gen.generate_one(index=0)
    u2 = gen.generate_one(index=0)
    assert u1.uuid == u2.uuid
    assert u1.first_name == u2.first_name
    assert u1.email_address == u2.email_address


def test_generate_count():
    gen = GenUser(seed=42)
    users = gen.generate(10)
    assert len(users) == 10
    uuids = {u.uuid for u in users}
    assert len(uuids) == 10
    emails = {u.email_address for u in users}
    assert len(emails) == 10


def test_generate_1000_unique():
    gen = GenUser(seed=42)
    users = gen.generate(1000)
    assert len(users) == 1000
    assert len({u.uuid for u in users}) == 1000
    assert len({u.email_address for u in users}) == 1000


def test_ndjson_round_trip():
    gen = GenUser(seed=42)
    users = gen.generate(5)
    fd, name = tempfile.mkstemp(suffix=".ndjson")
    os.close(fd)
    path = Path(name)
    try:
        GenUser.write_ndjson(path, users)
        read_back = GenUser.read_ndjson(path)
        assert len(read_back) == 5
        for a, b in zip(users, read_back):
            assert a.uuid == b.uuid
            assert a.email_address == b.email_address
            assert a.created.seconds == b.created.seconds
    finally:
        path.unlink(missing_ok=True)
