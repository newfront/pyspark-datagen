#!/usr/bin/env python3
"""Convert an NDJSON file to a Delta table. Run from project root: uv run python scripts/ndjson_to_delta.py <ndjson_path> <delta_path> [user.v1.User|order.v1.Order]."""

import sys
from pathlib import Path

_root = Path(__file__).resolve().parent.parent
for _path in (_root / "src", _root / "gen" / "python"):
    if _path.exists() and str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from learning_spark_datagen.utils import generate_spark_session, ndjson_file_to_delta

DESCRIPTOR_PATH = _root / "gen" / "descriptors" / "descriptor.bin"


def main():
    if len(sys.argv) < 4:
        print("Usage: ndjson_to_delta.py <ndjson_path> <delta_path> <message_name>", file=sys.stderr)
        print("  message_name: user.v1.User or order.v1.Order", file=sys.stderr)
        sys.exit(1)
    ndjson_path = Path(sys.argv[1]).expanduser()
    delta_path = Path(sys.argv[2]).expanduser()
    message_name = sys.argv[3]
    if not ndjson_path.exists():
        print(f"Error: {ndjson_path} not found", file=sys.stderr)
        sys.exit(1)
    if not DESCRIPTOR_PATH.exists():
        print(f"Error: descriptor not found at {DESCRIPTOR_PATH}", file=sys.stderr)
        sys.exit(1)
    spark = generate_spark_session()
    ndjson_file_to_delta(
        str(ndjson_path),
        message_name,
        spark,
        str(DESCRIPTOR_PATH),
        str(delta_path),
    )
    print(f"Wrote Delta table to {delta_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
