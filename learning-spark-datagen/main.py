"""learning-spark-datagen: generate fake users for learning Spark."""

import argparse
import json
import sys
from pathlib import Path

# When run from project root (e.g. uv run main.py), make the app and generated protos importable.
_root = Path(__file__).resolve().parent
for _path in (_root / "src", _root / "gen" / "python"):
    if _path.exists() and str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from learning_spark_datagen.datagen import GenUser


def main():
    parser = argparse.ArgumentParser(description="learning-spark-datagen")
    parser.add_argument("--generate", action="store_true", help="Run in generate mode")
    parser.add_argument(
        "--count", type=int, default=100, help="Number of records to generate"
    )
    parser.add_argument(
        "--seed", type=int, default=42, help="Random seed for deterministic generation"
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        metavar="FILE",
        help="Write users to newline-delimited JSON (stable UUIDs and timestamps)",
    )
    args = parser.parse_args()

    if args.generate:
        gen = GenUser(seed=args.seed)
        users = gen.generate(args.count)
        if args.output:
            GenUser.write_ndjson(args.output, users)
            print(f"Wrote {len(users)} users to {args.output}", file=sys.stderr)
        else:
            for user in users:
                print(json.dumps(GenUser.user_to_dict(user)))
        return
    print("Hello from learning-spark-datagen! Use --generate --count N [--output FILE] to generate users.")


if __name__ == "__main__":
    main()
