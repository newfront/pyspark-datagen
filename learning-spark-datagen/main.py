"""learning-spark-datagen: generate fake users and orders for learning Spark."""

import argparse
import json
import sys
from pathlib import Path

# When run from project root (e.g. uv run main.py), make the app and generated protos importable.
_root = Path(__file__).resolve().parent
for _path in (_root / "src", _root / "gen" / "python"):
    if _path.exists() and str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from learning_spark_datagen.datagen import GenUser, GenOrder  # noqa: E402
from learning_spark_datagen.utils import Converters, generate_spark_session  # noqa: E402

_DESCRIPTOR_PATH = _root / "gen" / "descriptors" / "descriptor.bin"


def main():
    parser = argparse.ArgumentParser(description="learning-spark-datagen")
    parser.add_argument("--generate", action="store_true", help="Run in generate mode")
    parser.add_argument(
        "--type",
        choices=("users", "orders"),
        default="users",
        help="Type of records to generate (default: users)",
    )
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
        metavar="PATH",
        help="Output path: file for JSON, Delta table directory for delta format",
    )
    parser.add_argument(
        "--format",
        choices=("json", "delta", "console"),
        default="json",
        help="Output format: json (default), delta (Delta table), or console (stdout)",
    )
    parser.add_argument(
        "--users-file",
        type=Path,
        default=None,
        metavar="FILE",
        help="NDJSON of users; order user_id will link to these UUIDs (for --type orders)",
    )
    args = parser.parse_args()

    if args.generate:
        if args.type == "users":
            gen = GenUser(seed=args.seed)
            records = gen.generate(args.count)
            to_dict = GenUser.user_to_dict
            write_ndjson = GenUser.write_ndjson
            message_name = "user.v1.User"
        else:
            user_ids = None
            if args.users_file and args.users_file.exists():
                user_ids = [u.uuid for u in GenUser.read_ndjson(args.users_file)]
            gen = GenOrder(seed=args.seed, user_ids=user_ids)
            records = gen.generate(args.count)
            to_dict = GenOrder.order_to_dict
            write_ndjson = GenOrder.write_ndjson
            message_name = "order.v1.Order"

        if args.format == "delta":
            if not args.output:
                print("error: --output PATH is required when --format delta", file=sys.stderr)
                sys.exit(1)
            if not _DESCRIPTOR_PATH.exists():
                print(
                    f"error: descriptor not found at {_DESCRIPTOR_PATH}; run 'make descriptor' or 'make build'",
                    file=sys.stderr,
                )
                sys.exit(1)
            data = [r.SerializeToString() for r in records]
            spark = generate_spark_session()
            df = Converters.protobuf_to_df(
                data=data,
                spark=spark,
                descriptor_path=_DESCRIPTOR_PATH,
                message_name=message_name,
                column_name="wrapper",
            )
            out_path = str(args.output.resolve())
            df.coalesce(1).write.format("delta").mode("overwrite").save(out_path)
            print(f"Wrote {len(records)} {args.type} to Delta table {out_path}", file=sys.stderr)
        elif args.format == "console" or (args.format == "json" and not args.output):
            for rec in records:
                print(json.dumps(to_dict(rec)))
        else:
            # format == "json" and args.output
            write_ndjson(args.output, records)
            print(f"Wrote {len(records)} {args.type} to {args.output}", file=sys.stderr)
        return
    print(
        "Hello from learning-spark-datagen! Use --generate --type users|orders --count N "
        "[--output PATH] [--format json|delta|console]."
    )


if __name__ == "__main__":
    main()
