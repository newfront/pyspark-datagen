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

from learning_spark_datagen.datagen import (  # noqa: E402
    GenUser,
    GenOrder,
    GenGame,
    GenLeaderboardEntry,
)
from learning_spark_datagen.utils import Converters, generate_spark_session  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description="learning-spark-datagen")
    parser.add_argument("--generate", action="store_true", help="Run in generate mode")
    parser.add_argument(
        "--type",
        choices=("users", "orders", "games", "leaderboard_entries"),
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
        metavar="FILE",
        help="Write records to newline-delimited JSON",
    )
    parser.add_argument(
        "--users-file",
        type=Path,
        default=None,
        metavar="FILE",
        help="NDJSON of users; order/leaderboard user_id will link to these UUIDs",
    )
    parser.add_argument(
        "--games-file",
        type=Path,
        default=None,
        metavar="FILE",
        help="NDJSON of games; leaderboard game_id will link to these UUIDs (for --type leaderboard_entries)",
    )
    parser.add_argument(
        "--format",
        choices=("json", "delta"),
        default="json",
        help="Output format: json (NDJSON) or delta (Delta table); default json",
    )
    args = parser.parse_args()

    if args.generate:
        if args.type == "users":
            gen = GenUser(seed=args.seed)
            to_dict = GenUser.user_to_dict
            write_ndjson = GenUser.write_ndjson
            message_name = "user.v1.User"
        elif args.type == "orders":
            user_ids = None
            if args.users_file and args.users_file.exists():
                user_ids = [u.uuid for u in GenUser.read_ndjson(args.users_file)]
            gen = GenOrder(seed=args.seed, user_ids=user_ids)
            to_dict = GenOrder.order_to_dict
            write_ndjson = GenOrder.write_ndjson
            message_name = "order.v1.Order"
        elif args.type == "games":
            gen = GenGame(seed=args.seed)
            to_dict = GenGame.game_to_dict
            write_ndjson = GenGame.write_ndjson
            message_name = "game.v1.Game"
        else:  # leaderboard_entries
            user_ids = None
            game_ids = None
            if args.users_file and args.users_file.exists():
                user_ids = [u.uuid for u in GenUser.read_ndjson(args.users_file)]
            if args.games_file and args.games_file.exists():
                game_ids = [g.uuid for g in GenGame.read_ndjson(args.games_file)]
            gen = GenLeaderboardEntry(
                seed=args.seed, user_ids=user_ids, game_ids=game_ids
            )
            to_dict = GenLeaderboardEntry.entry_to_dict
            write_ndjson = GenLeaderboardEntry.write_ndjson
            message_name = "leaderboard.v1.LeaderboardEntry"
        if args.output:
            if args.format == "delta":
                descriptor_path = _root / "gen" / "descriptors" / "descriptor.bin"
                if not descriptor_path.exists():
                    print(
                        f"Error: descriptor not found at {descriptor_path}. Run 'make descriptor'.",
                        file=sys.stderr,
                    )
                    sys.exit(1)
                spark = generate_spark_session()
                # Write in batches to avoid OOM when passing huge lists to Spark
                batch_size = 100_000
                total = args.count
                for start in range(0, total, batch_size):
                    end = min(start + batch_size, total)
                    batch = gen.generate_range(start, end)
                    data = [r.SerializeToString() for r in batch]
                    df = Converters.protobuf_to_df(
                        data=data,
                        spark=spark,
                        descriptor_path=descriptor_path,
                        message_name=message_name,
                    )
                    mode = "overwrite" if start == 0 else "append"
                    Converters.write_df_to_delta(df, args.output, mode=mode)
                print(f"Wrote {total} {args.type} to Delta table {args.output}", file=sys.stderr)
            else:
                records = gen.generate(args.count)
                write_ndjson(args.output, records)
                print(f"Wrote {len(records)} {args.type} to {args.output}", file=sys.stderr)
        else:
            records = gen.generate(args.count)
            for rec in records:
                print(json.dumps(to_dict(rec)))
        return
    print(
        "Hello from learning-spark-datagen! Use --generate --type users|orders|games|leaderboard_entries --count N [--output FILE]."
    )


if __name__ == "__main__":
    main()
