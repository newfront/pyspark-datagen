"""learning-spark-datagen: generate fake users, orders, leaderboard snapshots, and hockey data."""

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
    GenUser, GenOrder, GenLeaderboard,
    GenHockeyPlayer, GenPlayEvent, GenFantasyRoster,
)
from learning_spark_datagen.utils import Converters, generate_spark_session  # noqa: E402

_HOCKEY_TYPES = ("hockey_players", "play_events", "fantasy_rosters")


def main():
    parser = argparse.ArgumentParser(
        description="learning-spark-datagen",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Modes:\n"
            "  --generate   Generate fake data\n"
            "  --analyze    Analyze a Delta table and print reports\n"
            "\n"
            "Type choices for --generate:\n"
            "  users, orders, leaderboard_snapshots\n"
            "  hockey_players, play_events, fantasy_rosters\n"
            "\n"
            "Analyze type choices for --analyze:\n"
            "  leaderboard_snapshots (default), fantasy_rosters\n"
        ),
    )

    # ── Mode flags ────────────────────────────────────────────────────────
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--generate", action="store_true", help="Run in generate mode")
    mode_group.add_argument(
        "--analyze",
        action="store_true",
        help="Analyze a Delta table and print reports",
    )

    # ── Generate flags ────────────────────────────────────────────────────
    parser.add_argument(
        "--type",
        choices=(
            "users", "orders", "leaderboard_snapshots",
            "hockey_players", "play_events", "fantasy_rosters",
        ),
        default="users",
        help="Type of records to generate (default: users)",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=100,
        help=(
            "Number of records to generate. "
            "For leaderboard_snapshots: number of days (each day = 24 hourly snapshots). "
            "For fantasy_rosters: number of scoring weeks (each week × 10 teams per batch)."
        ),
    )
    parser.add_argument(
        "--seed", type=int, default=42, help="Random seed for deterministic generation"
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        metavar="PATH",
        help="Write records to newline-delimited JSON or Delta table (see --format)",
    )
    parser.add_argument(
        "--users-file",
        type=Path,
        default=None,
        metavar="FILE",
        help=(
            "NDJSON of users; order user_id will link to these UUIDs (for --type orders); "
            "player_id will link to these UUIDs (for --type leaderboard_snapshots)."
        ),
    )
    parser.add_argument(
        "--players-file",
        type=Path,
        default=None,
        metavar="FILE",
        help=(
            "NDJSON of hockey players; player_id will link to these UUIDs "
            "(for --type play_events and --type fantasy_rosters)."
        ),
    )
    parser.add_argument(
        "--format",
        choices=("json", "delta"),
        default="json",
        help="Output format: json (NDJSON) or delta (Delta table); default json",
    )

    # ── Analyze flags ─────────────────────────────────────────────────────
    parser.add_argument(
        "--input",
        type=Path,
        default=None,
        metavar="PATH",
        help="Delta table directory to analyze (required for --analyze)",
    )
    parser.add_argument(
        "--analyze-type",
        choices=("leaderboard_snapshots", "fantasy_rosters"),
        default="leaderboard_snapshots",
        help="Kind of Delta table to analyze (default: leaderboard_snapshots)",
    )

    args = parser.parse_args()

    # ── Analyze mode ──────────────────────────────────────────────────────
    if args.analyze:
        if not args.input:
            print("Error: --analyze requires --input PATH (Delta table directory).", file=sys.stderr)
            sys.exit(1)
        if not args.input.exists():
            print(f"Error: Delta table not found at {args.input}.", file=sys.stderr)
            sys.exit(1)
        spark = generate_spark_session()
        if args.analyze_type == "fantasy_rosters":
            from learning_spark_datagen.analysis import analyze_fantasy_rosters
            analyze_fantasy_rosters(args.input, spark)
        else:
            from learning_spark_datagen.analysis import analyze_leaderboard
            analyze_leaderboard(args.input, spark)
        return

    # ── Generate mode ─────────────────────────────────────────────────────
    if args.generate:
        if args.type == "users":
            gen = GenUser(seed=args.seed)
            to_dict = GenUser.user_to_dict
            write_ndjson = GenUser.write_ndjson
            message_name = "user.v1.User"
            records_label = "users"

        elif args.type == "orders":
            user_ids = None
            if args.users_file and args.users_file.exists():
                user_ids = [u.uuid for u in GenUser.read_ndjson(args.users_file)]
            gen = GenOrder(seed=args.seed, user_ids=user_ids)
            to_dict = GenOrder.order_to_dict
            write_ndjson = GenOrder.write_ndjson
            message_name = "order.v1.Order"
            records_label = "orders"

        elif args.type == "leaderboard_snapshots":
            user_ids = None
            if args.users_file and args.users_file.exists():
                user_ids = [u.uuid for u in GenUser.read_ndjson(args.users_file)]
            gen = GenLeaderboard(seed=args.seed, user_ids=user_ids, days=args.count)
            to_dict = GenLeaderboard.snapshot_to_dict
            write_ndjson = GenLeaderboard.write_ndjson
            message_name = "leaderboard.v1.LeaderboardSnapshot"
            records_label = "leaderboard_snapshots"

        elif args.type == "hockey_players":
            gen = GenHockeyPlayer(seed=args.seed)
            to_dict = GenHockeyPlayer.player_to_dict
            write_ndjson = GenHockeyPlayer.write_ndjson
            message_name = "hockey.v1.HockeyPlayer"
            records_label = "hockey_players"

        elif args.type == "play_events":
            player_ids = None
            if args.players_file and args.players_file.exists():
                player_ids = [p.player_id for p in GenHockeyPlayer.read_ndjson(args.players_file)]
            gen = GenPlayEvent(seed=args.seed, player_ids=player_ids)
            to_dict = GenPlayEvent.event_to_dict
            write_ndjson = GenPlayEvent.write_ndjson
            message_name = "hockey.v1.PlayEvent"
            records_label = "play_events"

        else:  # fantasy_rosters
            player_ids = None
            if args.players_file and args.players_file.exists():
                player_ids = [p.player_id for p in GenHockeyPlayer.read_ndjson(args.players_file)]
            gen = GenFantasyRoster(seed=args.seed, player_ids=player_ids, weeks=args.count)
            to_dict = GenFantasyRoster.snapshot_to_dict
            write_ndjson = GenFantasyRoster.write_ndjson
            message_name = "hockey.v1.FantasyRosterSnapshot"
            records_label = "fantasy_rosters"

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

                if args.type == "leaderboard_snapshots":
                    total_snapshots = args.count * 24
                    batch_size = 240
                    for start in range(0, total_snapshots, batch_size):
                        end = min(start + batch_size, total_snapshots)
                        batch = gen.generate_range(start, end)
                        data = [r.SerializeToString() for r in batch]
                        df = Converters.protobuf_to_df(
                            data=data, spark=spark,
                            descriptor_path=descriptor_path, message_name=message_name,
                        )
                        mode = "overwrite" if start == 0 else "append"
                        Converters.write_df_to_delta(df, args.output, mode=mode)
                    print(
                        f"Wrote {total_snapshots} {records_label} ({args.count} days × 24 h) "
                        f"to Delta table {args.output}",
                        file=sys.stderr,
                    )

                elif args.type == "fantasy_rosters":
                    # Batch by week: total = weeks × n_teams.
                    from learning_spark_datagen.datagen.gen_fantasy_roster import _N_TEAMS
                    total_snapshots = args.count * _N_TEAMS
                    batch_size = 50  # 5 weeks × 10 teams
                    for start in range(0, total_snapshots, batch_size):
                        end = min(start + batch_size, total_snapshots)
                        batch = gen.generate_range(start, end)
                        data = [r.SerializeToString() for r in batch]
                        df = Converters.protobuf_to_df(
                            data=data, spark=spark,
                            descriptor_path=descriptor_path, message_name=message_name,
                        )
                        mode = "overwrite" if start == 0 else "append"
                        Converters.write_df_to_delta(df, args.output, mode=mode)
                    print(
                        f"Wrote {total_snapshots} {records_label} ({args.count} weeks × {_N_TEAMS} teams) "
                        f"to Delta table {args.output}",
                        file=sys.stderr,
                    )

                else:
                    batch_size = 100_000
                    total = args.count
                    for start in range(0, total, batch_size):
                        end = min(start + batch_size, total)
                        batch = gen.generate_range(start, end)
                        data = [r.SerializeToString() for r in batch]
                        df = Converters.protobuf_to_df(
                            data=data, spark=spark,
                            descriptor_path=descriptor_path, message_name=message_name,
                        )
                        mode = "overwrite" if start == 0 else "append"
                        Converters.write_df_to_delta(df, args.output, mode=mode)
                    print(
                        f"Wrote {total} {records_label} to Delta table {args.output}",
                        file=sys.stderr,
                    )

            else:  # json / NDJSON
                if args.type == "leaderboard_snapshots":
                    records = gen.generate()
                elif args.type == "fantasy_rosters":
                    records = gen.generate()
                else:
                    records = gen.generate(args.count)
                write_ndjson(args.output, records)
                print(f"Wrote {len(records)} {records_label} to {args.output}", file=sys.stderr)

        else:
            if args.type == "leaderboard_snapshots":
                records = gen.generate()
            elif args.type == "fantasy_rosters":
                records = gen.generate()
            else:
                records = gen.generate(args.count)
            for rec in records:
                print(json.dumps(to_dict(rec)))
        return

    print(
        "Hello from learning-spark-datagen! "
        "Use --generate --type users|orders|leaderboard_snapshots|hockey_players|play_events|fantasy_rosters "
        "--count N [--output FILE] "
        "or --analyze --analyze-type leaderboard_snapshots|fantasy_rosters --input DELTA_PATH."
    )


if __name__ == "__main__":
    main()
