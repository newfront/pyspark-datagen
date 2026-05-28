"""learning-spark-datagen: generate fake users, orders, products, sessions, and rain-sensor readings."""

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path

# When run from project root (e.g. uv run main.py), make the app and generated protos importable.
_root = Path(__file__).resolve().parent
for _path in (_root / "src", _root / "gen" / "python"):
    if _path.exists() and str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from learning_spark_datagen.datagen import (  # noqa: E402
    FUNNEL_PROFILES,
    GenOrder,
    GenProduct,
    GenRainSensor,
    GenSession,
    GenUser,
)
from learning_spark_datagen.utils import Converters, generate_spark_session  # noqa: E402


def _parse_iso_date(s: str) -> date:
    """Parse a YYYY-MM-DD string into a date (argparse type)."""
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError as e:
        raise argparse.ArgumentTypeError(
            f"invalid date {s!r}, expected YYYY-MM-DD"
        ) from e


def main():
    parser = argparse.ArgumentParser(description="learning-spark-datagen")
    parser.add_argument("--generate", action="store_true", help="Run in generate mode")
    parser.add_argument(
        "--type",
        choices=("users", "orders", "rain_sensors", "products", "sessions"),
        default="users",
        help="Type of records to generate (default: users)",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=100,
        help="Number of records (or sessions, for --type sessions) to generate",
    )
    parser.add_argument(
        "--seed", type=int, default=42, help="Random seed for deterministic generation"
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        metavar="FILE",
        help="Write records to newline-delimited JSON (or Delta dir with --format delta)",
    )
    users_src = parser.add_mutually_exclusive_group()
    users_src.add_argument(
        "--users-file",
        type=Path,
        default=None,
        metavar="FILE",
        help="NDJSON of users; --type orders|sessions links user_id to these UUIDs",
    )
    users_src.add_argument(
        "--users-table",
        type=Path,
        default=None,
        metavar="DELTA_DIR",
        help="Delta table of users (alternative to --users-file). Reads via Spark.",
    )
    products_src = parser.add_mutually_exclusive_group()
    products_src.add_argument(
        "--products-file",
        type=Path,
        default=None,
        metavar="FILE",
        help="NDJSON of products; --type sessions links event items[] to these SKUs",
    )
    products_src.add_argument(
        "--products-table",
        type=Path,
        default=None,
        metavar="DELTA_DIR",
        help="Delta table of products (alternative to --products-file). Reads via Spark.",
    )
    parser.add_argument(
        "--start-date",
        type=_parse_iso_date,
        default=date(2026, 1, 1),
        help="--type sessions: earliest session date (YYYY-MM-DD, default 2026-01-01)",
    )
    parser.add_argument(
        "--end-date",
        type=_parse_iso_date,
        default=date(2026, 5, 28),
        help="--type sessions: latest session date (YYYY-MM-DD, default 2026-05-28)",
    )
    parser.add_argument(
        "--sessions-per-user-alpha",
        type=float,
        default=1.5,
        help="--type sessions: Zipf exponent for user picking (higher => more skew, default 1.5)",
    )
    parser.add_argument(
        "--funnel-profile",
        choices=tuple(FUNNEL_PROFILES.keys()),
        default="realistic",
        help="--type sessions: funnel drop-off profile (default realistic)",
    )
    parser.add_argument(
        "--num-sensors",
        type=int,
        default=20,
        help="Number of IoT sensor sites for --type rain_sensors (default: 20, max 20 unique locations)",
    )
    parser.add_argument(
        "--interval-minutes",
        type=int,
        default=5,
        help="Sampling interval in minutes for --type rain_sensors (default: 5)",
    )
    parser.add_argument(
        "--format",
        choices=("json", "delta"),
        default=None,
        help=(
            "Output format: json (NDJSON) or delta (Delta table). "
            "Defaults to 'delta' when --users-table or --products-table is set, "
            "otherwise 'json'."
        ),
    )
    args = parser.parse_args()

    # Smart default for --format: if any input is a Delta table, default output to delta.
    if args.format is None:
        args.format = "delta" if (args.users_table or args.products_table) else "json"

    if args.generate:
        # Single Spark session shared between Delta inputs (--*-table) and Delta output.
        # Created lazily on first need so json-only flows keep their fast start.
        spark = None

        def _ensure_spark():
            nonlocal spark
            if spark is None:
                spark = generate_spark_session()
            return spark

        def _load_users() -> list[str]:
            """Load user UUIDs from --users-file or --users-table (whichever was given)."""
            if args.users_file and args.users_file.exists():
                return [u.uuid for u in GenUser.read_ndjson(args.users_file)]
            if args.users_table and args.users_table.exists():
                return [
                    u.uuid
                    for u in GenUser.read_delta(_ensure_spark(), args.users_table)
                ]
            return []

        def _load_products():
            """Load Product messages from --products-file or --products-table."""
            if args.products_file and args.products_file.exists():
                return GenProduct.read_ndjson(args.products_file)
            if args.products_table and args.products_table.exists():
                return GenProduct.read_delta(_ensure_spark(), args.products_table)
            return []

        if args.type == "users":
            gen = GenUser(seed=args.seed)
            to_dict = GenUser.user_to_dict
            write_ndjson = GenUser.write_ndjson
            message_name = "user.v1.User"
        elif args.type == "orders":
            user_ids = _load_users() or None
            gen = GenOrder(seed=args.seed, user_ids=user_ids)
            to_dict = GenOrder.order_to_dict
            write_ndjson = GenOrder.write_ndjson
            message_name = "order.v1.Order"
        elif args.type == "products":
            gen = GenProduct(seed=args.seed)
            to_dict = GenProduct.product_to_dict
            write_ndjson = GenProduct.write_ndjson
            message_name = "product.v1.Product"
        elif args.type == "sessions":
            user_ids = _load_users()
            products = _load_products()
            if not user_ids:
                print(
                    "Error: --type sessions requires --users-file or --users-table.",
                    file=sys.stderr,
                )
                sys.exit(1)
            if not products:
                print(
                    "Error: --type sessions requires --products-file or --products-table.",
                    file=sys.stderr,
                )
                sys.exit(1)
            if args.end_date < args.start_date:
                print(
                    f"Error: --end-date {args.end_date} must be >= --start-date {args.start_date}",
                    file=sys.stderr,
                )
                sys.exit(1)
            gen = GenSession(
                seed=args.seed,
                user_ids=user_ids,
                products=products,
                start_date=args.start_date,
                end_date=args.end_date,
                funnel_config=FUNNEL_PROFILES[args.funnel_profile],
                sessions_per_user_alpha=args.sessions_per_user_alpha,
            )
            to_dict = GenSession.event_to_dict
            write_ndjson = GenSession.write_ndjson
            message_name = "event.v1.Event"
        else:
            gen = GenRainSensor(
                seed=args.seed,
                num_sensors=args.num_sensors,
                interval_minutes=args.interval_minutes,
            )
            to_dict = GenRainSensor.reading_to_dict
            write_ndjson = GenRainSensor.write_ndjson
            message_name = "rain_sensor.v1.RainSensorReading"
        if args.output:
            if args.format == "delta":
                descriptor_path = _root / "gen" / "descriptors" / "descriptor.bin"
                if not descriptor_path.exists():
                    print(
                        f"Error: descriptor not found at {descriptor_path}. Run 'make descriptor'.",
                        file=sys.stderr,
                    )
                    sys.exit(1)
                spark = _ensure_spark()
                # Write in batches to avoid OOM when passing huge lists to Spark.
                # For --type sessions, each "record" in the range is a full session that
                # expands to ~6 Event rows on average; we batch by session count.
                batch_size = 20_000 if args.type == "sessions" else 100_000
                total = args.count
                total_events_written = 0
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
                    total_events_written += len(batch)
                if args.type == "sessions":
                    print(
                        f"Wrote {total} sessions ({total_events_written} events) "
                        f"to Delta table {args.output}",
                        file=sys.stderr,
                    )
                else:
                    print(
                        f"Wrote {total} {args.type} to Delta table {args.output}",
                        file=sys.stderr,
                    )
            else:
                records = gen.generate(args.count)
                write_ndjson(args.output, records)
                if args.type == "sessions":
                    print(
                        f"Wrote {args.count} sessions ({len(records)} events) to {args.output}",
                        file=sys.stderr,
                    )
                else:
                    print(
                        f"Wrote {len(records)} {args.type} to {args.output}",
                        file=sys.stderr,
                    )
        else:
            records = gen.generate(args.count)
            for rec in records:
                print(json.dumps(to_dict(rec)))
        return
    print(
        "Hello from learning-spark-datagen! "
        "Use --generate --type users|orders|products|sessions|rain_sensors "
        "--count N [--output FILE]."
    )


if __name__ == "__main__":
    main()
