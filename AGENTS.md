# Agent guide: learning-spark-datagen

Quick reference for AI agents (and humans) so you don’t have to scan the README or multiple files to run data generation and Delta workflows.

## Where to run

All commands below assume you’re in **`learning-spark-datagen/`** (the package with `main.py` and `pyproject.toml`). Use `uv run` to execute the CLI.

```bash
cd learning-spark-datagen
```

## CLI: `main.py`

**Entry point:** `uv run main.py` (or `uv run main.py --generate ...`).

| Flag | Purpose |
|------|--------|
| `--generate` | Enable generate mode (required for data gen). |
| `--type users \| orders \| products \| sessions \| rain_sensors` | Entity to generate (default: `users`). |
| `--count N` | Number of records — or **sessions** for `--type sessions` — (default: 100). |
| `--output PATH` | File path for JSON/NDJSON; **required** for `--format delta` (Delta table directory). |
| `--format json \| delta` | `json` = NDJSON to file (or stdout if `--output` omitted); `delta` = Delta table. Auto-defaults to `delta` when `--users-table` or `--products-table` is set; otherwise `json`. |
| `--users-file PATH` | For `--type orders` and `--type sessions`. NDJSON file of users; `user_id` will be one of these UUIDs. |
| `--users-table DELTA_DIR` | Alternative to `--users-file`; reads users from a Delta table via Spark. Mutually exclusive with `--users-file`. |
| `--products-file PATH` | **For `--type sessions` only.** NDJSON file of products; every event's `items[].item_id` will reference these products. |
| `--products-table DELTA_DIR` | Alternative to `--products-file`; reads the product catalog from a Delta table via Spark. Mutually exclusive with `--products-file`. |
| `--start-date YYYY-MM-DD` | **For `--type sessions` only.** Earliest session date (default `2026-01-01`). |
| `--end-date YYYY-MM-DD` | **For `--type sessions` only.** Latest session date (default `2026-05-28`). |
| `--sessions-per-user-alpha FLOAT` | **For `--type sessions` only.** Zipf exponent for user picking (default 1.5; higher = more skew toward loyal customers). |
| `--funnel-profile realistic \| aggressive \| conservative` | **For `--type sessions` only.** Built-in funnel drop-off profile (default `realistic`). |
| `--seed N` | Random seed (default: 42). |

**Prerequisite for Delta:** `gen/descriptors/descriptor.bin` must exist (e.g. `make descriptor` or `make build` from `learning-spark-datagen/`).

## Common workflows

**1. Generate users as NDJSON**

```bash
uv run main.py --generate --type users --count 5000 --output /path/to/users
```

Writes one JSON object per line (NDJSON) to the given path. No file extension required.

**2. Generate orders linked to those users, as Delta**

```bash
uv run main.py --generate --type orders --count 100000 --users-file /path/to/users --output /path/to/delta/orders --format delta
```

Use the same users file from step 1 so `user_id` in orders references real user UUIDs.

**3. Convert existing NDJSON to a Delta table**

The main CLI does **not** convert NDJSON → Delta. Use the helper script:

```bash
uv run python scripts/ndjson_to_delta.py <ndjson_path> <delta_path> <message_name>
```

- `message_name`: `user.v1.User`, `order.v1.Order`, `product.v1.Product`, `event.v1.Event`, or `rain_sensor.v1.RainSensorReading`.
- Example: `uv run python scripts/ndjson_to_delta.py ~/Desktop/users ~/Desktop/delta/users user.v1.User`

## Tidewell buy-flow (GA4-style sessions)

A GA4-style ecommerce dataset for a fictional Tidewell (coastal athleisure) brand. Each "session" is a sequence of `event.v1.Event` records sharing one `session_id` that walks the funnel: `session_start → view_item_list → view_item → add_to_cart → view_cart → (remove_from_cart) → begin_checkout → add_shipping_info → add_payment_info → purchase → (refund)`. Drop-off probabilities at each step come from `FunnelConfig`.

**Three-step recipe** (Users → Products → Sessions; NDJSON inputs):

```bash
# 1. Users (parent identities)
uv run main.py --generate --type users --count 10000 --output ~/Desktop/users

# 2. Tidewell product catalog
uv run main.py --generate --type products --count 300 --output ~/Desktop/products

# 3. 500k GA4 sessions over a date range
uv run main.py --generate --type sessions \
  --count 500000 \
  --users-file ~/Desktop/users \
  --products-file ~/Desktop/products \
  --start-date 2026-01-01 --end-date 2026-05-28 \
  --output ~/Desktop/delta/events --format delta
```

500k sessions expands to ~3.0–3.5M `event.v1.Event` rows. Sessions are written in 20k-session batches (~120k events per batch) by `main.py`. Purchase events carry a `transaction_id`, so the dataset is self-contained — you do **not** need a separate `--type orders` run for analytics that key off transactions.

**All-Delta variant** (read parents from Delta tables; `--format delta` is auto-elevated):

```bash
# 1. Users -> Delta
uv run main.py --generate --type users --count 10000 \
  --output ~/Desktop/delta/users --format delta

# 2. Products -> Delta
uv run main.py --generate --type products --count 300 \
  --output ~/Desktop/delta/products --format delta

# 3. Sessions reading from Delta, writing to Delta (no --format needed)
uv run main.py --generate --type sessions --count 500000 \
  --users-table    ~/Desktop/delta/users \
  --products-table ~/Desktop/delta/products \
  --start-date 2026-01-01 --end-date 2026-05-28 \
  --output ~/Desktop/delta/events
```

Rules:
- `--users-file` and `--users-table` are **mutually exclusive** (argparse-enforced). Same for products.
- When **any** `--*-table` flag is set, `--format` defaults to `delta`. Pass `--format json` explicitly to override.
- Delta reads use `GenUser.read_delta(spark, path)` / `GenProduct.read_delta(spark, path)` — Spark `toJSON().collect()` then `json_format.ParseDict` into the proto. The single Spark session is shared between input reads and output writes.
- `--type orders` also accepts `--users-table` for users sourced from Delta.

**Funnel profiles** (in `src/learning_spark_datagen/datagen/gen_session.py`):

| Profile | Buy-flow shape |
|---------|-----------------|
| `realistic` (default) | ~5% purchase rate; standard GA4-like decay. |
| `aggressive` | ~15% purchase rate; higher add-to-cart and checkout completion. Good for "high-intent" demos. |
| `conservative` | ~1% purchase rate; lots of window shopping. Good for retention/abandonment studies. |

**User skew (Zipf):** `--sessions-per-user-alpha` controls how concentrated sessions are. With 50 users and 1000 sessions at `alpha=1.5`, roughly ~80 users (when given 200) appear, with a power-law tail — a small set of "loyal customers" appear many times.

## Key file locations

| What | Where |
|------|--------|
| CLI entry point | `learning-spark-datagen/main.py` |
| NDJSON → Delta script | `learning-spark-datagen/scripts/ndjson_to_delta.py` |
| Protobuf→DataFrame + Delta write (API) | `src/learning_spark_datagen/utils/converters.py` |
| Descriptor (required for Delta) | `learning-spark-datagen/gen/descriptors/descriptor.bin` |
| Generators | `src/learning_spark_datagen/datagen/gen_user.py`, `gen_order.py`, `gen_product.py`, `gen_session.py`, `gen_rain_sensor.py` |
| Shared `Amount` proto | `protos/commerce/v1/amount.proto` |
| Product proto | `protos/product/v1/product.proto` |
| Event proto (envelope + oneof payload) | `protos/event/v1/event.proto` |
| Funnel profiles + `FunnelConfig` | `src/learning_spark_datagen/datagen/gen_session.py` |

## Dependency source notes

- Keep repository/index URLs vendor-neutral in project files (`pyproject.toml`, `uv.lock`, Spark config).
- Do not hardcode Databricks proxy URLs; if a proxy is required in a local environment, set it via user/tooling config outside the repo.

### Maven proxy for Spark `--packages`

Spark's Delta/protobuf jars come from `spark.jars.packages`. By default the resolver hits `repo1.maven.org`; if that's blocked in your environment, set the `MAVEN_PROXY` env var to your mirror URL **before** invoking the CLI or running tests:

```bash
export MAVEN_PROXY=https://your-maven-mirror.example.com
uv run main.py --generate --type users --count 200 --output ~/Desktop/delta/users --format delta
```

When `MAVEN_PROXY` is set, [src/learning_spark_datagen/utils/spark_session.py](learning-spark-datagen/src/learning_spark_datagen/utils/spark_session.py) wires it into Spark via `spark.jars.repositories` at SparkSession builder time. It must be set at build time (not via `spark.conf.set(...)` after the session exists) because package resolution runs during launch. The repo never hardcodes a vendor URL — the env var is the only source.

## Summary

- **Users then orders:** Generate users to NDJSON → generate orders with `--users-file` (and optionally `--format delta`).
- **Tidewell buy-flow:** Users → Products → Sessions. Sessions produce GA4-style events (`event.v1.Event`) with a single envelope + `oneof` payload per event type. Use `--funnel-profile` to tune drop-off and `--sessions-per-user-alpha` to tune user skew.
- **Existing NDJSON → Delta:** Use `scripts/ndjson_to_delta.py` (supports `user.v1.User`, `order.v1.Order`, `product.v1.Product`, `event.v1.Event`, `rain_sensor.v1.RainSensorReading`); not exposed on the main CLI.
- **Delta writes:** Always need `--output` and `gen/descriptors/descriptor.bin` present.
