# pyspark-datagen

Need data? Need data that feels real? What about fake real data? This is the project for you. Sales pitch complete.

Generates deterministic, validated Protobuf records you can save as NDJSON or Delta tables. Five entity types live here today:

| `--type` | What | Links to |
|----------|------|----------|
| `users` | Identities (UUID, name, email, locale, status) | — |
| `orders` | Purchase orders with embedded line items | `users` |
| `products` | A retail catalog (defaults to "Tidewell" coastal athleisure) | — |
| `sessions` | GA4-style ecommerce event streams (one row per event) | `users` + `products` |
| `rain_sensors` | IoT time-series readings | — |

All messages pass `protovalidate` and are deterministic given a `--seed` + index.

## Quick try with Cursor or Claude Code

```text
Let's generate some data. 

1. please generate 10000 users. Save them to ~/Desktop/users as ndjson

2. then generate 1.5 million orders utilizing the 10k users we just generated and save the orders to ~/Desktop/delta/orders as a delta table. Do not generate json first for the orders.

3. Then convert the users from step 1 into a Delta table and save this to ~/Desktop/delta/users
```

## Using all three generators together (Tidewell buy-flow)

A linked workflow that produces a realistic GA4-style ecommerce dataset. Each session walks the standard buy funnel — `session_start → view_item_list → view_item → add_to_cart → view_cart → (remove_from_cart) → begin_checkout → add_shipping_info → add_payment_info → purchase → (refund)` — with configurable drop-off at every step.

### NDJSON inputs

```bash
cd learning-spark-datagen

# 1. Users (parent identities; 10k UUIDs to attribute sessions to)
uv run main.py --generate --type users --count 10000 --output ~/Desktop/users

# 2. Tidewell product catalog (300 SKUs of coastal athleisure)
uv run main.py --generate --type products --count 300 --output ~/Desktop/products

# 3. 500k GA4 sessions spread across 2026-01-01 to 2026-05-28
uv run main.py --generate --type sessions \
  --count 500000 \
  --users-file ~/Desktop/users \
  --products-file ~/Desktop/products \
  --start-date 2026-01-01 --end-date 2026-05-28 \
  --output ~/Desktop/delta/events --format delta
```

### All-Delta variant (`--users-table` / `--products-table`)

Generate users and products as Delta tables once, then point session generation at them. `--format` auto-elevates to `delta` whenever a `--*-table` flag is used:

```bash
cd learning-spark-datagen

uv run main.py --generate --type users    --count 10000 --output ~/Desktop/delta/users    --format delta
uv run main.py --generate --type products --count 300   --output ~/Desktop/delta/products --format delta

uv run main.py --generate --type sessions --count 500000 \
  --users-table    ~/Desktop/delta/users \
  --products-table ~/Desktop/delta/products \
  --start-date 2026-01-01 --end-date 2026-05-28 \
  --output ~/Desktop/delta/events
```

500k sessions expands to roughly 3.0–3.5 million `event.v1.Event` rows. Each event carries the user, session, device, geo, traffic source, an `items[]` array referencing real product SKUs, and a strongly typed `payload` (`oneof`) for the per-event fields (e.g. `purchase.transaction_id`, `add_shipping_info.shipping_tier`, `refund.refund_amount`).

### Maven proxy for Spark

If `repo1.maven.org` is blocked in your environment, export `MAVEN_PROXY` before running any Delta workflow — the SparkSession builder wires it into `spark.jars.repositories` so `delta-spark` and `spark-protobuf` are fetched through the mirror:

```bash
export MAVEN_PROXY=https://your-maven-mirror.example.com
```

No vendor URL is hardcoded in the repo; the env var is the only source.

**Tune the funnel:**

```bash
# Higher purchase rate, more impulsive shoppers
--funnel-profile aggressive

# Lots of window shopping, low conversion
--funnel-profile conservative

# Power-law of "loyal customers" (higher alpha = more skew)
--sessions-per-user-alpha 2.0
```

**Why no separate `--type orders` run?** Purchase events already carry `transaction_id`, `items[]`, `tax`, `shipping`, `payment_method`, and `shipping_tier`. The session events table is self-contained for transactional analytics. `--type orders` is still available for users who specifically want the legacy `order.v1.Order` shape.

## Agent guide

For a deeper dive (proto layout, file locations, funnel internals, contributing a new generator), see [AGENTS.md](AGENTS.md) and [.cursor/skills/add-datagen-generator/SKILL.md](.cursor/skills/add-datagen-generator/SKILL.md).
