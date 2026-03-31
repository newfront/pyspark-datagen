# learning-spark-datagen

Generate fake data for learning Spark. Protos (e.g. `user.v1`) live under `protos/`; generated Python and descriptors live under `gen/`.

Run from the **project root** (`learning-spark-datagen/`). No `PYTHONPATH` or extra setup needed—the app adds `src` and `gen/python` to the path automatically.

## Prerequisites

### Buf CLI

Install the [Buf CLI](https://buf.build/docs/installation) so you can lint, format, and generate from the protos:

- **macOS (Homebrew):** `brew install bufbuild/buf/buf`
- **Other:** see [buf.build/docs/installation](https://buf.build/docs/installation)

### Buf registry login (required for `make build` / `make generate`)

Code generation uses remote Buf plugins (e.g. `buf.build/protocolbuffers/python`). To pull them, you must be logged in to the Buf Schema Registry:

1. Create a Buf account at [buf.build](https://buf.build) if needed.
2. Create a token: **Buf dashboard → Settings → Create a token** (or [buf.build/settings](https://buf.build/settings)).
3. Log in from this repo so the CLI can use the token for the current session:

   ```bash
   buf registry login
   ```

   When prompted, paste your token. This configures the Buf CLI for active use; repeat after opening a new terminal if you see auth errors during `buf generate`.

Without `buf registry login`, targets like `make build` and `make generate` can fail when Buf tries to fetch remote plugins.

## Project layout

- `protos/` — Protobuf definitions: `user/v1/user.proto`, `order/v1/order.proto`, `leaderboard/v1/leaderboard.proto`, and `hockey/v1/player.proto`, `hockey/v1/play_event.proto`, `hockey/v1/fantasy_roster.proto`.
- `gen/python/` — Generated Python packages from those protos (created by `buf generate`).
- `src/learning_spark_datagen/datagen/` — Data generators (`gen_user.py`, `gen_order.py`, `gen_leaderboard.py`, `gen_hockey_player.py`, `gen_play_event.py`, `gen_fantasy_roster.py`); each follows the same pattern (deterministic from seed, NDJSON I/O).
- `gen/descriptors/descriptor.bin` — Serialized `FileDescriptorSet` for Spark/ingest (created by `make descriptor`).
- `src/learning_spark_datagen/utils/` — `Converters.protobuf_to_df` for turning generated protobuf bytes into Spark DataFrames (e.g. for Delta Lake).
- `src/learning_spark_datagen/analysis/` — `analyze_leaderboard` and `analyze_fantasy_rosters` PySpark report functions.

## Commands

From the `learning-spark-datagen` directory:

| Command           | Description |
|-------------------|-------------|
| `make build`      | Lint/format protos, run `buf generate`, then format/lint Python. |
| `make generate`   | Run `make build`, then `main.py --generate --count 100`. |
| `make test`       | Run pytest. |
| `make descriptor` | Write `gen/descriptors/descriptor.bin` for Spark (all protos in one descriptor). |
| `make release [bump=patch\|minor\|major]` | Bump version in `pyproject.toml`. |
| `make package`    | Build wheel and sdist into `dist/`. |
| `make install-dist` | Install from `dist/` using Buf index for deps. |
| `make publish`    | Publish the package (e.g. to PyPI). |

## Data generation examples

All generators produce deterministic output: the same `--seed` and `--count` yield the same records (including UUIDs and timestamps).

### Users only

**Print users as newline-delimited JSON to stdout:**

```bash
# 10 users, default seed 42
uv run main.py --generate --type users --count 10

# 1000 users, custom seed
uv run main.py --generate --type users --count 1000 --seed 12345
```

**Write users to a file:**

```bash
uv run main.py --generate --type users --count 1000 --output users.ndjson
```

### Orders only (standalone)

Orders can be generated without a user file; `user_id` will be random deterministic UUIDs (no link to a User table):

```bash
uv run main.py --generate --type orders --count 500 --output orders.ndjson
```

### Using both generators together (design pattern)

Linking dependent data (e.g. orders to users) is a core design pattern: generate the parent entity first, write it to NDJSON, then generate the child entity and pass the parent file so foreign keys match.

1. **Generate users** and write them to a file (these UUIDs become the canonical “user table”).
2. **Generate orders** with `--users-file` pointing at that file. Every order’s `user_id` will be one of those user UUIDs, so orders are linked to users for joins, streaming, or analytics.

**Example: users then orders (linked):**

```bash
# 1. Generate users and save to NDJSON
uv run main.py --generate --type users --count 100 --output users.ndjson

# 2. Generate orders whose user_id values reference those users
uv run main.py --generate --type orders --count 500 --users-file users.ndjson --output orders.ndjson
```

Same seed and counts produce the same `users.ndjson` and `orders.ndjson`; order rows can be joined to user rows on `user_id` = `User.uuid`.

**Example: different seeds or counts**

```bash
# Reproducible pipeline
uv run main.py --generate --type users --count 200 --seed 42 --output users.ndjson
uv run main.py --generate --type orders --count 1000 --seed 42 --users-file users.ndjson --output orders.ndjson
```

Use this pattern whenever you add a new generator that references another entity (e.g. a future “shipments” generator that references orders).


---

## Fantasy hockey dataset (US Hockey / NHL-style)

Three generators model a complete fantasy hockey ecosystem: roster data, play-by-play events, and weekly fantasy team standings.

### Entity overview

| Entity | Generator | CLI `--type` | Proto message |
|--------|-----------|-------------|---------------|
| NHL player roster | `GenHockeyPlayer` | `hockey_players` | `hockey.v1.HockeyPlayer` |
| In-game play event | `GenPlayEvent` | `play_events` | `hockey.v1.PlayEvent` |
| Fantasy team weekly snapshot | `GenFantasyRoster` | `fantasy_rosters` | `hockey.v1.FantasyRosterSnapshot` |

### HockeyPlayer

Represents an NHL-style player with position, team, nationality, handedness, and jersey number. Positions follow realistic NHL roster construction (~20% C, 20% LW, 20% RW, 30% D, 10% G).

```bash
uv run main.py --generate --type hockey_players --count 500 --output players.ndjson
```

### PlayEvent (child of HockeyPlayer)

Captures individual in-game actions: goals, assists, shots on goal, saves, hits, blocked shots, penalties, faceoffs, takeaways, giveaways. Use `--players-file` to link `player_id` to player UUIDs.

```bash
uv run main.py --generate --type play_events --count 100000 \
  --players-file players.ndjson --output events.ndjson
```

### FantasyRosterSnapshot (child of HockeyPlayer)

Models a Yahoo-style 10-team H2H-points league. Each snapshot captures one team's full 18-slot roster (2 C, 2 LW, 2 RW, 4 D, 2 G, 2 UTIL, 4 BN) plus weekly stats and computed fantasy points. **`--count` = weeks**; total snapshots = weeks x 10.

```bash
uv run main.py --generate --type fantasy_rosters --count 25 \
  --players-file players.ndjson --output /path/to/delta/fantasy --format delta
```

### Analyze fantasy standings

```bash
uv run main.py --analyze --analyze-type fantasy_rosters --input /path/to/delta/fantasy
```

### Full fantasy hockey pipeline

```bash
uv run main.py --generate --type hockey_players --count 500 --output players.ndjson
uv run main.py --generate --type play_events --count 200000 \
  --players-file players.ndjson --output /path/to/delta/events --format delta
uv run main.py --generate --type fantasy_rosters --count 25 \
  --players-file players.ndjson --output /path/to/delta/fantasy --format delta
uv run main.py --analyze --analyze-type fantasy_rosters --input /path/to/delta/fantasy
```

---

### Leaderboard snapshots (temporal gameplay dynamics)

`GenLeaderboard` simulates a Steam-style game leaderboard over time. It generates one `LeaderboardSnapshot` per hour for a configurable number of days. Each snapshot contains up to 1 000 `LeaderboardEntry` records with rank, score, `score_delta`, `rank_delta`, hours played, and prior-rank bookkeeping — everything needed to analyse score velocity, rank churn, grinder vs. casual player archetypes, and new-entrant survival.

Player archetypes that shape the time series:

| Archetype | Behaviour |
|-----------|-----------|
| **Grinder** | High steady velocity, active most hours |
| **Burst** | Very high velocity but only active in short windows |
| **Casual** | Low velocity, sparse activity |
| **Newcomer** | Arrives mid-simulation, climbs fast then plateaus |
| **Stalled** | Active early, stops gaining at a random point |

**`--count` means *days*** for this generator; each day produces 24 hourly snapshots.

**Standalone (no user file):**

```bash
# 30 days x 24 h = 720 snapshots; player_ids are deterministic UUIDs
uv run main.py --generate --type leaderboard_snapshots --count 30 --output leaderboard.ndjson
```

**Linked to an existing user pool (recommended):**

```bash
# 1. Generate the player pool
uv run main.py --generate --type users --count 1000 --output players.ndjson

# 2. Generate leaderboard snapshots whose player_id values link to those users
uv run main.py --generate --type leaderboard_snapshots \
  --count 90 \
  --users-file players.ndjson \
  --output leaderboard.ndjson
```

**As a Delta table:**

```bash
uv run main.py --generate --type leaderboard_snapshots \
  --count 90 \
  --users-file players.ndjson \
  --output /path/to/delta/leaderboard \
  --format delta
```

The message name for `Converters.protobuf_to_df` is `leaderboard.v1.LeaderboardSnapshot`.

**Example analytics queries on the resulting dataset:**

```sql
-- Score velocity per player over time
SELECT entry.player_id, entry.player_name, captured_at, entry.score, entry.score_delta
FROM leaderboard_snapshots
LATERAL VIEW explode(entries) t AS entry
ORDER BY entry.player_id, captured_at;

-- Rank churn: fraction of new entrants per snapshot
SELECT snapshot_id, captured_at,
       SUM(CASE WHEN entry.previous_rank = 0 THEN 1 ELSE 0 END) / COUNT(*) AS new_entrant_rate
FROM leaderboard_snapshots
LATERAL VIEW explode(entries) t AS entry
GROUP BY snapshot_id, captured_at;

-- Top climbers: players with greatest cumulative rank improvement
SELECT entry.player_id, entry.player_name, SUM(entry.rank_delta) AS total_rank_gain
FROM leaderboard_snapshots
LATERAL VIEW explode(entries) t AS entry
GROUP BY entry.player_id, entry.player_name
ORDER BY total_rank_gain DESC;
```

### Protobuf to Spark DataFrame (Delta Lake)

The `utils.Converters` class converts serialized protobuf bytes into Spark DataFrames for ingestion (e.g. into Delta Lake):

1. Run `make descriptor` to produce `gen/descriptors/descriptor.bin` (single descriptor for all protos).
2. Generate data (e.g. `list[User]` or `list[Order]`), then serialize: `[msg.SerializeToString() for msg in messages]`.
3. Call `Converters.protobuf_to_df(data, spark, descriptor_path, message_name)` with the correct fully qualified message name (`user.v1.User`, `order.v1.Order`, etc.).
4. Write the result with `df.write.format("delta").mode("overwrite").save(path)`.

Tests in `tests/test_converters.py` demonstrate this using static NDJSON in `tests/resources/` and write Delta tables to `tests/resources/delta/users/` and `tests/resources/delta/orders/` for reference. Use `tests.spark_session.generate_spark_session()` for a local SparkSession with Delta and Protobuf support.

### Output behavior

- With `--output FILE`: the script writes all records to that file and prints a short message to stderr (e.g. `Wrote 100 users to users.ndjson`).
- Without `--output`: each record is printed as one line of JSON to stdout.

**Redirect or pipe (users example):**

```bash
uv run main.py --generate --type users --count 100 > users.ndjson
uv run main.py --generate --type users --count 1 | head -n 1 | jq .
```

## Quick start

From the `learning-spark-datagen` directory:

```bash
uv sync
uv run main.py --generate --type users --count 5
```

You’ll get 5 users as one JSON object per line on stdout. Use `--output FILE` to write to a file. For the full “users then orders” workflow, see [Using both generators together](#using-both-generators-together-design-pattern) above.

## Configuration

### Maven / package resolution (`MAVEN_PROXY`)

By default, Spark resolves `spark.jars.packages` (Delta Lake, Spark Protobuf) directly from Maven Central. If you are on a private or corporate network that blocks Maven Central, or if your organisation runs an internal Artifactory / Nexus mirror, set the `MAVEN_PROXY` environment variable to your mirror URL before running any `uv run` command.

When `MAVEN_PROXY` is set, `generate_spark_session` automatically adds it as `spark.jars.repositories` so Ivy routes all package downloads through that proxy. When the variable is absent the config key is omitted and Spark falls back to its built-in defaults.

```bash
# Point Spark at an internal Maven mirror (safe, vetted proxy)
export MAVEN_PROXY="https://maven-proxy.internal.example.com"

# All uv run commands now resolve packages through that mirror
uv run main.py --generate --type users --count 100
```

The proxy is only used for resolving Spark JVM packages at session start-up — it does not affect Python dependency resolution (that is handled by `uv` / PyPI). Any proxy URL that serves the Maven 2 repository layout works (Artifactory, Nexus, an internal mirror, etc.).

```bash
# Unset to go back to direct Maven Central resolution
unset MAVEN_PROXY
```

---

## Development
