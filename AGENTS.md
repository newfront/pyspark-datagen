# Agent guide: learning-spark-datagen

Quick reference for AI agents (and humans) so you don't have to scan the README or multiple files to run data generation and Delta workflows.

## Where to run

All commands below assume you're in **`learning-spark-datagen/`** (the package with `main.py` and `pyproject.toml`). Use `uv run` to execute the CLI.

```bash
cd learning-spark-datagen
```

## CLI: `main.py`

**Entry point:** `uv run main.py` — two mutually exclusive modes: `--generate` and `--analyze`.

### Generate flags

| Flag | Purpose |
|------|--------|
| `--generate` | Enable generate mode (required for data gen). |
| `--type users \| orders \| leaderboard_snapshots \| hockey_players \| play_events \| fantasy_rosters` | Entity to generate (default: `users`). |
| `--count N` | Number of records (default: 100). For `leaderboard_snapshots`: number of **days** (each day = 24 hourly snapshots). For `fantasy_rosters`: number of **weeks** (each week × 10 teams). |
| `--output PATH` | File path for JSON/NDJSON; **required** for `--format delta` (Delta table directory). |
| `--format json \| delta` | `json` = NDJSON to file (if `--output` set); `delta` = Delta table; default `json`. |
| `--users-file PATH` | **For orders and leaderboard_snapshots.** NDJSON file of users; foreign keys (`user_id` / `player_id`) will reference these UUIDs. |
| `--players-file PATH` | **For play_events and fantasy_rosters.** NDJSON file of hockey players; `player_id` will reference these UUIDs. |
| `--seed N` | Random seed (default: 42). |

### Analyze flags

| Flag | Purpose |
|------|--------|
| `--analyze` | Enable analyze mode (mutually exclusive with `--generate`). |
| `--analyze-type leaderboard_snapshots \| fantasy_rosters` | Which Delta table schema to analyze (default: `leaderboard_snapshots`). |
| `--input PATH` | **Required.** Delta table directory to analyze (written by `--format delta`). |

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

**3. Generate leaderboard snapshots linked to the same user pool**

```bash
uv run main.py --generate --type leaderboard_snapshots --count 90 --users-file /path/to/users --output /path/to/delta/leaderboard --format delta
```

`--count` is **days**; 90 days × 24 h = 2 160 hourly `LeaderboardSnapshot` records, each with up to 1 000 player entries.  `player_id` values in each snapshot reference UUIDs from the users file, so you can join leaderboard entries to the user table.

**4. Analyze a leaderboard Delta table**

```bash
uv run main.py --analyze --input /path/to/delta/leaderboard
```

Loads the Delta table, runs six PySpark analysis sections, and prints formatted ASCII tables:

| Section | What it shows |
|---------|---------------|
| Overview | Board name, game, date range, total snapshots, unique players |
| Current standings | Top 10 players from the most recent hourly snapshot |
| Top climbers | Players with greatest total rank positions gained |
| Score velocity leaders | Players with highest total score gain |
| Rank volatility | Most unstable rank trajectories (highest std dev of rank) |
| Daily score momentum | Aggregate score gain + new-entrant rate per calendar day |

**5. Convert existing NDJSON to a Delta table**

The main CLI does **not** convert NDJSON → Delta. Use the helper script:

```bash
uv run python scripts/ndjson_to_delta.py <ndjson_path> <delta_path> <message_name>
```

- `message_name`: `user.v1.User`, `order.v1.Order`, `leaderboard.v1.LeaderboardSnapshot`, `hockey.v1.HockeyPlayer`, `hockey.v1.PlayEvent`, or `hockey.v1.FantasyRosterSnapshot`.

**6. Generate hockey players (NHL-style roster)**

```bash
uv run main.py --generate --type hockey_players --count 500 --output /path/to/players
```

Generates NHL-style players with position, team, nationality, handedness, jersey number.  The resulting NDJSON can be used as `--players-file` for the `play_events` and `fantasy_rosters` generators.

**7. Generate play events linked to those players**

```bash
uv run main.py --generate --type play_events --count 200000 --players-file /path/to/players --output /path/to/delta/events --format delta
```

Generates in-game events (goals, assists, shots, saves, hits, etc.) with realistic NHL frequency distribution.  `player_id` links to `HockeyPlayer.player_id`.

**8. Generate a fantasy hockey season**

```bash
uv run main.py --generate --type fantasy_rosters --count 25 --players-file /path/to/players --output /path/to/delta/fantasy --format delta
```

`--count` is **weeks**; 25 weeks × 10 teams = 250 `FantasyRosterSnapshot` records.  Each snapshot contains 18 roster slots (2C, 2LW, 2RW, 4D, 2G, 2UTIL, 4BN) with weekly stats and fantasy points computed using Yahoo H2H-points defaults (G=6, A=4, +/-=2, SOG=0.9, HIT=1, BLK=1 | W=5, GA=-3, SV=0.6).

**9. Analyze a fantasy roster Delta table**

```bash
uv run main.py --analyze --analyze-type fantasy_rosters --input /path/to/delta/fantasy
```

Prints six sections: league overview, current standings, top fantasy scorers, position breakdown, weekly movers, roster construction efficiency.

**10. Full fantasy hockey pipeline**

```bash
uv run main.py --generate --type hockey_players --count 500 --output /path/to/players
uv run main.py --generate --type play_events --count 200000 --players-file /path/to/players --output /path/to/delta/events --format delta
uv run main.py --generate --type fantasy_rosters --count 25 --players-file /path/to/players --output /path/to/delta/fantasy --format delta
uv run main.py --analyze --analyze-type fantasy_rosters --input /path/to/delta/fantasy
```

## Key file locations

| What | Where |
|------|--------|
| CLI entry point | `learning-spark-datagen/main.py` |
| Leaderboard analysis | `src/learning_spark_datagen/analysis/leaderboard_analysis.py` |
| Fantasy hockey analysis | `src/learning_spark_datagen/analysis/fantasy_hockey_analysis.py` |
| NDJSON → Delta script | `learning-spark-datagen/scripts/ndjson_to_delta.py` |
| Protobuf→DataFrame + Delta write (API) | `src/learning_spark_datagen/utils/converters.py`, `ndjson_reader.py` |
| Descriptor (required for Delta) | `learning-spark-datagen/gen/descriptors/descriptor.bin` |
| Generators | `src/learning_spark_datagen/datagen/gen_user.py`, `gen_order.py`, `gen_leaderboard.py`, `gen_hockey_player.py`, `gen_play_event.py`, `gen_fantasy_roster.py` |
| Hockey protos | `protos/hockey/v1/player.proto`, `play_event.proto`, `fantasy_roster.proto` |

## Summary

- **Users then orders:** Generate users to NDJSON → generate orders with `--users-file` (and optionally `--format delta`).
- **Users then leaderboard snapshots:** Generate users to NDJSON → generate leaderboard snapshots with `--users-file`. `--count` = number of days; outputs one snapshot per hour.
- **Analyze leaderboard:** `--analyze --input DELTA_PATH` loads the table and prints six gameplay-dynamics reports.
- **Hockey players then play events:** Generate hockey players to NDJSON → generate play events with `--players-file`. `player_id` links to `HockeyPlayer.player_id`.
- **Hockey players then fantasy rosters:** Generate hockey players to NDJSON → generate fantasy rosters with `--players-file`. `--count` = number of weeks; each week × 10 teams.
- **Analyze fantasy rosters:** `--analyze --analyze-type fantasy_rosters --input DELTA_PATH` prints six fantasy-dynamics reports.
- **Existing NDJSON → Delta:** Use `scripts/ndjson_to_delta.py`; not exposed on the main CLI.
- **Delta writes:** Always need `--output` and `gen/descriptors/descriptor.bin` present.
