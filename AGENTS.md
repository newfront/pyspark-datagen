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
| `--type users \| orders` | Entity to generate (default: `users`). |
| `--count N` | Number of records (default: 100). |
| `--output PATH` | File path for JSON/NDJSON; **required** for `--format delta` (Delta table directory). |
| `--format json \| delta \| console` | `json` = NDJSON to file (if `--output` set); `delta` = Delta table; `console` = stdout. |
| `--users-file PATH` | **For orders only.** NDJSON file of users; every order’s `user_id` will be one of these UUIDs. |
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

- `message_name`: `user.v1.User` or `order.v1.Order`.
- Example: `uv run python scripts/ndjson_to_delta.py ~/Desktop/users ~/Desktop/delta/users user.v1.User`

## Key file locations

| What | Where |
|------|--------|
| CLI entry point | `learning-spark-datagen/main.py` |
| NDJSON → Delta script | `learning-spark-datagen/scripts/ndjson_to_delta.py` |
| Protobuf→DataFrame + Delta write (API) | `src/learning_spark_datagen/utils/converters.py`, `ndjson_reader.py` |
| Descriptor (required for Delta) | `learning-spark-datagen/gen/descriptors/descriptor.bin` |
| Generators | `src/learning_spark_datagen/datagen/gen_user.py`, `gen_order.py` |

## Summary

- **Users then orders:** Generate users to NDJSON → generate orders with `--users-file` (and optionally `--format delta`).
- **Existing NDJSON → Delta:** Use `scripts/ndjson_to_delta.py`; not exposed on the main CLI.
- **Delta writes:** Always need `--output` and `gen/descriptors/descriptor.bin` present.
