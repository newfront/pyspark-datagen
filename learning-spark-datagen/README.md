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

- `protos/` — Protobuf definitions (e.g. `user/v1/user.proto`, `order/v1/order.proto`).
- `gen/python/` — Generated Python packages from those protos (created by `buf generate`).
- `src/learning_spark_datagen/datagen/` — Data generators (`gen_user.py`, `gen_order.py`); each follows the same pattern (deterministic from seed, NDJSON I/O).
- `gen/descriptors/descriptor.bin` — Serialized `FileDescriptorSet` for Spark/ingest (created by `make descriptor`).
- `src/learning_spark_datagen/utils/` — `Converters.protobuf_to_df` for turning generated protobuf bytes into Spark DataFrames (e.g. for Delta Lake).

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

## Development
