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

- `protos/` — Protobuf definitions (e.g. `user/v1/user.proto`).
- `gen/python/` — Generated Python packages from those protos (created by `buf generate`).
- `gen/python/user/v1/descriptor.bin` — Serialized `FileDescriptorSet` for Spark/ingest (created by `make descriptor`).

## Commands

From the `learning-spark-datagen` directory:

| Command           | Description |
|-------------------|-------------|
| `make build`      | Lint/format protos, run `buf generate`, then format/lint Python. |
| `make generate`   | Run `make build`, then `main.py --generate --count 100`. |
| `make test`       | Run pytest. |
| `make descriptor` | Write `gen/python/user/v1/descriptor.bin` for Spark. |
| `make release [bump=patch\|minor\|major]` | Bump version in `pyproject.toml`. |
| `make package`    | Build wheel and sdist into `dist/`. |
| `make install-dist` | Install from `dist/` using Buf index for deps. |
| `make publish`    | Publish the package (e.g. to PyPI). |

## Data generation examples

Generate fake users with deterministic output (same `--seed` and `--count` produce the same users, including UUIDs).

**Print users as newline-delimited JSON to stdout (one JSON object per line):**

```bash
# 10 users, default seed 42
uv run main.py --generate --count 10

# 1000 users, custom seed for reproducibility
uv run main.py --generate --count 1000 --seed 12345
```

**Write users to a file (stable UUIDs and timestamps for later runs):**

```bash
# Write 1000 users to users.ndjson
uv run main.py --generate --count 1000 --output users.ndjson

# Same seed and count → same file contents
uv run main.py --generate --count 1000 --seed 42 --output users.ndjson
```

**Redirect or pipe stdout:**

```bash
# Save NDJSON to a file from stdout
uv run main.py --generate --count 100 > users.ndjson

# Pipe into another tool (e.g. jq for one record)
uv run main.py --generate --count 1 | head -n 1 | jq .
```

When `--output` is set, the script writes to that file and prints a short message to stderr (e.g. `Wrote 1000 users to users.ndjson`). When `--output` is omitted, each user is printed as a single line of JSON to stdout.

## Quick start

From the `learning-spark-datagen` directory:

```bash
uv sync
uv run main.py --generate --count 5
```

You’ll get 5 users as one JSON object per line on stdout. Use `--output FILE` to write to a file instead.

## Development
