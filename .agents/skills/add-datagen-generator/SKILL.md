---
name: add-datagen-generator
description: Add a new Protobuf-backed data generator to the learning-spark-datagen project (new proto, GenX class, CLI, tests). Use when adding a new entity type (e.g. Shipment, Payment), a new message with deterministic fake data, or when the user asks to create a generator following the User/Order pattern.
---

# Add a New Data Generator

Use this skill when adding a new message type and generator to `learning-spark-datagen/` (proto + Python generator + CLI + tests). Follow the existing **User** and **Order** generators as the reference pattern.

## 1. Proto and validation

- Add `protos/<domain>/v1/<message>.proto` (e.g. `order/v1/order.proto`).
- Use `buf.validate` so every generated message passes `protovalidate.validate(message)`:
  - **IDs / FKs**: `(buf.validate.field).string.uuid = true` and `(buf.validate.field).required = true` for string IDs that link to other entities.
  - **Required fields**: `(buf.validate.field).required = true` on message fields and on scalars where zero is invalid.
  - **Nested messages**: `required = true` on any message field that must be set (e.g. `total`, `unit_cost`).
  - **Strings**: `(buf.validate.field).string = { min_len: 1 }` for non-empty (e.g. `currency`).
  - **Repeated**: `(buf.validate.field).repeated = { min_items: 1 }` when at least one item is required.
  - **Numbers**: e.g. `(buf.validate.field).uint32 = { gte: 1 }` for counts.
- Import `buf/validate/validate.proto` and keep `syntax = "proto3"` and package/options consistent with existing protos.

## 2. Generate and package layout

- Run `buf generate` (or `make build`) from the project root so `gen/python/<domain>/v1/<message>_pb2.py` is created.
- If a new top-level package is created under `gen/python/`, add `__init__.py` in each directory (e.g. `gen/python/order/__init__.py`, `gen/python/order/v1/__init__.py`) so `from order.v1 import order_pb2` works.

## 3. Generator class

- Add `src/learning_spark_datagen/datagen/gen_<name>.py` (e.g. `gen_order.py`).
- Mirror the structure of `GenUser` / `GenOrder`:
  - `__init__(self, seed, <parent_ids>=None)` — e.g. `user_ids` so child records can reference parent UUIDs.
  - `generate_one(self, index)` — deterministic from `seed + index` (same index ⇒ same message). Use `Random(seed + index)` and a fixed pool of IDs (e.g. product UUIDs) when you need deterministic sub-messages.
  - `generate(self, count)` — return `[self.generate_one(i) for i in range(count)]`.
  - `@staticmethod to_dict(msg)`, `write_ndjson(path, messages)`, `read_ndjson(path)` using `json_format.MessageToDict` / `ParseDict` and `preserving_proto_field_name=True`.
- If the message has a parent link (e.g. `user_id`), accept an optional list of parent IDs and assign `parent_ids[index % len(parent_ids)]` (or generate deterministic UUIDs when the list is not provided).
- Ensure every generated message passes `protovalidate.validate(message)` (required fields set, UUIDs valid, min_items satisfied, etc.).

## 4. CLI and exports

- In `src/learning_spark_datagen/datagen/__init__.py`: add `from ...gen_<name> import GenX` and include `GenX` in `__all__`.
- In `main.py`: add a `--type` choice for the new generator (e.g. `users`, `orders`). For child entities, add an optional `--<parent>-file` (e.g. `--users-file`) and, when set, load parent NDJSON and pass the list of parent IDs (e.g. `user.uuid`) into the generator.

## 5. Tests

- Add `tests/test_gen_<name>.py`.
- For every generated message (single, batch, round-trip), call `protovalidate.validate(message)`. No message should be considered valid without passing validation.
- If a test supplies IDs (e.g. `user_ids=[...]`), use **valid UUIDs** for any field that has `string.uuid` validation (e.g. `user_id` in Order).

## Design pattern: linked generators

To link two generators (e.g. Orders to Users):

1. Generate the **parent** entity and write to NDJSON (e.g. `users.ndjson`).
2. Generate the **child** entity with `--<parent>-file` pointing at that NDJSON so child foreign keys (e.g. `user_id`) are chosen from the parent IDs.

Document this in the README as “Using both generators together” so future generators (e.g. Shipments → Orders) follow the same pattern.

## Checklist

- [ ] Proto has buf.validate rules so all required fields and formats are enforced.
- [ ] `buf generate` run; `gen/python` package has `__init__.py` where needed.
- [ ] Generator is deterministic (seed + index), supports optional parent ID list, and always produces valid messages.
- [ ] `GenX` exported in `datagen/__init__.py`; `main.py` has `--type` and optional `--<parent>-file`.
- [ ] Tests call `protovalidate.validate(...)` on every generated/round-tripped message; tests use valid UUIDs when the proto has `string.uuid`.
- [ ] README updated if adding a new “linked” workflow (e.g. users → orders).
