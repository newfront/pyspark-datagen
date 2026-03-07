"""Generate deterministic fake Order protobufs with Product sub-messages and NDJSON I/O."""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from random import Random
from uuid import UUID

# Ensure generated protos are importable when run from project root (gen/python).
_here = Path(__file__).resolve().parent
_root = _here.parent.parent.parent.parent
_gen = _root / "gen" / "python"
if _gen.exists():
    sys.path.insert(0, str(_gen))

from google.protobuf import json_format  # noqa: E402

from order.v1 import order_pb2  # noqa: E402

# Size of the deterministic product pool (same product_ids for all orders).
_PRODUCT_POOL_SIZE = 100
# Max number of line items per order (min 1 by proto).
_MAX_PRODUCTS_PER_ORDER = 5
# Base time in the past for created_at/updated_at (Unix seconds).
_BASE_TIME = datetime(2020, 1, 1, tzinfo=timezone.utc)


def _make_amount(
    units: int, nanos: int, currency: str = "USD"
) -> order_pb2.Order.Amount:
    """Build an Order.Amount (USD: units = whole dollars, nanos = fractional)."""
    amt = order_pb2.Order.Amount()
    amt.currency = currency
    amt.units = units
    amt.nanos = nanos
    return amt


def _total_from_products(
    products: list[order_pb2.Order.Product],
) -> order_pb2.Order.Amount:
    """Compute order total from product line items (before discount, then sum)."""
    total_nanos = 0
    for p in products:
        # Line total in nanos: num_items * (units*1e9 + nanos) * (1 - percent_discount/100)
        unit_nanos = p.unit_cost.units * 1_000_000_000 + p.unit_cost.nanos
        line_nanos = int(p.num_items * unit_nanos * (100 - p.percent_discount) / 100)
        total_nanos += line_nanos
    units = total_nanos // 1_000_000_000
    nanos = int(total_nanos % 1_000_000_000)
    return _make_amount(units, nanos)


class GenOrder:
    """Generate one or more fake Order protobufs with deterministic Product sub-messages."""

    def __init__(self, seed: int = 42, user_ids: list[str] | None = None) -> None:
        self._seed = seed
        self._rng = Random(seed)
        self._user_ids = user_ids
        # Deterministic product UUID pool (same for all orders).
        self._product_ids = [
            str(UUID(int=Random(seed + i).getrandbits(128)))
            for i in range(_PRODUCT_POOL_SIZE)
        ]

    def generate_one(self, index: int = 0) -> order_pb2.Order:
        """Generate a single Order. Same (seed, index) yields the same order and products."""
        rng = Random(self._seed + index)

        if self._user_ids:
            user_id = self._user_ids[index % len(self._user_ids)]
        else:
            user_id = str(UUID(int=rng.getrandbits(128)))

        # Deterministic timestamps (uint64 Unix seconds).
        days_ago_created = rng.randint(1, 1800)
        days_ago_updated = rng.randint(0, days_ago_created)
        created_dt = _BASE_TIME - timedelta(days=days_ago_created)
        updated_dt = _BASE_TIME - timedelta(days=days_ago_updated)
        created_at = int(created_dt.timestamp())
        updated_at = int(updated_dt.timestamp())

        # Deterministic number of line items (1 to _MAX_PRODUCTS_PER_ORDER).
        num_lines = rng.randint(1, _MAX_PRODUCTS_PER_ORDER)
        # Deterministic choice of product indices (with replacement allowed).
        product_indices = [
            rng.randint(0, _PRODUCT_POOL_SIZE - 1) for _ in range(num_lines)
        ]

        products = []
        for pi in product_indices:
            product_id = self._product_ids[pi]
            num_items = rng.randint(1, 10)
            # Unit cost: dollars and fractional (e.g. $12.99 -> 12, 990000000).
            unit_dollars = rng.randint(1, 500)
            unit_nanos = rng.randint(0, 999_999_999)
            unit_cost = _make_amount(unit_dollars, unit_nanos)
            percent_discount = rng.choice([0, 0, 0, 5, 10, 15, 20])
            prod = order_pb2.Order.Product(
                product_id=product_id,
                num_items=num_items,
                unit_cost=unit_cost,
                percent_discount=percent_discount,
            )
            products.append(prod)

        total = _total_from_products(products)
        coupon_code_used = rng.random() < 0.2

        order = order_pb2.Order(
            created_at=created_at,
            updated_at=updated_at,
            user_id=user_id,
            products=products,
            total=total,
            coupon_code_used=coupon_code_used,
        )
        return order

    def generate(self, count: int) -> list[order_pb2.Order]:
        """Generate `count` deterministic orders (same seed => same sequence)."""
        if count <= 0:
            return []
        return [self.generate_one(index=i) for i in range(count)]

    def generate_range(self, start: int, end: int) -> list[order_pb2.Order]:
        """Generate orders for indices [start, end). Useful for batched Delta writes."""
        if start >= end:
            return []
        return [self.generate_one(index=i) for i in range(start, end)]

    @staticmethod
    def order_to_dict(order: order_pb2.Order) -> dict:
        """Convert an Order proto to a JSON-serializable dict (NDJSON format)."""
        return json_format.MessageToDict(
            order,
            always_print_fields_with_no_presence=False,
            preserving_proto_field_name=True,
        )

    @staticmethod
    def write_ndjson(path: str | Path, orders: list[order_pb2.Order]) -> None:
        """Write orders to newline-delimited JSON (one JSON object per line)."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w") as f:
            for order in orders:
                f.write(json.dumps(GenOrder.order_to_dict(order)) + "\n")

    @staticmethod
    def read_ndjson(path: str | Path) -> list[order_pb2.Order]:
        """Read orders from a newline-delimited JSON file."""
        path = Path(path)
        orders = []
        with path.open() as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                d = json.loads(line)
                order = order_pb2.Order()
                json_format.ParseDict(d, order)
                orders.append(order)
        return orders
