"""Generate deterministic fake Product protobufs for a Tidewell-style athleisure catalog."""

from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from random import Random
from uuid import UUID

# Ensure generated protos are importable when run from project root (gen/python).
_here = Path(__file__).resolve().parent
_root = _here.parent.parent.parent.parent
_gen = _root / "gen" / "python"
if _gen.exists():
    sys.path.insert(0, str(_gen))

from google.protobuf import json_format, timestamp_pb2  # noqa: E402

from commerce.v1 import amount_pb2  # noqa: E402
from product.v1 import product_pb2  # noqa: E402

# Default brand. Override via constructor for other catalogs.
_DEFAULT_BRAND = "Tidewell"

# Catalog vocabulary. Subcategories per top-level category.
_SUBCATEGORIES: dict[str, tuple[str, ...]] = {
    "Tops": ("Sports Bra", "Tank", "Tee", "Long Sleeve", "Crop"),
    "Bottoms": ("Legging", "Short", "Jogger", "Skirt"),
    "Outerwear": ("Jacket", "Hoodie", "Pullover"),
    "Swim": ("One-Piece", "Bikini Top", "Bikini Bottom", "Rashguard"),
    "Accessories": ("Hat", "Bag", "Socks", "Water Bottle"),
}

# Price bands per top-level category (USD, inclusive low/high).
_PRICE_BANDS: dict[str, tuple[int, int]] = {
    "Tops": (38, 78),
    "Bottoms": (58, 118),
    "Outerwear": (98, 188),
    "Swim": (48, 98),
    "Accessories": (15, 58),
}

# Coastal-themed name prefixes.
_NAME_PREFIXES: tuple[str, ...] = (
    "Tidal",
    "Surfside",
    "Saltwater",
    "Coastline",
    "Headland",
    "Cove",
    "Breakwater",
    "Driftline",
    "Seabreeze",
    "Capri",
    "Anchor",
    "Bayview",
    "Shoreline",
    "Lighthouse",
    "Reef",
)

# Color palette.
_COLORS: tuple[str, ...] = (
    "Sandstone",
    "Slate",
    "Reef Black",
    "Sea Glass",
    "Surf White",
    "Coral",
    "Storm Blue",
    "Kelp Green",
)

# Sizes by category (apparel uses standard sizing, accessories use one-size).
_APPAREL_SIZES: tuple[str, ...] = ("XS", "S", "M", "L", "XL")
_ACCESSORY_SIZES: tuple[str, ...] = ("OS",)

# Short SKU codes per category for readable SKUs (e.g. TW-LEG-SLT-M).
_SKU_CATEGORY_CODES: dict[str, str] = {
    "Tops": "TOP",
    "Bottoms": "BOT",
    "Outerwear": "OUT",
    "Swim": "SWM",
    "Accessories": "ACC",
}

# Base time used for "created" timestamps so they are not in the future.
_BASE_TIME = datetime(2024, 1, 1, tzinfo=timezone.utc)


def _color_code(color: str) -> str:
    """3-letter abbreviation of a color name for SKU strings."""
    parts = color.replace("-", " ").split()
    if len(parts) == 1:
        return parts[0][:3].upper()
    return (parts[0][:1] + parts[-1][:2]).upper()


class GenProduct:
    """Generate one or more fake Product protobufs (Tidewell athleisure by default)."""

    def __init__(self, seed: int = 42, brand: str = _DEFAULT_BRAND) -> None:
        self._seed = seed
        self._brand = brand
        self._rng = Random(seed)

    def generate_one(self, index: int = 0) -> product_pb2.Product:
        """Generate a single Product. Same (seed, index) always yields the same SKU."""
        rng = Random(self._seed + index)

        product_id = str(UUID(int=rng.getrandbits(128)))

        category = rng.choice(tuple(_SUBCATEGORIES.keys()))
        subcategory = rng.choice(_SUBCATEGORIES[category])
        variant = rng.choice(_COLORS)
        sizes = _ACCESSORY_SIZES if category == "Accessories" else _APPAREL_SIZES
        size = rng.choice(sizes)
        prefix = rng.choice(_NAME_PREFIXES)
        name = f"{prefix} {subcategory}"

        sku_cat = _SKU_CATEGORY_CODES[category]
        sku = f"TW-{sku_cat}-{_color_code(variant)}-{size}-{index:05d}"

        low, high = _PRICE_BANDS[category]
        units = rng.randint(low, high)
        nanos = rng.choice([0, 490_000_000, 950_000_000, 990_000_000])
        price = amount_pb2.Amount(currency="USD", units=units, nanos=nanos)

        percent_discount = rng.choice([0, 0, 0, 0, 0, 10, 15, 20, 25, 30])

        days_ago = rng.randint(7, 730)
        created_dt = _BASE_TIME - timedelta(days=days_ago)
        created_ts = timestamp_pb2.Timestamp()
        created_ts.FromDatetime(created_dt)

        in_stock = rng.random() < 0.92
        inventory_count = rng.randint(0, 800) if in_stock else 0

        product = product_pb2.Product(
            product_id=product_id,
            sku=sku,
            name=name,
            brand=self._brand,
            category=category,
            category2=subcategory,
            variant=variant,
            size=size,
            price=price,
            percent_discount=percent_discount,
            created=created_ts,
            in_stock=in_stock,
            inventory_count=inventory_count,
        )
        return product

    def generate(self, count: int) -> list[product_pb2.Product]:
        """Generate `count` deterministic products (same seed => same catalog)."""
        if count <= 0:
            return []
        return [self.generate_one(index=i) for i in range(count)]

    def generate_range(self, start: int, end: int) -> list[product_pb2.Product]:
        """Generate products for indices [start, end). Useful for batched Delta writes."""
        if start >= end:
            return []
        return [self.generate_one(index=i) for i in range(start, end)]

    @staticmethod
    def product_to_dict(product: product_pb2.Product) -> dict:
        """Convert a Product proto to a JSON-serializable dict (NDJSON format)."""
        return json_format.MessageToDict(
            product,
            always_print_fields_with_no_presence=False,
            preserving_proto_field_name=True,
        )

    @staticmethod
    def write_ndjson(path: str | Path, products: list[product_pb2.Product]) -> None:
        """Write products to newline-delimited JSON (one JSON object per line)."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w") as f:
            for product in products:
                f.write(json.dumps(GenProduct.product_to_dict(product)) + "\n")

    @staticmethod
    def read_ndjson(path: str | Path) -> list[product_pb2.Product]:
        """Read products from a newline-delimited JSON file."""
        path = Path(path)
        products = []
        with path.open() as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                d = json.loads(line)
                product = product_pb2.Product()
                json_format.ParseDict(d, product)
                products.append(product)
        return products

    @staticmethod
    def read_delta(spark, path: str | Path) -> list[product_pb2.Product]:
        """Read products from a Delta table written via `Converters.write_df_to_delta`.

        Spark serializes each row to JSON; we parse that JSON into a `Product` proto
        with `json_format.ParseDict`, mirroring `read_ndjson` exactly. Requires a
        SparkSession (created via `generate_spark_session()` or the builder).
        """
        df = spark.read.format("delta").load(str(path))
        products = []
        for s in df.toJSON().collect():
            d = json.loads(s)
            product = product_pb2.Product()
            json_format.ParseDict(d, product, ignore_unknown_fields=True)
            products.append(product)
        return products
