"""Generate deterministic GA4-style ecommerce session event streams.

Each "session" is a sequence of `event.v1.Event` messages sharing a `session_id` that
walks the GA4 buy flow: session_start -> view_item_list -> view_item* -> add_to_cart*
-> view_cart -> (remove_from_cart) -> begin_checkout -> add_shipping_info ->
add_payment_info -> purchase -> (refund). Drop-off probabilities at each step are
controlled by `FunnelConfig`. See `FUNNEL_PROFILES` for built-in tunings.

Sessions are distributed across a configurable date range with weekend lift and a
mild May taper. Users are picked via a Zipf-weighted index over `user_ids` so a
small set of "loyal customers" appears in many sessions while most appear in 1-2.
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
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

from commerce.v1 import amount_pb2  # noqa: E402
from event.v1 import event_pb2  # noqa: E402
from product.v1 import product_pb2  # noqa: E402

# ----------------------------------------------------------------------------
# Funnel configuration
# ----------------------------------------------------------------------------


@dataclass(frozen=True)
class FunnelConfig:
    """Per-step drop-off probabilities for a GA4-style buy flow.

    Each `*_rate` is the conditional probability of advancing to the next step
    given the current step occurred. `view_items_mean` is the mean number of
    `view_item` events per session (sampled per session as a small integer).
    `cart_size_*` controls how many distinct items end up in the cart.
    `refund_rate` is the marginal probability of a session ending in a refund
    given that a purchase happened.
    """

    name: str = "realistic"
    # Funnel rates.
    view_item_list_rate: float = 0.85
    view_items_mean: float = 3.0  # Average view_item events per session.
    add_to_cart_rate: float = 0.30  # Per view_item.
    view_cart_rate: float = 0.50  # Given >=1 add_to_cart.
    remove_from_cart_rate: float = 0.15  # Given view_cart.
    begin_checkout_rate: float = 0.40  # Given view_cart.
    add_shipping_info_rate: float = 0.95  # Given begin_checkout.
    add_payment_info_rate: float = 0.95  # Given add_shipping_info.
    purchase_rate: float = 0.95  # Given add_payment_info.
    refund_rate: float = 0.05  # Given purchase.
    # Cart sizing.
    cart_size_min: int = 1
    cart_size_max: int = 5
    # Inter-event delays (seconds) — random within these bounds.
    delay_min_s: int = 3
    delay_max_s: int = 90
    refund_delay_days_min: int = 1
    refund_delay_days_max: int = 21


FUNNEL_PROFILES: dict[str, FunnelConfig] = {
    "realistic": FunnelConfig(name="realistic"),
    "aggressive": FunnelConfig(
        name="aggressive",
        view_item_list_rate=0.95,
        view_items_mean=4.5,
        add_to_cart_rate=0.45,
        view_cart_rate=0.70,
        begin_checkout_rate=0.55,
        purchase_rate=0.98,
        refund_rate=0.03,
    ),
    "conservative": FunnelConfig(
        name="conservative",
        view_item_list_rate=0.65,
        view_items_mean=2.0,
        add_to_cart_rate=0.18,
        view_cart_rate=0.35,
        begin_checkout_rate=0.25,
        purchase_rate=0.85,
        refund_rate=0.08,
    ),
}


# ----------------------------------------------------------------------------
# Catalogs (device, geo, traffic source)
# ----------------------------------------------------------------------------


_DEVICE_CATEGORIES: tuple[tuple[str, float], ...] = (
    ("mobile", 0.62),
    ("desktop", 0.28),
    ("tablet", 0.10),
)

_OPERATING_SYSTEMS_BY_DEVICE: dict[str, tuple[tuple[str, float], ...]] = {
    "mobile": (("iOS", 0.55), ("Android", 0.45)),
    "desktop": (("macOS", 0.40), ("Windows", 0.50), ("Linux", 0.10)),
    "tablet": (("iPadOS", 0.85), ("Android", 0.15)),
}

_GEOS: tuple[tuple[str, str, str, float], ...] = (
    ("US", "California", "Los Angeles", 0.18),
    ("US", "California", "San Diego", 0.10),
    ("US", "California", "San Francisco", 0.08),
    ("US", "New York", "New York", 0.10),
    ("US", "Texas", "Austin", 0.07),
    ("US", "Florida", "Miami", 0.07),
    ("US", "Washington", "Seattle", 0.06),
    ("US", "Hawaii", "Honolulu", 0.05),
    ("US", "Oregon", "Portland", 0.04),
    ("US", "Colorado", "Denver", 0.04),
    ("CA", "British Columbia", "Vancouver", 0.04),
    ("CA", "Ontario", "Toronto", 0.04),
    ("GB", "England", "London", 0.05),
    ("AU", "New South Wales", "Sydney", 0.04),
    ("FR", "Ile-de-France", "Paris", 0.02),
    ("JP", "Tokyo", "Tokyo", 0.02),
)

_SOURCE_MEDIUMS: tuple[tuple[str, str, str, float], ...] = (
    # (source, medium, default_campaign, weight)
    ("google", "organic", "(not set)", 0.32),
    ("direct", "(none)", "(not set)", 0.22),
    ("google", "cpc", "summer_2026_search", 0.12),
    ("instagram", "paid_social", "ig_carousel_athleisure", 0.10),
    ("tiktok", "paid_social", "tt_creators_q2", 0.07),
    ("facebook", "paid_social", "fb_retargeting_q2", 0.05),
    ("email", "email", "newsletter_weekly", 0.05),
    ("youtube", "video", "yt_brand_2026", 0.03),
    ("pinterest", "referral", "pin_seasonal", 0.02),
    ("reddit", "referral", "(not set)", 0.02),
)

_PAYMENT_METHODS: tuple[str, ...] = (
    "credit_card",
    "credit_card",
    "credit_card",
    "paypal",
    "apple_pay",
    "google_pay",
    "shop_pay",
)

_SHIPPING_TIERS: tuple[tuple[str, int, int, float], ...] = (
    # (tier, dollars, nanos, weight)
    ("Ground", 5, 990_000_000, 0.65),
    ("Express", 12, 990_000_000, 0.25),
    ("Overnight", 24, 990_000_000, 0.10),
)

_REFUND_REASONS: tuple[str, ...] = (
    "size_too_small",
    "size_too_large",
    "didnt_like_fabric",
    "wrong_color",
    "changed_mind",
    "defect",
    "arrived_late",
)

_REMOVE_REASONS: tuple[str, ...] = (
    "price",
    "size_doubt",
    "saving_for_later",
    "changed_mind",
)

_COUPON_CODES: tuple[str, ...] = (
    "TIDE10",
    "SUMMER15",
    "WELCOME20",
    "FRIENDS25",
    "STAFF30",
)

# Day-of-week multipliers (Mon=0 ... Sun=6). Weekends lift, midweek dips.
_DOW_WEIGHTS: tuple[float, ...] = (0.85, 0.90, 0.95, 1.00, 1.10, 1.30, 1.25)


# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------


def _make_amount(units: int, nanos: int, currency: str = "USD") -> amount_pb2.Amount:
    return amount_pb2.Amount(currency=currency, units=units, nanos=nanos)


def _amount_to_nanos(a: amount_pb2.Amount) -> int:
    return a.units * 1_000_000_000 + a.nanos


def _nanos_to_amount(total_nanos: int, currency: str = "USD") -> amount_pb2.Amount:
    units = total_nanos // 1_000_000_000
    nanos = int(total_nanos % 1_000_000_000)
    return _make_amount(units, nanos, currency=currency)


def _weighted_choice(rng: Random, items: tuple, weight_index: int = -1):
    """Choose from a tuple of tuples using the last element of each as the weight."""
    weights = [t[weight_index] for t in items]
    return rng.choices(items, weights=weights, k=1)[0]


def _zipf_index(rng: Random, n: int, alpha: float) -> int:
    """Sample an index in [0, n) from a discrete Zipf distribution with exponent `alpha`."""
    if n <= 1:
        return 0
    # Inverse-transform via cumulative weights (fine for n up to ~1M; pure-stdlib).
    weights = [1.0 / ((i + 1) ** alpha) for i in range(n)]
    return rng.choices(range(n), weights=weights, k=1)[0]


def _pick_date(rng: Random, start_date: date, end_date: date) -> date:
    """Pick a date in [start_date, end_date] with day-of-week + mild seasonal weighting."""
    days = (end_date - start_date).days
    if days <= 0:
        return start_date
    candidates = []
    weights = []
    for offset in range(days + 1):
        d = start_date + timedelta(days=offset)
        w = _DOW_WEIGHTS[d.weekday()]
        # Mild seasonal: a small dip toward late May (end of typical Q2 buying cycle).
        if d.month == 5 and d.day > 20:
            w *= 0.85
        candidates.append(d)
        weights.append(w)
    return rng.choices(candidates, weights=weights, k=1)[0]


def _pick_session_start_dt(rng: Random, d: date) -> datetime:
    """Pick a datetime within `d` weighted toward 11am-9pm local."""
    # Hours 0..23 with peak at midday and evening.
    hour_weights = [
        0.2,  # 0
        0.1,
        0.1,
        0.1,
        0.1,
        0.2,
        0.4,
        0.7,
        0.9,
        1.0,
        1.1,
        1.4,  # 11
        1.5,
        1.4,
        1.3,
        1.3,
        1.4,
        1.5,
        1.6,
        1.5,  # 19
        1.3,
        1.0,
        0.7,
        0.4,
    ]
    hour = rng.choices(range(24), weights=hour_weights, k=1)[0]
    minute = rng.randint(0, 59)
    second = rng.randint(0, 59)
    return datetime(d.year, d.month, d.day, hour, minute, second, tzinfo=timezone.utc)


def _build_item(
    rng: Random, product: product_pb2.Product, quantity: int
) -> event_pb2.Item:
    """Build an Item from a Product (used in events' items[] arrays)."""
    return event_pb2.Item(
        item_id=product.product_id,
        sku=product.sku,
        item_name=product.name,
        item_brand=product.brand,
        item_category=product.category,
        item_category2=product.category2,
        item_variant=product.variant,
        size=product.size,
        price=product.price,
        quantity=quantity,
        percent_discount=product.percent_discount,
    )


def _items_total(items: list[event_pb2.Item]) -> amount_pb2.Amount:
    """Sum item price * quantity with percent_discount applied."""
    total_nanos = 0
    for it in items:
        unit_nanos = _amount_to_nanos(it.price)
        line_nanos = int(it.quantity * unit_nanos * (100 - it.percent_discount) / 100)
        total_nanos += line_nanos
    return _nanos_to_amount(total_nanos)


def _new_uuid(rng: Random) -> str:
    return str(UUID(int=rng.getrandbits(128)))


# ----------------------------------------------------------------------------
# GenSession
# ----------------------------------------------------------------------------


class GenSession:
    """Generate one or more deterministic GA4-style buy-flow sessions as `Event` streams.

    A "session" is a list of `event.v1.Event` records sharing one `session_id`. The
    primary deterministic unit is *one session*: `generate_one_session(index)` and
    `generate_one(index)` both return `list[Event]` for a deterministic walk through
    the funnel keyed on `(seed, index)`.

    `generate(count)` and `generate_range(start, end)` return flattened `list[Event]`
    across `count` (or `end - start`) sessions so the existing batched Delta writer in
    `main.py` can stream them directly to a single table.
    """

    def __init__(
        self,
        seed: int = 42,
        user_ids: list[str] | None = None,
        products: list[product_pb2.Product] | None = None,
        start_date: date = date(2026, 1, 1),
        end_date: date = date(2026, 5, 28),
        funnel_config: FunnelConfig | None = None,
        sessions_per_user_alpha: float = 1.5,
    ) -> None:
        self._seed = seed
        self._user_ids = user_ids or []
        self._products = products or []
        self._start_date = start_date
        self._end_date = end_date
        self._funnel = funnel_config or FUNNEL_PROFILES["realistic"]
        self._alpha = sessions_per_user_alpha
        # Cache list of in-stock products to sample from.
        self._sellable = [p for p in self._products if p.in_stock] or list(
            self._products
        )

    # -- core ----------------------------------------------------------------

    def generate_one_session(self, index: int = 0) -> list[event_pb2.Event]:
        """Generate all events for one deterministic session keyed on (seed, index)."""
        rng = Random(self._seed + index)

        # 1) Pick user via Zipf-weighted index (loyal customers appear more often).
        if self._user_ids:
            ui = _zipf_index(rng, len(self._user_ids), self._alpha)
            user_id = self._user_ids[ui]
        else:
            user_id = _new_uuid(rng)

        # 2) Pick a date in range, then a session start datetime.
        d = _pick_date(rng, self._start_date, self._end_date)
        session_start_dt = _pick_session_start_dt(rng, d)
        ts_ms = int(session_start_dt.timestamp() * 1000)

        # 3) Session-wide context.
        session_id = _new_uuid(rng)
        cart_id = _new_uuid(rng)
        device = _weighted_choice(rng, _DEVICE_CATEGORIES)[0]
        os = _weighted_choice(rng, _OPERATING_SYSTEMS_BY_DEVICE[device])[0]
        country, region, city, _ = _weighted_choice(rng, _GEOS)
        source, medium, campaign, _ = _weighted_choice(rng, _SOURCE_MEDIUMS)
        page_referrer = f"https://{source}.com/" if source not in ("direct",) else ""
        page_base = "https://shop.tidewell.com"

        ctx = _SessionCtx(
            rng=rng,
            user_id=user_id,
            session_id=session_id,
            cart_id=cart_id,
            device=device,
            os=os,
            country=country,
            region=region,
            city=city,
            source=source,
            medium=medium,
            campaign=campaign,
            page_referrer=page_referrer,
            page_base=page_base,
        )

        events: list[event_pb2.Event] = []
        funnel = self._funnel

        def _advance_ts():
            nonlocal ts_ms
            ts_ms += rng.randint(funnel.delay_min_s, funnel.delay_max_s) * 1000

        # ---- session_start ----
        first_session = rng.random() < 0.3
        e = self._make_event(ctx, ts_ms, "session_start", page_path="/")
        e.session_start.is_first_session = first_session
        e.session_start.landing_page = page_base + "/"
        events.append(e)
        _advance_ts()

        # ---- view_item_list (browsing the catalog) ----
        viewed_items: list[event_pb2.Item] = []
        if rng.random() < funnel.view_item_list_rate and self._sellable:
            list_categories = list({p.category for p in self._sellable})
            list_cat = rng.choice(list_categories)
            list_id = f"list_{list_cat.lower().replace(' ', '_')}"
            list_name = f"{list_cat} collection"
            # Sample up to 8 products of this category for the list view.
            in_cat = [p for p in self._sellable if p.category == list_cat]
            shown = rng.sample(in_cat, k=min(len(in_cat), rng.randint(3, 8)))
            items_for_list = [_build_item(rng, p, quantity=1) for p in shown]
            e = self._make_event(
                ctx,
                ts_ms,
                "view_item_list",
                items=items_for_list,
                page_path=f"/c/{list_cat.lower()}",
            )
            e.view_item_list.item_list_id = list_id
            e.view_item_list.item_list_name = list_name
            events.append(e)
            _advance_ts()

            # ---- view_item (one or more) ----
            n_views = max(
                0,
                int(
                    round(rng.gauss(funnel.view_items_mean, funnel.view_items_mean / 2))
                ),
            )
            n_views = min(n_views, len(shown))
            for product in rng.sample(shown, k=n_views):
                item = _build_item(rng, product, quantity=1)
                e = self._make_event(
                    ctx,
                    ts_ms,
                    "view_item",
                    items=[item],
                    page_path=f"/p/{product.sku.lower()}",
                )
                e.view_item.item_list_id = list_id
                e.view_item.item_list_name = list_name
                events.append(e)
                _advance_ts()
                viewed_items.append(item)

        # ---- add_to_cart (one event per add) ----
        cart_items: list[event_pb2.Item] = []
        for item in viewed_items:
            if rng.random() < funnel.add_to_cart_rate:
                # Use a deterministic quantity for the cart add.
                quantity = rng.choices([1, 1, 1, 2, 3], weights=[5, 5, 5, 2, 1], k=1)[0]
                cart_item = event_pb2.Item()
                cart_item.CopyFrom(item)
                cart_item.quantity = quantity
                e = self._make_event(ctx, ts_ms, "add_to_cart", items=[cart_item])
                e.add_to_cart.cart_id = cart_id
                events.append(e)
                _advance_ts()
                cart_items.append(cart_item)
                if len(cart_items) >= funnel.cart_size_max:
                    break

        # No items in cart -> session ends here.
        if not cart_items:
            return events

        # ---- view_cart ----
        if rng.random() >= funnel.view_cart_rate:
            return events
        cart_value = _items_total(cart_items)
        e = self._make_event(
            ctx, ts_ms, "view_cart", items=cart_items, value=cart_value
        )
        e.view_cart.cart_id = cart_id
        e.view_cart.num_items = sum(i.quantity for i in cart_items)
        events.append(e)
        _advance_ts()

        # ---- remove_from_cart (optional) ----
        if len(cart_items) > 1 and rng.random() < funnel.remove_from_cart_rate:
            victim_idx = rng.randint(0, len(cart_items) - 1)
            removed = cart_items.pop(victim_idx)
            e = self._make_event(ctx, ts_ms, "remove_from_cart", items=[removed])
            e.remove_from_cart.cart_id = cart_id
            e.remove_from_cart.reason = rng.choice(_REMOVE_REASONS)
            events.append(e)
            _advance_ts()
            cart_value = _items_total(cart_items)

        # ---- begin_checkout ----
        if rng.random() >= funnel.begin_checkout_rate:
            return events
        coupon = rng.choice(_COUPON_CODES) if rng.random() < 0.25 else ""
        e = self._make_event(
            ctx,
            ts_ms,
            "begin_checkout",
            items=cart_items,
            value=cart_value,
            page_path="/checkout",
        )
        e.begin_checkout.cart_id = cart_id
        if coupon:
            e.begin_checkout.coupon = coupon
        events.append(e)
        _advance_ts()

        # ---- add_shipping_info ----
        if rng.random() >= funnel.add_shipping_info_rate:
            return events
        tier, ship_units, ship_nanos, _ = _weighted_choice(rng, _SHIPPING_TIERS)
        shipping_amount = _make_amount(ship_units, ship_nanos)
        e = self._make_event(
            ctx,
            ts_ms,
            "add_shipping_info",
            items=cart_items,
            value=cart_value,
            page_path="/checkout/shipping",
        )
        e.add_shipping_info.cart_id = cart_id
        e.add_shipping_info.shipping_tier = tier
        e.add_shipping_info.shipping_cost.CopyFrom(shipping_amount)
        events.append(e)
        _advance_ts()

        # ---- add_payment_info ----
        if rng.random() >= funnel.add_payment_info_rate:
            return events
        payment_method = rng.choice(_PAYMENT_METHODS)
        e = self._make_event(
            ctx,
            ts_ms,
            "add_payment_info",
            items=cart_items,
            value=cart_value,
            page_path="/checkout/payment",
        )
        e.add_payment_info.cart_id = cart_id
        e.add_payment_info.payment_method = payment_method
        events.append(e)
        _advance_ts()

        # ---- purchase ----
        if rng.random() >= funnel.purchase_rate:
            return events
        transaction_id = _new_uuid(rng)
        tax_nanos = int(_amount_to_nanos(cart_value) * 0.085)
        tax = _nanos_to_amount(tax_nanos)
        order_total_nanos = (
            _amount_to_nanos(cart_value) + _amount_to_nanos(shipping_amount) + tax_nanos
        )
        order_total = _nanos_to_amount(order_total_nanos)
        e = self._make_event(
            ctx,
            ts_ms,
            "purchase",
            items=cart_items,
            value=order_total,
            page_path="/checkout/confirmation",
        )
        e.purchase.transaction_id = transaction_id
        if coupon:
            e.purchase.coupon = coupon
        e.purchase.tax.CopyFrom(tax)
        e.purchase.shipping.CopyFrom(shipping_amount)
        e.purchase.payment_method = payment_method
        e.purchase.shipping_tier = tier
        events.append(e)
        _advance_ts()

        # ---- refund (delayed, optional) ----
        if rng.random() < funnel.refund_rate:
            refund_delay_days = rng.randint(
                funnel.refund_delay_days_min, funnel.refund_delay_days_max
            )
            refund_ts_ms = ts_ms + refund_delay_days * 86_400 * 1000
            # Refund amount: full or partial.
            full_refund = rng.random() < 0.6
            if full_refund:
                refund_amount = order_total
            else:
                # Refund one item's line total + tax.
                victim = cart_items[rng.randint(0, len(cart_items) - 1)]
                unit_nanos = _amount_to_nanos(victim.price)
                line_nanos = int(
                    victim.quantity * unit_nanos * (100 - victim.percent_discount) / 100
                )
                refund_amount = _nanos_to_amount(line_nanos + int(line_nanos * 0.085))
            e = self._make_event(
                ctx,
                refund_ts_ms,
                "refund",
                items=cart_items,
                value=refund_amount,
                page_path="/account/orders",
            )
            e.refund.transaction_id = transaction_id
            e.refund.refund_amount.CopyFrom(refund_amount)
            e.refund.reason = rng.choice(_REFUND_REASONS)
            events.append(e)

        return events

    def generate_one(self, index: int = 0) -> list[event_pb2.Event]:
        """Alias for `generate_one_session` (a session is the deterministic unit)."""
        return self.generate_one_session(index)

    def generate(self, count: int) -> list[event_pb2.Event]:
        """Generate events from `count` sessions, flattened in session order."""
        if count <= 0:
            return []
        events: list[event_pb2.Event] = []
        for i in range(count):
            events.extend(self.generate_one_session(i))
        return events

    def generate_range(self, start: int, end: int) -> list[event_pb2.Event]:
        """Generate events for sessions [start, end), flattened.

        Used by the batched Delta writer in main.py. Each batch covers a slice of
        sessions; total event-row count per batch is ~6x session count on average.
        """
        if start >= end:
            return []
        events: list[event_pb2.Event] = []
        for i in range(start, end):
            events.extend(self.generate_one_session(i))
        return events

    # -- helpers -------------------------------------------------------------

    def _make_event(
        self,
        ctx: "_SessionCtx",
        ts_ms: int,
        event_name: str,
        items: list[event_pb2.Item] | None = None,
        value: amount_pb2.Amount | None = None,
        page_path: str = "/",
    ) -> event_pb2.Event:
        e = event_pb2.Event()
        e.event_id = _new_uuid(ctx.rng)
        e.event_name = event_name
        e.user_id = ctx.user_id
        e.session_id = ctx.session_id
        e.event_timestamp_ms = ts_ms
        e.device_category = ctx.device
        e.operating_system = ctx.os
        e.country = ctx.country
        e.region = ctx.region
        e.city = ctx.city
        e.source = ctx.source
        e.medium = ctx.medium
        e.campaign = ctx.campaign
        e.page_location = ctx.page_base + page_path
        e.page_referrer = ctx.page_referrer
        if items:
            e.items.extend(items)
            e.currency = "USD"
        if value is not None:
            e.value.CopyFrom(value)
            e.currency = "USD"
        return e

    # -- I/O -----------------------------------------------------------------

    @staticmethod
    def event_to_dict(event: event_pb2.Event) -> dict:
        """Convert an Event proto to a JSON-serializable dict (NDJSON format)."""
        return json_format.MessageToDict(
            event,
            always_print_fields_with_no_presence=False,
            preserving_proto_field_name=True,
        )

    @staticmethod
    def write_ndjson(path: str | Path, events: list[event_pb2.Event]) -> None:
        """Write events to newline-delimited JSON (one JSON object per line)."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w") as f:
            for event in events:
                f.write(json.dumps(GenSession.event_to_dict(event)) + "\n")

    @staticmethod
    def read_ndjson(path: str | Path) -> list[event_pb2.Event]:
        """Read events from a newline-delimited JSON file."""
        path = Path(path)
        events = []
        with path.open() as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                d = json.loads(line)
                event = event_pb2.Event()
                json_format.ParseDict(d, event)
                events.append(event)
        return events


# ----------------------------------------------------------------------------
# Internal session context
# ----------------------------------------------------------------------------


@dataclass
class _SessionCtx:
    """Per-session context shared by every Event in the session."""

    rng: Random
    user_id: str
    session_id: str
    cart_id: str
    device: str
    os: str
    country: str
    region: str
    city: str
    source: str
    medium: str
    campaign: str
    page_referrer: str
    page_base: str
