"""Tests for GenSession (GA4-style buy-flow session generator) and NDJSON round-trip."""

import os
import tempfile
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path

import protovalidate
import pytest
from learning_spark_datagen.datagen import (
    FUNNEL_PROFILES,
    GenProduct,
    GenSession,
    GenUser,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def user_ids() -> list[str]:
    users = GenUser(seed=1).generate(50)
    return [u.uuid for u in users]


@pytest.fixture(scope="module")
def products():
    return GenProduct(seed=2).generate(100)


@pytest.fixture(scope="module")
def gen(user_ids, products):
    return GenSession(
        seed=42,
        user_ids=user_ids,
        products=products,
        start_date=date(2026, 1, 1),
        end_date=date(2026, 5, 28),
        funnel_config=FUNNEL_PROFILES["realistic"],
    )


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


def test_session_deterministic(gen):
    a = gen.generate_one_session(0)
    b = gen.generate_one_session(0)
    assert len(a) == len(b)
    for x, y in zip(a, b):
        assert x.event_id == y.event_id
        assert x.event_name == y.event_name
        assert x.event_timestamp_ms == y.event_timestamp_ms
        assert x.session_id == y.session_id
        assert x.user_id == y.user_id


def test_generate_one_alias_matches(gen):
    s1 = gen.generate_one_session(7)
    s2 = gen.generate_one(7)
    assert len(s1) == len(s2)
    for x, y in zip(s1, s2):
        assert x.event_id == y.event_id


def test_generate_range_matches_generate(gen):
    flat = gen.generate(20)
    sliced = []
    for i in range(20):
        sliced.extend(gen.generate_one_session(i))
    assert len(flat) == len(sliced)
    for a, b in zip(flat, sliced):
        assert a.event_id == b.event_id
        assert a.event_name == b.event_name


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def test_protovalidate_passes_on_every_event(gen):
    events = gen.generate(50)
    assert events
    for e in events:
        protovalidate.validate(e)


# ---------------------------------------------------------------------------
# Funnel invariants
# ---------------------------------------------------------------------------


def test_every_session_starts_with_session_start(gen):
    for i in range(50):
        s = gen.generate_one_session(i)
        assert s, f"session {i} should produce at least one event"
        assert s[0].event_name == "session_start"


def test_events_within_session_are_time_ordered(gen):
    for i in range(50):
        s = gen.generate_one_session(i)
        for prev, cur in zip(s, s[1:]):
            assert cur.event_timestamp_ms >= prev.event_timestamp_ms


def test_session_id_is_stable_within_session(gen):
    for i in range(20):
        s = gen.generate_one_session(i)
        sids = {e.session_id for e in s}
        uids = {e.user_id for e in s}
        assert len(sids) == 1
        assert len(uids) == 1


def test_purchase_implies_begin_checkout(gen):
    # Walk many sessions to find purchase ones.
    purchase_sessions = 0
    for i in range(500):
        s = gen.generate_one_session(i)
        names = [e.event_name for e in s]
        if "purchase" in names:
            purchase_sessions += 1
            assert "begin_checkout" in names
            assert "add_to_cart" in names
            assert names.index("begin_checkout") < names.index("purchase")
    assert purchase_sessions > 0, "expected at least one purchase across 500 sessions"


def test_refund_matches_purchase_transaction_id(gen):
    found = False
    for i in range(2000):
        s = gen.generate_one_session(i)
        purchase = next((e for e in s if e.event_name == "purchase"), None)
        refund = next((e for e in s if e.event_name == "refund"), None)
        if purchase and refund:
            found = True
            assert refund.refund.transaction_id == purchase.purchase.transaction_id
    assert found, "expected at least one refund across 2000 sessions"


# ---------------------------------------------------------------------------
# Referential integrity
# ---------------------------------------------------------------------------


def test_user_id_belongs_to_users_file(gen, user_ids):
    seen = set()
    for i in range(200):
        s = gen.generate_one_session(i)
        for e in s:
            seen.add(e.user_id)
    assert seen, "expected events to be produced"
    assert seen.issubset(set(user_ids))


def test_item_ids_reference_products(gen, products):
    product_ids = {p.product_id for p in products}
    for i in range(200):
        s = gen.generate_one_session(i)
        for e in s:
            for item in e.items:
                assert item.item_id in product_ids


# ---------------------------------------------------------------------------
# Date range
# ---------------------------------------------------------------------------


def test_session_start_dates_within_range(gen):
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    # Refunds happen up to 21 days AFTER purchase, so the upper bound includes
    # that delay. We bound by end_date + refund_delay_days_max from FUNNEL_PROFILES.
    end_strict = datetime(2026, 5, 28, 23, 59, 59, tzinfo=timezone.utc)
    refund_max_delay_ms = 21 * 86_400 * 1000
    for i in range(200):
        s = gen.generate_one_session(i)
        # First event in the session must be on a date in range.
        first_dt_ms = s[0].event_timestamp_ms
        assert first_dt_ms >= int(start.timestamp() * 1000)
        assert first_dt_ms <= int(end_strict.timestamp() * 1000)
        # Subsequent events (including refunds) can slip up to refund_delay_max past end_date.
        last_dt_ms = s[-1].event_timestamp_ms
        assert last_dt_ms <= int(end_strict.timestamp() * 1000) + refund_max_delay_ms


# ---------------------------------------------------------------------------
# Funnel distribution
# ---------------------------------------------------------------------------


def test_funnel_rates_match_config(gen):
    """Over 1000 sessions, observed rates should be within tolerance of FunnelConfig."""
    n_sessions = 1000
    counts: Counter = Counter()
    purchase_in_session = 0
    refund_in_session = 0
    for i in range(n_sessions):
        s = gen.generate_one_session(i)
        names = [e.event_name for e in s]
        for n in names:
            counts[n] += 1
        if "purchase" in names:
            purchase_in_session += 1
        if "refund" in names:
            refund_in_session += 1

    cfg = FUNNEL_PROFILES["realistic"]
    # session_start exactly equals n_sessions.
    assert counts["session_start"] == n_sessions

    # view_item_list_rate ~ 85%.
    list_rate = counts["view_item_list"] / n_sessions
    assert abs(list_rate - cfg.view_item_list_rate) < 0.05, (
        f"view_item_list rate {list_rate:.3f} not near {cfg.view_item_list_rate}"
    )

    # purchase: ratio of sessions with purchase to total should be ~5%.
    purchase_rate = purchase_in_session / n_sessions
    assert 0.02 <= purchase_rate <= 0.10, (
        f"expected ~5% purchase rate, got {purchase_rate:.3f}"
    )

    # refund: should be ~refund_rate fraction of purchases.
    if purchase_in_session > 0:
        refund_rate = refund_in_session / purchase_in_session
        # Allow a wide window since 50 expected refunds out of ~50 purchases has high variance.
        assert refund_rate <= 0.15, (
            f"refund rate {refund_rate:.3f} should be near {cfg.refund_rate}"
        )


def test_aggressive_profile_yields_more_purchases(user_ids, products):
    aggressive = GenSession(
        seed=42,
        user_ids=user_ids,
        products=products,
        start_date=date(2026, 1, 1),
        end_date=date(2026, 5, 28),
        funnel_config=FUNNEL_PROFILES["aggressive"],
    )
    realistic = GenSession(
        seed=42,
        user_ids=user_ids,
        products=products,
        start_date=date(2026, 1, 1),
        end_date=date(2026, 5, 28),
        funnel_config=FUNNEL_PROFILES["realistic"],
    )
    agg_purchases = sum(
        1
        for i in range(500)
        if any(e.event_name == "purchase" for e in aggressive.generate_one_session(i))
    )
    real_purchases = sum(
        1
        for i in range(500)
        if any(e.event_name == "purchase" for e in realistic.generate_one_session(i))
    )
    assert agg_purchases > real_purchases, (
        f"aggressive purchases={agg_purchases} should exceed realistic={real_purchases}"
    )


# ---------------------------------------------------------------------------
# Items / value semantics
# ---------------------------------------------------------------------------


def test_purchase_events_have_items_and_value(gen):
    for i in range(500):
        s = gen.generate_one_session(i)
        for e in s:
            if e.event_name == "purchase":
                assert len(e.items) >= 1
                assert e.currency == "USD"
                assert e.value.units > 0 or e.value.nanos > 0
                assert e.purchase.transaction_id


def test_session_start_has_no_items(gen):
    for i in range(50):
        s = gen.generate_one_session(i)
        first = s[0]
        assert first.event_name == "session_start"
        assert len(first.items) == 0


# ---------------------------------------------------------------------------
# NDJSON round-trip
# ---------------------------------------------------------------------------


def test_ndjson_round_trip(gen):
    events = gen.generate(5)
    fd, name = tempfile.mkstemp(suffix=".ndjson")
    os.close(fd)
    path = Path(name)
    try:
        GenSession.write_ndjson(path, events)
        read_back = GenSession.read_ndjson(path)
        assert len(read_back) == len(events)
        for a, b in zip(events, read_back):
            protovalidate.validate(a)
            protovalidate.validate(b)
            assert a.event_id == b.event_id
            assert a.event_name == b.event_name
            assert a.user_id == b.user_id
            assert a.session_id == b.session_id
            assert a.event_timestamp_ms == b.event_timestamp_ms
            assert a.WhichOneof("payload") == b.WhichOneof("payload")
            assert len(a.items) == len(b.items)
    finally:
        path.unlink(missing_ok=True)
