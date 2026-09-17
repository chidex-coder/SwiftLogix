"""The two-partner drift story: random-but-reproducible timing, a second
partner that v2 does not cover, and recovery of both through v2 + v3."""

from __future__ import annotations

import random

import pytest

from swiftlogix.config import Config
from swiftlogix.generator import DRIFT_KINDS, EventGenerator
from swiftlogix.incidents import plan_incidents
from swiftlogix.ingest import FirehoseDelivery
from swiftlogix.registry import SchemaRegistry
from swiftlogix.replay import remediate_schema, replay_quarantine
from swiftlogix.warehouse import Warehouse

from conftest import _payload, _record  # noqa: E402  (shared helpers)


# ----------------------------------------------------------------------
# scheduling
# ----------------------------------------------------------------------
def test_first_drift_is_random_within_the_window_and_reproducible():
    cfg = Config()
    a, b = plan_incidents(cfg), plan_incidents(cfg)
    lo, hi = cfg.drift_window_seconds
    assert lo <= a[0].at <= hi
    assert a[0].at == b[0].at, "same seed, same timing"

    other = Config()
    other.seed = cfg.seed + 1
    seen = {plan_incidents(Config(seed=s))[0].at for s in range(20)}
    assert len(seen) > 5, "different seeds spread the drift across the window"


def test_second_partner_is_scheduled_after_the_first_replay_not_at_a_fixed_second():
    cfg = Config()
    first, second = plan_incidents(cfg)
    assert second.at is None
    lo, hi = cfg.second_drift_delay_seconds
    assert lo <= second.delay_after_previous <= hi
    assert second.source != first.source and second.kind != first.kind
    assert second.contract_version == "v3"


def test_drift_times_can_be_pinned_from_config():
    cfg = Config()
    cfg.drift_at_second, cfg.second_drift_at_second = 20.0, 55.0
    first, second = plan_incidents(cfg)
    assert (first.at, second.at) == (20.0, 55.0)


# ----------------------------------------------------------------------
# generator
# ----------------------------------------------------------------------
def _events_by_source(gen: EventGenerator, n: int = 3000) -> dict:
    out: dict = {}
    for e in gen.emit(n):
        out.setdefault(e["source"], []).append(e)
    return out


def test_second_partner_moves_geo_and_only_that_partner():
    cfg = Config()
    gen = EventGenerator(cfg)
    assert not gen.drift_active
    gen.trigger_drift(cfg.second_drift_source, "geo")
    assert gen.drift_active

    by = _events_by_source(gen)
    driver = by[cfg.second_drift_source]
    assert all("position" in e and "geo" not in e for e in driver)
    assert all(set(e["position"]) == {"latitude", "longitude"} for e in driver)
    # the courier API has not drifted yet: still the canonical shape
    assert all("geo" in e and "delivery_window" in e for e in by[cfg.drift_source])
    assert gen.drifted_by_source == {cfg.second_drift_source: len(driver)}


def test_both_partners_can_drift_in_different_ways_at_once():
    cfg = Config()
    gen = EventGenerator(cfg)
    gen.trigger_drift()  # courier, delivery_window
    gen.trigger_drift(cfg.second_drift_source, "geo")
    by = _events_by_source(gen)
    assert all("delivery" in e and "geo" in e for e in by[cfg.drift_source])
    assert all("position" in e and "delivery_window" in e for e in by[cfg.second_drift_source])
    assert all("geo" in e and "delivery_window" in e for e in by["telematics_gateway"])


def test_unknown_drift_kind_is_rejected():
    with pytest.raises(ValueError):
        EventGenerator(Config()).trigger_drift("driver_mobile_app", "temperature_units")
    assert set(DRIFT_KINDS) == {"delivery_window", "geo"}


# ----------------------------------------------------------------------
# registry: v3 covers the geo move, v2 keeps covering the window rename
# ----------------------------------------------------------------------
def _geo_drifted(**overrides) -> dict:
    p = _payload(**overrides)
    geo = p.pop("geo")
    p["position"] = {"latitude": geo["lat"], "longitude": geo["lon"]}
    return p


def test_v2_does_not_cover_the_second_partner(registry: SchemaRegistry):
    registry.load("v2"); registry.activate("v2")
    assert not registry.validate(_geo_drifted()).ok, "a different rename needs its own contract"


def test_v3_maps_the_geo_move_back_to_canonical_and_versions_coexist(registry: SchemaRegistry):
    registry.load("v2"); registry.activate("v2")
    registry.load("v3"); registry.activate("v3")

    verdict = registry.validate(_geo_drifted())
    assert verdict.ok and verdict.schema_version == "v3"
    assert verdict.canonical["geo"] == _payload()["geo"]
    assert "position" not in verdict.canonical

    courier = _payload(); courier.pop("delivery_window")
    courier["delivery"] = {"window_start": "2026-08-17T10:00:00+00:00", "window_end": "2026-08-17T14:00:00+00:00"}
    assert registry.validate(courier).schema_version == "v2"
    assert registry.validate(_payload()).schema_version == "v1"


def test_moving_geo_is_a_breaking_change_by_the_compatibility_rules(registry: SchemaRegistry):
    registry.load("v3")
    compatible, problems = registry.check_compatibility("v1", "v3")
    assert not compatible and any("geo" in p for p in problems)


# ----------------------------------------------------------------------
# replay: both incidents recover through their own contract
# ----------------------------------------------------------------------
def test_replay_recovers_both_drifts_without_duplicates(cfg: Config, registry: SchemaRegistry):
    delivery = FirehoseDelivery(cfg, registry)
    wh = Warehouse(cfg)

    records = []
    for i in range(30):
        p = _payload(event_id=f"{i:032x}"); p.pop("delivery_window")
        p["delivery"] = {"window_start": "2026-08-17T10:00:00+00:00", "window_end": "2026-08-17T14:00:00+00:00"}
        records.append(_record(p, i))
    for i in range(30, 50):
        records.append(_record(_geo_drifted(event_id=f"{i:032x}"), i))

    batch = delivery.deliver(records)
    assert batch.accepted == 0 and batch.quarantined == 50

    remediate_schema(registry, wh, version="v2")
    first = replay_quarantine(cfg, registry, delivery, wh)
    assert first.recovered == 30 and first.still_rejected == 20, "v2 fixes the courier, not the driver app"

    remediate_schema(registry, wh, version="v3", note="driver app moved geo")
    second = replay_quarantine(cfg, registry, delivery, wh)
    assert second.recovered == 20 and second.still_rejected == 0
    assert first.duplicates_created == second.duplicates_created == 0
    assert wh.duplicate_check() == 0

    versions = dict(wh.con.execute(
        "SELECT schema_version, count(*) FROM gold.fact_shipment_events GROUP BY 1").fetchall())
    assert versions == {"v2": 30, "v3": 20}
    with_geo = wh.con.execute(
        "SELECT count(*) FROM gold.fact_shipment_events WHERE geo_lat IS NOT NULL AND geo_lon IS NOT NULL").fetchone()[0]
    assert with_geo == 50, "every recovered row carries the position that was lost"
    wh.close()
