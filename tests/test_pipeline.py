"""Tests for the properties the design actually depends on.

These are not smoke tests. Each one pins a claim made in the architecture:
idempotency under redelivery, the 24h watermark boundary, the contract gate,
the circuit breaker, and recovery via replay.
"""

from __future__ import annotations

import json
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from swiftlogix.config import Config
from swiftlogix.costs import CostModel
from swiftlogix.generator import EventGenerator
from swiftlogix.ingest import FirehoseDelivery
from swiftlogix.monitors import CRITICAL, MonitorSuite
from swiftlogix.registry import SchemaRegistry
from swiftlogix.replay import remediate_schema, replay_quarantine
from swiftlogix.stream import Consumer, KinesisLikeStream, Record
from swiftlogix.warehouse import Warehouse


@pytest.fixture
def cfg(tmp_path: Path) -> Config:
    c = Config()
    c.data_dir = tmp_path / "data"
    c.ensure_dirs()
    return c


@pytest.fixture
def registry(cfg: Config) -> SchemaRegistry:
    return SchemaRegistry(cfg)


def _record(payload: dict, seq: int = 1) -> Record:
    return Record("shardId-000000000000", seq, payload.get("shipment_id", "pk"),
                  datetime.now(timezone.utc), payload)


def _payload(**overrides) -> dict:
    now = datetime.now(timezone.utc)
    base = {
        "event_id": "a" * 32,
        "event_type": "status",
        "event_ts": now.isoformat(),
        "shipment_id": "SHP-20260817-00000001",
        "vehicle_id": "VEH-000001",
        "hub_code": "HUB-GB-001",
        "source": "partner_courier_api",
        "status": "in_transit",
        "geo": {"lat": 51.5, "lon": -0.12},
        "temperature_c": None,
        "delivery_window": {
            "start": now.isoformat(),
            "end": (now + timedelta(hours=4)).isoformat(),
        },
        "metadata": {"partner_ref": "abc123"},
    }
    base.update(overrides)
    return base


# ----------------------------------------------------------------------
# contract
# ----------------------------------------------------------------------
def test_v1_payload_passes_contract(registry: SchemaRegistry):
    assert registry.validate(_payload()).ok


def test_renamed_field_is_rejected_not_nulled(registry: SchemaRegistry):
    """The core of the $480K incident: a rename must fail loudly."""
    drifted = _payload()
    drifted.pop("delivery_window")
    drifted["delivery"] = {"window_start": "2026-08-17T10:00:00+00:00",
                           "window_end": "2026-08-17T14:00:00+00:00"}

    verdict = registry.validate(drifted)
    assert not verdict.ok
    assert "delivery" in verdict.reason
    # The raw payload survives so a replay is possible.
    assert "delivery" in verdict.observed_fields


def test_v2_activation_maps_drifted_payload_to_canonical(registry: SchemaRegistry):
    drifted = _payload()
    drifted.pop("delivery_window")
    drifted["delivery"] = {"window_start": "2026-08-17T10:00:00+00:00",
                           "window_end": "2026-08-17T14:00:00+00:00"}
    assert not registry.validate(drifted).ok

    registry.load("v2")
    registry.activate("v2")

    verdict = registry.validate(drifted)
    assert verdict.ok
    assert verdict.schema_version == "v2"
    # Mapped into the canonical shape, so downstream SQL never learns about v2.
    assert verdict.canonical["delivery_window"]["start"] == "2026-08-17T10:00:00+00:00"
    assert "delivery" not in verdict.canonical


def test_unknown_extra_field_is_rejected(registry: SchemaRegistry):
    assert not registry.validate(_payload(surprise_column="boom")).ok


def test_metadata_is_open_by_design(registry: SchemaRegistry):
    """metadata is the VARIANT escape hatch: new keys must never break a load."""
    assert registry.validate(_payload(metadata={"brand_new_key": [1, 2, 3]})).ok


def test_compatibility_check_flags_the_rename_as_breaking(registry: SchemaRegistry):
    registry.load("v2")
    compatible, problems = registry.check_compatibility("v1", "v2")
    assert not compatible
    assert any("delivery_window" in p for p in problems)


# ----------------------------------------------------------------------
# stream
# ----------------------------------------------------------------------
def test_partition_key_distributes_across_shards(cfg: Config):
    stream = KinesisLikeStream(cfg)
    gen = EventGenerator(cfg)
    stream.put_records(list(gen.emit(2_000)))
    stream.close()

    sizes = [
        sum(1 for _ in open(cfg.stream_dir / stream.name / f"{stream.shard_id(i)}.jsonl"))
        for i in range(cfg.shards)
    ]
    assert all(s > 0 for s in sizes), "a hot shard would starve the others"
    assert max(sizes) < sum(sizes) * 0.35


def test_consumer_rewind_redelivers(cfg: Config):
    stream = KinesisLikeStream(cfg)
    gen = EventGenerator(cfg)
    stream.put_records(list(gen.emit(400)))

    consumer = Consumer(cfg, stream, "test")
    first = list(consumer.read())
    consumer.commit(first)
    assert list(consumer.read()) == []

    consumer.rewind(80)
    assert len(list(consumer.read())) > 0
    stream.close()


# ----------------------------------------------------------------------
# warehouse
# ----------------------------------------------------------------------
def test_merge_is_idempotent_under_redelivery(cfg: Config, registry: SchemaRegistry):
    """Kinesis is at-least-once; the sink has to make it effectively-once."""
    delivery = FirehoseDelivery(cfg, registry)
    wh = Warehouse(cfg)

    records = [_record(_payload(event_id=f"{i:032x}"), i) for i in range(50)]
    batch = delivery.deliver(records)
    first = wh.merge_batch([batch.bronze_path])
    assert first.rows_merged == 50

    # The identical file, merged again -- exactly what a replay or a consumer
    # restart produces.
    wh.con.execute(
        "UPDATE meta.load_watermark SET watermark_ts = TIMESTAMP '1970-01-01'"
    )
    second = wh.merge_batch([batch.bronze_path])
    assert second.rows_merged == 0
    assert wh.duplicate_check() == 0
    wh.close()


def test_within_batch_duplicates_are_collapsed(cfg: Config, registry: SchemaRegistry):
    delivery = FirehoseDelivery(cfg, registry)
    wh = Warehouse(cfg)

    payload = _payload(event_id="d" * 32)
    records = [_record(payload, i) for i in range(10)]  # same event, ten times
    batch = delivery.deliver(records)
    stats = wh.merge_batch([batch.bronze_path])

    assert stats.rows_merged == 1
    assert stats.duplicates_suppressed == 9
    wh.close()


def test_events_beyond_the_watermark_are_quarantined_not_dropped(
    cfg: Config, registry: SchemaRegistry
):
    delivery = FirehoseDelivery(cfg, registry)
    wh = Warehouse(cfg)

    stale = (datetime.now(timezone.utc) - timedelta(hours=30)).isoformat()
    records = [_record(_payload(event_id="e" * 32, event_ts=stale))]
    batch = delivery.deliver(records)
    stats = wh.merge_batch([batch.bronze_path])

    assert stats.beyond_watermark == 1
    assert wh.counts()["late_arrivals"] == 1
    wh.close()


def test_late_arrival_inside_window_does_not_roll_back_current_status(
    cfg: Config, registry: SchemaRegistry
):
    """Out-of-order delivery must not un-deliver a shipment."""
    delivery = FirehoseDelivery(cfg, registry)
    wh = Warehouse(cfg)
    now = datetime.now(timezone.utc)

    delivered = _payload(event_id="1" * 32, status="delivered", event_ts=now.isoformat())
    wh.merge_batch([delivery.deliver([_record(delivered)]).bronze_path])

    late_pickup = _payload(
        event_id="2" * 32, status="pickup",
        event_ts=(now - timedelta(hours=3)).isoformat(),
    )
    wh.merge_batch([delivery.deliver([_record(late_pickup)]).bronze_path])

    status = wh.con.execute(
        "SELECT current_status FROM gold.fact_shipment_status_current"
    ).fetchone()[0]
    assert status == "delivered"
    wh.close()


def test_write_audit_publish_blocks_a_bad_batch(cfg: Config, registry: SchemaRegistry):
    """Stale-but-correct beats fresh-but-wrong."""
    cfg.null_rate_alert_pct = 10.0
    delivery = FirehoseDelivery(cfg, registry)
    wh = Warehouse(cfg)

    # Contract-valid, but the business-critical column is absent en masse --
    # the class of drift that type checks alone cannot see.
    records = [
        _record(_payload(event_id=f"{i:032x}", delivery_window=None), i) for i in range(20)
    ]
    batch = delivery.deliver(records)
    stats = wh.merge_batch([batch.bronze_path])

    assert stats.promotion_state == "BLOCKED_AUDIT"
    assert any("null rate" in f for f in stats.audit_failures)
    assert wh.counts()["fact_events"] == 0, "nothing corrupt reached gold"
    wh.close()


def test_circuit_breaker_halts_promotion(cfg: Config, registry: SchemaRegistry):
    delivery = FirehoseDelivery(cfg, registry)
    wh = Warehouse(cfg)
    batch = delivery.deliver([_record(_payload())])

    stats = wh.merge_batch([batch.bronze_path], quarantine_pct=42.0)

    assert stats.promotion_state == "BLOCKED_CIRCUIT_BREAKER"
    assert wh.counts()["fact_events"] == 0
    wh.close()


def test_vault_satellites_version_changes_not_redeliveries(
    cfg: Config, registry: SchemaRegistry
):
    delivery = FirehoseDelivery(cfg, registry)
    wh = Warehouse(cfg)
    now = datetime.now(timezone.utc)

    window = {"start": now.isoformat(), "end": (now + timedelta(hours=4)).isoformat()}
    fixed = dict(event_ts=now.isoformat(), delivery_window=window,
                 geo={"lat": 51.5, "lon": -0.12})

    wh.merge_batch([delivery.deliver(
        [_record(_payload(event_id="a" * 32, status="pickup", **fixed))]
    ).bronze_path])
    after_first = wh.counts()["sat_status"]

    # Identical business content under a new event_id -- a redelivery in all
    # but name. hash_diff has to collapse it.
    wh.merge_batch([delivery.deliver(
        [_record(_payload(event_id="b" * 32, status="pickup", **fixed))]
    ).bronze_path])
    assert wh.counts()["sat_status"] == after_first, "hash_diff should suppress this"

    changed = _payload(event_id="c" * 32, status="delivered", **fixed)
    wh.merge_batch([delivery.deliver([_record(changed)]).bronze_path])
    assert wh.counts()["sat_status"] > after_first, "a real change must be versioned"
    wh.close()


# ----------------------------------------------------------------------
# monitors
# ----------------------------------------------------------------------
def test_null_rate_monitor_fires_on_the_rename_signature(cfg: Config, registry: SchemaRegistry):
    monitors = MonitorSuite(cfg)
    delivery = FirehoseDelivery(cfg, registry)
    records = [
        _record(_payload(event_id=f"{i:032x}", delivery_window=None), i) for i in range(30)
    ]
    batch = delivery.deliver(records)

    alerts = monitors.check_null_rates(batch)
    assert alerts and alerts[0].severity == CRITICAL
    assert "delivery_window_start" in alerts[0].message


def test_alert_cooldown_prevents_a_page_storm(cfg: Config, registry: SchemaRegistry):
    monitors = MonitorSuite(cfg)
    delivery = FirehoseDelivery(cfg, registry)
    batch = delivery.deliver(
        [_record(_payload(event_id=f"{i:032x}", delivery_window=None), i) for i in range(30)]
    )
    assert monitors.check_null_rates(batch)
    assert not monitors.check_null_rates(batch), "second identical alert must be suppressed"


# ----------------------------------------------------------------------
# replay
# ----------------------------------------------------------------------
def test_replay_recovers_quarantined_records_without_duplicates(
    cfg: Config, registry: SchemaRegistry
):
    """The 11-day backfill, in miniature."""
    delivery = FirehoseDelivery(cfg, registry)
    wh = Warehouse(cfg)

    drifted = []
    for i in range(40):
        p = _payload(event_id=f"{i:032x}")
        p.pop("delivery_window")
        p["delivery"] = {"window_start": "2026-08-17T10:00:00+00:00",
                         "window_end": "2026-08-17T14:00:00+00:00"}
        drifted.append(_record(p, i))

    batch = delivery.deliver(drifted)
    assert batch.accepted == 0 and batch.quarantined == 40

    remediate_schema(registry, wh)
    result = replay_quarantine(cfg, registry, delivery, wh)

    assert result.recovered == 40
    assert result.duplicates_created == 0
    assert wh.duplicate_check() == 0

    # And the recovered rows carry the delivery window that was lost.
    populated = wh.con.execute(
        "SELECT count(*) FROM gold.fact_shipment_events WHERE delivery_window_start IS NOT NULL"
    ).fetchone()[0]
    assert populated == 40, "every recovered row carries the window that was lost"
    wh.close()


def test_replay_archives_quarantine_rather_than_deleting_it(
    cfg: Config, registry: SchemaRegistry
):
    delivery = FirehoseDelivery(cfg, registry)
    wh = Warehouse(cfg)
    delivery.deliver([_record(_payload(surprise="x"))])

    remediate_schema(registry, wh)
    replay_quarantine(cfg, registry, delivery, wh)

    archived = list((cfg.lake_dir / "quarantine_archive").glob("*.parquet"))
    assert archived, "the audit trail for the restatement must survive"
    wh.close()


# ----------------------------------------------------------------------
# cost model
# ----------------------------------------------------------------------
def test_cost_model_clears_the_budget_at_a_realistic_reprocess_ratio():
    model = CostModel(reprocess_ratio=30.0, compression_ratio=6.0)
    summary = model.summary()
    assert summary["under_budget"]
    assert summary["proposed"] < 42_000
    # And it still clears if incremental loading only half delivers.
    assert summary["conservative"] < 42_000


def test_cost_model_is_honest_when_the_lever_fails():
    """No incremental saving at all means the mandate is missed. Say so."""
    model = CostModel(reprocess_ratio=1.0, compression_ratio=1.0)
    assert not model.summary()["under_budget"]
