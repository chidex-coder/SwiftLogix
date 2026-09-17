"""Shared fixtures and payload helpers for the test suite."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from swiftlogix.config import Config
from swiftlogix.registry import SchemaRegistry
from swiftlogix.stream import Record


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


