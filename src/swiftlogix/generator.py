"""Synthetic SwiftLogix event generator.

Models the real fleet: shipments move through a lifecycle state machine while
their carrying vehicle emits GPS, temperature and status events, and sorting
hubs emit scan events at conveyor checkpoints.

The generator deliberately reproduces the four things that make this pipeline
hard in production:

  1. at-least-once redelivery  -> byte-identical duplicates with the same event_id
  2. late arrivals             -> events with an event_ts up to 24h in the past
  3. heterogeneous metadata    -> a nested object whose keys vary by source
  4. silent schema drift       -> partners change their payload shape mid-stream,
                                 without notice: the courier API renames
                                 delivery_window.start to delivery.window_start,
                                 and later the driver app moves geo.lat/lon to
                                 position.latitude/longitude
"""

from __future__ import annotations

import hashlib
import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterator, List, Optional

from .config import Config

STATUS_FLOW = ["pickup", "in_transit", "exception", "delivered"]

COUNTRIES = [
    "GB", "IE", "FR", "DE", "NL", "BE", "ES", "PT", "IT", "PL", "CZ", "AT",
    "SE", "NO", "DK", "FI", "CH", "RO", "HU", "GR", "US", "CA", "MX", "BR",
    "AE", "SA", "ZA", "IN", "SG", "JP", "AU", "NZ",
]

# The ways a partner can silently change shape. Each is a structural move --
# nothing errors, the old path just reads as null -- which is exactly the class
# of failure the contract layer exists to catch.
DRIFT_KINDS = {
    "delivery_window": "delivery_window.start/end -> delivery.window_start/window_end",
    "geo": "geo.lat/lon -> position.latitude/longitude",
}

SOURCES = [
    ("telematics_gateway", 0.48),
    ("hub_scanner_net", 0.22),
    ("partner_courier_api", 0.18),
    ("driver_mobile_app", 0.12),  # large enough that its drift trips the 5% breaker in one batch
]


def _hash(*parts: Any) -> str:
    """Deterministic surrogate/diff hash.

    Data Vault hash keys are computed at the edge, not looked up in the
    warehouse -- that is what lets hubs, links and satellites load in parallel
    with no sequence contention at streaming speed.
    """
    joined = "||".join("" if p is None else str(p).strip().upper() for p in parts)
    return hashlib.sha1(joined.encode()).hexdigest()


@dataclass
class Shipment:
    shipment_id: str
    vehicle_id: str
    hub_code: str
    status_idx: int = 0
    events_emitted: int = 0
    delivery_start: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    delivery_end: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    lat: float = 51.5074
    lon: float = -0.1278
    temp_c: float = 4.0
    done: bool = False


class EventGenerator:
    """Produces raw event payloads exactly as an upstream producer would."""

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.rng = random.Random(cfg.seed)
        self.vehicles = [f"VEH-{i:06d}" for i in range(cfg.vehicles)]
        self.hubs = [
            f"HUB-{COUNTRIES[i % len(COUNTRIES)]}-{i:03d}" for i in range(cfg.hubs)
        ]
        self.active: Dict[str, Shipment] = {}
        self.drifts: Dict[str, str] = {}  # source -> DRIFT_KINDS key, once that partner has drifted
        self._seq = 0
        self._recent: List[Dict[str, Any]] = []  # pool for duplicate redelivery

        self.counters = {
            "generated": 0,
            "duplicates": 0,
            "late": 0,
            "drifted": 0,
        }
        self.drifted_by_source: Dict[str, int] = {}

    @property
    def drift_active(self) -> bool:
        return bool(self.drifts)

    # ------------------------------------------------------------------
    # shipment lifecycle
    # ------------------------------------------------------------------
    def _new_shipment(self) -> Shipment:
        now = datetime.now(timezone.utc)
        self._seq += 1
        start = now + timedelta(minutes=self.rng.randint(30, 600))
        return Shipment(
            shipment_id=f"SHP-{now:%Y%m%d}-{self._seq:08d}",
            vehicle_id=self.rng.choice(self.vehicles),
            hub_code=self.rng.choice(self.hubs),
            delivery_start=start,
            delivery_end=start + timedelta(hours=self.rng.choice([2, 4, 8])),
            lat=self.rng.uniform(-56.0, 68.0),
            lon=self.rng.uniform(-125.0, 175.0),
            temp_c=self.rng.uniform(-22.0, 8.0),
        )

    def _pick_shipment(self) -> Shipment:
        # Keep a working set roughly proportional to the fleet size.
        if len(self.active) < max(200, self.cfg.vehicles // 4) and self.rng.random() < 0.35:
            s = self._new_shipment()
            self.active[s.shipment_id] = s
            return s
        if not self.active:
            s = self._new_shipment()
            self.active[s.shipment_id] = s
            return s
        return self.rng.choice(list(self.active.values()))

    def _pick_source(self) -> str:
        r = self.rng.random()
        cum = 0.0
        for name, share in SOURCES:
            cum += share
            if r <= cum:
                return name
        return SOURCES[0][0]

    def _metadata(self, source: str) -> Optional[Dict[str, Any]]:
        """Source-specific metadata. Shape varies by source on purpose."""
        if self.rng.random() < 0.25:
            return None
        if source == "telematics_gateway":
            return {
                "device_fw": f"4.{self.rng.randint(0, 9)}.{self.rng.randint(0, 20)}",
                "signal_dbm": self.rng.randint(-115, -60),
                "odometer_km": round(self.rng.uniform(1_000, 480_000), 1),
            }
        if source == "hub_scanner_net":
            return {
                "conveyor": f"CV-{self.rng.randint(1, 48):02d}",
                "scan_quality": self.rng.choice(["ok", "ok", "ok", "reread"]),
                "operator_id": f"OP-{self.rng.randint(1, 900):04d}",
            }
        if source == "partner_courier_api":
            return {
                "partner_ref": uuid.uuid4().hex[:12],
                "sla_tier": self.rng.choice(["standard", "express", "cold_chain"]),
            }
        return {"app_version": f"2.{self.rng.randint(0, 6)}", "battery_pct": self.rng.randint(5, 100)}

    # ------------------------------------------------------------------
    # payload construction
    # ------------------------------------------------------------------
    def _payload(self, shipment: Shipment, source: str) -> Dict[str, Any]:
        now = datetime.now(timezone.utc)

        # ~1% of events arrive late, up to the 24h watermark.
        event_ts = now
        if self.rng.random() < self.cfg.late_arrival_rate:
            event_ts = now - timedelta(
                minutes=self.rng.randint(5, self.cfg.late_arrival_hours * 60)
            )
            self.counters["late"] += 1

        event_type = self.rng.choices(
            ["gps_ping", "temperature", "status", "hub_scan"],
            weights=[0.55, 0.22, 0.13, 0.10],
        )[0]

        shipment.lat = max(-89.9, min(89.9, shipment.lat + self.rng.uniform(-0.05, 0.05)))
        shipment.lon = max(-179.9, min(179.9, shipment.lon + self.rng.uniform(-0.05, 0.05)))
        shipment.temp_c = round(
            max(-40.0, min(40.0, shipment.temp_c + self.rng.uniform(-0.4, 0.4))), 2
        )

        status = None
        if event_type == "status":
            if self.rng.random() < 0.06 and shipment.status_idx < 2:
                status = "exception"
            else:
                shipment.status_idx = min(shipment.status_idx + 1, len(STATUS_FLOW) - 1)
                status = STATUS_FLOW[shipment.status_idx]
            if status == "delivered":
                shipment.done = True

        drift = self.drifts.get(source)
        geo = {"lat": round(shipment.lat, 5), "lon": round(shipment.lon, 5)}

        payload: Dict[str, Any] = {
            "event_id": uuid.uuid4().hex,
            "event_type": event_type,
            "event_ts": event_ts.isoformat(),
            "shipment_id": shipment.shipment_id,
            "vehicle_id": shipment.vehicle_id if event_type != "hub_scan" else None,
            # Every event carries the shipment's origin hub, not just conveyor
            # scans -- Finance reconciles by hub, so an event that cannot be
            # attributed to one is an event Finance cannot reconcile.
            "hub_code": shipment.hub_code,
            "source": source,
            "status": status,
            "temperature_c": shipment.temp_c if event_type == "temperature" else None,
            "metadata": self._metadata(source),
        }

        # The position block -- and, for the driver app after its drift, the
        # field that moved: a new app release started emitting position.latitude
        # instead of geo.lat. No changelog entry, no version bump.
        if drift == "geo":
            payload["position"] = {"latitude": geo["lat"], "longitude": geo["lon"]}
        else:
            payload["geo"] = geo

        # The delivery window block -- and the field that silently moved.
        window = {
            "start": shipment.delivery_start.isoformat(),
            "end": shipment.delivery_end.isoformat(),
        }
        if drift == "delivery_window":
            # v2 shape emitted with no version bump, no announcement, no error.
            payload["delivery"] = {
                "window_start": window["start"],
                "window_end": window["end"],
            }
        else:
            payload["delivery_window"] = window

        if drift:
            self.counters["drifted"] += 1
            self.drifted_by_source[source] = self.drifted_by_source.get(source, 0) + 1

        shipment.events_emitted += 1
        if shipment.done and shipment.events_emitted > 6:
            self.active.pop(shipment.shipment_id, None)

        return payload

    # ------------------------------------------------------------------
    # public API
    # ------------------------------------------------------------------
    def trigger_drift(self, source: Optional[str] = None, kind: str = "delivery_window") -> None:
        """Flip one partner onto a changed schema.

        There is deliberately no way to turn this off. The partner is not going
        to roll back; remediation happens on our side, in the registry.
        """
        if kind not in DRIFT_KINDS:
            raise ValueError(f"unknown drift kind {kind!r}; expected one of {sorted(DRIFT_KINDS)}")
        self.drifts[source or self.cfg.drift_source] = kind

    def emit(self, n: int) -> Iterator[Dict[str, Any]]:
        """Yield n payloads, including redelivered duplicates."""
        for _ in range(n):
            if self._recent and self.rng.random() < self.cfg.redelivery_rate:
                # Kinesis is at-least-once: the same record shows up again after
                # a shard resharding or a consumer restart. Byte-identical.
                self.counters["duplicates"] += 1
                yield dict(self.rng.choice(self._recent))
                continue

            shipment = self._pick_shipment()
            payload = self._payload(shipment, self._pick_source())
            self.counters["generated"] += 1

            self._recent.append(payload)
            if len(self._recent) > 500:
                self._recent.pop(0)
            yield payload


def payload_hash(payload: Dict[str, Any]) -> str:
    """Content hash used to distinguish a genuine correction from a redelivery."""
    material = {k: v for k, v in sorted(payload.items()) if k != "event_id"}
    return hashlib.sha1(repr(material).encode()).hexdigest()
