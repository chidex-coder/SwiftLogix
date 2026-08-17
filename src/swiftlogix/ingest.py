"""Ingest layer: Kinesis Data Firehose + its transform Lambda.

Firehose buffers records, hands each buffered batch to a transform function,
and delivers the result to object storage in Parquet. That batching matters
commercially as well as architecturally: validating 3.6B records/month one at
a time would cost ~$720/month in Lambda invocations alone, while validating
~1.2M buffered batches costs about $40.

Two destinations, and the split is the whole point:

  bronze/     conforming records, typed columns + a VARIANT-style metadata blob
  quarantine/ everything else, with the rejection reason, the expected schema
              version, the observed field set and the complete raw payload

Bronze never rejects a record for being unfamiliar and quarantine never drops
one. Losing the raw payload is what turns a 20-minute contract fix into an
unrecoverable data loss, because the replay has nothing to replay from.
"""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pyarrow as pa
import pyarrow.parquet as pq

from .config import Config
from .generator import payload_hash
from .registry import SchemaRegistry
from .stream import Record

# Typed columns for everything the contract guarantees; metadata_json is the
# schema-on-read escape hatch that absorbs source-specific drift.
BRONZE_SCHEMA = pa.schema(
    [
        ("event_id", pa.string()),
        ("event_type", pa.string()),
        ("event_ts", pa.timestamp("us", tz="UTC")),
        ("ingest_ts", pa.timestamp("us", tz="UTC")),
        ("event_date", pa.date32()),
        ("shipment_id", pa.string()),
        ("vehicle_id", pa.string()),
        ("hub_code", pa.string()),
        ("source", pa.string()),
        ("status", pa.string()),
        ("geo_lat", pa.float64()),
        ("geo_lon", pa.float64()),
        ("temperature_c", pa.float64()),
        ("delivery_window_start", pa.timestamp("us", tz="UTC")),
        ("delivery_window_end", pa.timestamp("us", tz="UTC")),
        ("metadata_json", pa.string()),
        ("schema_version", pa.string()),
        ("payload_hash", pa.string()),
        ("shard_id", pa.string()),
        ("kinesis_seq", pa.int64()),
    ]
)

QUARANTINE_SCHEMA = pa.schema(
    [
        ("event_id", pa.string()),
        ("source", pa.string()),
        ("ingest_ts", pa.timestamp("us", tz="UTC")),
        ("rejection_reason", pa.string()),
        ("expected_schema_version", pa.string()),
        ("observed_fields", pa.string()),
        ("raw_payload", pa.string()),
    ]
)


def _ts(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


@dataclass
class BatchResult:
    accepted: int = 0
    quarantined: int = 0
    bronze_path: Optional[Path] = None
    quarantine_path: Optional[Path] = None
    bytes_written: int = 0
    violations_by_field: Dict[str, int] = field(default_factory=dict)
    null_counts: Dict[str, int] = field(default_factory=dict)
    sources_seen: Dict[str, int] = field(default_factory=dict)

    @property
    def total(self) -> int:
        return self.accepted + self.quarantined

    @property
    def quarantine_pct(self) -> float:
        return 100.0 * self.quarantined / self.total if self.total else 0.0


class FirehoseDelivery:
    """Buffers, validates, converts to Parquet, and lands the two streams."""

    # Columns the Ops dashboard and the Finance report actually depend on.
    # A spike in the null rate of any of these is the business-level canary
    # that catches drift which is type-valid but semantically wrong.
    WATCHED_COLUMNS = ("delivery_window_start", "delivery_window_end", "shipment_id")

    def __init__(self, cfg: Config, registry: SchemaRegistry):
        self.cfg = cfg
        self.registry = registry
        self._lock = threading.Lock()
        self._part = 0
        self.total_accepted = 0
        self.total_quarantined = 0
        self.total_bytes = 0

    def _partition_path(self, root: Path, ts: datetime) -> Path:
        # Hive-style partitioning: dt=/hr= gives the merge a pruning predicate
        # and gives the lifecycle policy something to tier on.
        p = root / f"dt={ts:%Y-%m-%d}" / f"hr={ts:%H}"
        p.mkdir(parents=True, exist_ok=True)
        return p

    def deliver(self, records: List[Record]) -> BatchResult:
        """Process one buffered batch. This is the transform Lambda."""
        result = BatchResult()
        if not records:
            return result

        now = datetime.now(timezone.utc)
        good: List[Dict[str, Any]] = []
        bad: List[Dict[str, Any]] = []

        for rec in records:
            payload = rec.data
            source = payload.get("source", "unknown")
            result.sources_seen[source] = result.sources_seen.get(source, 0) + 1

            verdict = self.registry.validate(payload)
            if not verdict.ok:
                for err in verdict.errors:
                    key = err.split(":")[0]
                    result.violations_by_field[key] = result.violations_by_field.get(key, 0) + 1
                bad.append(
                    {
                        "event_id": payload.get("event_id"),
                        "source": source,
                        "ingest_ts": now,
                        "rejection_reason": verdict.reason,
                        "expected_schema_version": ",".join(self.registry.active_versions),
                        "observed_fields": ",".join(verdict.observed_fields),
                        "raw_payload": json.dumps(payload, separators=(",", ":")),
                    }
                )
                continue

            c = verdict.canonical or {}
            geo = c.get("geo") or {}
            window = c.get("delivery_window") or {}
            event_ts = _ts(c.get("event_ts")) or now
            good.append(
                {
                    "event_id": c.get("event_id"),
                    "event_type": c.get("event_type"),
                    "event_ts": event_ts,
                    "ingest_ts": now,
                    "event_date": event_ts.date(),
                    "shipment_id": c.get("shipment_id"),
                    "vehicle_id": c.get("vehicle_id"),
                    "hub_code": c.get("hub_code"),
                    "source": source,
                    "status": c.get("status"),
                    "geo_lat": geo.get("lat"),
                    "geo_lon": geo.get("lon"),
                    "temperature_c": c.get("temperature_c"),
                    "delivery_window_start": _ts(window.get("start")),
                    "delivery_window_end": _ts(window.get("end")),
                    "metadata_json": json.dumps(c.get("metadata"), separators=(",", ":"))
                    if c.get("metadata")
                    else None,
                    "schema_version": verdict.schema_version,
                    "payload_hash": payload_hash(c),
                    "shard_id": rec.shard_id,
                    "kinesis_seq": rec.sequence_number,
                }
            )

        # Null-rate telemetry on the watched columns, computed per batch so the
        # monitor can alarm inside one buffer window rather than one nightly run.
        for col in self.WATCHED_COLUMNS:
            result.null_counts[col] = sum(1 for r in good if r.get(col) is None)

        with self._lock:
            self._part += 1
            part = self._part

        if good:
            table = pa.Table.from_pylist(good, schema=BRONZE_SCHEMA)
            path = self._partition_path(self.cfg.bronze_dir, now) / f"part-{part:06d}.parquet"
            # ZSTD over Snappy: ~30% smaller at comparable read speed, and the
            # bytes saved are scan bytes, which is what actually drives spend.
            pq.write_table(table, path, compression="zstd", compression_level=3)
            result.bronze_path = path
            result.bytes_written = path.stat().st_size
            result.accepted = len(good)

        if bad:
            table = pa.Table.from_pylist(bad, schema=QUARANTINE_SCHEMA)
            path = self._partition_path(self.cfg.quarantine_dir, now) / f"part-{part:06d}.parquet"
            pq.write_table(table, path, compression="zstd", compression_level=3)
            result.quarantine_path = path
            result.quarantined = len(bad)

        self.total_accepted += result.accepted
        self.total_quarantined += result.quarantined
        self.total_bytes += result.bytes_written
        return result
