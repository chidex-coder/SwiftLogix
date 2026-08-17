"""Replay and remediation.

Recovery from the drift incident is a bounded backfill, not a table rebuild:

  1. register schema v2 with an explicit field mapping (never COALESCE sprawl
     scattered through the transform layer -- that hides the next drift)
  2. re-validate every quarantined payload against the now-active contract set
  3. re-land the ones that now conform into bronze
  4. re-run the same incremental MERGE

Step 4 is safe to run over an arbitrary window precisely because the merge is
keyed on event_id. Replaying 11 days produces zero duplicate rows, which is
what turns "rebuild the warehouse" into a 20-minute backfill.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import pyarrow.parquet as pq

from .config import Config
from .ingest import FirehoseDelivery
from .registry import SchemaRegistry
from .stream import Record
from .warehouse import Warehouse


@dataclass
class ReplayResult:
    quarantined_scanned: int = 0
    recovered: int = 0
    still_rejected: int = 0
    rows_merged: int = 0
    duplicates_created: int = 0
    elapsed_s: float = 0.0


def remediate_schema(
    registry: SchemaRegistry,
    warehouse: Warehouse,
    version: str = "v2",
    actor: str = "platform-oncall",
) -> Dict[str, object]:
    """Register the new contract version after a compatibility review."""
    registry.load(version)
    compatible, problems = registry.check_compatibility("v1", version)

    registry.activate(
        version,
        actor=actor,
        note="partner renamed delivery_window.start -> delivery.window_start",
    )
    warehouse.log_schema_change(
        version,
        "ACTIVATE",
        actor,
        f"field mapping registered; compatibility={'FULL' if compatible else 'BREAKING'}; "
        f"issues={'; '.join(problems) if problems else 'none'}",
    )
    return {
        "version": version,
        "compatible": compatible,
        "problems": problems,
        "mappings": registry.mappings(version),
    }


def replay_quarantine(
    cfg: Config,
    registry: SchemaRegistry,
    delivery: FirehoseDelivery,
    warehouse: Warehouse,
) -> ReplayResult:
    """Re-drive quarantined payloads through the (now updated) contract."""
    import time

    started = time.perf_counter()
    result = ReplayResult()

    dupes_before = warehouse.duplicate_check()
    files = sorted(cfg.quarantine_dir.rglob("*.parquet"))
    if not files:
        return result

    recovered_records: List[Record] = []
    for path in files:
        table = pq.read_table(path)
        for row in table.to_pylist():
            result.quarantined_scanned += 1
            payload = json.loads(row["raw_payload"])
            verdict = registry.validate(payload)
            if verdict.ok:
                recovered_records.append(
                    Record(
                        shard_id="replay",
                        sequence_number=-1,
                        partition_key=payload.get("shipment_id", ""),
                        approximate_arrival_ts=datetime.now(timezone.utc),
                        data=payload,
                    )
                )
            else:
                result.still_rejected += 1

    if recovered_records:
        # Re-land through the normal ingest path so bronze stays the single
        # source of truth for any future replay.
        batch = delivery.deliver(recovered_records)
        result.recovered = batch.accepted
        if batch.bronze_path:
            stats = warehouse.merge_batch([batch.bronze_path], batch.quarantine_pct)
            result.rows_merged = stats.rows_merged

    # Quarantine files are archived, not deleted -- they are the audit trail
    # for the restatement Finance has to sign off on.
    archive = cfg.lake_dir / "quarantine_archive"
    archive.mkdir(parents=True, exist_ok=True)
    for path in files:
        target = archive / f"{path.parent.parent.name}_{path.parent.name}_{path.name}"
        path.rename(target)

    result.duplicates_created = warehouse.duplicate_check() - dupes_before
    result.elapsed_s = time.perf_counter() - started
    return result
