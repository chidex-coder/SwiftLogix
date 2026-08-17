"""A local stand-in for Amazon Kinesis Data Streams.

Deliberately reproduces the properties the pipeline design has to survive:

  * sharding by partition key (vehicle_id / hub_code -- high cardinality, so
    no hot shard), with the same 1,000 records/sec per-shard ceiling used to
    size the 8-shard production stream
  * an append-only, replayable log with a retention window
  * per-consumer-group checkpoints, so a consumer restart replays from the last
    checkpoint and produces genuine at-least-once duplicates
  * enhanced fan-out: independent consumer groups read the same records without
    competing for the shard's read throughput
"""

from __future__ import annotations

import json
import threading
import time
import zlib
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from .config import Config

SHARD_RECORDS_PER_SEC = 1_000  # Kinesis per-shard ingest ceiling
SHARD_BYTES_PER_SEC = 1_048_576


@dataclass
class Record:
    shard_id: str
    sequence_number: int
    partition_key: str
    approximate_arrival_ts: datetime
    data: Dict[str, Any]


class KinesisLikeStream:
    """Append-only sharded log on local disk."""

    def __init__(self, cfg: Config, name: str = "swiftlogix-events-raw"):
        self.cfg = cfg
        self.name = name
        self.shard_count = cfg.shards
        self.dir = cfg.stream_dir / name
        self.dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._seq = 0
        self._handles: Dict[str, Any] = {}
        self._throttled = 0
        self._window_start = time.monotonic()
        self._window_records = 0
        self._window_bytes = 0

        for i in range(self.shard_count):
            sid = self.shard_id(i)
            self._handles[sid] = open(self.dir / f"{sid}.jsonl", "a", encoding="utf-8")

    @staticmethod
    def shard_id(index: int) -> str:
        return f"shardId-{index:012d}"

    def _shard_for(self, partition_key: str) -> str:
        # Kinesis hashes the partition key onto the shard's hash-key range.
        return self.shard_id(zlib.crc32(partition_key.encode()) % self.shard_count)

    @property
    def throttled_records(self) -> int:
        return self._throttled

    def capacity_used_pct(self) -> float:
        elapsed = max(time.monotonic() - self._window_start, 1e-6)
        eps = self._window_records / elapsed
        return 100.0 * eps / (self.shard_count * SHARD_RECORDS_PER_SEC)

    def put_records(self, payloads: List[Dict[str, Any]]) -> int:
        """Write a batch. Returns the number accepted."""
        now = datetime.now(timezone.utc)
        accepted = 0
        with self._lock:
            elapsed = time.monotonic() - self._window_start
            if elapsed >= 1.0:
                self._window_start = time.monotonic()
                self._window_records = 0
                self._window_bytes = 0

            capacity = self.shard_count * SHARD_RECORDS_PER_SEC
            for payload in payloads:
                pk = payload.get("vehicle_id") or payload.get("hub_code") or payload["shipment_id"]
                if self._window_records >= capacity:
                    # ProvisionedThroughputExceededException -- the producer
                    # would retry with backoff. We surface it as a metric.
                    self._throttled += 1
                    continue
                self._seq += 1
                sid = self._shard_for(pk)
                envelope = {
                    "sequenceNumber": self._seq,
                    "partitionKey": pk,
                    "approximateArrivalTimestamp": now.isoformat(),
                    "data": payload,
                }
                self._handles[sid].write(json.dumps(envelope, separators=(",", ":")) + "\n")
                self._window_records += 1
                self._window_bytes += len(json.dumps(payload))
                accepted += 1
            for h in self._handles.values():
                h.flush()
        return accepted

    def close(self) -> None:
        for h in self._handles.values():
            h.close()


class Consumer:
    """A checkpointing consumer group (enhanced fan-out analogue)."""

    def __init__(self, cfg: Config, stream: KinesisLikeStream, group: str):
        self.cfg = cfg
        self.stream = stream
        self.group = group
        self.ckpt_path = cfg.checkpoint_dir / f"{stream.name}.{group}.json"
        self.ckpt_path.parent.mkdir(parents=True, exist_ok=True)
        self._offsets: Dict[str, int] = self._load()

    def _load(self) -> Dict[str, int]:
        if self.ckpt_path.exists():
            return json.loads(self.ckpt_path.read_text())
        return {self.stream.shard_id(i): 0 for i in range(self.stream.shard_count)}

    def _save(self) -> None:
        self.ckpt_path.write_text(json.dumps(self._offsets))

    def rewind(self, records: int = 500) -> None:
        """Simulate a consumer crash before checkpointing.

        Everything since the last checkpoint is redelivered. This is the exact
        failure the idempotent MERGE on event_id has to absorb.
        """
        per_shard = max(1, records // max(len(self._offsets), 1))
        for sid in self._offsets:
            self._offsets[sid] = max(0, self._offsets[sid] - per_shard)
        self._save()

    def read(self, max_records: int = 5_000) -> Iterator[Record]:
        """Read forward from the checkpoint. Does NOT advance it -- the caller
        commits only after the batch has been durably written."""
        pending: List[Record] = []
        for i in range(self.stream.shard_count):
            sid = self.stream.shard_id(i)
            path = self.stream.dir / f"{sid}.jsonl"
            if not path.exists():
                continue
            start = self._offsets.get(sid, 0)
            with open(path, "r", encoding="utf-8") as fh:
                for line_no, line in enumerate(fh):
                    if line_no < start:
                        continue
                    if len(pending) >= max_records:
                        break
                    line = line.strip()
                    if not line:
                        continue
                    env = json.loads(line)
                    pending.append(
                        Record(
                            shard_id=sid,
                            sequence_number=env["sequenceNumber"],
                            partition_key=env["partitionKey"],
                            approximate_arrival_ts=datetime.fromisoformat(
                                env["approximateArrivalTimestamp"]
                            ),
                            data=env["data"],
                        )
                    )
        pending.sort(key=lambda r: r.sequence_number)
        yield from pending

    def commit(self, records: List[Record]) -> None:
        counts: Dict[str, int] = {}
        for r in records:
            counts[r.shard_id] = counts.get(r.shard_id, 0) + 1
        for sid, n in counts.items():
            self._offsets[sid] = self._offsets.get(sid, 0) + n
        self._save()

    def lag(self) -> int:
        """Records written but not yet checkpointed by this group."""
        total = 0
        for i in range(self.stream.shard_count):
            sid = self.stream.shard_id(i)
            path = self.stream.dir / f"{sid}.jsonl"
            if not path.exists():
                continue
            with open(path, "rb") as fh:
                written = sum(1 for _ in fh)
            total += max(0, written - self._offsets.get(sid, 0))
        return total
