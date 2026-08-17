"""Central configuration.

Production constants come straight from the SwiftLogix business case. The
simulator scales volume down by SCALE so a full pipeline run fits on a laptop,
but every ratio (peak/average, duplicate rate, late-arrival rate, drift blast
radius) is preserved so the observed metrics extrapolate honestly.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

# --------------------------------------------------------------------------
# Production scale (from the business case)
# --------------------------------------------------------------------------
PROD_EVENTS_PER_DAY = 120_000_000
PROD_PEAK_EPS = 4_500
PROD_AVG_EPS = PROD_EVENTS_PER_DAY / 86_400  # ~1,389/sec
PROD_VEHICLES = 85_000
PROD_HUBS = 240
PROD_COUNTRIES = 32
PROD_AVG_EVENT_BYTES = 1_024

# Latency and cost mandate
SLA_SECONDS = 300  # shipment status queryable within 5 minutes
QUERY_SLA_SECONDS = 10  # dashboard queries over 90 days
BUDGET_MONTHLY_USD = 42_000
CURRENT_MONTHLY_USD = 61_000

ROOT = Path(__file__).resolve().parents[2]


@dataclass
class Config:
    """Runtime knobs for a simulated pipeline run."""

    # ---- scale ----------------------------------------------------------
    scale: float = 1 / 2000  # 4,500 peak eps -> ~2.25 eps... see target_eps
    target_eps: int = 1_200  # synthetic events/sec the generator aims for
    vehicles: int = 4_000
    hubs: int = 240
    countries: int = 32

    # ---- stream (Kinesis Data Streams analogue) --------------------------
    shards: int = 8  # 4,500 peak eps / 1,000 rec/s per shard + headroom
    stream_retention_hours: int = 168  # 7 days, extended for replay
    redelivery_rate: float = 0.004  # at-least-once duplicate rate

    # ---- ingest (Firehose analogue) -------------------------------------
    buffer_seconds: float = 2.0  # stands in for the 60s production buffer
    buffer_max_records: int = 4_000
    quarantine_circuit_breaker_pct: float = 5.0  # abort promotion above this

    # ---- warehouse ------------------------------------------------------
    merge_interval_seconds: float = 4.0  # stands in for 1-minute TARGET_LAG
    late_arrival_hours: int = 24
    late_arrival_rate: float = 0.01

    # ---- drift injection -------------------------------------------------
    drift_at_second: float = 20.0
    drift_source: str = "partner_courier_api"
    drift_share: float = 0.18  # share of traffic from the drifting partner
    auto_remediate_after_seconds: float = 18.0  # 0 disables auto-remediation

    # ---- run ------------------------------------------------------------
    duration_seconds: float = 90.0
    seed: int = 20260817
    data_dir: Path = field(default_factory=lambda: ROOT / "data")

    # ---- monitors --------------------------------------------------------
    null_rate_alert_pct: float = 15.0
    violation_rate_alert_pct: float = 0.5
    freshness_alert_seconds: float = 30.0

    @property
    def lake_dir(self) -> Path:
        return self.data_dir / "lake"

    @property
    def bronze_dir(self) -> Path:
        return self.lake_dir / "bronze"

    @property
    def quarantine_dir(self) -> Path:
        return self.lake_dir / "quarantine"

    @property
    def stream_dir(self) -> Path:
        return self.data_dir / "stream"

    @property
    def checkpoint_dir(self) -> Path:
        return self.data_dir / "checkpoints"

    @property
    def warehouse_path(self) -> Path:
        return self.data_dir / "warehouse.duckdb"

    @property
    def contracts_dir(self) -> Path:
        return ROOT / "contracts"

    @property
    def sql_dir(self) -> Path:
        return ROOT / "sql"

    def ensure_dirs(self) -> None:
        for p in (
            self.data_dir,
            self.lake_dir,
            self.bronze_dir,
            self.quarantine_dir,
            self.stream_dir,
            self.checkpoint_dir,
        ):
            p.mkdir(parents=True, exist_ok=True)


def from_env() -> Config:
    """Build a Config, allowing SWIFTLOGIX_* env overrides for CI runs."""
    cfg = Config()
    if v := os.getenv("SWIFTLOGIX_DURATION"):
        cfg.duration_seconds = float(v)
    if v := os.getenv("SWIFTLOGIX_EPS"):
        cfg.target_eps = int(v)
    if v := os.getenv("SWIFTLOGIX_DATA_DIR"):
        cfg.data_dir = Path(v)
    return cfg
