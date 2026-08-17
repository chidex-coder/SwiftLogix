"""Detection layer.

The 2025 incident ran for 11 days because nothing was watching the right
signal. A nightly job that "succeeds" while writing nulls is indistinguishable
from a healthy job unless you monitor the data, not the task.

Four independent detectors, deliberately overlapping:

  contract   schema violations per source, per batch      -> catches the rename itself
  null_rate  null share of business-critical columns      -> catches type-valid nonsense
  freshness  age of the newest row in the serving layer   -> catches a stalled pipeline
  canary     business metric vs seasonal baseline         -> catches everything else

Any one of them fires inside a single buffer window.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from statistics import mean, pstdev
from typing import Any, Deque, Dict, List, Optional

from .config import Config
from .ingest import BatchResult

CRITICAL = "CRITICAL"
WARNING = "WARNING"
INFO = "INFO"


@dataclass
class Alert:
    raised_at: datetime
    severity: str
    monitor: str
    message: str
    detail: Dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:
        return f"[{self.severity}] {self.monitor}: {self.message}"


class MonitorSuite:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.alerts: List[Alert] = []
        self._fired: Dict[str, datetime] = {}
        self._canary_history: Deque[float] = deque(maxlen=12)
        self._last_publish: Optional[datetime] = None

    # ------------------------------------------------------------------
    def _raise(self, severity: str, monitor: str, message: str, detail: Dict[str, Any],
               dedupe_key: Optional[str] = None, cooldown_s: float = 15.0) -> Optional[Alert]:
        """Alert with a cooldown, so an incident pages once rather than 400 times."""
        key = dedupe_key or f"{monitor}:{message}"
        now = datetime.now(timezone.utc)
        last = self._fired.get(key)
        if last and (now - last).total_seconds() < cooldown_s:
            return None
        self._fired[key] = now
        alert = Alert(now, severity, monitor, message, detail)
        self.alerts.append(alert)
        return alert

    # ------------------------------------------------------------------
    def check_contract(self, batch: BatchResult) -> List[Alert]:
        """Schema violation rate, attributed to the offending source."""
        out: List[Alert] = []
        if not batch.total:
            return out

        violation_pct = 100.0 * batch.quarantined / batch.total
        if violation_pct > self.cfg.violation_rate_alert_pct:
            severity = CRITICAL if violation_pct > self.cfg.quarantine_circuit_breaker_pct else WARNING
            top_field = max(batch.violations_by_field, key=batch.violations_by_field.get, default="?")
            alert = self._raise(
                severity,
                "contract_violation",
                f"schema violation rate {violation_pct:.1f}% ({batch.quarantined}/{batch.total})",
                {
                    "violation_pct": round(violation_pct, 2),
                    "top_violation": top_field,
                    "by_field": dict(sorted(batch.violations_by_field.items(),
                                            key=lambda kv: -kv[1])[:5]),
                    "sources": batch.sources_seen,
                },
                dedupe_key="contract_violation",
            )
            if alert:
                out.append(alert)
        return out

    def check_null_rates(self, batch: BatchResult) -> List[Alert]:
        """A required column going null en masse is the rename's fingerprint."""
        out: List[Alert] = []
        if not batch.accepted:
            return out
        for column, nulls in batch.null_counts.items():
            pct = 100.0 * nulls / batch.accepted
            if pct > self.cfg.null_rate_alert_pct:
                alert = self._raise(
                    CRITICAL,
                    "null_rate",
                    f"{column} null rate {pct:.1f}% in the last buffer window",
                    {"column": column, "null_pct": round(pct, 2), "sample_size": batch.accepted},
                    dedupe_key=f"null_rate:{column}",
                )
                if alert:
                    out.append(alert)
        return out

    def check_freshness(self, seconds_since_publish: float) -> List[Alert]:
        """Guards the 5-minute SLA directly."""
        out: List[Alert] = []
        if seconds_since_publish > self.cfg.freshness_alert_seconds:
            alert = self._raise(
                WARNING,
                "freshness",
                f"serving layer is {seconds_since_publish:.0f}s stale "
                f"(SLA budget {self.cfg.freshness_alert_seconds:.0f}s)",
                {"staleness_s": round(seconds_since_publish, 1)},
                dedupe_key="freshness",
            )
            if alert:
                out.append(alert)
        return out

    def check_canary(self, non_null_window_share: float) -> List[Alert]:
        """Business-level canary against a rolling seasonal baseline.

        Catches drift that passes every type check -- the class of failure that
        schema validation alone cannot see.
        """
        out: List[Alert] = []
        history = list(self._canary_history)
        self._canary_history.append(non_null_window_share)
        if len(history) < 5:
            return out

        baseline, sigma = mean(history), pstdev(history) or 1e-6
        deviation = (baseline - non_null_window_share) / sigma
        if deviation > 3.0:
            alert = self._raise(
                CRITICAL,
                "business_canary",
                f"shipments with a delivery window fell {deviation:.1f}σ below the "
                f"rolling baseline ({non_null_window_share:.1f}% vs {baseline:.1f}%)",
                {"observed": round(non_null_window_share, 2),
                 "baseline": round(baseline, 2),
                 "sigma": round(sigma, 3)},
                dedupe_key="business_canary",
            )
            if alert:
                out.append(alert)
        return out

    def check_circuit_breaker(self, batch: BatchResult) -> List[Alert]:
        out: List[Alert] = []
        if batch.quarantine_pct > self.cfg.quarantine_circuit_breaker_pct:
            alert = self._raise(
                CRITICAL,
                "circuit_breaker",
                f"promotion to gold ABORTED - quarantine rate {batch.quarantine_pct:.1f}% "
                f"exceeds the {self.cfg.quarantine_circuit_breaker_pct:.0f}% threshold; "
                f"last good partition still serving",
                {"quarantine_pct": round(batch.quarantine_pct, 2)},
                dedupe_key="circuit_breaker",
            )
            if alert:
                out.append(alert)
        return out

    # ------------------------------------------------------------------
    def evaluate(self, batch: BatchResult, seconds_since_publish: float) -> List[Alert]:
        fired: List[Alert] = []
        fired += self.check_contract(batch)
        fired += self.check_null_rates(batch)
        fired += self.check_circuit_breaker(batch)
        fired += self.check_freshness(seconds_since_publish)
        if batch.accepted:
            nulls = batch.null_counts.get("delivery_window_start", 0)
            fired += self.check_canary(100.0 * (batch.accepted - nulls) / batch.accepted)
        return fired

    @property
    def critical_count(self) -> int:
        return sum(1 for a in self.alerts if a.severity == CRITICAL)

    def recent(self, n: int = 6) -> List[Alert]:
        return self.alerts[-n:]
