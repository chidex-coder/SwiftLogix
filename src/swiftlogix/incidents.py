"""The scripted incidents a run drives, and when.

Only the *causes* are scripted here (which partner changes shape, and roughly
when). Detection, the circuit breaker, contract registration and replay are
not scripted: they react to the data exactly as they would in production.

Timing is deliberately not fixed. The first drift lands at a random second
inside a window, and the second partner drifts a random few seconds after the
first incident has been fully replayed -- late enough that the on-call believes
the problem is over. Both draws come from a seeded RNG, so a run is
reproducible with the same --seed and either time can be pinned from the CLI.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from typing import List, Optional

from .config import Config


@dataclass
class Incident:
    name: str
    source: str
    kind: str  # key of generator.DRIFT_KINDS
    contract_version: str  # the version the on-call registers to remediate it
    announcement: str  # what the timeline says when it happens
    remediation_note: str  # what the registry change log records
    at: Optional[float] = None  # seconds into the run; None = scheduled later
    remediate_after: float = 18.0
    triggered_at: Optional[float] = None
    remediated_at: Optional[float] = None
    replayed_at: Optional[float] = None
    replay_summary: str = ""
    delay_after_previous: float = 0.0  # seconds after the previous incident's replay, when `at` is None

    @property
    def open(self) -> bool:
        return self.triggered_at is not None and self.replayed_at is None

    @property
    def done(self) -> bool:
        return self.replayed_at is not None


def plan_incidents(cfg: Config, rng: Optional[random.Random] = None) -> List[Incident]:
    """Build the run's incident list with its (seeded) random timings."""
    rng = rng or random.Random(cfg.seed + 7919)  # separate stream from the generator's
    first_at = (
        cfg.drift_at_second
        if cfg.drift_at_second is not None
        else rng.uniform(*cfg.drift_window_seconds)
    )
    second_delay = rng.uniform(*cfg.second_drift_delay_seconds)
    return [
        Incident(
            name="courier field rename",
            source=cfg.drift_source,
            kind="delivery_window",
            contract_version="v2",
            announcement=(
                f"UPSTREAM: {cfg.drift_source} renamed delivery_window.start "
                "-> delivery.window_start (no notice, no version bump)"
            ),
            remediation_note="partner renamed delivery_window.start -> delivery.window_start",
            at=round(first_at, 1),
            remediate_after=cfg.auto_remediate_after_seconds,
        ),
        Incident(
            name="driver app geo move",
            source=cfg.second_drift_source,
            kind="geo",
            contract_version="v3",
            announcement=(
                f"UPSTREAM: {cfg.second_drift_source} release 4.12 moved geo.lat/lon "
                "-> position.latitude/longitude (no changelog entry) - a different "
                "partner, a different field, and contract v2 does not cover it"
            ),
            remediation_note="driver app moved geo.lat/lon -> position.latitude/longitude",
            at=cfg.second_drift_at_second,  # None: scheduled once the first replay lands
            remediate_after=cfg.auto_remediate_after_seconds,
            delay_after_previous=round(second_delay, 1),
        ),
    ]
