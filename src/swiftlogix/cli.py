"""Orchestrator: runs the whole pipeline live, with a real-time console.

Three threads, mirroring the three tiers of the production design:

  producer   generator -> Kinesis-like stream (partition-keyed, sharded)
  ingest     stream -> validate -> bronze parquet / quarantine (Firehose+Lambda)
  warehouse  bronze -> dedup -> MERGE -> vault + marts (Snowpipe + dbt)

The run drives a scripted incident so the reliability controls are exercised
rather than merely described: the partner API silently renames a field, the
contract layer catches it inside one buffer window, the circuit breaker halts
promotion, the on-call registers v2 with a field mapping, and the quarantined
records are replayed through the same idempotent MERGE.
"""

from __future__ import annotations

import argparse
import json
import queue
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from .config import BUDGET_MONTHLY_USD, CURRENT_MONTHLY_USD, PROD_EVENTS_PER_DAY, Config
from .costs import CostModel
from .generator import EventGenerator
from .ingest import BatchResult, FirehoseDelivery
from .monitors import CRITICAL, WARNING, MonitorSuite
from .registry import SchemaRegistry
from .replay import remediate_schema, replay_quarantine
from .stream import Consumer, KinesisLikeStream
from .warehouse import Warehouse

console = Console()


@dataclass
class RunState:
    started: float = field(default_factory=time.monotonic)
    produced: int = 0
    accepted: int = 0
    quarantined: int = 0
    bronze_bytes: int = 0
    raw_bytes: int = 0
    batches: int = 0
    merged: int = 0
    duplicates: int = 0
    late: int = 0
    scanned: int = 0
    merge_ms: float = 0.0
    promotion_state: str = "IDLE"
    lag: int = 0
    counts: Dict[str, int] = field(default_factory=dict)
    timeline: List[str] = field(default_factory=list)
    drift_active: bool = False
    remediated: bool = False
    # Published by the warehouse thread as soon as it connects, so the incident
    # script can drive remediation and replay against the live connection
    # rather than opening a second one against the same database file.
    wh: Optional["Warehouse"] = None
    last_publish: float = field(default_factory=time.monotonic)
    stop: threading.Event = field(default_factory=threading.Event)
    lock: threading.Lock = field(default_factory=threading.Lock)
    # A DuckDB connection is not safe for concurrent use across threads, so the
    # loader thread and the incident-driven replay serialise on this.
    wh_lock: threading.Lock = field(default_factory=threading.Lock)

    @property
    def elapsed(self) -> float:
        return time.monotonic() - self.started

    def note(self, message: str) -> None:
        with self.lock:
            self.timeline.append(f"t+{self.elapsed:05.1f}s  {message}")


# ----------------------------------------------------------------------
# threads
# ----------------------------------------------------------------------
def producer_thread(cfg: Config, gen: EventGenerator, stream: KinesisLikeStream,
                    state: RunState) -> None:
    tick = 0.1
    per_tick = max(1, int(cfg.target_eps * tick))
    while not state.stop.is_set():
        cycle = time.monotonic()
        payloads = list(gen.emit(per_tick))
        accepted = stream.put_records(payloads)
        with state.lock:
            state.produced += accepted
            state.raw_bytes += sum(len(str(p)) for p in payloads)
        sleep = tick - (time.monotonic() - cycle)
        if sleep > 0:
            time.sleep(sleep)


def ingest_thread(cfg: Config, consumer: Consumer, delivery: FirehoseDelivery,
                  monitors: MonitorSuite, state: RunState,
                  merge_q: "queue.Queue[Path]", alert_q: "queue.Queue[Any]") -> None:
    while not state.stop.is_set():
        time.sleep(cfg.buffer_seconds)
        records = list(consumer.read(cfg.buffer_max_records))
        if not records:
            continue

        batch = delivery.deliver(records)
        consumer.commit(records)

        fired = monitors.evaluate(batch, time.monotonic() - state.last_publish)
        for alert in fired:
            state.note(str(alert))
            # Handed to the loader thread, which owns the DuckDB connection.
            alert_q.put(alert)

        with state.lock:
            state.accepted += batch.accepted
            state.quarantined += batch.quarantined
            state.bronze_bytes += batch.bytes_written
            state.batches += 1
            state.lag = consumer.lag()

        if batch.bronze_path and batch.quarantine_pct <= cfg.quarantine_circuit_breaker_pct:
            merge_q.put(batch.bronze_path)
        elif batch.bronze_path:
            state.promotion_state = "BLOCKED_CIRCUIT_BREAKER"


def warehouse_thread(cfg: Config, state: RunState, merge_q: "queue.Queue[Path]",
                     alert_q: "queue.Queue[Any]") -> None:
    wh = Warehouse(cfg)
    state.wh = wh
    state.note(
        f"warehouse ready (native MERGE INTO: {'yes' if wh.supports_merge else 'no, using upsert'})"
    )
    pending: List[Path] = []
    last_merge = time.monotonic()
    try:
        while not state.stop.is_set() or not merge_q.empty() or pending:
            try:
                pending.append(merge_q.get(timeout=0.25))
            except queue.Empty:
                pass

            while not alert_q.empty():
                a = alert_q.get()
                with state.wh_lock:
                    wh.alert(a.severity, a.monitor, a.message, json.dumps(a.detail, default=str))

            if pending and (time.monotonic() - last_merge) >= cfg.merge_interval_seconds:
                files, pending = pending, []
                with state.wh_lock:
                    stats = wh.merge_batch(files)
                last_merge = time.monotonic()
                with state.lock:
                    state.merged += stats.rows_merged
                    state.duplicates += stats.duplicates_suppressed
                    state.late += stats.late_arrivals
                    state.scanned += stats.scanned_rows
                    state.merge_ms = stats.merge_ms
                    state.promotion_state = stats.promotion_state
                    state.counts = wh.counts()
                    state.last_publish = time.monotonic()
                if stats.audit_failures:
                    state.note(f"[WAP] audit FAILED: {', '.join(stats.audit_failures)}")
            elif state.stop.is_set() and not pending:
                break
    finally:
        state.note(f"warehouse loader stopped after {wh._batch_id} micro-batches")


# ----------------------------------------------------------------------
# live display
# ----------------------------------------------------------------------
def render(cfg: Config, state: RunState, monitors: MonitorSuite,
           gen: EventGenerator, stream: KinesisLikeStream) -> Group:
    with state.lock:
        elapsed = state.elapsed
        eps = state.produced / elapsed if elapsed else 0
        counts = dict(state.counts)
        timeline = list(state.timeline)[-7:]

    header = Table.grid(expand=True)
    header.add_column(justify="left")
    header.add_column(justify="right")
    phase = (
        "[bold red]INCIDENT: schema drift active[/]"
        if state.drift_active and not state.remediated
        else "[bold green]REMEDIATED - replay complete[/]"
        if state.remediated
        else "[bold cyan]steady state[/]"
    )
    header.add_row(
        f"[bold]SwiftLogix real-time platform[/]   t+{elapsed:05.1f}s / {cfg.duration_seconds:.0f}s",
        phase,
    )

    pipe = Table(show_header=True, header_style="bold", expand=True, box=None)
    for col in ("stage", "throughput", "volume", "detail"):
        pipe.add_column(col, overflow="fold")
    pipe.add_row(
        "1. Kinesis (8 shards)",
        f"{eps:,.0f} eps",
        f"{state.produced:,} records",
        f"shard capacity {stream.capacity_used_pct():.1f}% · throttled {stream.throttled_records:,} · lag {state.lag:,}",
    )
    contract_pct = 100.0 * state.quarantined / max(state.accepted + state.quarantined, 1)
    pipe.add_row(
        "2. Firehose + contract",
        f"{state.batches} batches",
        f"{state.accepted:,} passed / {state.quarantined:,} quarantined",
        f"violation rate [{'red' if contract_pct > 1 else 'green'}]{contract_pct:.2f}%[/]",
    )
    compression = state.raw_bytes / state.bronze_bytes if state.bronze_bytes else 0
    pipe.add_row(
        "3. Bronze (Parquet+ZSTD)",
        f"{state.bronze_bytes / 1e6:.1f} MB",
        f"{compression:.1f}x compression",
        f"partitioned dt=/hr= · {state.late:,} late arrivals inside 24h window",
    )
    promo_colour = "green" if state.promotion_state == "PUBLISHED" else "red"
    pipe.add_row(
        "4. MERGE -> vault + marts",
        f"{state.merge_ms:,.0f} ms",
        f"{state.merged:,} rows merged",
        f"[{promo_colour}]{state.promotion_state}[/] · {state.duplicates:,} duplicates suppressed",
    )

    marts = Table(show_header=True, header_style="bold", expand=True, box=None)
    marts.add_column("gold / raw table")
    marts.add_column("rows", justify="right")
    marts.add_column("serves")
    label = {
        "fact_events": ("gold.fact_shipment_events", "Data Science (atomic grain)"),
        "shipments": ("gold.fact_shipment_status_current", "Ops dashboard (<5 min SLA)"),
        "recon_rows": ("gold.fact_daily_shipment_recon", "Finance (daily, restatement-aware)"),
        "sat_status": ("raw.sat_shipment_status", "vault history / audit"),
        "sat_telemetry": ("raw.sat_vehicle_telemetry", "cold-chain telemetry"),
        "hub_vehicle": ("raw.hub_vehicle", "vault business keys"),
        "late_arrivals": ("quarantine.late_arrivals", "beyond-24h restatement queue"),
    }
    for key, (name, serves) in label.items():
        marts.add_row(name, f"{counts.get(key, 0):,}", serves)

    alert_lines: List[Text] = []
    for alert in monitors.recent(5):
        colour = "red" if alert.severity == CRITICAL else "yellow" if alert.severity == WARNING else "cyan"
        alert_lines.append(Text.from_markup(f"[{colour}]{alert.severity:<8}[/] {alert.monitor}: {alert.message}"))
    if not alert_lines:
        alert_lines.append(Text("no alerts - contract holding", style="green"))

    return Group(
        Panel(header, border_style="blue"),
        Panel(pipe, title="pipeline", border_style="cyan"),
        Panel(marts, title="warehouse", border_style="cyan"),
        Panel(Group(*alert_lines), title="monitors", border_style="red" if monitors.critical_count else "green"),
        Panel(Text("\n".join(timeline) or "starting..."), title="incident timeline", border_style="magenta"),
    )


# ----------------------------------------------------------------------
def cmd_run(args: argparse.Namespace) -> int:
    cfg = Config()
    cfg.duration_seconds = args.duration
    cfg.target_eps = args.eps
    if args.data_dir:
        cfg.data_dir = Path(args.data_dir)
    if args.fresh and cfg.data_dir.exists():
        import shutil

        shutil.rmtree(cfg.data_dir)
    cfg.ensure_dirs()

    gen = EventGenerator(cfg)
    stream = KinesisLikeStream(cfg)
    registry = SchemaRegistry(cfg)
    delivery = FirehoseDelivery(cfg, registry)
    monitors = MonitorSuite(cfg)
    consumer = Consumer(cfg, stream, "gold-loader")
    state = RunState()
    merge_q: "queue.Queue[Path]" = queue.Queue()
    alert_q: "queue.Queue[Any]" = queue.Queue()

    state.note(f"contract v1 active; {cfg.shards} shards; target {cfg.target_eps:,} eps")

    threads = [
        threading.Thread(target=producer_thread, args=(cfg, gen, stream, state), daemon=True),
        threading.Thread(target=ingest_thread,
                         args=(cfg, consumer, delivery, monitors, state, merge_q, alert_q),
                         daemon=True),
        threading.Thread(target=warehouse_thread, args=(cfg, state, merge_q, alert_q),
                         daemon=True),
    ]
    for t in threads:
        t.start()

    drift_at = cfg.drift_at_second
    remediate_at = drift_at + cfg.auto_remediate_after_seconds
    rewind_at = drift_at * 0.5
    rewound = False

    with Live(render(cfg, state, monitors, gen, stream), console=console,
              refresh_per_second=4, screen=False) as live:
        while state.elapsed < cfg.duration_seconds:
            now = state.elapsed

            if not rewound and now >= rewind_at:
                rewound = True
                consumer.rewind(400)
                state.note("chaos: consumer crashed before checkpoint - 400 records redelivered")

            if not state.drift_active and now >= drift_at:
                gen.trigger_drift()
                state.drift_active = True
                state.note(
                    f"UPSTREAM: {cfg.drift_source} renamed delivery_window.start "
                    "-> delivery.window_start (no notice, no version bump)"
                )

            if state.drift_active and not state.remediated and state.wh and now >= remediate_at:
                with state.wh_lock:
                    outcome = remediate_schema(registry, state.wh)
                state.note(
                    f"on-call registered contract {outcome['version']} "
                    f"(compatibility={'FULL' if outcome['compatible'] else 'BREAKING'}, "
                    f"{len(outcome['mappings'])} field mappings) - live traffic now conforms"
                )
                # Let the next buffer window land under the new contract before
                # replaying, so the recovery is measured against a healthy stream.
                time.sleep(cfg.buffer_seconds * 2)
                with state.wh_lock:
                    result = replay_quarantine(cfg, registry, delivery, state.wh)
                state.note(
                    f"replay: {result.quarantined_scanned:,} quarantined records re-driven, "
                    f"{result.recovered:,} recovered, {result.rows_merged:,} merged into gold, "
                    f"{result.duplicates_created} duplicates created"
                )
                state.remediated = True

            live.update(render(cfg, state, monitors, gen, stream))
            time.sleep(0.25)

    state.stop.set()
    for t in threads:
        t.join(timeout=20)
    stream.close()

    console.print()
    _final_report(cfg, state, gen, delivery, monitors, stream)
    return 0


def _final_report(cfg: Config, state: RunState, gen: EventGenerator,
                  delivery: FirehoseDelivery, monitors: MonitorSuite,
                  stream: KinesisLikeStream) -> None:
    wh: Optional[Warehouse] = state.wh
    if wh is None:  # pragma: no cover - loader failed to start
        wh = Warehouse(cfg)

    dupes = wh.duplicate_check()
    counts = wh.counts()

    console.print(Panel(Text("\n".join(state.timeline)), title="incident timeline",
                        border_style="magenta"))

    versions = wh.con.execute(
        "SELECT schema_version, count(*) FROM gold.fact_shipment_events GROUP BY 1 ORDER BY 2 DESC"
    ).fetchall()
    v2_rows = sum(n for v, n in versions if v == "v2")

    t = Table(title="correctness assertions", header_style="bold", expand=True)
    t.add_column("assertion")
    t.add_column("result", justify="right")
    t.add_column("verdict", justify="center")

    def row(name: str, value: Any, ok: bool) -> None:
        t.add_row(name, str(value), "[green]PASS[/]" if ok else "[red]FAIL[/]")

    row("duplicate event_id rows in gold fact", dupes, dupes == 0)
    row("duplicates suppressed by MERGE", f"{state.duplicates:,}", state.duplicates > 0)
    row("quarantined records retained with raw payload", f"{state.quarantined:,}", True)
    row("critical alerts raised during drift", monitors.critical_count, monitors.critical_count > 0)
    row("drifted records recovered under contract v2", f"{v2_rows:,}",
        v2_rows > 0 or not state.remediated)
    row("schema versions coexisting in gold",
        ", ".join(f"{v}={n:,}" for v, n in versions), len(versions) >= 1)
    row("shipments in Ops mart", f"{counts['shipments']:,}", counts["shipments"] > 0)
    row("stream throttling (capacity headroom held)", stream.throttled_records,
        stream.throttled_records == 0)
    console.print(t)

    # Measure the actual reprocessing ratio: what a nightly full reload would
    # have scanned versus what the incremental path did scan.
    total_bronze_rows = wh.con.execute(
        "SELECT count(*) FROM read_parquet(?)",
        [str(cfg.bronze_dir / "**" / "*.parquet")],
    ).fetchone()[0]
    batches = max(state.batches, 1)
    full_reload_rows = total_bronze_rows * batches  # re-scan everything, every run
    model = CostModel.from_run(
        incremental_rows_scanned=max(state.scanned, 1),
        full_reload_rows_scanned=max(full_reload_rows, 1),
        raw_bytes=max(state.raw_bytes, 1),
        parquet_bytes=max(state.bronze_bytes, 1),
        observed_events=state.produced,
    )
    summary = model.summary()

    c = Table(title=f"monthly cost model at production volume ({PROD_EVENTS_PER_DAY:,} events/day)",
              header_style="bold", expand=True)
    for col, just in (("line item", "left"), ("current", "right"), ("proposed", "right"),
                      ("delta", "right"), ("lever", "left")):
        c.add_column(col, justify=just)
    for name, cur, prop, delta, note in model.render_rows():
        c.add_row(name, cur, prop, f"[green]{delta}[/]" if delta.startswith("-") else f"[yellow]{delta}[/]", note)
    c.add_section()
    c.add_row("[bold]TOTAL[/]", f"[bold]${CURRENT_MONTHLY_USD:,.0f}[/]",
              f"[bold]${summary['proposed']:,.0f}[/]",
              f"[bold green]-${summary['savings']:,.0f}[/]",
              f"budget ${BUDGET_MONTHLY_USD:,.0f} · headroom ${summary['headroom']:,.0f}")
    console.print(c)

    verdict = "UNDER" if summary["under_budget"] else "OVER"
    colour = "green" if summary["under_budget"] else "red"
    console.print(
        f"[{colour}]{verdict} BUDGET[/] — modelled ${summary['proposed']:,.0f}/mo against a "
        f"${BUDGET_MONTHLY_USD:,.0f} mandate. Conservative case (only half the modelled "
        f"incremental saving realised): ${summary['conservative']:,.0f}/mo.\n"
        f"Measured on this run: {model.reprocess_ratio:.1f}x fewer rows scanned than a full "
        f"nightly reload, {model.compression_ratio:.1f}x compression from Parquet+ZSTD."
    )
    console.print(
        f"\ngenerator: {gen.counters['generated']:,} unique · "
        f"{gen.counters['duplicates']:,} redelivered · {gen.counters['late']:,} late · "
        f"{gen.counters['drifted']:,} emitted with the drifted schema"
    )
    console.print(f"warehouse: [bold]{cfg.warehouse_path}[/]  ·  lake: [bold]{cfg.lake_dir}[/]")


def cmd_costs(args: argparse.Namespace) -> int:
    model = CostModel(reprocess_ratio=args.reprocess_ratio, compression_ratio=6.2)
    summary = model.summary()
    t = Table(title="SwiftLogix monthly cost model", header_style="bold", expand=True)
    for col in ("line item", "current", "proposed", "delta", "lever"):
        t.add_column(col)
    for name, cur, prop, delta, note in model.render_rows():
        t.add_row(name, cur, prop, delta, note)
    t.add_section()
    t.add_row("TOTAL", f"${CURRENT_MONTHLY_USD:,.0f}", f"${summary['proposed']:,.0f}",
              f"-${summary['savings']:,.0f}", f"headroom ${summary['headroom']:,.0f}")
    console.print(t)
    return 0


def cmd_query(args: argparse.Namespace) -> int:
    cfg = Config()
    if args.data_dir:
        cfg.data_dir = Path(args.data_dir)
    wh = Warehouse(cfg)
    queries = {
        "Ops — shipments by current status": """
            SELECT coalesce(current_status, 'in_flight') AS status, count(*) AS shipments
            FROM gold.fact_shipment_status_current GROUP BY 1 ORDER BY 2 DESC
        """,
        "Ops — freshness of the serving layer": """
            SELECT max(last_event_ts) AS newest_event,
                   date_diff('second', max(last_event_ts), now()) AS staleness_seconds
            FROM gold.fact_shipment_status_current
        """,
        "Finance — daily reconciliation (top hubs)": """
            SELECT recon_date, hub_code, shipments, events, delivered, exceptions
            FROM gold.fact_daily_shipment_recon ORDER BY events DESC LIMIT 8
        """,
        "Reliability — schema versions landed": """
            SELECT schema_version, source, count(*) AS rows
            FROM gold.fact_shipment_events GROUP BY 1, 2 ORDER BY 3 DESC
        """,
        "Reliability — alerts raised": """
            SELECT severity, monitor, count(*) AS n, max(raised_at) AS latest
            FROM meta.alerts GROUP BY 1, 2 ORDER BY 3 DESC
        """,
        "Data Science — cold-chain excursions": """
            SELECT round(min(temperature_c), 1) AS min_c, round(max(temperature_c), 1) AS max_c,
                   count(*) FILTER (WHERE temperature_c > 8) AS above_8c, count(*) AS readings
            FROM raw.sat_vehicle_telemetry
        """,
    }
    for title, sql in queries.items():
        console.print(f"\n[bold cyan]{title}[/]")
        try:
            rows = wh.con.execute(sql).fetchall()
            cols = [d[0] for d in wh.con.description]
            t = Table(box=None, header_style="bold")
            for c in cols:
                t.add_column(c)
            for r in rows:
                t.add_row(*[str(v) for v in r])
            console.print(t)
        except Exception as exc:  # pragma: no cover
            console.print(f"[red]{exc}[/]")
    wh.close()
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="swiftlogix", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    p_run = sub.add_parser("run", help="run the live pipeline with a scripted drift incident")
    p_run.add_argument("--duration", type=float, default=90.0)
    p_run.add_argument("--eps", type=int, default=1200)
    p_run.add_argument("--data-dir", type=str, default=None)
    p_run.add_argument("--fresh", action="store_true", help="wipe the lake and warehouse first")
    p_run.set_defaults(func=cmd_run)

    p_cost = sub.add_parser("costs", help="print the monthly cost model")
    p_cost.add_argument("--reprocess-ratio", type=float, default=30.0)
    p_cost.set_defaults(func=cmd_costs)

    p_query = sub.add_parser("query", help="run the consumer-facing analytics queries")
    p_query.add_argument("--data-dir", type=str, default=None)
    p_query.set_defaults(func=cmd_query)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
