"""Warehouse loader: incremental MERGE, deduplication, vault + mart population.

The single most consequential line in this module is the `event_date` predicate
on the MERGE join. Without it the engine scans every micro-partition of a
10.8B-row fact on every micro-batch; with it the merge touches two date
partitions. That one predicate is the mechanism behind Exhibit B's finding that
the nightly job burns 78% of platform spend on full-table reprocessing.

Kinesis is at-least-once. This module does not pretend otherwise: it makes the
*sink* idempotent so that redelivery, consumer restarts and an 11-day replay
all converge on the same row count.
"""

from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import duckdb

from .config import Config

EPOCH = datetime(1970, 1, 1)


def _hk(*parts: Any) -> str:
    joined = "||".join("" if p is None else str(p).strip().upper() for p in parts)
    return hashlib.sha1(joined.encode()).hexdigest()


@dataclass
class MergeStats:
    batch_id: int = 0
    files: int = 0
    records_in: int = 0
    duplicates_suppressed: int = 0
    rows_merged: int = 0
    rows_updated: int = 0
    late_arrivals: int = 0
    beyond_watermark: int = 0
    scanned_rows: int = 0
    merge_ms: float = 0.0
    promotion_state: str = "PUBLISHED"
    audit_failures: List[str] = field(default_factory=list)


class Warehouse:
    """DuckDB standing in for Snowflake gold + the Iceberg raw vault."""

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.cfg.warehouse_path.parent.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(str(cfg.warehouse_path))
        self._batch_id = 0
        self.total_rows_merged = 0
        self.total_duplicates = 0
        self.total_scanned_rows = 0
        self.supports_merge = self._detect_merge_support()
        self._bootstrap()

    # ------------------------------------------------------------------
    def _detect_merge_support(self) -> bool:
        try:
            self.con.execute("CREATE TEMP TABLE _t(k INT PRIMARY KEY, v INT)")
            self.con.execute("CREATE TEMP TABLE _s(k INT, v INT)")
            self.con.execute(
                "MERGE INTO _t t USING _s s ON t.k = s.k "
                "WHEN MATCHED THEN UPDATE SET v = s.v "
                "WHEN NOT MATCHED THEN INSERT VALUES (s.k, s.v)"
            )
            return True
        except duckdb.Error:
            return False
        finally:
            self.con.execute("DROP TABLE IF EXISTS _t")
            self.con.execute("DROP TABLE IF EXISTS _s")

    def _bootstrap(self) -> None:
        for name in ("010_raw_vault.sql", "020_star_marts.sql"):
            self.con.execute((self.cfg.sql_dir / name).read_text())
        self.con.execute(
            """
            INSERT INTO meta.load_watermark (table_name, watermark_ts, updated_at)
            SELECT 'fact_shipment_events', TIMESTAMP '1970-01-01', now()
            WHERE NOT EXISTS (
                SELECT 1 FROM meta.load_watermark WHERE table_name = 'fact_shipment_events'
            )
            """
        )
        self._seed_dim_date()

    def _seed_dim_date(self) -> None:
        self.con.execute(
            """
            INSERT INTO gold.dim_date
            SELECT d::DATE, year(d), quarter(d), month(d), day(d),
                   dayname(d), dayofweek(d) IN (0, 6)
            FROM range(DATE '2026-01-01', DATE '2027-01-01', INTERVAL 1 DAY) t(d)
            WHERE d::DATE NOT IN (SELECT date_key FROM gold.dim_date)
            """
        )

    # ------------------------------------------------------------------
    def watermark(self) -> datetime:
        row = self.con.execute(
            "SELECT watermark_ts FROM meta.load_watermark WHERE table_name = 'fact_shipment_events'"
        ).fetchone()
        return row[0] if row else EPOCH

    def _advance_watermark(self, ts: datetime) -> None:
        self.con.execute(
            "UPDATE meta.load_watermark SET watermark_ts = ?, updated_at = now() "
            "WHERE table_name = 'fact_shipment_events' AND ? > watermark_ts",
            [ts, ts],
        )

    # ------------------------------------------------------------------
    def _stage(self, files: Sequence[Path], stats: MergeStats) -> int:
        """Build the deduplicated staging set from new bronze files only.

        Reading an explicit file list, rather than globbing the whole lake, is
        what makes this incremental. The watermark then guards against a file
        being re-read after a crash.
        """
        paths = [str(p) for p in files]
        watermark = self.watermark()
        cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(
            hours=self.cfg.late_arrival_hours
        )

        self.con.execute("DROP TABLE IF EXISTS stg_events")
        self.con.execute(
            """
            CREATE TEMP TABLE stg_events AS
            WITH src AS (
                SELECT * EXCLUDE (event_ts, ingest_ts, delivery_window_start, delivery_window_end),
                       event_ts::TIMESTAMP              AS event_ts,
                       ingest_ts::TIMESTAMP             AS ingest_ts,
                       delivery_window_start::TIMESTAMP AS delivery_window_start,
                       delivery_window_end::TIMESTAMP   AS delivery_window_end
                FROM read_parquet(?)
            ),
            bounded AS (
                -- Watermark bounds the read; the 24h window bounds dedup state.
                SELECT * FROM src
                WHERE ingest_ts > ?
                  AND event_ts >= ?
            ),
            ranked AS (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY event_id
                           ORDER BY ingest_ts DESC, payload_hash
                       ) AS rn
                FROM bounded
            )
            SELECT * EXCLUDE (rn) FROM ranked WHERE rn = 1
            """,
            [paths, watermark, cutoff],
        )

        raw_count = self.con.execute(
            "SELECT count(*) FROM read_parquet(?)", [paths]
        ).fetchone()[0]
        staged = self.con.execute("SELECT count(*) FROM stg_events").fetchone()[0]

        # Events older than the 24h watermark are not silently dropped: they go
        # to a side table and drive an explicit daily restatement.
        beyond = self.con.execute(
            """
            SELECT count(*) FROM read_parquet(?)
            WHERE ingest_ts::TIMESTAMP > ? AND event_ts::TIMESTAMP < ?
            """,
            [paths, watermark, cutoff],
        ).fetchone()[0]
        if beyond:
            self.con.execute(
                """
                INSERT INTO quarantine.late_arrivals
                SELECT event_id, shipment_id, event_ts::TIMESTAMP, ingest_ts::TIMESTAMP,
                       date_diff('minute', event_ts::TIMESTAMP, ingest_ts::TIMESTAMP) / 60.0,
                       'beyond 24h watermark - requires partition restatement'
                FROM read_parquet(?)
                WHERE ingest_ts::TIMESTAMP > ? AND event_ts::TIMESTAMP < ?
                """,
                [paths, watermark, cutoff],
            )

        stats.records_in = raw_count
        stats.scanned_rows = raw_count
        stats.beyond_watermark = beyond
        stats.duplicates_suppressed = max(0, raw_count - staged - beyond)
        stats.late_arrivals = self.con.execute(
            "SELECT count(*) FROM stg_events WHERE event_ts < ingest_ts - INTERVAL 5 MINUTE"
        ).fetchone()[0]
        return staged

    # ------------------------------------------------------------------
    def _audit(self, stats: MergeStats) -> bool:
        """Write-Audit-Publish gate.

        The staged data is fully materialised but not yet visible to any
        consumer. If an expectation fails, the last-good gold partition keeps
        serving and the promotion aborts loudly. The 2025 failure mode was a job
        that reported success while emitting nulls -- stale-but-correct beats
        fresh-but-wrong every time.
        """
        failures: List[str] = []

        checks = {
            "event_id not null": "SELECT count(*) FROM stg_events WHERE event_id IS NULL",
            "shipment_id not null": "SELECT count(*) FROM stg_events WHERE shipment_id IS NULL",
            "event_id unique": (
                "SELECT count(*) FROM (SELECT event_id FROM stg_events "
                "GROUP BY event_id HAVING count(*) > 1)"
            ),
            "status in accepted set": (
                "SELECT count(*) FROM stg_events WHERE status IS NOT NULL AND status NOT IN "
                "('pickup','in_transit','exception','delivered')"
            ),
            "geo within bounds": (
                "SELECT count(*) FROM stg_events WHERE geo_lat NOT BETWEEN -90 AND 90 "
                "OR geo_lon NOT BETWEEN -180 AND 180"
            ),
        }
        for name, sql in checks.items():
            if self.con.execute(sql).fetchone()[0]:
                failures.append(name)

        # The business-level canary: a required column going null en masse is
        # exactly the signature of the delivery_window rename.
        total = self.con.execute("SELECT count(*) FROM stg_events").fetchone()[0]
        if total:
            null_pct = self.con.execute(
                "SELECT 100.0 * count(*) FILTER (WHERE delivery_window_start IS NULL) / count(*) "
                "FROM stg_events"
            ).fetchone()[0]
            if null_pct > self.cfg.null_rate_alert_pct:
                failures.append(f"delivery_window_start null rate {null_pct:.1f}%")

        stats.audit_failures = failures
        return not failures

    # ------------------------------------------------------------------
    def _merge_events(self) -> int:
        """Partition-pruned, idempotent upsert into the atomic fact."""
        before = self.con.execute("SELECT count(*) FROM gold.fact_shipment_events").fetchone()[0]

        if self.supports_merge:
            self.con.execute(
                """
                MERGE INTO gold.fact_shipment_events AS t
                USING (
                    SELECT event_id, event_date, event_ts, ingest_ts,
                           md5(shipment_id) AS shipment_key,
                           CASE WHEN vehicle_id IS NULL THEN NULL ELSE md5(vehicle_id) END AS vehicle_key,
                           CASE WHEN hub_code   IS NULL THEN NULL ELSE md5(hub_code)   END AS facility_key,
                           event_type, status, geo_lat, geo_lon, temperature_c,
                           delivery_window_start, delivery_window_end,
                           source, schema_version, payload_hash
                    FROM stg_events
                ) AS s
                   ON t.event_id = s.event_id
                  -- Partition pruning: restrict the target to the dates the
                  -- batch can possibly touch. Drop this and the merge degrades
                  -- into the full-table scan that costs $47.6K/month.
                  AND t.event_date = s.event_date
                  AND t.event_date >= (SELECT min(event_date) FROM stg_events)
                WHEN MATCHED AND t.payload_hash IS DISTINCT FROM s.payload_hash THEN UPDATE SET
                    status = s.status, geo_lat = s.geo_lat, geo_lon = s.geo_lon,
                    temperature_c = s.temperature_c,
                    delivery_window_start = s.delivery_window_start,
                    delivery_window_end = s.delivery_window_end,
                    schema_version = s.schema_version, payload_hash = s.payload_hash,
                    updated_at = now()
                WHEN NOT MATCHED THEN INSERT (
                    event_id, event_date, event_ts, ingest_ts, shipment_key, vehicle_key,
                    facility_key, event_type, status, geo_lat, geo_lon, temperature_c,
                    delivery_window_start, delivery_window_end, source, schema_version,
                    payload_hash, updated_at
                ) VALUES (
                    s.event_id, s.event_date, s.event_ts, s.ingest_ts, s.shipment_key,
                    s.vehicle_key, s.facility_key, s.event_type, s.status, s.geo_lat,
                    s.geo_lon, s.temperature_c, s.delivery_window_start, s.delivery_window_end,
                    s.source, s.schema_version, s.payload_hash, now()
                )
                """
            )
        else:  # pragma: no cover - fallback for engines without MERGE
            self.con.execute(
                """
                INSERT INTO gold.fact_shipment_events
                SELECT event_id, event_date, event_ts, ingest_ts, md5(shipment_id),
                       CASE WHEN vehicle_id IS NULL THEN NULL ELSE md5(vehicle_id) END,
                       CASE WHEN hub_code IS NULL THEN NULL ELSE md5(hub_code) END,
                       event_type, status, geo_lat, geo_lon, temperature_c,
                       delivery_window_start, delivery_window_end, source, schema_version,
                       payload_hash, now()
                FROM stg_events
                ON CONFLICT (event_id) DO UPDATE SET
                    status = excluded.status, payload_hash = excluded.payload_hash,
                    delivery_window_start = excluded.delivery_window_start,
                    updated_at = now()
                """
            )

        after = self.con.execute("SELECT count(*) FROM gold.fact_shipment_events").fetchone()[0]
        return after - before

    # ------------------------------------------------------------------
    def _load_vault(self) -> None:
        """Hubs, link and satellites -- all insert-only, all anti-joined."""
        self.con.execute(
            """
            INSERT INTO raw.hub_shipment
            SELECT DISTINCT md5(shipment_id), shipment_id, now(), any_value(source)
            FROM stg_events s
            WHERE NOT EXISTS (SELECT 1 FROM raw.hub_shipment h WHERE h.shipment_hk = md5(s.shipment_id))
            GROUP BY shipment_id
            """
        )
        self.con.execute(
            """
            INSERT INTO raw.hub_vehicle
            SELECT DISTINCT md5(vehicle_id), vehicle_id, now(), any_value(source)
            FROM stg_events s
            WHERE vehicle_id IS NOT NULL
              AND NOT EXISTS (SELECT 1 FROM raw.hub_vehicle h WHERE h.vehicle_hk = md5(s.vehicle_id))
            GROUP BY vehicle_id
            """
        )
        self.con.execute(
            """
            INSERT INTO raw.hub_facility
            SELECT DISTINCT md5(hub_code), hub_code, split_part(hub_code, '-', 2), now(), any_value(source)
            FROM stg_events s
            WHERE hub_code IS NOT NULL
              AND NOT EXISTS (SELECT 1 FROM raw.hub_facility h WHERE h.facility_hk = md5(s.hub_code))
            GROUP BY hub_code
            """
        )
        self.con.execute(
            """
            INSERT INTO raw.lnk_shipment_vehicle_facility
            SELECT DISTINCT
                md5(concat_ws('||', shipment_id, coalesce(vehicle_id,''), coalesce(hub_code,''))),
                md5(shipment_id),
                CASE WHEN vehicle_id IS NULL THEN NULL ELSE md5(vehicle_id) END,
                CASE WHEN hub_code IS NULL THEN NULL ELSE md5(hub_code) END,
                now(), any_value(source)
            FROM stg_events s
            WHERE NOT EXISTS (
                SELECT 1 FROM raw.lnk_shipment_vehicle_facility l
                WHERE l.link_hk = md5(concat_ws('||', s.shipment_id, coalesce(s.vehicle_id,''), coalesce(s.hub_code,'')))
            )
            GROUP BY shipment_id, vehicle_id, hub_code
            """
        )
        # Satellites: hash_diff makes a redelivered record a no-op and a genuine
        # correction a new version, with the full history preserved either way.
        self.con.execute(
            """
            INSERT INTO raw.sat_shipment_status
            SELECT DISTINCT ON (md5(shipment_id), event_ts, hash_diff)
                md5(shipment_id), event_ts, now(), hash_diff, status, geo_lat, geo_lon,
                delivery_window_start, delivery_window_end, schema_version, source
            FROM (
                SELECT *, md5(concat_ws('||', coalesce(status,''), coalesce(geo_lat::VARCHAR,''),
                                        coalesce(geo_lon::VARCHAR,''),
                                        coalesce(delivery_window_start::VARCHAR,''))) AS hash_diff
                FROM stg_events WHERE event_type IN ('status', 'hub_scan')
            ) s
            WHERE NOT EXISTS (
                SELECT 1 FROM raw.sat_shipment_status x
                WHERE x.shipment_hk = md5(s.shipment_id) AND x.event_ts = s.event_ts
                  AND x.hash_diff = s.hash_diff
            )
            """
        )
        self.con.execute(
            """
            INSERT INTO raw.sat_vehicle_telemetry
            SELECT DISTINCT ON (md5(vehicle_id), event_ts, hash_diff)
                md5(vehicle_id), event_ts, now(), hash_diff, temperature_c, geo_lat, geo_lon, source
            FROM (
                SELECT *, md5(concat_ws('||', coalesce(temperature_c::VARCHAR,''),
                                        coalesce(geo_lat::VARCHAR,''), coalesce(geo_lon::VARCHAR,''))) AS hash_diff
                FROM stg_events WHERE vehicle_id IS NOT NULL AND event_type IN ('gps_ping','temperature')
            ) s
            WHERE NOT EXISTS (
                SELECT 1 FROM raw.sat_vehicle_telemetry x
                WHERE x.vehicle_hk = md5(s.vehicle_id) AND x.event_ts = s.event_ts
                  AND x.hash_diff = s.hash_diff
            )
            """
        )
        self.con.execute(
            """
            INSERT INTO raw.sat_event_metadata
            SELECT event_id, now(), metadata_json, source
            FROM stg_events WHERE metadata_json IS NOT NULL
            ON CONFLICT (event_id) DO NOTHING
            """
        )

    # ------------------------------------------------------------------
    def _refresh_current_status(self) -> None:
        """The Ops mart. Equivalent to a Snowflake dynamic table at 1-min lag."""
        self.con.execute(
            """
            CREATE OR REPLACE TEMP TABLE delta_status AS
            SELECT
                md5(shipment_id) AS shipment_key,
                any_value(shipment_id)                                      AS shipment_id,
                max(event_ts)                                               AS last_event_ts,
                arg_max(status, event_ts) FILTER (WHERE status IS NOT NULL)  AS current_status,
                min(event_ts) FILTER (WHERE status = 'pickup')               AS pickup_ts,
                min(event_ts) FILTER (WHERE status = 'in_transit')           AS in_transit_ts,
                min(event_ts) FILTER (WHERE status = 'exception')            AS exception_ts,
                min(event_ts) FILTER (WHERE status = 'delivered')            AS delivered_ts,
                arg_max(vehicle_id, event_ts) FILTER (WHERE vehicle_id IS NOT NULL) AS last_vehicle_id,
                arg_max(hub_code, event_ts) FILTER (WHERE hub_code IS NOT NULL)     AS last_hub_code,
                arg_max(geo_lat, event_ts)                                  AS last_geo_lat,
                arg_max(geo_lon, event_ts)                                  AS last_geo_lon,
                arg_max(delivery_window_start, event_ts) FILTER (WHERE delivery_window_start IS NOT NULL) AS dws,
                arg_max(delivery_window_end, event_ts) FILTER (WHERE delivery_window_end IS NOT NULL)     AS dwe,
                count(*)                                                    AS event_count
            FROM stg_events
            GROUP BY md5(shipment_id)
            """
        )
        self.con.execute(
            """
            INSERT INTO gold.fact_shipment_status_current
            SELECT shipment_key, shipment_id, current_status, last_event_ts, pickup_ts,
                   in_transit_ts, exception_ts, delivered_ts, last_vehicle_id, last_hub_code,
                   last_geo_lat, last_geo_lon, dws, dwe, event_count,
                   delivered_ts IS NOT NULL AND dwe IS NOT NULL AND delivered_ts > dwe,
                   now()
            FROM delta_status
            ON CONFLICT (shipment_key) DO UPDATE SET
                -- Late and out-of-order events must never roll the snapshot
                -- backwards, so every field is guarded by the event timestamp.
                current_status = CASE WHEN excluded.last_event_ts >= fact_shipment_status_current.last_event_ts
                                      AND excluded.current_status IS NOT NULL
                                 THEN excluded.current_status ELSE fact_shipment_status_current.current_status END,
                last_event_ts  = greatest(fact_shipment_status_current.last_event_ts, excluded.last_event_ts),
                pickup_ts      = least(coalesce(fact_shipment_status_current.pickup_ts, excluded.pickup_ts), coalesce(excluded.pickup_ts, fact_shipment_status_current.pickup_ts)),
                in_transit_ts  = least(coalesce(fact_shipment_status_current.in_transit_ts, excluded.in_transit_ts), coalesce(excluded.in_transit_ts, fact_shipment_status_current.in_transit_ts)),
                exception_ts   = least(coalesce(fact_shipment_status_current.exception_ts, excluded.exception_ts), coalesce(excluded.exception_ts, fact_shipment_status_current.exception_ts)),
                delivered_ts   = least(coalesce(fact_shipment_status_current.delivered_ts, excluded.delivered_ts), coalesce(excluded.delivered_ts, fact_shipment_status_current.delivered_ts)),
                last_vehicle_id = coalesce(excluded.last_vehicle_id, fact_shipment_status_current.last_vehicle_id),
                last_hub_code   = coalesce(excluded.last_hub_code, fact_shipment_status_current.last_hub_code),
                last_geo_lat    = coalesce(excluded.last_geo_lat, fact_shipment_status_current.last_geo_lat),
                last_geo_lon    = coalesce(excluded.last_geo_lon, fact_shipment_status_current.last_geo_lon),
                delivery_window_start = coalesce(excluded.delivery_window_start, fact_shipment_status_current.delivery_window_start),
                delivery_window_end   = coalesce(excluded.delivery_window_end, fact_shipment_status_current.delivery_window_end),
                event_count     = fact_shipment_status_current.event_count + excluded.event_count,
                updated_at      = now()
            """
        )

    def _refresh_finance_recon(self) -> None:
        """Daily grain. Batch, not streaming -- Finance wants a settled figure."""
        self.con.execute(
            """
            INSERT INTO gold.fact_daily_shipment_recon
            SELECT event_date, coalesce(last_hub_code, 'UNASSIGNED'),
                   count(DISTINCT shipment_key), sum(events),
                   count(*) FILTER (WHERE delivered), count(*) FILTER (WHERE had_exception),
                   count(*) FILTER (WHERE breached), NULL, NULL, now()
            FROM (
                SELECT e.event_date, e.shipment_key,
                       arg_max(s.last_hub_code, e.event_ts) AS last_hub_code,
                       count(*) AS events,
                       bool_or(e.status = 'delivered') AS delivered,
                       bool_or(e.status = 'exception') AS had_exception,
                       bool_or(s.is_window_breached)   AS breached
                FROM gold.fact_shipment_events e
                JOIN gold.fact_shipment_status_current s USING (shipment_key)
                WHERE e.event_date >= (SELECT min(event_date) FROM stg_events)
                GROUP BY e.event_date, e.shipment_key
            )
            GROUP BY 1, 2
            ON CONFLICT (recon_date, hub_code) DO UPDATE SET
                shipments = excluded.shipments, events = excluded.events,
                delivered = excluded.delivered, exceptions = excluded.exceptions,
                window_breaches = excluded.window_breaches,
                restated_at = CASE WHEN excluded.events <> fact_daily_shipment_recon.events
                                   THEN now() ELSE fact_daily_shipment_recon.restated_at END,
                restatement_note = CASE WHEN excluded.events <> fact_daily_shipment_recon.events
                                   THEN 'late arrivals or replay applied' ELSE fact_daily_shipment_recon.restatement_note END,
                updated_at = now()
            """
        )

    # ------------------------------------------------------------------
    def merge_batch(self, files: Sequence[Path], quarantine_pct: float = 0.0) -> MergeStats:
        """Run one incremental micro-batch end to end."""
        self._batch_id += 1
        stats = MergeStats(batch_id=self._batch_id, files=len(files))
        if not files:
            stats.promotion_state = "IDLE"
            return stats

        started = time.perf_counter()

        # Circuit breaker: if a source is failing its contract en masse, do not
        # publish a partial picture. Fail loudly, keep the last good partition.
        if quarantine_pct > self.cfg.quarantine_circuit_breaker_pct:
            stats.promotion_state = "BLOCKED_CIRCUIT_BREAKER"
            stats.merge_ms = (time.perf_counter() - started) * 1000
            self._record_metrics(stats)
            return stats

        staged = self._stage(files, stats)
        if staged == 0:
            stats.promotion_state = "NO_NEW_ROWS"
            stats.merge_ms = (time.perf_counter() - started) * 1000
            self._record_metrics(stats)
            return stats

        if not self._audit(stats):
            stats.promotion_state = "BLOCKED_AUDIT"
            stats.merge_ms = (time.perf_counter() - started) * 1000
            self._record_metrics(stats)
            return stats

        self.con.execute("BEGIN TRANSACTION")
        try:
            stats.rows_merged = self._merge_events()
            self._load_vault()
            self._refresh_current_status()
            self._refresh_finance_recon()
            new_watermark = self.con.execute(
                "SELECT max(ingest_ts) FROM stg_events"
            ).fetchone()[0]
            if new_watermark:
                self._advance_watermark(new_watermark)
            self.con.execute("COMMIT")
        except Exception:
            self.con.execute("ROLLBACK")
            stats.promotion_state = "ROLLED_BACK"
            raise

        stats.rows_updated = max(0, staged - stats.rows_merged)
        stats.merge_ms = (time.perf_counter() - started) * 1000
        self.total_rows_merged += stats.rows_merged
        self.total_duplicates += stats.duplicates_suppressed
        self.total_scanned_rows += stats.scanned_rows
        self._record_metrics(stats)
        return stats

    def _record_metrics(self, stats: MergeStats) -> None:
        self.con.execute(
            """
            INSERT INTO meta.pipeline_metrics VALUES
            (now(), ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                stats.batch_id, stats.records_in, stats.rows_merged,
                stats.duplicates_suppressed, 0, stats.late_arrivals,
                stats.merge_ms, stats.scanned_rows, stats.promotion_state,
            ],
        )

    # ------------------------------------------------------------------
    def alert(self, severity: str, monitor: str, message: str, detail: Optional[str] = None) -> None:
        self.con.execute(
            "INSERT INTO meta.alerts VALUES (now(), ?, ?, ?, ?)",
            [severity, monitor, message, detail],
        )

    def log_schema_change(self, version: str, action: str, actor: str, note: str) -> None:
        self.con.execute(
            "INSERT INTO meta.schema_change_log VALUES (now(), ?, ?, ?, ?)",
            [version, action, actor, note],
        )

    # ------------------------------------------------------------------
    def counts(self) -> Dict[str, int]:
        q = lambda sql: self.con.execute(sql).fetchone()[0]  # noqa: E731
        return {
            "fact_events": q("SELECT count(*) FROM gold.fact_shipment_events"),
            "shipments": q("SELECT count(*) FROM gold.fact_shipment_status_current"),
            "hub_vehicle": q("SELECT count(*) FROM raw.hub_vehicle"),
            "sat_status": q("SELECT count(*) FROM raw.sat_shipment_status"),
            "sat_telemetry": q("SELECT count(*) FROM raw.sat_vehicle_telemetry"),
            "late_arrivals": q("SELECT count(*) FROM quarantine.late_arrivals"),
            "recon_rows": q("SELECT count(*) FROM gold.fact_daily_shipment_recon"),
        }

    def null_rate(self, column: str = "delivery_window_start") -> float:
        row = self.con.execute(
            f"""
            SELECT 100.0 * count(*) FILTER (WHERE {column} IS NULL) / nullif(count(*), 0)
            FROM gold.fact_shipment_events
            WHERE ingest_ts > now() - INTERVAL 60 SECOND
            """
        ).fetchone()
        return float(row[0] or 0.0)

    def duplicate_check(self) -> int:
        """Must always be zero. This is the idempotency proof."""
        return self.con.execute(
            "SELECT count(*) FROM (SELECT event_id FROM gold.fact_shipment_events "
            "GROUP BY event_id HAVING count(*) > 1)"
        ).fetchone()[0]

    def close(self) -> None:
        self.con.close()
