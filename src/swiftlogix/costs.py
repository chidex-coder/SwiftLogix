"""Cost model: the $61K -> under-$42K problem.

Exhibit B is unambiguous -- the nightly Spark job is 78% of spend and it is
full-table reprocessing. Everything else is rounding.

The model is driven by measurements taken during an actual pipeline run
(bytes landed, rows scanned incrementally vs rows that a full reprocess would
have touched) and extrapolated to production volume, so the savings claim is
anchored to an observed reprocessing ratio rather than an assumption.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple

from .config import (
    BUDGET_MONTHLY_USD,
    CURRENT_MONTHLY_USD,
    PROD_AVG_EVENT_BYTES,
    PROD_AVG_EPS,
    PROD_EVENTS_PER_DAY,
    PROD_PEAK_EPS,
)

# Published on-demand list prices, eu-west-2 / Snowflake Standard.
KINESIS_SHARD_HOUR = 0.015
KINESIS_PUT_PER_M = 0.014
KINESIS_EXT_RETENTION_SHARD_HOUR = 0.020
FIREHOSE_PER_GB = 0.029
FIREHOSE_PARQUET_PER_GB = 0.018
LAMBDA_PER_M_REQ = 0.20
LAMBDA_GB_SECOND = 0.0000166667
S3_STANDARD_PER_GB = 0.023
S3_IA_PER_GB = 0.0125
S3_PUT_PER_1K = 0.005
SNOWFLAKE_CREDIT = 3.00
EMR_ONDEMAND_VCPU_HOUR = 0.052
EMR_SPOT_DISCOUNT = 0.70

HOURS_PER_MONTH = 730
DAYS_PER_MONTH = 30


@dataclass
class LineItem:
    name: str
    current: float
    proposed: float
    note: str

    @property
    def delta(self) -> float:
        return self.proposed - self.current


@dataclass
class CostModel:
    """Measured inputs from a real run, extrapolated to production."""

    reprocess_ratio: float = 1.0   # rows a full reload scans / rows incremental scans
    compression_ratio: float = 6.0  # raw JSON bytes / parquet+zstd bytes
    observed_events: int = 0

    # ------------------------------------------------------------------
    @staticmethod
    def from_run(
        incremental_rows_scanned: int,
        full_reload_rows_scanned: int,
        raw_bytes: int,
        parquet_bytes: int,
        observed_events: int,
    ) -> "CostModel":
        ratio = (
            full_reload_rows_scanned / incremental_rows_scanned
            if incremental_rows_scanned
            else 1.0
        )
        compression = raw_bytes / parquet_bytes if parquet_bytes else 6.0
        return CostModel(
            reprocess_ratio=max(ratio, 1.0),
            compression_ratio=max(compression, 1.0),
            observed_events=observed_events,
        )

    # ------------------------------------------------------------------
    def _kinesis(self) -> float:
        # Peak 4,500 eps / 1,000 records-per-second per shard = 5 shards floor;
        # 8 provisioned gives ~78% headroom. Provisioned beats on-demand at a
        # steady 1,389 eps average -- on-demand is priced for unknown spikes.
        shards = 8
        shard_cost = shards * KINESIS_SHARD_HOUR * HOURS_PER_MONTH
        retention = shards * KINESIS_EXT_RETENTION_SHARD_HOUR * HOURS_PER_MONTH
        puts = (PROD_EVENTS_PER_DAY * DAYS_PER_MONTH / 1_000_000) * KINESIS_PUT_PER_M
        return shard_cost + retention + puts

    def _firehose(self) -> float:
        gb_month = PROD_EVENTS_PER_DAY * DAYS_PER_MONTH * PROD_AVG_EVENT_BYTES / 1e9
        return gb_month * (FIREHOSE_PER_GB + FIREHOSE_PARQUET_PER_GB)

    def _validation_lambda(self) -> float:
        # Buffered batches, not individual records. Validating 3.6B records one
        # at a time would cost ~$720/month in invocations alone; at ~3MB per
        # buffered batch it is ~1.2M invocations.
        batches = PROD_EVENTS_PER_DAY * DAYS_PER_MONTH / 3_000
        requests = (batches / 1_000_000) * LAMBDA_PER_M_REQ
        duration = batches * 0.9 * 1.0 * LAMBDA_GB_SECOND
        return requests + duration

    def _s3(self) -> Tuple[float, float]:
        raw_gb_day = PROD_EVENTS_PER_DAY * PROD_AVG_EVENT_BYTES / 1e9
        cur_gb = raw_gb_day * 90                      # 90 days of raw JSON
        new_gb = raw_gb_day * 90 / self.compression_ratio  # parquet + zstd
        current = cur_gb * S3_STANDARD_PER_GB
        # Tiering: 0-90d Standard, 90d-2y Intelligent-Tiering/IA.
        proposed = (new_gb * S3_STANDARD_PER_GB) + (new_gb * 7 * S3_IA_PER_GB)
        # Request cost: the nightly full reload re-lists and re-GETs everything.
        current += (raw_gb_day * 90 * 1000 / 1000) * S3_PUT_PER_1K * 30
        proposed += (new_gb * 1000 / 1000) * S3_PUT_PER_1K * 30
        return current, proposed

    def _spark(self) -> Tuple[float, float]:
        current = CURRENT_MONTHLY_USD * 0.78  # Exhibit B: 78% of spend

        # The incremental load path scans 1/reprocess_ratio of the rows. It runs
        # on-demand, NOT Spot: it sits on the 5-minute SLA, and an interrupted
        # instance there costs freshness. Spot applies only to the workloads that
        # genuinely tolerate interruption -- DS feature engineering and backfill.
        #
        # Modelling it the other way round (a blanket Spot discount across the
        # whole reload) would let this line clear the budget even when the
        # incremental lever delivers nothing, which would make the saving look
        # structural when it is really just a pricing trick.
        incremental = current / self.reprocess_ratio
        ds_and_backfill = 6_000 * (1 - EMR_SPOT_DISCOUNT)
        return current, incremental + ds_and_backfill

    def _snowflake(self) -> Tuple[float, float]:
        # Warehouse spend rises: we are buying 26h -> 5min freshness with
        # near-real-time dynamic tables. That is the latency-vs-cost purchase,
        # and it is funded many times over by killing the full reprocess.
        current = CURRENT_MONTHLY_USD * 0.155
        ops = 1.0 * 24 * 30 * 0.35 * SNOWFLAKE_CREDIT       # XS, 60s auto-suspend
        finance = 4.0 * 0.35 * 30 * SNOWFLAKE_CREDIT        # M, ~20 min/day
        streaming = 3_600                                    # Snowpipe Streaming ingest
        dynamic_tables = 5_200                               # 1-minute TARGET_LAG refresh
        # Data Science moves off the warehouse onto Spot EMR reading Iceberg.
        return current, ops + finance + streaming + dynamic_tables

    def _observability(self) -> Tuple[float, float]:
        # Schema registry, Glue catalog, CloudWatch metrics, expectations suite.
        # Against a $480K incident this is the highest-ROI line in the budget.
        return CURRENT_MONTHLY_USD * 0.023, 1_700.0

    # ------------------------------------------------------------------
    def line_items(self) -> List[LineItem]:
        spark_cur, spark_new = self._spark()
        sf_cur, sf_new = self._snowflake()
        s3_cur, s3_new = self._s3()
        obs_cur, obs_new = self._observability()

        return [
            LineItem(
                "EMR nightly full reprocess",
                spark_cur,
                spark_new,
                f"incremental MERGE ({self.reprocess_ratio:.0f}x less scanned); "
                f"Spot on backfill/DS only",
            ),
            LineItem("Snowflake compute", sf_cur, sf_new, "3 isolated WHs, 60s auto-suspend"),
            LineItem("S3 storage + requests", s3_cur, s3_new,
                     f"Parquet+ZSTD {self.compression_ratio:.1f}x, tiered at 90d"),
            LineItem("Kinesis Data Streams", 0.0, self._kinesis(), "8 shards provisioned, 7d retention"),
            LineItem("Firehose + Parquet conversion", 0.0, self._firehose(), "128MB / 300s buffer"),
            LineItem("Contract validation Lambda", 0.0, self._validation_lambda(), "batched, not per-record"),
            LineItem("Registry, catalog, monitoring", obs_cur, obs_new, "the $480K insurance policy"),
        ]

    def summary(self) -> Dict[str, float]:
        items = self.line_items()
        current = sum(i.current for i in items)
        proposed = sum(i.proposed for i in items)
        # Normalise the current column to the stated $61K baseline.
        scale = CURRENT_MONTHLY_USD / current if current else 1.0
        return {
            "current": CURRENT_MONTHLY_USD,
            "proposed": proposed,
            "savings": CURRENT_MONTHLY_USD - proposed,
            "budget": BUDGET_MONTHLY_USD,
            "headroom": BUDGET_MONTHLY_USD - proposed,
            "under_budget": proposed < BUDGET_MONTHLY_USD,
            "scale_factor": scale,
            # Sensitivity: if incremental loading delivers only half the modelled
            # saving, do we still clear the mandate?
            "conservative": proposed + (CURRENT_MONTHLY_USD - proposed) * 0.5,
        }

    def render_rows(self) -> List[Tuple[str, str, str, str, str]]:
        rows = []
        for item in self.line_items():
            rows.append(
                (
                    item.name,
                    f"${item.current:,.0f}",
                    f"${item.proposed:,.0f}",
                    f"{'+' if item.delta >= 0 else '-'}${abs(item.delta):,.0f}",
                    item.note,
                )
            )
        return rows
