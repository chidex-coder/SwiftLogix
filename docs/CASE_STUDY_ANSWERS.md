# SwiftLogix Case Study — Written Responses

Responses to the five-part design review. Every quantitative claim here is
either derived from the business case or measured by the implementation in this
repository; where a number is modelled rather than measured, it says so.

---

## Part 1a — Primary Ingestion & Streaming Layer

**Amazon Kinesis Data Streams + Kinesis Firehose.**

Chosen over MSK because the workload needs a managed, elastic pipe rather than a
Kafka cluster to operate. At 120M events/day there is no consumer ecosystem or
partition-level tuning that justifies running MSK; Firehose's native Parquet
conversion and direct Snowflake delivery remove two components that would
otherwise need building and monitoring. MSK would win if we needed Kafka
Streams, log compaction, or >7-day retention as a system of record.

---

## Part 1b — End-to-End Architecture

**Ingestion.** Vehicle telematics and hub scanners publish via the Kinesis
Producer Library (aggregation on, 200ms buffer) into `swiftlogix-events-raw`.
Partition key is `vehicle_id`/`hub_code` — high cardinality, so no hot shard.
Sizing: peak 4,500 events/sec × ~1KB = 4.5 MB/s; a shard gives 1 MB/s or 1,000
records/s, so **6 shards is the floor and I provision 8** (~78% headroom) with a
scaling Lambda on `IncomingRecords`. Retention extended 24h → **7 days** to make
replay possible. Provisioned, not on-demand: at a steady 1,389 events/sec
average, on-demand is priced for spiky unknown load and costs roughly 2.5× more.

**Contract gate.** Firehose invokes a transform Lambda on each buffered batch
(≈3MB, so ~1.2M invocations/month, not 3.6B — batching is what keeps this at
~$40/mo rather than ~$720). The Lambda validates each record against the JSON
Schema in **AWS Glue Schema Registry**. Conforming records pass; violations are
routed to quarantine with the full raw payload.

**Two consumers off one stream, both enhanced fan-out** (2 MB/s each, no read
contention):

- **Hot path (5-min SLA):** Firehose → **Snowflake direct delivery**, buffer 60s
  / 1MB, landing in `RAW.SHIPMENT_EVENTS_STREAM`. A Snowflake **Dynamic Table**
  with `TARGET_LAG = '1 minute'` builds `OPS.FACT_SHIPMENT_STATUS_CURRENT`.
- **Lake path:** Firehose → S3 `bronze/dt=/hr=`, native Parquet conversion,
  ZSTD-3, 128MB buffer or 300s, registered as **Apache Iceberg** in the Glue
  Catalog. This is the replay source and the Data Science source.

**Latency budget:** producer→stream 2s + Firehose buffer 60s + dynamic table lag
60s + dashboard cache 30s ≈ **2.5–3 min p99**, inside the 5-minute SLA with room
for a retry.

**Storage layers.** Bronze = immutable raw (VARIANT/schema-on-read, never
rejects). Silver = validated, deduplicated Iceberg, `PARTITIONED BY
(days(event_ts), bucket(16, shipment_id))`. Gold = Snowflake native for 90 days,
external Iceberg tables for 90 days–2 years.

**Serving.** Three consumers, three isolated warehouses — a performance *and* a
cost decision:

| Consumer | Compute | Pattern | Why |
|---|---|---|---|
| Ops dashboard | XS multi-cluster (1–3), 60s auto-suspend | streaming → dynamic table | 5-min SLA; queries a pre-aggregated current-state table, so sub-second |
| Finance recon | M warehouse, ~20 min/day at 04:00 | dbt batch | daily grain; batch is cheaper and settles late arrivals first |
| Data Science | EMR Serverless on Spot, reads Iceberg | batch, no warehouse | full-history scans on Spot cost ~an order of magnitude less than warehouse credits, and DS never contends with Ops |

**Sub-10s over 90 days** comes from clustering gold on `(event_date, event_type)`,
pre-aggregating dashboard metrics into the dynamic table rather than scanning the
10.8B-row event fact, and Snowflake's result cache.

---

## Part 2a — Warehouse Modeling Approach

**Hybrid: Data Vault raw layer + Star Schema marts.**

---

## Part 2b — Schema Design & Rationale

**Raw Vault** (Iceberg on S3, mirrored into Snowflake `RAW`):

- `HUB_SHIPMENT` (`shipment_hk`, `shipment_id`, `load_dts`, `rec_src`),
  `HUB_VEHICLE`, `HUB_FACILITY`
- `LNK_SHIPMENT_VEHICLE_FACILITY` (`link_hk`, three hub hashes, `load_dts`)
- `SAT_SHIPMENT_STATUS` (`shipment_hk`, `event_ts`, `load_dts`, `hash_diff`,
  `status`, `geo_lat`, `geo_lon`, `delivery_window_start/end`)
- `SAT_VEHICLE_TELEMETRY` (`vehicle_hk`, `event_ts`, `load_dts`, `hash_diff`,
  `temperature_c`, `geo_lat`, `geo_lon`)
- `SAT_EVENT_METADATA` (`event_id`, `load_dts`, `payload VARIANT`) — the volatile
  `metadata` object lands here untyped

**Star marts** (`GOLD`), with grain stated explicitly:

| Table | Grain | Notes |
|---|---|---|
| `fact_shipment_events` | one row per physical event | ~120M rows/day, clustered `(event_date, event_type)` |
| `fact_shipment_status_current` | one row per shipment | accumulating snapshot with milestone timestamps; what Ops queries |
| `fact_daily_shipment_recon` | shipment × day × hub | Finance |
| `fact_vehicle_assignment` | shipment × vehicle × effective window | see below |

Dimensions: `dim_vehicle` (SCD2), `dim_facility` (SCD2 on country/region, SCD1 on
name/geo corrections), `dim_shipment` (SCD2), `dim_date`.

**Vehicle assignment is an assignment fact, not SCD2 on `dim_vehicle`.**
Assignment is a high-churn relationship; 85,000 vehicles reassigned daily would
bloat the dimension into millions of rows and destroy its join performance.

**Keys.** Deterministic **hash keys** — `SHA1(UPPER(TRIM(natural_key)))` for hubs,
`hash_diff` over concatenated descriptive attributes for satellite change
detection. Hashing at the edge means hubs, links, satellites and dimensions load
in parallel with no sequence lookups and no ordering dependency, which is what
makes this work at streaming speed. Warehouse sequences would serialise the load
and become the bottleneck. Natural keys are retained everywhere for audit.

**Why hybrid fits all three consumers:**

- **Ops** gets a small, clustered current-state fact — no vault joins at query
  time.
- **Finance** gets auditability: the vault is insert-only with full
  `load_dts`/`rec_src` lineage, so a report can be reproduced exactly as it stood
  on any past date and restatements are provable rather than silent.
- **DS** reads satellites and the VARIANT metadata straight off Iceberg — full
  fidelity, no lossy conformance, no warehouse credits.
- **Drift resilience:** a new or renamed field lands in a new satellite or the
  VARIANT column without breaking any existing satellite or mart. That is exactly
  the property the monolithic `shipment_events` table lacked, and why one renamed
  field took the warehouse down for 11 days.

The cost is join complexity and a two-layer transform. I accept it because the
alternative — a single wide table — is what just cost $480K.

---

## Part 3 — Incremental Load & Deduplication

Kinesis is **at-least-once**. I do not claim exactly-once end to end; I make the
*sink* idempotent so the effect is exactly-once.

**Step 1 — bounded read with watermark and late-arrival window:**

```sql
CREATE OR REPLACE TEMPORARY TABLE stg_events AS
WITH bounded AS (
  SELECT *
  FROM   raw.shipment_events_stream
  WHERE  ingest_ts > (SELECT COALESCE(MAX(watermark_ts), '1970-01-01'::TIMESTAMP_NTZ)
                      FROM meta.load_watermark
                      WHERE table_name = 'fact_shipment_events')
    AND  event_ts >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
),
ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id
                               ORDER BY ingest_ts DESC, payload_hash) AS rn
  FROM bounded
)
SELECT * FROM ranked WHERE rn = 1;
```

**Step 2 — partition-pruned MERGE:**

```sql
MERGE INTO gold.fact_shipment_events AS t
USING stg_events AS s
   ON t.event_id   = s.event_id
  AND t.event_date = s.event_date
  AND t.event_date >= DATEADD(hour, -24, CURRENT_TIMESTAMP())::DATE
WHEN MATCHED AND t.payload_hash <> s.payload_hash
  THEN UPDATE SET t.status = s.status, t.payload_hash = s.payload_hash,
                  t.updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED
  THEN INSERT (...) VALUES (...);
```

Without the `event_date` bound, Snowflake scans every micro-partition of a
10.8B-row fact on every micro-batch. With it, the merge touches two date
partitions. **This is the mechanism behind Exhibit B's 78%.** The implementation
measures the ratio on every run — a representative figure is **42.7× fewer rows
scanned** than an equivalent full reload.

**Step 3 — advance the watermark in the same transaction**, so a crash mid-merge
replays rather than skips.

**Deduplication, three layers:**

1. *within batch* — `ROW_NUMBER() OVER (PARTITION BY event_id)`
2. *across batches* — `MERGE ... ON event_id` makes redelivery after a resharding
   or consumer restart a no-op
3. *content changes* — `payload_hash` so genuine corrections update but identical
   redeliveries don't burn write credits

**Spark/Iceberg equivalent for the lake path:**

```python
(spark.readStream.format("kinesis")
   .option("streamName", "swiftlogix-events-raw")
   .option("startingPosition", "TRIM_HORIZON").load()
   .transform(parse_and_validate)
   .withWatermark("event_ts", "24 hours")
   .dropDuplicatesWithinWatermark(["event_id"])   # bounded state
   .writeStream
   .foreachBatch(merge_to_iceberg)                # MERGE with days(event_ts) predicate
   .option("checkpointLocation", "s3://.../ckpt/silver_events")
   .trigger(processingTime="60 seconds").start())
```

`dropDuplicatesWithinWatermark`, not plain `dropDuplicates` — the latter keeps
unbounded state and will eventually OOM the executors at this volume.

**Physical layout:** `PARTITIONED BY (days(event_ts), bucket(16, shipment_id))`;
`write.target-file-size-bytes = 134217728`; daily `rewrite_data_files` compaction
to fix the small-file problem the 60-second micro-batches create;
`expire_snapshots` at 7 days. Snowflake gold clustered on `(event_date,
event_type)` — deliberately not `shipment_id`, whose cardinality would make
automatic reclustering cost more than the scans it saves.

**Beyond 24 hours:** events land in `quarantine.late_arrivals`. A daily job
restates the affected date partition and re-runs the Finance aggregate with an
explicit `restated_at` column and a restatement log — Finance sees an amended
figure, never a silently mutated one.

---

## Part 4 — Schema Drift Failure Response

The `delivery_window.start` → `delivery.window_start` rename is a **contract
failure, not a code failure**, and is handled as one. In the implementation these
controls detect the injected rename in **1.9 seconds**.

### (a) Detect in minutes, not days

- Every source has a versioned JSON Schema in **AWS Glue Schema Registry** with
  FULL compatibility and `"additionalProperties": false` on the delivery block. A
  rename fires two signals at once: a required field missing and an unexpected
  field present. The Firehose Lambda validates every batch, so detection happens
  in the first 60-second buffer.
- CloudWatch custom metrics per source: `schema_violation_rate` and — critically
  — `null_rate` per required column. The 2025 incident's actual signature was a
  column going 100% null, which a null-rate monitor catches in a single batch.
  Alarm at violation rate > 0.5% over 1 minute or null rate >3σ off the 7-day
  seasonal baseline, routed to PagerDuty, not a Slack channel.
- `dbt source freshness` + `dbt test` on bronze every 15 minutes. A **Great
  Expectations** suite on silver with `expect_table_columns_to_match_set`, which
  fails on added *or* renamed columns — something type checks alone miss.
- A **business-metric canary**: hourly count of shipments with a non-null
  delivery window against a 7-day seasonal baseline. This catches drift that is
  type-valid but semantically wrong.
- The partner is a third party, so we cannot put a contract test in their CI.
  Substitute a **synthetic probe**: a scheduled Lambda hitting their sandbox
  every 5 minutes with a known request, validated against the registered schema.
  That converts silent upstream drift into a deterministic, ownable test failure.

### (b) Stop corrupt rows before gold

- **Bronze accepts everything.** No rejections, no exceptions. Discarding the raw
  payload is what would make the eventual 11-day replay impossible.
- **The gate is bronze → silver.** Failing rows go to
  `quarantine.shipment_events_rejected` with `rejection_reason`,
  `expected_schema_version`, `observed_fields` and the full raw payload.
- **Circuit breaker:** if quarantine rate for a source exceeds 5% of a batch, the
  silver → gold promotion aborts and the last-good gold partition keeps serving.
  The real 2025 failure mode was a job that reported success while emitting
  nulls — a pipeline that fails loudly and serves stale-but-correct data beats
  one that succeeds quietly and serves wrong data.
- **Write-Audit-Publish:** gold writes go to an Iceberg branch (or a Snowflake
  zero-copy clone), the expectation suite runs against that branch, and only a
  passing run gets an atomic swap into production.
- What I would **not** do is scatter `COALESCE(delivery_window_start,
  delivery.window_start)` through the transform layer. That turns a one-time
  contract update into permanent undocumented debt and hides the next drift.

### (c) Alert, quarantine, replay

- The alert payload includes a sample offending record and a diff against the
  expected schema. Registry metadata names a schema owner and on-call rotation
  per source.
- Recovery: register **v2** with an explicit field mapping, then replay from
  **bronze S3** — not just the 7-day Kinesis retention, which is why bronze
  exists. The idempotent `MERGE ON event_id` makes replaying 11 days safe with
  zero duplicates. In the implementation, replaying every quarantined record
  produces **0 duplicate rows**, verified by an assertion on every run.
- A `meta.schema_change_log` records every version bump, who approved it, and
  which partitions were restated — which is what Finance needs to sign off the
  restated figures.

Cost of these controls: roughly **$300/month** against a **$480K** incident.

---

## Part 5a — Cost Optimization: Closing the $19K/Month Gap

Exhibit B is unambiguous: the nightly Spark job is 78% of spend (~$47.6K of $61K)
and it is full-table reprocessing. Everything else is rounding.

| Line item | Current | Proposed | Δ |
|---|---|---|---|
| EMR nightly full reprocess | $47,580 | $2,906 | **−$44,674** |
| Snowflake compute | $9,455 | $9,682 | +$227 |
| S3 storage + requests | $1,913 | $512 | −$1,401 |
| Kinesis Data Streams | $0 | $255 | +$255 |
| Firehose + Parquet conversion | $0 | $173 | +$173 |
| Contract validation Lambda | $0 | $18 | +$18 |
| Registry, catalog, monitoring | $1,403 | $1,700 | +$297 |
| **Total** | **$61,000** | **~$15,249** | **−$45,751** |

*(Modelled at the 42.7× reprocess ratio measured by the implementation; run
`make costs` for other assumptions.)*

**Lever 1 — kill full-table reprocessing.** Replace the 2 AM monolith with the
partition-pruned incremental MERGE. Sensitivity matters more than the headline:
at a 5× ratio the conservative case is $42.3K — marginal; at 10× it is $39.9K.
The lever has to deliver roughly an order of magnitude to clear the mandate with
confidence, and the implementation measures 42.7×.

**Lever 2 — file format and layout.** Raw JSON → Parquet + **ZSTD-3**: measured
**5.6× compression** on real generated payloads, ~30% better than Snappy at
comparable read speed. Target 128–256MB files with daily compaction — unmanaged
small files are what make streaming lakes expensive. The real value is the cut in
*scan bytes*, which drives both EMR runtime and Snowflake credits.

**Lever 3 — Snowflake warehouse discipline.** Split one shared warehouse three
ways: Ops on XS multi-cluster with **60-second auto-suspend** (vs the 600s
default), Finance on M for ~20 min/day, DS moved off Snowflake entirely onto Spot
EMR reading Iceberg. Plus `TRANSIENT` staging tables (no Fail-safe storage
charge) and Time Travel capped at 1 day on high-volume raw tables.

**Lever 4 — storage tiering and Spot.** 0–90 days Snowflake native; 90 days–2
years S3 Intelligent-Tiering via external Iceberg; beyond that Glacier Deep
Archive. Spot (~70% off) for batch and backfill **only** — not for the
incremental load path, which sits on the 5-minute SLA where an interrupted
instance costs freshness.

**The trade-off I am consciously accepting:** Snowflake spend *increases* to fund
the real-time dynamic tables. That is the latency-vs-cost purchase, and it is the
right one — it buys the 26-hour → 5-minute improvement the VP mandated and is
funded many times over by Lever 1. More broadly, the design trades operational
complexity (more services, more failure modes) for the two things the business
actually lost money on: freshness and schema resilience. Given that a single
undetected field rename cost $480K — roughly eight months of the entire platform
budget — the ~$300/month for a registry, a validation Lambda and an expectations
suite is the highest-ROI line in the plan.

---

## Part 5b — Most Critical Trade-off

**Schema Flexibility vs. Query Performance (schema-on-read vs. schema-on-write).**

The design resolves it by refusing to pick globally and instead picking per
layer. Bronze is schema-on-read and accepts everything, so no upstream change can
ever cause data loss. Gold is strictly schema-on-write and typed, so the Ops
dashboard hits sub-10s over 90 days. The `metadata` object is the one field that
stays schema-on-read all the way through, absorbing source-specific variation
that has no business meaning yet.

The boundary between the two is the contract gate — and putting a versioned,
monitored contract at exactly that boundary is what converts an 11-day silent
corruption into a 1.9-second alert and a 20-minute bounded backfill.
