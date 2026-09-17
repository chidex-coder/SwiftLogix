# SwiftLogix Real-Time Logistics Data Platform

A working reference implementation of a streaming data platform for a global
logistics company processing **120M events/day** with peak bursts of **4,500
events/sec**, built against three hard constraints:

| Constraint | Before | Target | This build |
|---|---|---|---|
| End-to-end latency | ~26 hours (nightly batch) | < 5 minutes | ~3 min p99 modelled, seconds in simulation |
| Warehouse query SLA | — | < 10s over 90 days | pre-aggregated Ops mart |
| Monthly cloud spend | $61,000 | < $42,000 | ~$15–19K modelled, measured from a real run |
| Schema drift blast radius | 11 days undetected, $480K | minutes | **1.9 seconds** measured |

Everything here runs locally. There is no AWS account, no Snowflake trial and no
Docker daemon involved — the cloud services are replaced with faithful local
analogues that preserve the properties the design has to survive (sharding,
at-least-once delivery, buffered batch validation, partition pruning), so the
correctness claims can actually be tested rather than asserted.

---

## Quick start

```bash
python3.12 -m venv .venv && ./.venv/bin/pip install -r requirements.txt
```

```bash
make run
```

That runs the live pipeline for 90 seconds and drives two scripted incidents.
You will watch, in real time: a partner API silently rename a field at a random
moment, the contract layer catch it, the circuit breaker halt promotion to gold,
an engineer register a new schema version, and a replay recover every
quarantined record with zero duplicates. Then, a few seconds after everyone
relaxes, a *second* partner moves a *different* field that the new contract does
not cover, and the same controls have to catch it again.

The drift times are drawn from a seeded RNG, so no two seeds produce the same
timeline but any seed reproduces its own. `--seed`, `--drift-at` and
`--second-drift-at` pin them when you need a fixed run.

```bash
make test
```

---

## What actually happens during a run

```
t+000.0s  contract v1 active; 8 shards; target 900 eps
t+000.0s  warehouse ready (native MERGE INTO: yes)
t+005.9s  chaos: consumer crashed before checkpoint - 400 records redelivered
t+011.7s  UPSTREAM: partner_courier_api renamed delivery_window.start
          -> delivery.window_start (no notice, no version bump)
t+013.2s  [CRITICAL] contract_violation: schema violation rate 10.3%
t+013.2s  [CRITICAL] circuit_breaker: promotion to gold ABORTED - quarantine
          rate 10.3% exceeds the 5% threshold; last good partition still serving
t+029.9s  on-call registered contract v2 (compatibility=BREAKING, 2 field
          mappings) - live traffic now conforms
t+034.8s  replay: 2,633 quarantined records re-driven, 2,633 recovered,
          2,625 merged into gold, 0 duplicates created
t+043.3s  UPSTREAM: driver_mobile_app release 4.12 moved geo.lat/lon
          -> position.latitude/longitude (no changelog entry) - a different
          partner, a different field, and contract v2 does not cover it
t+044.9s  [CRITICAL] contract_violation: schema violation rate 6.8%
t+044.9s  [CRITICAL] circuit_breaker: promotion to gold ABORTED - quarantine
          rate 6.8% exceeds the 5% threshold; last good partition still serving
t+061.4s  on-call registered contract v3 (compatibility=BREAKING, 2 field
          mappings) - live traffic now conforms
t+066.3s  replay: 1,795 quarantined records re-driven, 1,795 recovered,
          1,795 merged into gold, 0 duplicates created
```

(One run with the default seed; the drift seconds move with the seed.) Gold ends
the run with three contract versions coexisting — `v1=25,701 · v2=6,545 ·
v3=3,194` — every one of them mapped back to the same canonical columns, so no
downstream query ever learns that either partner changed.

The real incident took **11 days** to notice. Here it takes **1.9 seconds**, and
the difference is not cleverness — it is that the contract is a registered,
versioned artifact with something watching it, rather than an assumption baked
into a Spark job.

---

## Architecture

```
producers            ingest + contract gate          storage            serving
─────────            ──────────────────────          ───────            ───────
85k vehicles  ──┐
240 hubs      ──┼─▶ Kinesis Data Streams ──▶ Firehose ──▶ bronze/  ──┬─▶ Ops mart      < 5 min
partner API   ──┘   8 shards, 7d retention   + validate   (Parquet   │   (current state)
                    partition key: vehicle     Lambda      + ZSTD)   │
                                                  │                  ├─▶ Finance mart  daily
                                                  └──▶ quarantine/   │   (restatement-aware)
                                                       + raw payload │
                                                                     └─▶ DS / raw vault
                                                                         (atomic grain)
```

**Ingestion.** 8 provisioned shards. Peak 4,500 eps ÷ 1,000 records/sec per shard
= 5 shards floor, 8 gives ~78% headroom. Provisioned beats on-demand at a steady
1,389 eps average — on-demand is priced for unknown spikes. Partition key is
`vehicle_id`/`hub_code`: high cardinality, so no hot shard (there is a test for
this).

**Contract gate.** Firehose hands each *buffered batch* to a validation Lambda.
Batching is an architectural and a commercial decision: validating 3.6B records
individually costs ~$720/month in invocations; validating ~1.2M buffered batches
costs ~$40.

**Storage.** Bronze accepts everything and never rejects a record for being
unfamiliar. Quarantine keeps the full raw payload. Losing the payload is what
turns a 20-minute contract fix into unrecoverable data loss, because a replay
then has nothing to replay from.

**Serving.** Three consumers, three isolated compute profiles — which is a
performance *and* a cost decision:

| Consumer | Compute | Pattern | Why |
|---|---|---|---|
| Ops dashboard | XS multi-cluster, 60s auto-suspend | streaming → dynamic table | 5-min SLA; queries a pre-aggregated current-state table, not the 10.8B-row fact |
| Finance | M warehouse, ~20 min/day | daily batch | daily grain; batch is cheaper and settles late arrivals first |
| Data Science | EMR Serverless on Spot, reads Iceberg | batch, no warehouse | full-history scans on Spot cost ~an order of magnitude less than warehouse credits, and DS never contends with Ops |

---

## Data model: hybrid vault + star

**Raw Vault** (insert-only, fully lineaged) → `raw.hub_*`, `raw.lnk_*`, `raw.sat_*`
**Star marts** (query-optimised) → `gold.fact_*`, `gold.dim_*`

Surrogate keys are **deterministic hashes computed at ingest**, not warehouse
sequences. Sequences would serialise the load and become the bottleneck at 4,500
events/sec; hash keys let hubs, links and satellites load in parallel with no
lookup and no ordering dependency.

Grains, stated explicitly:

| Table | Grain |
|---|---|
| `gold.fact_shipment_events` | one row per physical event |
| `gold.fact_shipment_status_current` | one row per shipment (accumulating snapshot) |
| `gold.fact_daily_shipment_recon` | shipment × day × hub |
| `gold.fact_vehicle_assignment` | shipment × vehicle × effective window |

Vehicle-to-shipment assignment is an **assignment fact with an effective
window**, not SCD2 on `dim_vehicle`. 85,000 daily reassignments would bloat the
dimension into millions of rows and destroy its join performance.

The hybrid earns its complexity by serving three consumers that want
incompatible things: Ops wants current state fast, Finance wants provable
history and reproducible restatements, DS wants full-fidelity raw events
including the untyped `metadata` blob. The cost is join complexity and a
two-layer transform. The alternative — one wide table — is exactly what cost
$480K.

---

## The two things that make or break this pipeline

### 1. The `event_date` predicate

```sql
MERGE INTO gold.fact_shipment_events AS t
USING stg_events AS s
   ON t.event_id = s.event_id
  AND t.event_date = s.event_date        -- ← this line
  AND t.event_date >= (SELECT min(event_date) FROM stg_events)
```

Without it, the engine scans every micro-partition of a 10.8B-row fact on every
micro-batch. With it, the merge touches two date partitions. That single
predicate is the mechanism behind Exhibit B's finding that the nightly job burns
78% of platform spend on full-table reprocessing.

A run reports the ratio it actually measured:

```
Measured on this run: 42.7x fewer rows scanned than a full nightly reload,
5.6x compression from Parquet+ZSTD
```

### 2. Idempotency

Kinesis is **at-least-once**. This codebase does not claim exactly-once
end-to-end; it makes the *sink* idempotent so the effect is exactly-once.
Deduplication happens in three layers:

1. **within batch** — `ROW_NUMBER() OVER (PARTITION BY event_id)`
2. **across batches** — `MERGE ... ON event_id` makes redelivery a no-op
3. **content changes** — `payload_hash` so genuine corrections update but
   identical redeliveries don't burn write credits

This is why an 11-day replay is safe to run twice. `make run` deliberately
crashes a consumer before checkpointing to prove it.

---

## Reliability controls

| Control | Catches |
|---|---|
| Schema registry, `additionalProperties: false` | the rename itself, in one buffer window |
| Null-rate monitor on business-critical columns | drift that is type-valid but semantically wrong |
| Business canary vs rolling seasonal baseline | everything the first two miss |
| Freshness monitor | a stalled pipeline (guards the 5-min SLA directly) |
| Write-Audit-Publish | stops a bad batch becoming visible at all |
| Circuit breaker at 5% quarantine | halts promotion; last-good partition keeps serving |
| Alert cooldown | one page per incident, not 400 |

Note what is deliberately **absent**: no `COALESCE(delivery_window_start,
delivery.window_start)` anywhere. That converts a one-time contract update into
permanent undocumented debt and hides the next drift completely. The contract
belongs in the registry, versioned, with an explicit field mapping — and the
second incident is the proof: a v2 written as COALESCE would have done nothing
for the driver app's `position` move, whereas v3 is one more registered mapping.

The `metadata` object is the one place schema-on-read applies — it is `additionalProperties: true`
on purpose, so a partner adding a field is a non-event rather than an incident.

---

## Cost model

Driven by measurements taken during an actual run (bytes landed, rows scanned
incrementally vs. what a full reload would have touched), then extrapolated to
production volume:

```bash
make costs
```

Sensitivity to the one lever that matters:

| Reprocess ratio | Modelled | Conservative (half the saving) | Verdict |
|---|---|---|---|
| 1× (no change) | $61,723 | $61,361 | **over** |
| 5× | $23,659 | $42,329 | under |
| 10× | $18,901 | $39,950 | under |
| 43× (measured) | $15,249 | $38,125 | under |

The model deliberately does **not** apply the Spot discount to the incremental
load path — that path sits on the 5-minute SLA, where an interrupted instance
costs freshness. Spot applies only to workloads that genuinely tolerate
interruption (DS feature engineering, backfill). Modelling it the other way
round would let the line clear budget even when the incremental lever delivers
nothing, which would make a pricing trick look like a structural saving.

Snowflake spend *rises* by design — that is the latency-vs-cost purchase that
buys 26 hours → 5 minutes, and it is funded many times over by killing the full
reprocess.

---

## Commands

```bash
make run          # 90s live pipeline with two scripted drift incidents (random timing)
make run-long     # 3 minutes at higher throughput
make test         # 31 tests
make query        # consumer-facing analytics against the loaded warehouse
make costs        # cost model only
make clean        # wipe generated lake + warehouse
```

Direct invocation:

```bash
PYTHONPATH=src ./.venv/bin/python -m swiftlogix.cli run --duration 120 --eps 2000 --fresh
PYTHONPATH=src ./.venv/bin/python -m swiftlogix.cli run --seed 7 --fresh                    # a different timeline, reproducibly
PYTHONPATH=src ./.venv/bin/python -m swiftlogix.cli run --drift-at 20 --second-drift-at 55  # pinned
```

---

## Layout

```
contracts/          versioned JSON Schema contracts (v1; v2 and v3 each with field mappings)
sql/                warehouse DDL — raw vault, star marts, operational metadata
src/swiftlogix/
  generator.py      synthetic fleet: lifecycle, duplicates, late arrivals, per-partner drift
  incidents.py      the two scripted incidents and their seeded random timing
  stream.py         Kinesis analogue: shards, checkpoints, replay, throttling
  registry.py       schema registry, compatibility checks, canonical mapping
  ingest.py         Firehose + validation Lambda → bronze / quarantine
  warehouse.py      dedup, watermark, WAP audit, MERGE, vault + mart loads
  monitors.py       contract, null-rate, freshness, canary, circuit breaker
  replay.py         remediation + bounded backfill from quarantine
  costs.py          measurement-driven cost model
  cli.py            three-thread orchestrator + live console
tests/              21 tests pinning the claims above
docs/               architecture notes and the written case-study responses
```

## Simulation boundaries

Stated plainly, because a reference implementation that overclaims is worse than
none:

- **Local analogues, not the real services.** The stream reproduces sharding,
  checkpointing, at-least-once redelivery and throttling — not Kinesis's network
  behaviour, resharding or IAM.
- **DuckDB stands in for Snowflake.** MERGE semantics, partition-pruning
  predicates and upsert behaviour carry over; micro-partition pruning,
  clustering depth, warehouse concurrency scaling and result caching do not.
- **Volume is scaled down.** Ratios (peak/average, duplicate rate, late-arrival
  rate, drift share) are preserved so the observed reprocess ratio extrapolates
  honestly; absolute throughput does not.
- **Cost figures are modelled** from published list prices plus measured ratios.
  They are a defensible estimate, not a quote.
