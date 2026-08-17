# Architecture Decision Record

Decisions that were genuinely contested, with the reasoning and what would
change the answer. Decisions with an obvious right answer are not listed.

---

## ADR-1: Kinesis Data Streams over MSK

**Decision.** Kinesis Data Streams + Firehose.

**Why.** At 120M events/day the workload needs a managed elastic pipe, not a
Kafka cluster to operate. Firehose's native Parquet conversion and direct
Snowflake delivery remove two components that would otherwise need building,
monitoring and on-calling. Provisioned shards (8) rather than on-demand: at a
steady 1,389 events/sec average, on-demand is priced for unknown spikes and costs
roughly 2.5× more.

**What would change it.** Needing Kafka Streams, log compaction, >7-day retention
as a system of record, or an existing Kafka consumer ecosystem. Any of those and
MSK wins outright.

---

## ADR-2: Hybrid Data Vault + Star, not one or the other

**Decision.** Insert-only raw vault feeding dimensional marts.

**Why.** Three consumers want incompatible things. Ops wants current state in
under a second. Finance wants provable history and reproducible restatements. DS
wants full-fidelity raw events including untyped metadata. A pure star forces
Finance to accept overwrites; a pure vault forces Ops to join six tables per
dashboard load.

**Cost accepted.** Join complexity and a two-layer transform, which is real
ongoing engineering overhead.

**What would change it.** A single consumer profile, or no audit/restatement
requirement — then a plain star, and the vault would be pure overhead.

---

## ADR-3: Hash keys, not warehouse sequences

**Decision.** `SHA1(UPPER(TRIM(natural_key)))` computed at ingest.

**Why.** Sequences require a lookup and impose load ordering between hubs, links
and satellites. At 4,500 events/sec that serialises the load and becomes the
bottleneck. Deterministic hashing lets every table load in parallel with no
coordination.

**Cost accepted.** 40-byte keys instead of 8-byte integers, so larger joins and
more storage. At this volume the parallelism is worth more than the bytes.

---

## ADR-4: Vehicle assignment as a fact, not SCD2 on the dimension

**Decision.** `fact_vehicle_assignment` with `effective_from`/`effective_to`.

**Why.** Assignment is a high-churn relationship. Modelling it as SCD2 on
`dim_vehicle` means 85,000 new dimension rows per day, which within a year turns
a 85K-row dimension into a multi-million-row table and destroys the join
performance that makes it a dimension in the first place.

---

## ADR-5: Contract at the bronze→silver boundary, not at ingest

**Decision.** Bronze accepts everything. Validation gates the promotion to
silver.

**Why.** Rejecting at ingest means the raw payload is never durably stored, and a
replay has nothing to replay from. The 11-day incident is only recoverable
because bronze kept records that failed the contract.

**Consequence.** Bronze contains known-bad data by design. That is the point, and
it is why nothing queries bronze directly.

---

## ADR-6: Circuit breaker over best-effort promotion

**Decision.** Above 5% quarantine for a source, abort promotion entirely.

**Why.** The 2025 failure mode was a job that reported success while emitting
nulls. Partial publication is worse than no publication, because it is
indistinguishable from health. Serving stale-but-correct data and paging a human
beats serving fresh-but-wrong data silently.

**Cost accepted.** A noisy but harmless upstream change can stall the pipeline
until someone acknowledges it. That is the intended trade.

**Tuning note.** 5% is a starting point, not a law. It should be set per source
from the observed baseline violation rate — a source that normally runs 2% dirty
needs a different threshold from one that normally runs 0%.

---

## ADR-7: Explicit field mappings, never COALESCE

**Decision.** Drift is remediated by registering a new schema version with a
declared field mapping.

**Why.** `COALESCE(delivery_window_start, delivery.window_start)` scattered
through the transform layer fixes today's incident and hides tomorrow's. It also
leaves no record of what changed, when, or who approved it. The mapping in the
registry is versioned, reviewable, and logged to `meta.schema_change_log` for the
restatement Finance has to sign off.

---

## ADR-8: Idempotent sink, not exactly-once delivery

**Decision.** Accept at-least-once from Kinesis; dedupe at the sink on
`event_id`.

**Why.** Exactly-once end-to-end across a producer, a stream, a lake and a
warehouse is achievable only with heavy coordination that would not survive a
consumer restart or a resharding. Idempotency at the sink gets the same
observable outcome with none of the coupling, and it is what makes an 11-day
replay safe to run twice.

**Verification.** `make run` deliberately crashes a consumer before checkpointing
and asserts zero duplicate rows in gold.

---

## ADR-9: Spot for batch only, never for the incremental load path

**Decision.** EMR Spot for DS feature engineering and backfill. On-demand for the
streaming incremental load.

**Why.** The incremental path sits on the 5-minute SLA. An interrupted Spot
instance there costs freshness, which is the entire mandate. Applying a blanket
Spot discount across everything would also make the cost model dishonest — it
would let the spend line clear budget even when the incremental lever delivers
nothing, dressing a pricing trick up as a structural saving. The model in
`costs.py` deliberately does not do this, and there is a test pinning that
behaviour.

---

## Known gaps

Things a production build needs that this implementation does not have:

- **No dimension SCD2 loader.** The dimension tables exist with `valid_from` /
  `valid_to` / `is_current` columns, but only the facts and vault are populated.
- **No compaction job.** The small-file problem is described and its cost
  modelled, but `rewrite_data_files` is not implemented — DuckDB reads the
  Parquet files directly.
- **Single-node.** Sharding is real, but every consumer runs in one process. Real
  KCL lease-balancing across workers is not modelled.
- **Alert routing is console-only.** No PagerDuty integration; alerts land in
  `meta.alerts` and the live console.
- **The synthetic partner probe is described but not implemented** — there is no
  sandbox endpoint to probe.
