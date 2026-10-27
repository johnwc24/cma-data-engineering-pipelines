# Architecture and design decisions

This document explains *why* the pipelines are built the way they are. Each decision lists the
alternative that was considered and the trade-off accepted.

## Layers

| Layer | Hadoop | Google Cloud | Contents |
|---|---|---|---|
| Landing | HDFS `/data/cma/<env>/landing` | GCS `cma-data-<env>-landing` | files exactly as delivered, partitioned by `business_date` |
| Raw / bronze | Hive `cma_raw` | BigQuery `cma_raw` | every event as received, with Kafka / Pub/Sub coordinates |
| Staging | - | BigQuery `cma_staging` | validated, enriched day loads awaiting MERGE (35-day expiry) |
| Curated / gold | Hive `cma_curated`, Delta | BigQuery `cma_curated` | governed data products: deals (SCD2), league table, party, ratings, features |
| Quarantine | Hive `cma_quarantine` | BigQuery `cma_quarantine` | rejected rows with reasons (90-day retention) |
| Audit | Hive `cma_audit` | BigQuery `cma_audit` | run, DQ, assertion, migration, stream-ledger and reconciliation evidence |

## Decisions

**1. One transformation library shared by every engine.**
`DealPipeline` and `DealRules` live in `common`, and each job is a thin read → `process` → write
wrapper. *Alternative:* per-platform implementations. *Why not:* business rules would drift between
Hadoop and GCP, which is exactly the inconsistency a migration programme must avoid. Beam cannot use
Spark Columns, so `DealRecordValidator` re-implements the rules record-at-a-time, and a parity test
runs both on the same inputs and fails the build on any difference.

**2. Read raw text as strings, type afterwards.**
A typed read turns `"abc"` into NULL without a trace. Reading strings, then casting with explicit
rules, lets every bad value be quarantined with the rule it broke and the original value preserved.
*Cost:* one extra projection, which is negligible next to I/O.

**3. Quarantine, don't drop; gate, don't guess.**
Row-level problems are quarantined and the run continues. Dataset-level problems (empty feed,
duplicate keys, missing FX coverage) are a gate: Error severity blocks the write, Warn records and
alerts. Rejects and DQ results are written *before* the gate decision, so a blocked run still leaves
the evidence the data owner needs.

**4. Exit codes as the contract between jobs and the scheduler.**
`PipelineError` gives each failure class an exit code and a `retryable` flag. Airflow retries
infrastructure failures (7, 8, 1) with backoff but fails data failures (2–6) immediately with
`AirflowFailException`. *Why:* retrying bad data three times only delays the alert by an hour.

**5. Idempotent by construction.**
Day partitions are overwritten (`partitionOverwriteMode=dynamic`, Delta `replaceWhere`, BigQuery
`datePartition`). SCD2 compares row hashes, so an unchanged day is a no-op. Streams keep a batch
ledger so a replayed micro-batch is skipped. JDBC loads MERGE on the key. Any job can be re-run
for any date without clean-up.

**6. Reconcile by reading back.**
Every write is followed by a read of what actually landed, compared against what the job intended
to write (rows, amounts, and for migration an order-independent content checksum). This catches
silent truncation, connector bugs and concurrent writers, which a job's own counters cannot.

**7. Spark for parsing and enrichment, BigQuery SQL for set-based curation.**
On GCP, Spark on Dataproc loads `cma_staging`; the SCD2 MERGE, assertions and the league table run
as BigQuery scripts. *Why:* BigQuery executes large MERGEs and aggregations far more cheaply than
moving data back into Spark, and SQL is reviewable by analysts. Scripts run in transactions and
roll back and re-raise on error.

**8. Ephemeral Dataproc clusters for batch, a long-running cluster or Dataflow for streams.**
Batch DAGs create a cluster per run and delete it with `trigger_rule=ALL_DONE` (plus
`idle_delete_ttl` as a safety net), so a failure never leaves compute running. New data-product jobs
use Dataproc Serverless, removing cluster management altogether.

**9. Three streaming options, chosen per source.**
On-prem Kafka → Spark on YARN (where the data and the Kafka estate already are);
Managed Kafka → Spark on Dataproc (lift-and-shift of the same code); Pub/Sub → Dataflow (managed,
autoscaling, exactly what new GCP-native producers use). All three share the rules, write rejects to
a dead-letter topic with reasons, and never stop for bad data, only for sink failures.

**10. Beam Java SDK from Scala rather than Scio.**
Scio is more idiomatic, but the Java SDK is what most bank Dataflow estates standardise on, has the
widest operator familiarity, and pins to Beam releases directly. The code stays in Scala so the team
keeps one language.

**11. Delta Lake for mutable reference data on the lake.**
Party master (MERGE), vendor snapshots (`replaceWhere` per day, time travel for audits) and break
history (ageing via MERGE + UPDATE) need ACID updates that plain Parquet cannot give. Immutable,
append-only data stays Parquet.

**12. Incremental JDBC: fixed upper bound, look-back, verify then advance.**
The upper bound is read at the start, so rows committed mid-run are not half-read. A look-back
window re-reads recent rows to catch transactions that committed with an earlier timestamp. The
watermark only moves after the target is verified, so a failed run just re-reads.

**13. Licensing is enforced in code, not documentation.**
Vendor feeds declare their licence (expiry, permitted uses, redistribution). An expired licence or a
use outside it stops the load before parsing; loaded data carries the licence in every row and as
Delta table properties for catalog and governance tooling.

**14. Point-in-time correctness for features.**
Features join to the SCD2 version valid on the as-of date and only to events dated on or before it.
Tests assert that later amendments, later deals and later ratings never leak backwards, which
prevents look-ahead bias in models.

## Lineage and controls

Every curated row carries `run_id` (the Airflow run), `source_system` and, for files, `_source_file`.
Tables carry owner and classification in their options or properties. Credentials come from the
environment or mounted secret files, never configuration. CI checks compile, test, lint, render SQL
and execute the pipeline end to end; Jenkins adds a BigQuery dry run against the real environment,
approvals, smoke tests after each deployment and automatic rollback.
