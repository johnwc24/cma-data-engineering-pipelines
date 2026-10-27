# Interview guide: Data Engineer (CMA)

How each part of the role maps to something in this repository, with a short way to talk about it
and questions to expect.

## JD requirement → example to talk through

| JD requirement | Example in this repo | 30-second talking point |
|---|---|---|
| Translate STTM, business definitions and acceptance criteria into production code | `docs/source-to-target-mapping.md` → `DealRules`, `DealPipeline` | Every STTM rule is a named rule; the name appears in quarantine records, so analysts can trace a reject back to the mapping line. |
| Hadoop, Hive, HDFS, Spark, Scala | `hadoop-batch/DealIngestionJob`, Hive DDL | Dynamic partition overwrite for idempotent re-runs; `insertInto` column alignment; embedded-metastore tests run the real DDL. |
| BigQuery, Dataproc, GCS, Composer/Airflow | `gcp-dataproc`, `bigquery/`, `airflow/dags/cma_deals_gcp.py` | Spark validates and stages; BigQuery does the SCD2 MERGE in a transaction; ephemeral clusters deleted with `ALL_DONE`. |
| Pub/Sub, Dataflow | `dataflow-beam` | Dead letters with reasons for parse errors *and* failed BigQuery inserts; dedup by event_id; Flex Template with in-place `--update`. |
| Migration and modernisation onto GCP | `HiveToBigQueryMigrationJob`, `cma_hive_to_bq_migration` | Per-partition migration with rows, amounts and an order-independent checksum; one bad partition doesn't block the rest. |
| Select patterns by volume, latency, cost, reliability | `docs/architecture.md` decisions 7–9 | Why Spark + BigQuery SQL, why serverless for small jobs, three streaming options and when to use each. |
| Data modelling, relational and non-relational | BigQuery DDL (partition, cluster, SCD2), Delta, Hive | SCD2 with row hashes, partition expiry on staging, `require_partition_filter` to protect cost. |
| Data quality: completeness, validity, uniqueness, consistency, timeliness, reconciliation, RI | `common/quality`, `common/recon`, `bigquery/dq`, notebook | Row rules quarantine, dataset rules gate; Warn vs Error; evidence written before the gate decides. |
| Lineage, ownership, access, privacy, licensing, retention, permitted use | lineage columns, table options, `VendorFeedJob` | Licence expiry and permitted use enforced in code; licence carried on rows and as table properties. |
| Automated unit, integration and validation testing | every module's `src/test`, CI | All pipelines run locally: BigQuery and Kafka fakes, embedded Hive, H2 for PostgreSQL, DirectRunner for Beam. |
| Git, Jenkins, Ansible, CI/CD, rollback | `Jenkinsfile`, `ansible/`, `.github/workflows` | Approvals per environment, smoke tests after each deploy, automatic rollback on failure, PROD smoke tests read-only. |
| Monitor freshness, failures, DQ exceptions, performance | `cma_stream_health`, smoke tests, runbook | Freshness and DLQ-rate checks, run audit tables, exit codes that tell ops what kind of failure it is. |
| Debug code and data issues, communicate root cause | runbook, reconciliation notebook | The notebook re-derives the numbers independently; the runbook maps each exit code to a first action. |
| Front office, syndication, transaction lifecycle | deal model, league table, break reconciliation | Deal versions and amendments, bank share vs deal size, booking-system status mapping, break ageing. |
| Third-party data sources | `VendorFeedJob` | Manifest checksum and row count, versioned daily snapshots, change summary vs yesterday. |
| Data-science use cases | `PointInTimeFeaturesJob` | As-of joins over SCD2 to avoid look-ahead bias, with tests that prove nothing leaks backwards. |

## Questions to prepare for

**"A daily job failed at 2am. Walk me through what you do."**
Read the exit code first: 3 means the file didn't arrive, so contact the source; 5 means the DQ gate
blocked, so check `dq_results` and the quarantine table for that run; 6 means a reconciliation break,
so stop downstream and investigate; 7 means infrastructure, so check platform status and re-run.
Because writes are idempotent, a re-run needs no clean-up.

**"How do you make a pipeline safe to re-run?"**
Overwrite exactly the partition you own, MERGE on business keys for mutable data, hash-compare in
SCD2 so unchanged rows are no-ops, and keep a batch ledger for streams.

**"How do you know the data is right, not just that the job succeeded?"**
Read-back reconciliation after every write, an independent recompute in the reconciliation
notebook, BigQuery assertions after the MERGE, and evidence tables for audit.

**"How would you migrate a Hive workload to BigQuery?"**
Same transformation code on Dataproc, partition-by-partition data migration with checksums, a
parallel-run period comparing outputs, then switch consumers and decommission. Explain the trade-off
of keeping Spark versus rewriting in BigQuery SQL.

**"Batch, Kafka on Dataproc or Dataflow for a new feed?"**
Latency need, existing producer (Kafka vs Pub/Sub), operational model (managed vs cluster), and team
skills. Cost: Dataflow streaming engine vs an always-on cluster.

**"Late-arriving and amended data?"**
Deal versions (latest wins per day), SCD2 for history, a look-back window for JDBC sources, Delta
MERGE for reference data, and point-in-time joins for features.

**"How do you handle bad records in a stream?"**
Never block the stream for data: validate, dead-letter with reasons in headers or attributes, alert
on DLQ rate, fix at source and replay. Stop only for sink failures, and rely on checkpoints to resume.

## Demo script (10 minutes)

1. README diagram and pipelines table (1 min).
2. `DealRules` → a quarantine row with its reasons → the STTM line it came from (2 min).
3. `DealIngestionJob` control flow: gate, write, read-back reconciliation, audit (2 min).
4. A CI run: 72+ tests, the end-to-end job, and the executed reconciliation notebook artifact (2 min).
5. `Jenkinsfile`: approvals, smoke test, automatic rollback (2 min).
6. One design trade-off from `architecture.md` (1 min).
