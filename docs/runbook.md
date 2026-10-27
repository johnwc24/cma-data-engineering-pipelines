# CMA pipelines runbook

Operational guide for the CMA deal pipelines. Every Spark job logs one summary line on completion
(`... SUCCEEDED run_id=... metrics=...` or `... FAILED ... category=... exit_code=... retryable=...`)
and writes a row to the run audit table, so start there.

## Exit codes

| Code | Category | Retried by Airflow? | Typical cause | First action |
|---|---|---|---|---|
| 0 | success | - | - | - |
| 1 | unexpected | yes | bug or unclassified failure | read the driver stack trace; raise a defect |
| 2 | config | **no** | missing table/key, bad arguments, permission denied | check the environment config, DDL deployment, service-account roles |
| 3 | source missing | **no** | landing file never arrived | confirm with the source team; re-run once it lands |
| 4 | schema | **no** | source file missing contract columns | contact the data owner; do not change the contract without sign-off |
| 5 | data quality | **no** | blocking DQ gate breached | inspect `dq_results` and the quarantine table for the run |
| 6 | reconciliation | **no** | written totals differ from expected | **stop**: do not run downstream; investigate before re-running |
| 7 | transient I/O | yes | HDFS / GCS / BigQuery / network blip, retries exhausted | check platform status; re-run |
| 8 | sink write | depends | target unavailable or rejected the write | check target health and quotas |

Airflow applies this automatically (`FailFast*` operators). A data error fails the task immediately and
alerts, rather than retrying the same bad data.

## Where to look

| Question | Hadoop | GCP |
|---|---|---|
| Did the run succeed, how many rows? | `cma_audit.pipeline_run` | Composer task logs, `cma_audit.sql_assertions` |
| Which DQ rules failed? | `cma_audit.dq_results` (`dataset = deal_curated` is the gate) | `cma_audit.dq_results` |
| Which records were rejected and why? | `cma_quarantine.deal_reject._reject_reasons` | `cma_quarantine.deal_reject._reject_reasons` |
| Stream healthy? | `cma_audit.stream_batch` (last `committed_at`) | `cma_audit.stream_batch`, `cma_stream_health` DAG |
| Dataflow healthy? | - | Dataflow console, `cma_raw.deal_event_dataflow` freshness, counter `cma/dead_lettered_events` |
| Do the numbers add up end to end? | run the reconciliation notebook | run the reconciliation notebook |

## Common procedures

**Re-run a business date.** All batch writes are idempotent (partition overwrite, SCD2 no-op on unchanged
rows, Delta `replaceWhere`). Clear the Airflow task, or run the job with the same `--business-date`.

**Fix and replay rejected records.** Rejected batch rows keep every original value (and `_raw_record` for
unparseable lines). Once the source is corrected, re-deliver the file and re-run the date. For streams,
messages on the dead-letter topic carry `reject_reasons`; republish corrected messages to the main topic.

**Reconciliation break (exit 6).** Downstream tasks are blocked by design. Run the reconciliation notebook
for the date, compare the source and target totals it reports, check for concurrent writers or partial
writes, then re-run the date. Never mark the task as success to unblock.

**Streaming job restart.** Jobs resume from their checkpoint (HDFS/GCS). A replayed micro-batch is skipped by
the batch ledger, so restarts never double-load. If the checkpoint is lost, restart with
`--conf kafka.starting-offsets=<json offsets>` from the last committed batch in `stream_batch`.

**Kafka data loss error.** `failOnDataLoss=true` stops the query when retention deleted unread offsets.
Agree the gap with the source team, back-fill from the batch extract, then restart from `latest`.

**Rollback.** Jenkins rolls back automatically when a deployment or smoke test fails. Manually:
`ansible-playbook -i ansible/inventories/<env> ansible/playbooks/rollback.yml [-e target=hadoop|gcp]`,
or re-run Jenkins with `ROLLBACK_TO=<version>`.

## Smoke tests

`ansible-playbook -i ansible/inventories/<env> ansible/playbooks/smoke_test.yml` checks:

* deployed versions, Hive/BigQuery objects, Composer DAG import errors and registration
* streaming jobs running (YARN, Dataproc, Dataflow) and recent batch commits
* last daily run succeeded and is fresh; last BigQuery assertions passed
* BigQuery pipeline SQL compiles against the environment (dry run)
* DEV/UAT only: write-free `--dry-run` of the Spark jobs on yesterday's data

PROD smoke tests are read-only (`smoke_read_only: true`).

## Reconciliation notebook

`notebooks/pipeline_reconciliation.ipynb` re-derives the pipeline's numbers independently from the source
files and target tables. Run interactively on Dataproc Jupyter / Vertex AI Workbench (GCP) or JupyterHub on
YARN (Hadoop), or headless:

```bash
papermill notebooks/pipeline_reconciliation.ipynb out.ipynb \
  -p PLATFORM gcp -p ENV prod -p BUSINESS_DATE 2026-10-26 -p WRITE_RESULTS True
```

It raises at the end if any check breaks (`FAIL_ON_BREAK`), so it can gate downstream work.
