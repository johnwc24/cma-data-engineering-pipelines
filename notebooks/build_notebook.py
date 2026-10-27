"""Builds pipeline_reconciliation.ipynb from the cell sources below.

Keeping the notebook source in a .py file makes code review and diffs readable;
run `python notebooks/build_notebook.py` after editing and commit both files.
"""
import json
from pathlib import Path

CELLS = []


def md(text):
    CELLS.append({"cell_type": "markdown", "metadata": {}, "source": text.strip("\n")})


def code(text, tags=None):
    CELLS.append({
        "cell_type": "code", "execution_count": None, "outputs": [],
        "metadata": {"tags": tags} if tags else {}, "source": text.strip("\n"),
    })


md("""
# CMA pipeline reconciliation

Independently re-derives the numbers the deal pipelines report and checks that every record is
accounted for, end to end, for one business date. Read-only unless `WRITE_RESULTS = True`.

| Platform | Where it runs | What it reads |
|---|---|---|
| `gcp` | Dataproc Jupyter / Vertex AI Workbench / Dataproc Serverless (papermill) | GCS landing + BigQuery (staging, curated SCD2, audit, stream ledger, migration log) |
| `hadoop` | On-prem JupyterHub with PySpark on YARN | HDFS landing + Hive (curated, quarantine, audit, stream ledger) |
| `local` | Laptop / CI, after a local run of `DealIngestionJob --env local` | `data/sample` + `target/local-output` |

**Checks**

1. Landing file rows = rows the pipeline says it read
2. Every input row is loaded, rejected or superseded (nothing silently dropped)
3. Rows the pipeline says it loaded / rejected = rows actually in the curated / quarantine tables
4. One row per deal in the curated partition
5. Curated amounts = amounts in the source files for the loaded versions (independent recompute)
6. USD amounts = deal amount x FX rate
7. The curated DQ gate had no blocking failures
8. GCP: staging = current SCD2 rows (count and USD value), SQL assertions passed
9. Streaming: events in the batch ledger = raw events landed; migration partitions all reconciled

Run headless with papermill, e.g.

```
papermill pipeline_reconciliation.ipynb out.ipynb -p PLATFORM gcp -p ENV prod -p BUSINESS_DATE 2026-10-26
```
""")

code("""
# Parameters (papermill overrides these)
PLATFORM = "local"            # local | hadoop | gcp
ENV = "dev"                   # dev | uat | prod
BUSINESS_DATE = "2026-10-26"
REPO_ROOT = ".."              # local only: repo checkout containing data/ and target/
GCP_PROJECT = None            # gcp: defaults to cma-data-<ENV>
AMOUNT_TOLERANCE = 0.01
WRITE_RESULTS = False         # append results to the audit recon table
FAIL_ON_BREAK = True          # raise at the end if any check breaks (makes papermill / CI fail)
""", tags=["parameters"])

code("""
import os
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from decimal import Decimal

from pyspark.sql import SparkSession, DataFrame, functions as F

assert PLATFORM in ("local", "hadoop", "gcp"), f"unknown PLATFORM {PLATFORM}"
GCP_PROJECT = GCP_PROJECT or f"cma-data-{ENV}"
HIVE_SUFFIX = "" if ENV == "prod" else f"_{ENV}"
REPO_ROOT = os.path.abspath(REPO_ROOT)

builder = SparkSession.builder.appName(f"cma-reconciliation-{PLATFORM}-{ENV}-{BUSINESS_DATE}")
if PLATFORM == "local":
    builder = builder.master("local[*]").config("spark.sql.shuffle.partitions", "4").config("spark.ui.enabled", "false")
elif PLATFORM == "hadoop":
    builder = builder.enableHiveSupport()  # master/queue come from spark-defaults on the edge node (YARN)
elif PLATFORM == "gcp":
    builder = builder.config("viewsEnabled", "true").config("materializationDataset", "cma_staging")
spark = builder.config("spark.sql.session.timeZone", "UTC").getOrCreate()
print(f"Spark {spark.version} | platform={PLATFORM} env={ENV} business_date={BUSINESS_DATE}")
""")

md("## Data access\nOne small adapter per platform; the checks below never care where the data lives.")

code("""
bd = F.lit(BUSINESS_DATE).cast("date")


class Sources:
    \"\"\"Logical datasets -> DataFrames for the selected platform. Missing datasets return None.\"\"\"

    def landing_lines(self) -> DataFrame:
        root = {
            "local": f"{REPO_ROOT}/data/sample/landing/deals",
            "hadoop": f"hdfs:///data/cma/{ENV}/landing/deals",
            "gcp": f"gs://cma-data-{ENV}-landing/deals",
        }[PLATFORM]
        return spark.read.text(f"{root}/business_date={BUSINESS_DATE}")

    def landing_csv(self) -> DataFrame:
        root = self.landing_lines().inputFiles()[0].rsplit("/", 1)[0]
        return spark.read.option("header", "true").csv(root)  # all strings: an independent read

    def _local(self, name):
        path = f"{REPO_ROOT}/target/local-output/{name}"
        return spark.read.parquet(path) if os.path.exists(path) else None

    def _hive(self, db, table):
        name = f"{db}{HIVE_SUFFIX}.{table}"
        return spark.table(name) if spark.catalog.tableExists(name) else None

    def _bq(self, dataset, table, filter_=None):
        r = spark.read.format("bigquery").option("table", f"{GCP_PROJECT}.{dataset}.{table}")
        return (r.option("filter", filter_) if filter_ else r).load()

    def curated(self):
        df = {"local": lambda: self._local("curated/deal"),
              "hadoop": lambda: self._hive("cma_curated", "deal"),
              "gcp": lambda: self._bq("cma_staging", "deal_stg", f"business_date = DATE '{BUSINESS_DATE}'")}[PLATFORM]()
        return df.filter(F.col("business_date") == bd) if df is not None else None

    def rejects(self):
        df = {"local": lambda: self._local("quarantine/deal_reject"),
              "hadoop": lambda: self._hive("cma_quarantine", "deal_reject"),
              "gcp": lambda: self._bq("cma_quarantine", "deal_reject", f"business_date = DATE '{BUSINESS_DATE}'")}[PLATFORM]()
        if df is None:
            return None
        df = df.filter(F.col("business_date") == bd)
        # streamed rejects share the table; the batch reconciliation only counts the batch feed
        return df.filter(F.col("source_system") != "KAFKA") if "source_system" in df.columns else df

    def pipeline_run(self):
        if PLATFORM == "gcp":
            return None  # on GCP the run evidence is the DQ / assertion tables
        df = self._local("audit/pipeline_run") if PLATFORM == "local" else self._hive("cma_audit", "pipeline_run")
        return df.filter(F.col("business_date") == bd) if df is not None else None

    def dq_results(self):
        df = {"local": lambda: self._local("audit/dq_results"),
              "hadoop": lambda: self._hive("cma_audit", "dq_results"),
              "gcp": lambda: self._bq("cma_audit", "dq_results", f"business_date = DATE '{BUSINESS_DATE}'")}[PLATFORM]()
        return df.filter(F.col("business_date") == bd) if df is not None else None

    def fx_rates(self):
        if PLATFORM == "local":
            return spark.read.option("header", "true").csv(f"{REPO_ROOT}/data/sample/reference/fx_rates.csv")
        return self._hive("cma_ref", "fx_rate") if PLATFORM == "hadoop" else self._bq("cma_ref", "fx_rate")

    # streaming + GCP-only datasets
    def stream_ledger(self):
        if PLATFORM == "gcp":
            return self._bq("cma_audit", "stream_batch")
        return self._hive("cma_audit", "stream_batch") if PLATFORM == "hadoop" else None

    def stream_raw(self):
        if PLATFORM == "gcp":
            return self._bq("cma_raw", "deal_event_stream", f"ingest_date = DATE '{BUSINESS_DATE}'").withColumnRenamed("ingest_date", "business_date")
        return self._hive("cma_raw", "deal_event") if PLATFORM == "hadoop" else None

    def deal_current(self):
        return self._bq("cma_curated", "deal_current") if PLATFORM == "gcp" else None

    def sql_assertions(self):
        return self._bq("cma_audit", "sql_assertions", f"business_date = DATE '{BUSINESS_DATE}'") if PLATFORM == "gcp" else None

    def migration_log(self):
        return self._bq("cma_audit", "migration_log") if PLATFORM == "gcp" else None


src = Sources()
""")

md("## Check framework")

code("""
@dataclass
class Check:
    check: str
    source: str
    target: str
    difference: str
    status: str   # PASS | BREAK | SKIPPED
    detail: str = ""


results: list[Check] = []


def reconcile(name, source, target, tolerance=0, detail=""):
    s, t = Decimal(str(source)), Decimal(str(target))
    ok = abs(s - t) <= Decimal(str(tolerance))
    results.append(Check(name, str(s), str(t), str(s - t), "PASS" if ok else "BREAK", detail))


def skip(name, reason):
    results.append(Check(name, "-", "-", "-", "SKIPPED", reason))


def scalar(df, expr):
    return df.agg(expr).first()[0] or 0
""")

md("## 1-4. Row accounting")

code("""
lines = src.landing_lines()
data_lines = lines.filter((F.trim("value") != "") & ~F.col("value").startswith("deal_id,"))
landing_rows = data_lines.count()

curated, rejects, run = src.curated(), src.rejects(), src.pipeline_run()
curated_rows = curated.count() if curated is not None else None
reject_rows = rejects.count() if rejects is not None else None

if run is not None and run.count() > 0:
    last = run.filter(F.col("status") == "SUCCEEDED").orderBy(F.col("finished_at").desc()).first()
    if last is None:
        skip("landing_rows_vs_pipeline_input", "no SUCCEEDED run for this date")
    else:
        reconcile("landing_rows_vs_pipeline_input", landing_rows, last["input_rows"])
        reconcile("input_vs_loaded+rejected+superseded", last["input_rows"],
                  last["loaded_rows"] + last["rejected_rows"] + last["superseded_rows"])
        reconcile("reported_loaded_vs_curated_table", last["loaded_rows"], curated_rows)
        reconcile("reported_rejected_vs_quarantine_table", last["rejected_rows"], reject_rows)
else:
    # GCP: no run table, so derive superseded from the source and account for every landing row
    landing = src.landing_csv()
    superseded = landing.filter(F.col("deal_id").isNotNull()).count() - landing.select("deal_id").distinct().count()
    reconcile("landing_rows_vs_curated+rejected+superseded", landing_rows,
              (curated_rows or 0) + (reject_rows or 0) + max(superseded, 0),
              detail="superseded estimated as duplicate deal_id rows in the landing files")

reconcile("one_curated_row_per_deal", curated_rows, curated.select("deal_id").distinct().count())
""")

md("## 5-6. Value checks (independent recompute from the source files)")

code("""
landing = src.landing_csv().select(
    F.trim("deal_id").alias("deal_id"),
    F.trim("deal_version").cast("int").alias("deal_version"),
    F.trim("deal_amount").cast("decimal(20,2)").alias("src_amount"),
)
joined = curated.select("deal_id", "deal_version", "deal_amount", "fx_rate_to_usd", "deal_amount_usd") \\
    .join(landing, ["deal_id", "deal_version"], "left")

reconcile("curated_amount_vs_source_files",
          scalar(joined, F.sum("src_amount")), scalar(joined, F.sum("deal_amount")), AMOUNT_TOLERANCE)
reconcile("curated_rows_traceable_to_source", curated_rows, joined.filter(F.col("src_amount").isNotNull()).count())
reconcile("usd_amount_vs_amount_x_fx",
          scalar(joined, F.sum(F.round(F.col("deal_amount") * F.col("fx_rate_to_usd"), 2))),
          scalar(joined, F.sum("deal_amount_usd")), AMOUNT_TOLERANCE)
""")

md("## 7. Data-quality evidence")

code("""
dq = src.dq_results()
if dq is None or dq.count() == 0:
    skip("curated_dq_gate_blocking_failures", "no dq_results for this date")
else:
    gate = dq.filter(F.col("dataset") != "deal_raw")
    reconcile("curated_dq_gate_blocking_failures", 0,
              gate.filter((~F.col("passed")) & (F.col("severity") == "Error")).count())
    raw_failed = scalar(dq.filter(F.col("dataset") == "deal_raw"), F.sum("failed"))
    results.append(Check("raw_validation_rule_hits", str(raw_failed), "-", "-", "PASS",
                         "informational: rule hits on quarantined rows (a row can break several rules)"))
""")

md("## 8-9. Curated layer, streaming and migration (GCP / Hadoop)")

code("""
current = src.deal_current()
if current is not None:
    staged = curated.select("deal_id", F.col("deal_amount_usd").alias("stg_usd"))
    cur = current.select("deal_id", F.col("deal_amount_usd").alias("cur_usd"))
    j = staged.join(cur, "deal_id", "left")
    reconcile("staged_deals_current_in_scd2", curated_rows, j.filter(F.col("cur_usd").isNotNull()).count())
    reconcile("staged_vs_current_usd", scalar(j, F.sum("stg_usd")), scalar(j, F.sum("cur_usd")), AMOUNT_TOLERANCE)
else:
    skip("staged_deals_current_in_scd2", "GCP only")

assertions = src.sql_assertions()
if assertions is not None and assertions.count() > 0:
    reconcile("bigquery_sql_assertions_failed", 0, assertions.filter(~F.col("passed")).count())
elif PLATFORM == "gcp":
    skip("bigquery_sql_assertions_failed", "no assertion run recorded for this date")

ledger, raw = src.stream_ledger(), src.stream_raw()
if ledger is not None and raw is not None:
    day_ledger = ledger.filter(F.to_date("committed_at") == bd)
    reconcile("stream_ledger_events_vs_raw_landed",
              scalar(day_ledger, F.sum("events")), raw.filter(F.col("business_date") == bd).dropDuplicates(["event_id"]).count(),
              detail="ledger counts de-duplicated events per micro-batch")
else:
    skip("stream_ledger_events_vs_raw_landed", "no streaming tables on this platform")

mig = src.migration_log()
if mig is not None and mig.count() > 0:
    latest = mig.agg(F.max("logged_at")).first()[0]
    last_run = mig.filter(F.col("logged_at") >= F.lit(latest) - F.expr("INTERVAL 6 HOURS"))
    reconcile("migration_partitions_not_reconciled", 0, last_run.filter(F.col("status") != "OK").count())
else:
    skip("migration_partitions_not_reconciled", "no migration runs recorded")
""")

md("## Results")

code("""
import pandas as pd

report = pd.DataFrame([asdict(r) for r in results])
breaks = report[report.status == "BREAK"]
print(f"{len(report)} checks | {int((report.status == 'PASS').sum())} pass | {len(breaks)} break | "
      f"{int((report.status == 'SKIPPED').sum())} skipped")
report
""")

code("""
if WRITE_RESULTS:
    out = spark.createDataFrame(report.astype(str)) \\
        .withColumn("platform", F.lit(PLATFORM)).withColumn("env", F.lit(ENV)) \\
        .withColumn("business_date", bd).withColumn("checked_at", F.current_timestamp())
    if PLATFORM == "local":
        out.write.mode("append").parquet(f"{REPO_ROOT}/target/local-output/audit/recon_results")
    elif PLATFORM == "hadoop":
        out.write.mode("append").saveAsTable(f"cma_audit{HIVE_SUFFIX}.recon_results")
    else:
        out.write.format("bigquery").option("table", f"{GCP_PROJECT}.cma_audit.recon_results") \\
            .option("writeMethod", "direct").mode("append").save()
    print("results written")
""")

code("""
if FAIL_ON_BREAK and len(breaks) > 0:
    raise AssertionError(f"{len(breaks)} reconciliation break(s):\\n" + breaks.to_string(index=False))
print("Reconciliation complete: no breaks" if breaks.empty else "Breaks found (FAIL_ON_BREAK is off)")
""")

nb = {
    "cells": [{**c, "source": [l + "\n" for l in c["source"].split("\n")[:-1]] + [c["source"].split("\n")[-1]]} for c in CELLS],
    "metadata": {
        "kernelspec": {"display_name": "PySpark (Python 3)", "language": "python", "name": "python3"},
        "language_info": {"name": "python", "version": "3.11"},
    },
    "nbformat": 4,
    "nbformat_minor": 5,
}
for i, c in enumerate(nb["cells"]):
    c["id"] = f"cell-{i:02d}"
out = Path(__file__).with_name("pipeline_reconciliation.ipynb")
out.write_text(json.dumps(nb, indent=1) + "\n", encoding="utf-8")
print(f"wrote {out} ({len(CELLS)} cells)")
