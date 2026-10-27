"""CMA data products on Dataproc Serverless (Cloud Composer), daily after the deal load.

    wait_for_deals ─┬─► break_reconciliation
                    │
    party_master ───┤
    vendor_ratings ─┴─► pit_features

* party_master        PostgreSQL party master -> Delta (incremental, watermark + look-back)
* vendor_ratings      vendor rating feed: manifest checksum, licence check, Delta snapshot
* break_reconciliation CMA curated deals vs booking-system positions, aged breaks
* pit_features        point-in-time borrower features for data science
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor

from cma_common.operators import FailFastDataprocCreateBatchOperator
from cma_common.settings import CMA_ENV, default_args, load_config, sla_miss

cfg = load_config()
gcp = cfg["gcp"]
JAR = f"gs://{gcp['artifacts_bucket']}/jars/data-products/current/data-products-all.jar"


def batch(task_id: str, main_class: str, *extra: str) -> FailFastDataprocCreateBatchOperator:
    return FailFastDataprocCreateBatchOperator(
        task_id=task_id,
        project_id=gcp["project"],
        region=gcp["region"],
        batch_id=f"cma-{task_id.replace('_', '-')}-{{{{ ds_nodash }}}}-{{{{ ti.try_number }}}}",
        batch={
            "spark_batch": {
                "main_class": main_class,
                "jar_file_uris": [JAR],
                "args": ["--env", CMA_ENV, "--business-date", "{{ ds }}", "--run-id", "{{ run_id }}",
                         "--conf", ",".join(("platform=gcp",) + extra)],
            },
            "runtime_config": {"version": "2.2"},
            "environment_config": {
                "execution_config": {"service_account": gcp["service_account"], "subnetwork_uri": gcp["subnetwork"]},
            },
            "labels": {"pipeline": "cma-data-products", "env": CMA_ENV},
        },
    )


with DAG(
    dag_id="cma_data_products",
    description="PostgreSQL ingestion, vendor feed, break reconciliation and point-in-time features",
    start_date=datetime(2026, 10, 1),
    schedule="30 22 * * 1-5",  # same schedule as cma_deals_gcp, so the sensor lines up
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=6),
    default_args={**default_args(cfg), "sla": timedelta(hours=cfg["sla_hours"] + 1)},
    sla_miss_callback=sla_miss,
    tags=["cma", "gcp", "data-products", CMA_ENV],
) as dag:

    wait_for_deals = ExternalTaskSensor(
        task_id="wait_for_deals",
        external_dag_id="cma_deals_gcp",
        external_task_id="done",
        mode="reschedule",
        poke_interval=300,
        timeout=int(timedelta(hours=6).total_seconds()),
        retries=0,
    )

    party_master = batch("party_master", "com.example.cma.products.JdbcIncrementalIngestionJob", "source=party-master")
    vendor_ratings = batch("vendor_ratings", "com.example.cma.products.VendorFeedJob", "feed=agency-ratings")
    break_reconciliation = batch("break_reconciliation", "com.example.cma.products.BreakReconciliationJob")
    pit_features = batch("pit_features", "com.example.cma.products.PointInTimeFeaturesJob")

    wait_for_deals >> break_reconciliation
    [wait_for_deals, party_master, vendor_ratings] >> pit_features
