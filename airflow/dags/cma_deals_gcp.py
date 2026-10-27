"""CMA daily deal pipeline on Google Cloud (Cloud Composer).

    wait_for_landing ─► create_cluster ─► spark_gcs_to_bq ─► delete_cluster (always)
                                              │
                                              └─► scd2_merge ─► dq_assertions ─► league_table ─► done

Spark (Dataproc) parses, validates, quarantines and enriches into cma_staging;
BigQuery SQL does the set-based SCD2 merge, assertions and the front-office data product.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataproc import (
    ClusterGenerator,
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.utils.trigger_rule import TriggerRule

from cma_common.operators import FailFastDataprocSubmitJobOperator
from cma_common.settings import CMA_ENV, default_args, load_config, sla_miss, sql_search_path

cfg = load_config()
gcp = cfg["gcp"]
CLUSTER = "cma-deals-{{ ds_nodash }}"
JAR = f"gs://{gcp['artifacts_bucket']}/jars/gcp-dataproc/current/gcp-dataproc-all.jar"


def bq_job(task_id: str, sql_file: str, **kwargs) -> BigQueryInsertJobOperator:
    return BigQueryInsertJobOperator(
        task_id=task_id,
        project_id=gcp["project"],
        location=gcp["region"],
        configuration={
            "query": {"query": "{% include '" + sql_file + "' %}", "useLegacySql": False},
            "labels": {"pipeline": "cma-deals", "env": CMA_ENV, "task": task_id.replace("_", "-")},
        },
        params={"project": gcp["project"]},
        **kwargs,
    )


with DAG(
    dag_id="cma_deals_gcp",
    description="Syndicated deals: GCS -> Spark on Dataproc -> BigQuery SCD2 -> data products",
    start_date=datetime(2026, 10, 1),
    schedule="30 22 * * 1-5",  # after booking-system EOD extract
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=8),
    default_args={**default_args(cfg), "sla": timedelta(hours=cfg["sla_hours"])},
    sla_miss_callback=sla_miss,
    template_searchpath=sql_search_path(),
    tags=["cma", "gcp", "dataproc", "bigquery", CMA_ENV],
) as dag:

    wait_for_landing = GCSObjectsWithPrefixExistenceSensor(
        task_id="wait_for_landing",
        bucket=gcp["landing_bucket"],
        prefix="deals/business_date={{ ds }}/",
        mode="reschedule",  # frees the worker slot between pokes
        poke_interval=300,
        timeout=int(timedelta(hours=4).total_seconds()),
        retries=0,  # a sensor timeout is a real "file never arrived" incident
    )

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=gcp["project"],
        region=gcp["region"],
        cluster_name=CLUSTER,
        use_if_exists=True,  # safe on retry
        delete_on_error=True,  # never leave a half-built cluster billing
        cluster_config=ClusterGenerator(
            project_id=gcp["project"],
            image_version="2.2-debian12",
            num_workers=gcp["cluster_workers"],
            master_machine_type="n2-standard-4",
            worker_machine_type="n2-standard-8",
            subnetwork_uri=gcp["subnetwork"],
            internal_ip_only=True,
            service_account=gcp["service_account"],
            idle_delete_ttl=3600,  # safety net if delete_cluster never runs
            properties={"spark:spark.sql.ansi.enabled": "false"},
        ).make(),
    )

    spark_gcs_to_bq = FailFastDataprocSubmitJobOperator(
        task_id="spark_gcs_to_bq",
        project_id=gcp["project"],
        region=gcp["region"],
        job={
            "reference": {"job_id": "cma-deals-{{ ds_nodash }}-{{ ti.try_number }}"},
            "placement": {"cluster_name": CLUSTER},
            "labels": {"pipeline": "cma-deals", "env": CMA_ENV},
            "spark_job": {
                "main_class": "com.example.cma.gcp.GcsToBigQueryJob",
                "jar_file_uris": [JAR],
                "args": ["--env", CMA_ENV, "--business-date", "{{ ds }}", "--run-id", "{{ run_id }}"],
            },
        },
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=gcp["project"],
        region=gcp["region"],
        cluster_name=CLUSTER,
        trigger_rule=TriggerRule.ALL_DONE,  # tear down on success *and* failure
    )

    scd2_merge = bq_job("scd2_merge", "pipelines/merge_deal_history_scd2.sql")
    # Assertion failures are deterministic: retrying will not fix data
    dq_assertions = bq_job("dq_assertions", "dq/assert_deal_curated.sql", retries=0)
    league_table = bq_job("league_table", "pipelines/build_league_table_monthly.sql")
    done = EmptyOperator(task_id="done")

    wait_for_landing >> create_cluster >> spark_gcs_to_bq >> delete_cluster
    spark_gcs_to_bq >> scd2_merge >> dq_assertions >> league_table >> done
