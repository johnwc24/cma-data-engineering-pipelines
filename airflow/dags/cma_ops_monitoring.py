"""Operational checks for the CMA platform (hourly on Composer).

* stream_freshness   - the Kafka -> BigQuery stream committed a batch in the last 15 minutes
* stream_dlq_rate    - fewer than 5% of streamed events went to the dead-letter topic in the last hour
* hive_to_bq_migration (manual) - migrate and reconcile Hadoop partitions to BigQuery

Failures alert through the shared on_failure_callback.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.providers.google.cloud.operators.dataproc import (
    ClusterGenerator,
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
)
from airflow.utils.trigger_rule import TriggerRule

from cma_common.operators import FailFastDataprocSubmitJobOperator
from cma_common.settings import CMA_ENV, default_args, load_config

cfg = load_config()
gcp = cfg["gcp"]
LEDGER = f"`{gcp['project']}.cma_audit.stream_batch`"

with DAG(
    dag_id="cma_stream_health",
    start_date=datetime(2026, 10, 1),
    schedule="5 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={**default_args(cfg), "retries": 1},
    tags=["cma", "monitoring", "kafka", CMA_ENV],
) as stream_health:

    BigQueryCheckOperator(
        task_id="stream_freshness",
        use_legacy_sql=False,
        location=gcp["region"],
        sql=f"""
            SELECT TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(committed_at), MINUTE) < 15
            FROM {LEDGER}
            WHERE DATE(committed_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        """,
    ) >> BigQueryCheckOperator(
        task_id="stream_dlq_rate",
        use_legacy_sql=False,
        location=gcp["region"],
        sql=f"""
            SELECT IFNULL(SAFE_DIVIDE(SUM(rejected), SUM(events)), 0) < 0.05
            FROM {LEDGER}
            WHERE committed_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
        """,
    )


MIGRATION_CLUSTER = "cma-migration-{{ ts_nodash | lower }}"

with DAG(
    dag_id="cma_hive_to_bq_migration",
    description="Migrate Hadoop Hive partitions to BigQuery with row/amount/checksum reconciliation",
    start_date=datetime(2026, 10, 1),
    schedule=None,  # triggered per migration wave
    catchup=False,
    max_active_runs=1,
    params={"days_back": Param(0, type="integer", minimum=0, maximum=365)},
    default_args=default_args(cfg),
    tags=["cma", "migration", CMA_ENV],
) as migration:

    create = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=gcp["project"],
        region=gcp["region"],
        cluster_name=MIGRATION_CLUSTER,
        use_if_exists=True,
        delete_on_error=True,
        cluster_config=ClusterGenerator(
            project_id=gcp["project"],
            image_version="2.2-debian12",
            num_workers=gcp["cluster_workers"],
            subnetwork_uri=gcp["subnetwork"],
            internal_ip_only=True,
            service_account=gcp["service_account"],
            idle_delete_ttl=3600,
            # Points Spark at the Dataproc Metastore federated with the on-prem Hive metastore
            properties={"hive:hive.metastore.uris": "thrift://cma-metastore.internal:9083"},
        ).make(),
    )

    migrate = FailFastDataprocSubmitJobOperator(
        task_id="migrate_partitions",
        project_id=gcp["project"],
        region=gcp["region"],
        job={
            "placement": {"cluster_name": MIGRATION_CLUSTER},
            "spark_job": {
                "main_class": "com.example.cma.gcp.HiveToBigQueryMigrationJob",
                "jar_file_uris": [f"gs://{gcp['artifacts_bucket']}/jars/gcp-dataproc/current/gcp-dataproc-all.jar"],
                "args": [
                    "--env", CMA_ENV, "--business-date", "{{ ds }}", "--run-id", "{{ run_id }}",
                    "--conf", "migration.days-back={{ params.days_back }}",
                ],
            },
        },
        execution_timeout=timedelta(hours=6),
    )

    delete = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=gcp["project"],
        region=gcp["region"],
        cluster_name=MIGRATION_CLUSTER,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create >> migrate >> delete
