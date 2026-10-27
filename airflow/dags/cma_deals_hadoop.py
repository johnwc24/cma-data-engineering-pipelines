"""CMA daily deal pipeline on Hadoop (Airflow on-prem).

    wait_for_landing (WebHDFS) ─► spark_deal_ingestion (spark-submit, YARN cluster mode) ─► done

The Spark job owns validation, quarantine, the DQ gate, the Hive write and reconciliation;
its exit code tells Airflow whether a failure is worth retrying (see cma_common.operators).
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.hdfs.sensors.web_hdfs import WebHdfsSensor

from cma_common.operators import FailFastSparkSubmitOperator
from cma_common.settings import CMA_ENV, default_args, load_config, sla_miss

cfg = load_config()
hd = cfg["hadoop"]

with DAG(
    dag_id="cma_deals_hadoop",
    description="Syndicated deals: HDFS landing -> Spark/Scala on YARN -> Hive curated",
    start_date=datetime(2026, 10, 1),
    schedule="0 22 * * 1-5",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=6),
    default_args={**default_args(cfg), "sla": timedelta(hours=cfg["sla_hours"])},
    sla_miss_callback=sla_miss,
    tags=["cma", "hadoop", "spark", "hive", CMA_ENV],
) as dag:

    wait_for_landing = WebHdfsSensor(
        task_id="wait_for_landing",
        webhdfs_conn_id=hd["webhdfs_conn_id"],
        filepath=hd["landing_path"] + "/business_date={{ ds }}",
        mode="reschedule",
        poke_interval=300,
        timeout=int(timedelta(hours=4).total_seconds()),
        retries=0,
    )

    spark_deal_ingestion = FailFastSparkSubmitOperator(
        task_id="spark_deal_ingestion",
        conn_id=hd["spark_conn_id"],  # yarn master, cluster deploy mode, queue set on the connection
        application=hd["jar"],
        java_class="com.example.cma.hadoop.DealIngestionJob",
        name="cma-deal-ingestion-{{ ds_nodash }}",
        application_args=["--env", CMA_ENV, "--business-date", "{{ ds }}", "--run-id", "{{ run_id }}"],
        conf={
            "spark.yarn.maxAppAttempts": "1",  # Airflow owns retries, so YARN must not re-run silently
            "spark.dynamicAllocation.enabled": "true",
            "spark.dynamicAllocation.maxExecutors": "20",
            "spark.sql.adaptive.enabled": "true",
        },
        executor_memory="8g",
        driver_memory="4g",
        verbose=False,
    )

    done = EmptyOperator(task_id="done")

    wait_for_landing >> spark_deal_ingestion >> done
