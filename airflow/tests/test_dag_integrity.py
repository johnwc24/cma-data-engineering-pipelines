"""DAG integrity tests: every DAG imports, has alerting, and the SQL it includes renders.

Runs in CI with apache-airflow + the google, spark and hdfs providers installed
(see airflow/requirements-dev.txt). Skipped automatically if Airflow is unavailable.
"""
import os
import sys
from pathlib import Path

import pytest

DAGS = Path(__file__).resolve().parents[1] / "dags"
sys.path.insert(0, str(DAGS))
os.environ.setdefault("CMA_ENV", "dev")

airflow = pytest.importorskip("airflow")
from airflow.models import DagBag  # noqa: E402

EXPECTED = {"cma_deals_gcp", "cma_deals_hadoop", "cma_stream_health", "cma_hive_to_bq_migration"}


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAGS), include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}, dagbag.import_errors


def test_expected_dags_present(dagbag):
    assert EXPECTED <= set(dagbag.dag_ids)


@pytest.mark.parametrize("dag_id", sorted(EXPECTED))
def test_alerting_and_safety_defaults(dagbag, dag_id):
    dag = dagbag.get_dag(dag_id)
    assert dag.default_args.get("on_failure_callback") is not None
    assert dag.max_active_runs == 1
    assert not dag.catchup
    for task in dag.tasks:
        assert task.owner == "cma-data-engineering"


def test_clusters_are_always_deleted(dagbag):
    for dag_id in ("cma_deals_gcp", "cma_hive_to_bq_migration"):
        delete = dagbag.get_dag(dag_id).get_task("delete_cluster")
        assert delete.trigger_rule == "all_done"


def test_gcp_dependencies(dagbag):
    dag = dagbag.get_dag("cma_deals_gcp")
    assert dag.get_task("scd2_merge").upstream_task_ids == {"spark_gcs_to_bq"}
    assert dag.get_task("dq_assertions").retries == 0


def test_bigquery_sql_includes_resolve(dagbag):
    """Render every BigQuery task's SQL to catch broken {% include %} paths before deployment."""
    dag = dagbag.get_dag("cma_deals_gcp")
    env = dag.get_template_env()
    for task_id in ("scd2_merge", "dq_assertions", "league_table"):
        query = dag.get_task(task_id).configuration["query"]["query"]
        rendered = env.from_string(query).render(params={"project": "p"}, ds="2026-10-26", run_id="r")
        assert "`p." in rendered and "{{" not in rendered
