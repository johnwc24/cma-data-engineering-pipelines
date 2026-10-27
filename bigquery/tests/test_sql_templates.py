"""Local unit tests for the BigQuery SQL pipelines (no GCP access needed).

Renders every script exactly as Airflow / Ansible would and checks the structure that
matters operationally: no unresolved placeholders, transactional scripts roll back and
re-raise, assertions record evidence before asserting, and DDL is idempotent.
`ci/bq_dry_run.sh` complements this with a real BigQuery dry run in Jenkins.
"""
import re
from pathlib import Path

import pytest
from jinja2 import Environment, StrictUndefined

ROOT = Path(__file__).resolve().parents[1]
CTX = {
    "project": "test-project", "location": "europe-west2",
    "params": {"project": "test-project"}, "ds": "2026-10-26", "run_id": "manual__test",
}
SQL_FILES = sorted(ROOT.rglob("*.sql"))


def render(path: Path) -> str:
    return Environment(undefined=StrictUndefined).from_string(path.read_text()).render(**CTX)


def statements(sql: str) -> list[str]:
    no_comments = "\n".join(l for l in sql.splitlines() if not l.strip().startswith("--"))
    return [s.strip() for s in no_comments.split(";") if s.strip()]


@pytest.mark.parametrize("path", SQL_FILES, ids=lambda p: str(p.relative_to(ROOT)))
def test_renders_without_unresolved_placeholders(path):
    sql = render(path)
    assert "{{" not in sql and "{%" not in sql
    assert "test-project" in sql


@pytest.mark.parametrize("path", SQL_FILES, ids=lambda p: str(p.relative_to(ROOT)))
def test_every_table_reference_is_fully_qualified(path):
    sql = "\n".join(l for l in render(path).splitlines() if not l.strip().startswith("--"))
    for ref in re.findall(r"`([^`]+)`", sql):
        assert ref.count(".") in (1, 2) and ref.startswith("test-project."), ref


def test_ddl_is_idempotent():
    for stmt in statements(render(ROOT / "ddl" / "01_datasets_and_tables.sql")):
        head = " ".join(stmt.split()[:6]).upper()
        assert "IF NOT EXISTS" in head or head.startswith("CREATE OR REPLACE"), head


@pytest.mark.parametrize("name", ["merge_deal_history_scd2.sql", "build_league_table_monthly.sql"])
def test_transactional_scripts_roll_back_and_reraise(name):
    sql = render(ROOT / "pipelines" / name)
    assert "BEGIN TRANSACTION" in sql and "COMMIT TRANSACTION" in sql
    handler = sql[sql.index("EXCEPTION WHEN ERROR"):]
    assert "ROLLBACK TRANSACTION" in handler and "RAISE" in handler


def test_scd2_merge_refuses_to_run_on_empty_staging():
    sql = render(ROOT / "pipelines" / "merge_deal_history_scd2.sql")
    guard = sql.index("IF (SELECT COUNT(*) FROM src) = 0")
    assert guard < sql.index("MERGE `"), "empty-staging guard must run before the MERGE"
    # the business date variable must not shadow the business_date column
    assert "DECLARE business_date" not in sql


def test_assertions_record_evidence_before_failing():
    sql = render(ROOT / "dq" / "assert_deal_curated.sql")
    assert sql.index("INSERT INTO `test-project.cma_audit.sql_assertions`") < sql.index("ASSERT (")
    assert "blocking AND failing_rows > 0" in sql
