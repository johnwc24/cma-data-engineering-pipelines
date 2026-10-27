"""Shared settings, default_args and failure handling for the CMA DAGs."""
from __future__ import annotations

import logging
import os
from datetime import timedelta
from pathlib import Path
from typing import Any

import yaml

log = logging.getLogger(__name__)

DAGS_DIR = Path(__file__).resolve().parent.parent
CMA_ENV = os.environ.get("CMA_ENV", "dev")



def load_config(env: str = CMA_ENV) -> dict[str, Any]:
    with open(Path(__file__).with_name("config.yaml"), encoding="utf-8") as f:
        all_envs = yaml.safe_load(f)
    if env not in all_envs:
        raise ValueError(f"CMA_ENV={env!r} not in config.yaml ({', '.join(all_envs)})")
    return all_envs[env]


def sql_search_path() -> list[str]:
    """SQL ships to <dags>/sql on Composer; in the repo it lives in ../bigquery."""
    candidates = [DAGS_DIR / "sql", DAGS_DIR.parent.parent / "bigquery"]
    return [str(p) for p in candidates if p.exists()]


def notify_failure(context: dict[str, Any]) -> None:
    """on_failure_callback: log a structured alert and email the on-call list.

    Must never raise: a failing callback would hide the original task error.
    """
    try:
        ti = context["task_instance"]
        exc = context.get("exception")
        msg = (
            f"[CMA {CMA_ENV.upper()}] {ti.dag_id}.{ti.task_id} failed "
            f"(run {context.get('run_id')}, try {ti.try_number}): {exc}"
        )
        log.error(msg)
        emails = context["dag"].default_args.get("email") or []
        if emails:
            from airflow.utils.email import send_email

            send_email(to=emails, subject=msg[:200], html_content=f"<pre>{msg}</pre><p>Log: {ti.log_url}</p>")
    except Exception:  # noqa: BLE001 - deliberate: callbacks must not fail
        log.exception("notify_failure callback itself failed")


def sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:  # noqa: ANN001
    log.warning("SLA missed for %s: %s", dag.dag_id, task_list)


def default_args(cfg: dict[str, Any]) -> dict[str, Any]:
    return {
        "owner": "cma-data-engineering",
        "depends_on_past": False,
        "email": cfg.get("alert_emails", []),
        "email_on_failure": False,  # handled by notify_failure (single, richer alert)
        "email_on_retry": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=30),
        "execution_timeout": timedelta(hours=2),
        "on_failure_callback": notify_failure,
    }
