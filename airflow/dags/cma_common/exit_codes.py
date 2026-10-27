"""Spark job exit-code contract (mirrors PipelineError.ExitCodes in the Scala code).

Kept free of Airflow imports so it can be unit-tested anywhere.
"""
from __future__ import annotations

import re

# Data errors: retrying reprocesses the same bad input, so fail fast and alert the data owner.
NON_RETRYABLE_EXIT_CODES = {2: "config", 3: "source missing", 4: "schema", 5: "data quality", 6: "reconciliation"}
# Infrastructure errors: worth an Airflow retry with backoff.
RETRYABLE_EXIT_CODES = {1: "unexpected", 7: "transient io", 8: "sink write"}

_EXIT_CODE = re.compile(r"(?:exit(?:ed)?(?: with)?(?: status| code)?|Error code is)[:\s=]+(\d+)", re.IGNORECASE)


def exit_code_from(message: str | None) -> int | None:
    """Extract the driver exit code from a spark-submit or Dataproc failure message."""
    match = _EXIT_CODE.search(message or "")
    return int(match.group(1)) if match else None


def is_retryable(message: str | None) -> bool:
    code = exit_code_from(message)
    return code not in NON_RETRYABLE_EXIT_CODES
