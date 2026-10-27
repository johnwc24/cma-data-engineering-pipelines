"""Exit-code parsing decides whether Airflow retries a failed Spark job."""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

from cma_common.exit_codes import exit_code_from, is_retryable  # noqa: E402


@pytest.mark.parametrize(
    "message, code",
    [
        ("Cannot execute: spark-submit ... Error code is: 5.", 5),
        ("Job failed with message [Driver exited with code 3]", 3),
        ("Google Cloud Dataproc Agent reports job failure. ... exit status: 6", 6),
        ("Container exited with a non-zero exit code 7", 7),
        ("Some unrelated failure", None),
        (None, None),
    ],
)
def test_exit_code_from(message, code):
    assert exit_code_from(message) == code


@pytest.mark.parametrize("code, retry", [(2, False), (3, False), (5, False), (6, False), (1, True), (7, True), (8, True)])
def test_retry_policy(code, retry):
    assert is_retryable(f"Error code is: {code}") is retry


def test_unknown_failures_are_retried():
    assert is_retryable("Connection to YARN ResourceManager lost") is True
