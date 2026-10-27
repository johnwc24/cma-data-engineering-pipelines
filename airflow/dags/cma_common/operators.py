"""Operators that turn Spark job exit codes into the right Airflow behaviour.

Infrastructure failures (exit code 7/8/1, YARN/Dataproc errors) are retried by Airflow.
Data failures (bad config, missing source, schema, DQ, reconciliation) fail the task
immediately with AirflowFailException: retrying would just reprocess the same bad data
and delay the alert to the data owner.
"""
from __future__ import annotations

from airflow.exceptions import AirflowException, AirflowFailException
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator, DataprocSubmitJobOperator

from cma_common.exit_codes import NON_RETRYABLE_EXIT_CODES, exit_code_from


def reraise_with_retry_policy(exc: Exception) -> None:
    code = exit_code_from(str(exc))
    if code in NON_RETRYABLE_EXIT_CODES:
        raise AirflowFailException(
            f"Spark job failed with a data error (exit {code}: {NON_RETRYABLE_EXIT_CODES[code]}); "
            f"not retrying. Check quarantine / dq_results for this run. Original: {exc}"
        ) from exc
    raise exc


class FailFastSparkSubmitOperator(SparkSubmitOperator):
    """spark-submit to YARN; data errors are not retried."""

    def execute(self, context):  # noqa: ANN001
        try:
            return super().execute(context)
        except AirflowException as exc:
            reraise_with_retry_policy(exc)


class FailFastDataprocSubmitJobOperator(DataprocSubmitJobOperator):
    """Dataproc job submit; data errors are not retried."""

    def execute(self, context):  # noqa: ANN001
        try:
            return super().execute(context)
        except AirflowException as exc:
            reraise_with_retry_policy(exc)


class FailFastDataprocCreateBatchOperator(DataprocCreateBatchOperator):
    """Dataproc Serverless batch; data errors are not retried."""

    def execute(self, context):  # noqa: ANN001
        try:
            return super().execute(context)
        except AirflowException as exc:
            reraise_with_retry_policy(exc)
