package com.example.cma.common.errors

/**
 * Typed error hierarchy for every pipeline in this repo.
 *
 * Each error carries:
 *  - an exit code, so the scheduler (Airflow / Control-M / YARN) can tell failure classes apart
 *  - a `retryable` flag, used by [[com.example.cma.common.util.Retry]] and surfaced to Airflow
 *    so transient infrastructure faults are retried but bad data is not.
 */
sealed abstract class PipelineError(
    val message: String,
    val exitCode: Int,
    val retryable: Boolean,
    underlying: Option[Throwable] = None
) extends RuntimeException(message, underlying.orNull) {
  def category: String = getClass.getSimpleName
}

object PipelineError {

  /** Exit codes are part of the operational contract documented in docs/runbook.md. */
  object ExitCodes {
    val Success          = 0
    val Config           = 2
    val SourceMissing    = 3
    val Schema           = 4
    val DataQuality      = 5
    val Reconciliation   = 6
    val TransientIO      = 7
    val SinkWrite        = 8
    val Unexpected       = 1
  }

  /** Map any throwable to a typed error. Already-typed errors pass through unchanged. */
  def classify(t: Throwable): PipelineError = t match {
    case p: PipelineError => p
    case e if messageContains(e, "Path does not exist") || messageContains(e, "No such file") =>
      SourceNotFoundError(Option(e.getMessage).getOrElse("unknown path"), Some(e))
    case e if isTransient(e) =>
      TransientIOError(s"Transient failure: ${e.getMessage}", e)
    case e =>
      UnexpectedError(e)
  }

  /**
   * Heuristic for transient infrastructure failures (HDFS, GCS, BigQuery, network).
   * Matching on class names and messages avoids a compile-time dependency on
   * shaded cloud client classes, which differ between connector versions.
   */
  def isTransient(t: Throwable): Boolean = {
    val transientClassNames = Set(
      "java.io.IOException",
      "java.net.SocketTimeoutException",
      "java.net.ConnectException",
      "java.util.concurrent.TimeoutException",
      "org.apache.hadoop.ipc.RetriableException",
      "org.apache.hadoop.hdfs.BlockMissingException"
    )
    val transientMessages = Seq(
      "rateLimitExceeded", "backendError", "internalError", "503", "502",
      "Connection reset", "Read timed out", "UNAVAILABLE", "DEADLINE_EXCEEDED"
    )
    causeChain(t).exists { c =>
      transientClassNames.contains(c.getClass.getName) ||
      transientMessages.exists(m => Option(c.getMessage).exists(_.contains(m)))
    }
  }

  private def messageContains(t: Throwable, s: String): Boolean =
    causeChain(t).exists(c => Option(c.getMessage).exists(_.contains(s)))

  private[errors] def causeChain(t: Throwable): Seq[Throwable] =
    Iterator.iterate(t)(_.getCause).takeWhile(_ != null).take(20).toSeq
}

import PipelineError.ExitCodes

final case class ConfigError(detail: String)
    extends PipelineError(s"Configuration error: $detail", ExitCodes.Config, retryable = false)

final case class SourceNotFoundError(path: String, cause0: Option[Throwable] = None)
    extends PipelineError(s"Source not found: $path", ExitCodes.SourceMissing, retryable = false, cause0)

final case class SchemaValidationError(detail: String)
    extends PipelineError(s"Schema validation failed: $detail", ExitCodes.Schema, retryable = false)

final case class DataQualityError(dataset: String, failedRules: Seq[String])
    extends PipelineError(
      s"Data quality gate failed for $dataset: ${failedRules.mkString("; ")}",
      ExitCodes.DataQuality,
      retryable = false
    )

final case class ReconciliationError(dataset: String, breaks: Seq[String])
    extends PipelineError(
      s"Reconciliation failed for $dataset: ${breaks.mkString("; ")}",
      ExitCodes.Reconciliation,
      retryable = false
    )

final case class TransientIOError(detail: String, cause0: Throwable)
    extends PipelineError(detail, ExitCodes.TransientIO, retryable = true, Some(cause0))

final case class SinkWriteError(target: String, cause0: Throwable)
    extends PipelineError(
      s"Failed writing to $target: ${cause0.getMessage}",
      ExitCodes.SinkWrite,
      retryable = PipelineError.isTransient(cause0),
      Some(cause0)
    )

final case class UnexpectedError(cause0: Throwable)
    extends PipelineError(
      s"Unexpected failure: ${cause0.getClass.getName}: ${cause0.getMessage}",
      ExitCodes.Unexpected,
      retryable = false,
      Some(cause0)
    )
