package com.example.cma.common.job

import com.example.cma.common.config.{JobArgs, PipelineConfig}
import com.example.cma.common.errors.PipelineError
import com.example.cma.common.errors.PipelineError.ExitCodes
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import scala.util.Try

final case class JobSummary(runId: String, metrics: Map[String, Long], notes: Seq[String] = Seq.empty)

/**
 * Template for every batch/streaming entry point.
 *
 * Handles argument parsing, layered config, Spark session lifecycle, error
 * classification and process exit codes, so each job only implements `run`.
 *
 * Exit codes (see docs/runbook.md) let Airflow and YARN distinguish
 * "bad data, do not retry" from "infrastructure blip, retry".
 */
trait SparkJob {
  protected val log = LoggerFactory.getLogger(getClass)

  def appName: String

  /** Bundled HOCON resource name (without .conf). */
  def configResource: String

  /** Keys that must be present before any work starts. */
  def requiredConfig: Seq[String] = Seq.empty

  def run(spark: SparkSession, args: JobArgs, conf: Config): Either[PipelineError, JobSummary]

  /** Override to enable Hive support, BigQuery settings, etc. */
  protected def buildSession(args: JobArgs, conf: Config): SparkSession = {
    val builder = SparkSession.builder().appName(s"$appName-${args.env}")
    if (args.env == "local") builder.master("local[*]").config("spark.sql.shuffle.partitions", "4")
    builder.getOrCreate()
  }

  final def main(argv: Array[String]): Unit = {
    val code = execute(argv)
    if (code != ExitCodes.Success) sys.exit(code)
  }

  /** Separated from `main` so tests can assert on exit codes without killing the JVM. */
  final def execute(argv: Array[String], session: Option[SparkSession] = None): Int = {
    val started = System.currentTimeMillis()

    val outcome: Either[PipelineError, JobSummary] = for {
      args <- JobArgs.parse(appName, argv.toSeq)
      loaded <- PipelineConfig.load(args, configResource)
      conf   <- PipelineConfig.requirePaths(loaded, requiredConfig: _*)
      summary <- {
        log.info(s"Starting $appName run_id=${args.runId} env=${args.env} business_date=${args.businessDate} dry_run=${args.dryRun}")
        val ownsSession = session.isEmpty
        Try(session.getOrElse(buildSession(args, conf))).toEither.left
          .map(PipelineError.classify)
          .flatMap { spark =>
            // Invalid dates (e.g. 2026-02-30) must become NULL and be quarantined, not throw
            // SparkUpgradeException under the default EXCEPTION policy.
            spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
            try Try(run(spark, args, conf)).toEither.left.map(PipelineError.classify).flatMap(identity)
            finally if (ownsSession) Try(spark.stop())
          }
      }
    } yield summary

    val elapsed = (System.currentTimeMillis() - started) / 1000
    outcome match {
      case Right(s) =>
        log.info(s"$appName SUCCEEDED run_id=${s.runId} in ${elapsed}s metrics=${s.metrics.mkString(", ")}")
        s.notes.foreach(n => log.warn(s"$appName note: $n"))
        ExitCodes.Success
      case Left(e) =>
        log.error(
          s"$appName FAILED in ${elapsed}s category=${e.category} exit_code=${e.exitCode} retryable=${e.retryable}: ${e.message}",
          e
        )
        e.exitCode
    }
  }
}
