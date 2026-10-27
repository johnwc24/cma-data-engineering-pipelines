package com.example.cma.gcp

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.example.cma.common.errors.{ConfigError, PipelineError, SinkWriteError}
import com.example.cma.common.util.{Retry, RetryPolicy}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.util.control.NonFatal

/**
 * Thin wrapper over the spark-bigquery connector that adds:
 *  - fully-qualified table naming per environment
 *  - partition-scoped overwrite (re-runs replace one day, never the whole table)
 *  - retry on transient BigQuery/GCS errors, fail-fast on permanent ones
 */
final class BigQueryIO(spark: SparkSession, conf: Config, retry: RetryPolicy) {
  private val project    = conf.getString("gcp.project")
  private val tempBucket = conf.getString("gcs.temp-bucket")
  private val method     = conf.getString("bigquery.write-method")

  def table(datasetKey: String, name: String): String =
    s"$project.${conf.getString(s"bigquery.datasets.$datasetKey")}.$name"

  def read(table: String, filter: Option[String] = None): DataFrame =
    guarded(s"read $table") {
      val r = spark.read.format("bigquery").option("table", table).option("viewsEnabled", "true")
      filter.fold(r)(f => r.option("filter", f)).load()
    }

  /** Overwrite a single DAY partition of a table partitioned on `partitionField`. */
  def overwritePartition(df: DataFrame, table: String, partitionField: String, day: LocalDate): Unit =
    write(df, table, SaveMode.Overwrite, Map(
      "partitionField" -> partitionField,
      "partitionType"  -> "DAY",
      "datePartition"  -> day.format(DateTimeFormatter.BASIC_ISO_DATE)
    ))

  def append(df: DataFrame, table: String): Unit = write(df, table, SaveMode.Append, Map.empty)

  private def write(df: DataFrame, table: String, mode: SaveMode, extra: Map[String, String]): Unit =
    guarded(s"write $table") {
      df.write
        .format("bigquery")
        .option("table", table)
        .option("writeMethod", method)
        .option("temporaryGcsBucket", tempBucket)
        .option("createDisposition", "CREATE_NEVER") // tables are owned by bigquery/ddl, never auto-created
        .option("intermediateFormat", "parquet")
        .options(extra)
        .mode(mode)
        .save()
    }

  /** Retry transient errors; translate permanent ones into typed pipeline errors. */
  private def guarded[T](label: String)(op: => T): T =
    try Retry.withBackoff(label, retry)(op)
    catch {
      case e: PipelineError => throw e
      case NonFatal(e) if BigQueryIO.isMissingTable(e) =>
        throw ConfigError(s"$label: BigQuery table not found — apply bigquery/ddl for this environment (${e.getMessage})")
      case NonFatal(e) if BigQueryIO.isPermissionDenied(e) =>
        throw ConfigError(s"$label: permission denied for the Dataproc service account (${e.getMessage})")
      case NonFatal(e) => throw SinkWriteError(label, e)
    }
}

object BigQueryIO {
  private def messages(t: Throwable): Seq[String] =
    Iterator.iterate(t)(_.getCause).takeWhile(_ != null).take(20).flatMap(c => Option(c.getMessage)).toSeq

  def isMissingTable(t: Throwable): Boolean     = messages(t).exists(m => m.contains("Not found: Table") || m.contains("notFound"))
  def isPermissionDenied(t: Throwable): Boolean = messages(t).exists(m => m.contains("Access Denied") || m.contains("PERMISSION_DENIED"))
}
