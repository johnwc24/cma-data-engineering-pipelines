package com.example.cma.gcp

import java.sql.Date
import java.time.LocalDate

import com.example.cma.common.config.JobArgs
import com.example.cma.common.errors.{PipelineError, ReconciliationError}
import com.example.cma.common.job.{JobSummary, SparkJob}
import com.example.cma.common.recon.{ReconCheck, Reconciliation}
import com.example.cma.common.util.RetryPolicy
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
 * Migrates Hive partitions (Hadoop) to BigQuery and proves each one landed intact.
 *
 * Per partition:
 *   read Hive ─► overwrite BigQuery day partition ─► read back ─► reconcile
 *     rows, sum(amount) and an order-independent content checksum
 *
 * Partitions are migrated independently: one bad partition is logged to
 * cma_audit.migration_log and reported at the end without blocking the others,
 * and re-running the job only redoes what is asked for (idempotent overwrite).
 *
 * Runs on a Dataproc cluster configured with the on-prem (or Dataproc Metastore)
 * Hive metastore, so the Hadoop tables are visible to Spark.
 */
class HiveToBigQueryMigration(warehouse: WarehouseIO.Factory) extends SparkJob {
  import HiveToBigQueryMigrationJob.fingerprint

  val appName        = "cma-hive-to-bq-migration"
  val configResource = "gcp-dataproc"
  override val requiredConfig: Seq[String] = Seq("gcp.project", "gcs.temp-bucket", "migration.tables")

  final case class TableSpec(source: String, target: String, partitionColumn: String, amountColumn: String)
  final case class PartitionOutcome(table: String, day: LocalDate, rows: Long, status: String, detail: String)

  override protected def buildSession(args: JobArgs, conf: Config): SparkSession =
    SparkSession.builder().appName(s"$appName-${args.env}").enableHiveSupport().getOrCreate()

  def run(spark: SparkSession, args: JobArgs, conf: Config): Either[PipelineError, JobSummary] = {
    val retry = RetryPolicy(conf.getInt("retry.max-attempts"), conf.getLong("retry.initial-delay-ms"))
    val bq    = warehouse(spark, conf, retry)
    val days  = (0 to conf.getInt("migration.days-back")).map(n => args.businessDate.minusDays(n.toLong))
    val specs = conf.getConfigList("migration.tables").asScala.map { t =>
      TableSpec(t.getString("source"), t.getString("target"), t.getString("partition-column"), t.getString("amount-column"))
    }

    val outcomes = for {
      spec <- specs
      day  <- days
    } yield migratePartition(spark, bq, spec, day, args.dryRun)

    if (!args.dryRun) {
      import spark.implicits._
      val log = outcomes
        .map(o => (args.runId, o.table, Date.valueOf(o.day), o.rows, o.status, o.detail))
        .toDF("run_id", "table_name", "partition_date", "rows", "status", "detail")
        .withColumn("logged_at", current_timestamp())
      Try(bq.append(log, bq.table("audit", "migration_log"))).failed.foreach(e => this.log.warn(s"migration_log write failed: ${e.getMessage}"))
    }

    val failed = outcomes.filter(_.status != "OK")
    if (failed.nonEmpty)
      Left(ReconciliationError("hive_to_bigquery", failed.map(f => s"${f.table}@${f.day}: ${f.status} ${f.detail}")))
    else
      Right(JobSummary(args.runId, Map("partitions" -> outcomes.size.toLong, "rows" -> outcomes.map(_.rows).sum)))
  }

  private def migratePartition(
      spark: SparkSession,
      bq: WarehouseIO,
      spec: TableSpec,
      day: LocalDate,
      dryRun: Boolean
  ): PartitionOutcome = {
    val target = bq.table("curated", spec.target)
    val attempt = Try {
      val source = spark.table(spec.source).filter(col(spec.partitionColumn) === lit(Date.valueOf(day))).cache()
      val src    = fingerprint(source, spec.amountColumn)
      if (src.rows == 0) PartitionOutcome(spec.source, day, 0, "SKIPPED_EMPTY", "no rows in source partition")
      else if (dryRun) PartitionOutcome(spec.source, day, src.rows, "OK", "dry run")
      else {
        bq.overwritePartition(source, target, spec.partitionColumn, day)
        val written = bq.read(target, Some(s"${spec.partitionColumn} = DATE '$day'"))
          .filter(col(spec.partitionColumn) === lit(Date.valueOf(day))) // belt and braces over the pushed-down filter
        val tgt     = fingerprint(written.select(source.columns.map(col): _*), spec.amountColumn)
        Reconciliation.check(
          s"${spec.target}@$day",
          Seq(
            ReconCheck("rows", BigDecimal(src.rows), BigDecimal(tgt.rows), BigDecimal(0)),
            ReconCheck("amount", src.amount, tgt.amount, BigDecimal("0.01")),
            ReconCheck("checksum", src.checksum, tgt.checksum, BigDecimal(0))
          )
        ) match {
          case Right(_) => PartitionOutcome(spec.source, day, src.rows, "OK", "reconciled")
          case Left(e)  => PartitionOutcome(spec.source, day, src.rows, "RECON_BREAK", e.message)
        }
      }
    }
    attempt match {
      case Success(o) =>
        log.info(s"migration ${o.table}@${o.day}: ${o.status} (${o.rows} rows)")
        o
      case Failure(e) =>
        val err = PipelineError.classify(e)
        log.error(s"migration ${spec.source}@$day failed: ${err.message}", e)
        PartitionOutcome(spec.source, day, 0, s"FAILED_${err.category}", err.message)
    }
  }
}

/** Production entry point; also hosts the reusable fingerprint function. */
object HiveToBigQueryMigrationJob extends HiveToBigQueryMigration(WarehouseIO.bigQuery) {
  final case class Fingerprint(rows: Long, amount: BigDecimal, checksum: BigDecimal)

  /**
   * Row count, amount total and an order-independent checksum (sum of per-row xxhash64
   * over all columns, cast to string so Hive/BigQuery type differences don't create false breaks).
   */
  def fingerprint(df: DataFrame, amountColumn: String): Fingerprint = {
    val rowHash = xxhash64(df.columns.sorted.map(c => coalesce(col(c).cast("string"), lit("\u0000"))): _*)
    val r = df
      .agg(
        count(lit(1)),
        coalesce(sum(col(amountColumn).cast("decimal(38,4)")), lit(0).cast("decimal(38,4)")),
        coalesce(sum(rowHash.cast("decimal(38,0)")), lit(0).cast("decimal(38,0)"))
      )
      .head()
    Fingerprint(r.getLong(0), BigDecimal(r.getDecimal(1)), BigDecimal(r.getDecimal(2)))
  }
}
