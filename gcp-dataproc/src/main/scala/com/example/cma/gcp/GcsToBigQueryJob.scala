package com.example.cma.gcp

import java.sql.Date

import com.example.cma.common.config.JobArgs
import com.example.cma.common.errors.PipelineError
import com.example.cma.common.job.{JobSummary, SparkJob}
import com.example.cma.common.quality.{DataQuality, DqReport}
import com.example.cma.common.recon.{ReconCheck, Reconciliation}
import com.example.cma.common.util.{Retry, RetryPolicy}
import com.example.cma.deals.{DealPipeline, DealRules, RawDealReader}
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.util.Try

/**
 * Daily deal load on Google Cloud.
 *
 *   GCS landing (CSV) ─► DealPipeline ─► DQ gate ─► BigQuery cma_staging.deal_stg (day partition)
 *                                                 ├─► cma_quarantine.deal_reject
 *                                                 └─► cma_audit.dq_results
 *
 * The staging -> curated SCD2 MERGE and the downstream data products run as
 * BigQuery SQL from the Composer DAG (bigquery/pipelines), keeping heavy set-based
 * work in BigQuery and Spark for parsing, validation and enrichment.
 *
 * gcloud dataproc jobs submit spark --cluster=<c> --region=europe-west2 \
 *   --class=com.example.cma.gcp.GcsToBigQueryJob --jars=gs://<artifacts>/gcp-dataproc-all.jar \
 *   -- --env prod --business-date 2026-10-26
 */
class GcsToBigQuery(warehouse: WarehouseIO.Factory) extends SparkJob {
  val appName        = "cma-deal-gcs-to-bq"
  val configResource = "gcp-dataproc"
  override val requiredConfig: Seq[String] = Seq("gcp.project", "gcs.landing", "gcs.temp-bucket")

  override protected def buildSession(args: JobArgs, conf: Config): SparkSession =
    SparkSession
      .builder()
      .appName(s"$appName-${args.env}")
      .config("spark.sql.ansi.enabled", "false")
      .config("spark.sql.session.timeZone", "UTC")
      .config("viewsEnabled", "true")
      .config("materializationDataset", conf.getString("bigquery.datasets.staging"))
      .getOrCreate()

  def run(spark: SparkSession, args: JobArgs, conf: Config): Either[PipelineError, JobSummary] = {
    val retry = RetryPolicy(conf.getInt("retry.max-attempts"), conf.getLong("retry.initial-delay-ms"))
    val bq    = warehouse(spark, conf, retry)
    val bd    = args.businessDate

    val raw = Retry.withBackoff("read-landing", retry) {
      RawDealReader.readCsv(spark, RawDealReader.landingPath(conf.getString("gcs.landing"), bd))
    }
    // Filters are pushed down to the BigQuery Storage Read API, so only needed rows leave BigQuery
    val parties = bq.read(bq.table("reference", "party"), Some("is_active = true")).cache()
    val fx      = bq.read(bq.table("reference", "fx_rate"), Some(s"rate_date <= DATE '$bd' AND rate_date >= DATE_SUB(DATE '$bd', INTERVAL 7 DAY)")).cache()

    val result  = DealPipeline.process(raw, parties, fx, bd, args.runId, conf.getString("source.system"))
    val curated = result.curated.cache()

    val gate  = DataQuality.evaluate("deal_staging", curated, DealRules.curatedGate(parties, conf.getLong("dq.min-rows")))
    val dqAll = DqReport("deal", result.validationReport.results ++ gate.results)

    if (!args.dryRun) {
      bq.overwritePartition(result.rejected, bq.table("quarantine", "deal_reject"), "business_date", bd)
      bq.append(dqAll.toDataFrame(spark, args.runId, bd.toString), bq.table("audit", "dq_results"))
    }

    for {
      _ <- (gate.enforce(): Either[PipelineError, DqReport])
      _ <- Try(if (!args.dryRun) bq.overwritePartition(curated, bq.table("staging", "deal_stg"), "business_date", bd))
             .toEither.left.map(PipelineError.classify)
      checks <- if (args.dryRun) Right(Seq.empty[ReconCheck]) else reconcile(bq, curated, bd, conf)
    } yield JobSummary(
      args.runId,
      Map(
        "input_rows"      -> result.inputRows,
        "rejected_rows"   -> result.rejectedRows,
        "superseded_rows" -> result.supersededRows,
        "staged_rows"     -> curated.count(),
        "recon_checks"    -> checks.size.toLong
      ),
      gate.warnings.map(w => s"DQ warning ${w.rule}: ${w.failed}/${w.checked}")
    )
  }

  /** Read the partition back from BigQuery and compare control totals with what Spark produced. */
  private def reconcile(bq: WarehouseIO, curated: org.apache.spark.sql.DataFrame, bd: java.time.LocalDate, conf: Config) = {
    val expected = Reconciliation.totals(curated, Some("deal_amount"))
    val written  = bq.read(bq.table("staging", "deal_stg"), Some(s"business_date = DATE '$bd'"))
      .filter(col("business_date") === lit(Date.valueOf(bd)))
    val actual = Reconciliation.totals(written, Some("deal_amount"))
    Reconciliation.check(
      "deal_stg",
      Seq(
        ReconCheck("rows", BigDecimal(expected.rows), BigDecimal(actual.rows), BigDecimal(0)),
        ReconCheck("deal_amount", expected.amount, actual.amount, BigDecimal(conf.getString("recon.amount-tolerance")))
      )
    )
  }
}

/** Production entry point (spark main class). */
object GcsToBigQueryJob extends GcsToBigQuery(WarehouseIO.bigQuery)
