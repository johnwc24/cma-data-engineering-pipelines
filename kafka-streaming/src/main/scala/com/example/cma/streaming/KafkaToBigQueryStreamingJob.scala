package com.example.cma.streaming

import java.time.LocalDate

import com.example.cma.common.config.JobArgs
import com.example.cma.common.util.RetryPolicy
import com.example.cma.gcp.BigQueryIO
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Google Cloud Managed Service for Apache Kafka ─► Spark on Dataproc ─► BigQuery
 *
 *   cma_raw.deal_event_stream     every event with Kafka coordinates (bronze)
 *   cma_staging.deal_intraday     validated, enriched deals; Composer MERGEs these into curated
 *   cma_quarantine.deal_reject    rejected events (also on the DLQ topic)
 *   cma_audit.stream_batch        batch ledger for replay protection
 *
 * Uses the BigQuery Storage Write API (writeMethod=direct) for low latency, and
 * OAUTHBEARER auth with the Dataproc service account against Managed Kafka.
 *
 * gcloud dataproc jobs submit spark --cluster=cma-streaming --region=europe-west2 \
 *   --class=com.example.cma.streaming.KafkaToBigQueryStreamingJob \
 *   --jars=gs://<artifacts>/kafka-streaming-all.jar --max-failures-per-hour=5 -- --env prod
 */
object KafkaToBigQueryStreamingJob extends StreamingDealJob {
  val appName        = "cma-deal-stream-bq"
  val configResource = "kafka-gcp"

  override def requiredConfig: Seq[String] =
    super.requiredConfig ++ Seq("gcp.project", "gcs.temp-bucket", "bigquery.write-method")

  override protected def buildSession(args: JobArgs, conf: Config): SparkSession =
    SparkSession.builder()
      .appName(s"$appName-${args.env}")
      .config("spark.sql.ansi.enabled", "false")
      .config("spark.sql.streaming.stopGracefullyOnShutdown", "true")
      .getOrCreate()

  @transient private var io: BigQueryIO = _

  private def bq(spark: SparkSession, conf: Config): BigQueryIO = {
    if (io == null) io = new BigQueryIO(spark, conf, RetryPolicy(conf.getInt("retry.max-attempts"), conf.getLong("retry.initial-delay-ms")))
    io
  }

  protected def readParties(spark: SparkSession, conf: Config): DataFrame =
    bq(spark, conf).read(bq(spark, conf).table("reference", "party"), Some("is_active = true"))

  protected def readFxRates(spark: SparkSession, conf: Config): DataFrame =
    bq(spark, conf).read(bq(spark, conf).table("reference", "fx_rate"), Some("rate_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)"))

  protected def alreadyCommitted(spark: SparkSession, conf: Config, batchId: Long): Boolean = {
    val ledger = bq(spark, conf).table("audit", "stream_batch")
    !bq(spark, conf).read(ledger, Some(s"query_name = '$appName' AND batch_id = $batchId")).isEmpty
  }

  protected def writeRawEvents(spark: SparkSession, conf: Config, events: DataFrame): Unit =
    bq(spark, conf).append(events.withColumn("ingest_date", current_date()), bq(spark, conf).table("raw", "deal_event_stream"))

  protected def writeCurated(spark: SparkSession, conf: Config, curated: DataFrame, bd: LocalDate): Unit =
    bq(spark, conf).append(curated, bq(spark, conf).table("staging", "deal_intraday"))

  protected def writeQuarantine(spark: SparkSession, conf: Config, rejected: DataFrame, bd: LocalDate): Unit =
    bq(spark, conf).append(rejected, bq(spark, conf).table("quarantine", "deal_reject"))

  protected def recordBatch(spark: SparkSession, conf: Config, batchId: Long, metrics: Map[String, Long]): Unit = {
    import spark.implicits._
    val row = Seq((appName, batchId, metrics.getOrElse("events", 0L), metrics.getOrElse("rejected", 0L)))
      .toDF("query_name", "batch_id", "events", "rejected")
      .withColumn("committed_at", current_timestamp())
    bq(spark, conf).append(row, bq(spark, conf).table("audit", "stream_batch"))
  }
}
