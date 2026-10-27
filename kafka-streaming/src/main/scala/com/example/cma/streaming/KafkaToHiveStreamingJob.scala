package com.example.cma.streaming

import java.time.LocalDate

import com.example.cma.common.config.JobArgs
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Kafka (on-prem, SASL_SSL) ─► Spark on YARN ─► Hive
 *
 *   cma_raw.deal_event          every event as received, with topic/partition/offset (bronze)
 *   cma_curated.deal_intraday   validated, enriched deals (append, partitioned by business_date)
 *   cma_quarantine.deal_reject  rejected events (also published to the DLQ topic)
 *   cma_audit.stream_batch      batch ledger for replay protection
 *
 * spark-submit --master yarn --deploy-mode cluster --supervise \
 *   --class com.example.cma.streaming.KafkaToHiveStreamingJob kafka-streaming-all.jar --env prod
 */
object KafkaToHiveStreamingJob extends StreamingDealJob {
  val appName        = "cma-deal-stream-hive"
  val configResource = "kafka-hadoop"

  override protected def buildSession(args: JobArgs, conf: Config): SparkSession =
    SparkSession.builder()
      .appName(s"$appName-${args.env}")
      .config("spark.sql.ansi.enabled", "false")
      .config("spark.sql.streaming.stopGracefullyOnShutdown", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .enableHiveSupport()
      .getOrCreate()

  private def t(conf: Config, k: String) = conf.getString(s"tables.$k")

  protected def readParties(spark: SparkSession, conf: Config): DataFrame =
    spark.table(t(conf, "parties")).filter(col("is_active") === true)

  protected def readFxRates(spark: SparkSession, conf: Config): DataFrame = spark.table(t(conf, "fx-rates"))

  protected def alreadyCommitted(spark: SparkSession, conf: Config, batchId: Long): Boolean =
    !spark.table(t(conf, "batch-ledger"))
      .filter(col("query_name") === appName && col("batch_id") === batchId)
      .isEmpty

  private def append(spark: SparkSession, df: DataFrame, table: String): Unit =
    df.select(spark.table(table).columns.map(c => if (df.columns.contains(c)) col(c) else lit(null).as(c)): _*)
      .write.mode(SaveMode.Append).insertInto(table)

  protected def writeRawEvents(spark: SparkSession, conf: Config, events: DataFrame): Unit =
    append(spark, events.withColumn("business_date", current_date()), t(conf, "raw-events"))

  protected def writeCurated(spark: SparkSession, conf: Config, curated: DataFrame, bd: LocalDate): Unit =
    append(spark, curated, t(conf, "curated"))

  protected def writeQuarantine(spark: SparkSession, conf: Config, rejected: DataFrame, bd: LocalDate): Unit =
    append(spark, rejected.withColumn("_reject_reasons", concat_ws("|", col("_reject_reasons"))), t(conf, "rejects"))

  protected def recordBatch(spark: SparkSession, conf: Config, batchId: Long, metrics: Map[String, Long]): Unit = {
    import spark.implicits._
    val row = Seq((appName, batchId, metrics.getOrElse("events", 0L), metrics.getOrElse("rejected", 0L)))
      .toDF("query_name", "batch_id", "events", "rejected")
      .withColumn("committed_at", current_timestamp())
    append(spark, row, t(conf, "batch-ledger"))
  }
}
