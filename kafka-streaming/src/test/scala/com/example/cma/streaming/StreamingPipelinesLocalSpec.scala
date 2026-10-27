package com.example.cma.streaming

import java.nio.file.Files
import java.sql.{Date, Timestamp}

import com.example.cma.common.config.{JobArgs, PipelineConfig}
import com.example.cma.common.util.RetryPolicy
import com.example.cma.deals.DealSchemas
import com.example.cma.gcp.InMemoryWarehouse
import com.example.cma.testing.SparkTestSession
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/** Shape of a record from Spark's Kafka source. */
final case class KafkaRecord(topic: String, partition: Int, offset: Long, timestamp: Timestamp, value: Array[Byte])

/**
 * Runs the Managed Kafka -> BigQuery streaming pipeline locally: Kafka is a MemoryStream,
 * BigQuery is the InMemoryWarehouse and the DLQ topic is captured in a buffer.
 * The real query, checkpointing, foreachBatch, batch ledger and DealPipeline all run.
 */
class StreamingPipelinesLocalSpec extends AnyFunSuite with Matchers with SparkTestSession {
  import spark.implicits._
  implicit lazy val sqlCtx: org.apache.spark.sql.SQLContext = spark.sqlContext

  private lazy val events: Seq[String] = {
    val src = Source.fromFile("../data/sample/kafka/deal_events.jsonl", "UTF-8")
    try src.getLines().filter(_.nonEmpty).toList finally src.close()
  }

  private def records(lines: Seq[String], startOffset: Long): Seq[KafkaRecord] =
    lines.zipWithIndex.map { case (l, i) =>
      KafkaRecord("cma.deal.events", 0, startOffset + i, new Timestamp(System.currentTimeMillis()), l.getBytes("UTF-8"))
    }

  private def seededWarehouse(): InMemoryWarehouse = {
    val wh = new InMemoryWarehouse(spark)
    wh.seed("reference.party", Seq(
      ("PTY-001", "Acme Holdings plc", "LEI1", "GB", "Industrials", true),
      ("PTY-002", "Globex SA", "LEI2", "FR", "Utilities", true)
    ).toDF("party_id", "legal_name", "lei", "country_code", "sector", "is_active"))
    wh.seed("reference.fx_rate", Seq(
      ("GBP", BigDecimal("1.27"), Date.valueOf("2026-01-01")),
      ("EUR", BigDecimal("1.08"), Date.valueOf("2026-01-01"))
    ).toDF("currency", "rate_to_usd", "rate_date"))
    val ledgerSchema = StructType(Seq(
      StructField("query_name", StringType), StructField("batch_id", LongType), StructField("events", LongType),
      StructField("rejected", LongType), StructField("committed_at", TimestampType)))
    wh.seed("audit.stream_batch", spark.createDataFrame(spark.sparkContext.emptyRDD[Row], ledgerSchema))
    wh
  }

  private def localConf(): (JobArgs, Config) = {
    val args = JobArgs(env = "dev", runId = "stream-test", overrides = Map(
      "stream.checkpoint"      -> Files.createTempDirectory("ckpt").toString,
      "stream.trigger-seconds" -> "1",
      "retry.initial-delay-ms" -> "1"
    ))
    (args, PipelineConfig.load(args, "kafka-gcp").toOption.get)
  }

  private class LocalStreamJob(wh: InMemoryWarehouse, input: MemoryStream[KafkaRecord])
      extends KafkaToBigQueryStreaming((_, _, _) => wh) {
    val deadLetters = ArrayBuffer.empty[Row]
    override protected def readSource(spark: SparkSession, conf: Config): DataFrame = input.toDF()
    override protected def publishDeadLetters(conf: Config, dl: DataFrame): Unit = deadLetters ++= dl.collect()
  }

  test("stream: good events reach BigQuery staging, bad ones go to the DLQ with reasons") {
    val wh    = seededWarehouse()
    val input = MemoryStream[KafkaRecord]
    val job   = new LocalStreamJob(wh, input)
    val (args, conf) = localConf()

    val query = job.startQuery(spark, args, conf)
    try {
      input.addData(records(events, 0))
      query.processAllAvailable()

      wh.count("raw.deal_event_stream") shouldBe 3
      wh.count("staging.deal_intraday") shouldBe 1
      wh.count("quarantine.deal_reject") shouldBe 2
      job.deadLetters.size shouldBe 2
      wh.count("audit.stream_batch") shouldBe 1

      // second micro-batch: one more valid event
      val next = events.head.replace("e-0001", "e-0100").replace("DL-2001", "DL-2100")
      input.addData(records(Seq(next), 3))
      query.processAllAvailable()
      wh.count("staging.deal_intraday") shouldBe 2
      wh.count("audit.stream_batch") shouldBe 2
    } finally query.stop()
  }

  test("stream: a replayed micro-batch is skipped by the batch ledger (no double load)") {
    val wh  = seededWarehouse()
    val job = new LocalStreamJob(wh, MemoryStream[KafkaRecord])
    val (_, conf) = localConf()
    val batch = records(events.take(1), 0).toDF()
    val retry = RetryPolicy(maxAttempts = 1, initialDelayMs = 0)

    job.processBatch(spark, conf, retry, DealEvents.parse(batch), batchId = 7L)
    job.processBatch(spark, conf, retry, DealEvents.parse(batch), batchId = 7L) // replay after a crash

    wh.count("staging.deal_intraday") shouldBe 1
    wh.count("audit.stream_batch") shouldBe 1
  }

  test("stream: a failing sink stops the query instead of silently dropping data") {
    val wh = new InMemoryWarehouse(spark) {
      override def append(df: DataFrame, table: String): Unit =
        if (table == "staging.deal_intraday") throw new IllegalStateException("permanent sink failure")
        else super.append(df, table)
    }
    Seq("reference.party", "reference.fx_rate", "audit.stream_batch").foreach(t => wh.seed(t, seededWarehouse().tables(t)))
    val input = MemoryStream[KafkaRecord]
    val job   = new LocalStreamJob(wh, input)
    val (args, conf) = localConf()

    val query = job.startQuery(spark, args, conf)
    input.addData(records(events.take(1), 0))
    an[Exception] should be thrownBy query.processAllAvailable()
    query.isActive shouldBe false
    wh.count("audit.stream_batch") shouldBe 0 // batch not recorded, so it is reprocessed on restart
  }

  test("raw schema of the stream matches the batch raw deal schema") {
    val parsed = DealEvents.parse(records(events, 0).toDF())
    DealSchemas.rawDeal.fieldNames.foreach(f => parsed.columns should contain(f))
  }
}
