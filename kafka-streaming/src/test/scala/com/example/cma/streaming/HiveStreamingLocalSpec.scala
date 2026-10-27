package com.example.cma.streaming

import java.io.File
import java.nio.file.Files
import java.sql.Timestamp

import com.example.cma.common.config.{JobArgs, PipelineConfig}
import com.example.cma.deals.DealSchemas
import com.example.cma.testing.EmbeddedHiveSession
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/** Kafka -> Hive streaming pipeline against an embedded Hive metastore and an in-memory Kafka source. */
class HiveStreamingLocalSpec extends AnyFunSuite with Matchers with EmbeddedHiveSession {
  import spark.implicits._
  implicit lazy val sqlCtx: org.apache.spark.sql.SQLContext = spark.sqlContext

  private val sample = new File("../data/sample").getCanonicalFile

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    runHql(new File("../hadoop-batch/src/main/resources/hive/ddl.hql"), Map("env_suffix" -> "_dev"))
    spark.read.option("header", "true").schema(DealSchemas.party).csv(s"$sample/reference/parties.csv")
      .write.mode("overwrite").insertInto("cma_ref_dev.party")
    spark.read.option("header", "true").schema(DealSchemas.fxRate).csv(s"$sample/reference/fx_rates.csv")
      .write.mode("overwrite").insertInto("cma_ref_dev.fx_rate")
  }

  test("stream lands raw events, curated deals, rejects and the batch ledger in Hive") {
    val input = MemoryStream[KafkaRecord]
    val dlq   = ArrayBuffer.empty[Row]
    val job = new KafkaToHiveStreaming {
      override protected def readSource(spark: SparkSession, conf: Config): DataFrame = input.toDF()
      override protected def publishDeadLetters(conf: Config, dl: DataFrame): Unit = dlq ++= dl.collect()
    }
    val args = JobArgs(env = "dev", runId = "hive-stream", overrides = Map(
      "stream.checkpoint" -> Files.createTempDirectory("ckpt").toString,
      "stream.trigger-seconds" -> "1", "retry.initial-delay-ms" -> "1"))
    val conf = PipelineConfig.load(args, "kafka-hadoop").toOption.get

    val lines = {
      val s = Source.fromFile(s"$sample/kafka/deal_events.jsonl", "UTF-8")
      try s.getLines().filter(_.nonEmpty).toList finally s.close()
    }
    val query = job.startQuery(spark, args, conf)
    try {
      input.addData(lines.zipWithIndex.map { case (l, i) =>
        KafkaRecord("cma.deal.events", 0, i.toLong, new Timestamp(0L), l.getBytes("UTF-8"))
      })
      query.processAllAvailable()
    } finally query.stop()

    spark.table("cma_raw_dev.deal_event").count() shouldBe 3
    spark.table("cma_curated_dev.deal_intraday").count() shouldBe 1
    spark.table("cma_quarantine_dev.deal_reject").count() shouldBe 2
    spark.table("cma_audit_dev.stream_batch").count() shouldBe 1
    dlq.size shouldBe 2
  }
}
