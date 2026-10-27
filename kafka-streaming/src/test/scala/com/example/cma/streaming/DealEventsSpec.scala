package com.example.cma.streaming

import java.sql.{Date, Timestamp}
import java.time.LocalDate

import com.example.cma.common.config.{JobArgs, PipelineConfig}
import com.example.cma.deals.DealPipeline
import com.example.cma.testing.SparkTestSession
import org.apache.spark.sql.DataFrame
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.io.Source

class DealEventsSpec extends AnyFunSuite with Matchers with SparkTestSession {
  import spark.implicits._

  /** Simulates the Kafka source schema using data/sample/kafka/deal_events.jsonl. */
  private def kafkaBatch(lines: Seq[String]): DataFrame =
    lines.zipWithIndex
      .map { case (l, i) => ("cma.deal.events", 0, i.toLong, new Timestamp(0L), l.getBytes("UTF-8")) }
      .toDF("topic", "partition", "offset", "timestamp", "value")

  private lazy val sampleLines: Seq[String] = {
    val src = Source.fromFile("../data/sample/kafka/deal_events.jsonl", "UTF-8")
    try src.getLines().filter(_.nonEmpty).toList finally src.close()
  }

  private lazy val parties = Seq(("PTY-001", "Acme", "L1", "GB", "Industrials", true), ("PTY-002", "Globex", "L2", "FR", "Utilities", true))
    .toDF("party_id", "legal_name", "lei", "country_code", "sector", "is_active")
  private lazy val fx = Seq(("GBP", BigDecimal("1.27"), Date.valueOf("2026-10-26")), ("EUR", BigDecimal("1.08"), Date.valueOf("2026-10-26")))
    .toDF("currency", "rate_to_usd", "rate_date")

  test("parses events and flags unreadable JSON as corrupt") {
    val parsed = DealEvents.parse(kafkaBatch(sampleLines)).collect()
    parsed.length shouldBe 3
    parsed.count(_.getAs[String]("_corrupt_record") != null) shouldBe 1
    parsed.flatMap(r => Option(r.getAs[String]("deal_id"))).toSet shouldBe Set("DL-2001", "DL-2002")
  }

  test("shared pipeline loads good events and routes bad ones to dead letters with reasons") {
    val events = DealEvents.parse(kafkaBatch(sampleLines))
    val result = DealPipeline.process(events, parties, fx, LocalDate.parse("2026-10-26"), "batch-1", "KAFKA")
    result.curated.select("deal_id").as[String].collect().toSeq shouldBe Seq("DL-2001")
    result.rejectedRows shouldBe 2

    val dlq = DealEvents.toDeadLetters(result.rejected, "cma.deal.events")
    dlq.columns shouldBe Array("key", "value", "headers")
    val reasons = dlq.selectExpr("CAST(headers[0].value AS STRING)").as[String].collect().toSet
    reasons.exists(_.contains("positive:deal_amount")) shouldBe true
    reasons should contain("malformed_record")
  }

  test("duplicate events within a batch are collapsed by event_id") {
    val doubled = DealEvents.parse(kafkaBatch(sampleLines.take(1) ++ sampleLines.take(1)))
    DealEvents.dedupeEvents(doubled).count() shouldBe 1
  }

  test("Hadoop and GCP streaming configs resolve for every environment") {
    for {
      (job, resource) <- Seq(KafkaToHiveStreamingJob -> "kafka-hadoop", KafkaToBigQueryStreamingJob -> "kafka-gcp")
      env             <- Seq("dev", "uat", "prod")
    } {
      val conf = PipelineConfig.load(JobArgs(env = env), resource)
      withClue(s"$resource/$env: ") {
        conf.flatMap(c => PipelineConfig.requirePaths(c, job.requiredConfig: _*)).isRight shouldBe true
      }
    }
  }
}
