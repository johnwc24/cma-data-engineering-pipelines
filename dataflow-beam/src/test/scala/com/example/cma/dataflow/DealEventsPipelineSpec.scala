package com.example.cma.dataflow

import java.lang.{Iterable => JIterable}
import java.nio.charset.StandardCharsets.UTF_8

import com.google.api.services.bigquery.model.TableRow
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder
import org.apache.beam.sdk.io.gcp.pubsub.{PubsubMessage, PubsubMessageWithAttributesAndMessageIdCoder}
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.transforms.{Create, SerializableFunction}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._
import scala.io.Source

/**
 * Runs the Dataflow pipeline's transforms on Beam's DirectRunner (in-process, no GCP):
 * Pub/Sub input is a Create of real sample messages, outputs are checked with PAssert.
 */
class DealEventsPipelineSpec extends AnyFunSuite with Matchers {

  private val sample: Seq[String] = {
    val src = Source.fromFile("../data/sample/kafka/deal_events.jsonl", "UTF-8")
    try src.getLines().filter(_.nonEmpty).toList finally src.close()
  }

  private def message(json: String, id: String): PubsubMessage =
    new PubsubMessage(json.getBytes(UTF_8), Map("origin" -> "test").asJava, id)

  private def pipeline(): Pipeline = Pipeline.create(PipelineOptionsFactory.create()) // DirectRunner

  import BeamChecks.{check, expect}

  test("valid events become typed BigQuery rows; invalid and malformed ones go to the dead-letter output") {
    val p = pipeline()
    val input = p.apply(Create.of(sample.zipWithIndex.map { case (l, i) => message(l, s"m$i") }.asJava)
      .withCoder(PubsubMessageWithAttributesAndMessageIdCoder.of()))
    val out = input.apply(new ParseAndValidate("projects/p/subscriptions/deals"))

    PAssert.that(out.get(DealEventsPubSubToBigQuery.ValidTag)).satisfies(check[TableRow] { rows =>
      expect(rows.map(_.get("deal_id")), Seq("DL-2001"))
      expect(rows.head.get("currency"), "GBP")
      expect(rows.head.get("event_id"), "e-0001")
      expect(rows.head.get("pubsub_message_id"), "m0")
    })
    PAssert.that(out.get(DealEventsPubSubToBigQuery.DeadLetterTag)).satisfies(check[PubsubMessage] { dl =>
      val reasons = dl.map(_.getAttribute("reject_reasons")).toSet
      expect(reasons.contains("malformed_record"), true)
      expect(reasons.exists(_.contains("positive:deal_amount")), true)
      expect(dl.map(_.getAttribute("source_message_id")).toSet, Set("m1", "m2"))
    })
    p.run().waitUntilFinish()
  }

  test("standardisation is applied before rows are written (trimmed, upper-cased codes)") {
    val p = pipeline()
    val messy = sample.head.replace("\"GBP\"", "\" gbp \"").replace("\"ARRANGER\"", "\"arranger\"")
    val out = p.apply(Create.of(message(messy, "m9")).withCoder(PubsubMessageWithAttributesAndMessageIdCoder.of()))
      .apply(new ParseAndValidate("s"))
    PAssert.that(out.get(DealEventsPubSubToBigQuery.ValidTag)).satisfies(check[TableRow] { rows =>
      expect(rows.map(r => (r.get("currency"), r.get("deal_role"))), Seq(("GBP", "ARRANGER")))
    })
    p.run().waitUntilFinish()
  }

  test("duplicate deliveries of the same event_id are written once") {
    val p = pipeline()
    val row = new TableRow().set("event_id", "e-1").set("deal_id", "D1")
    val rows = p.apply(Create.of(Seq(row, row.clone(), new TableRow().set("event_id", "e-2")).asJava).withCoder(TableRowJsonCoder.of()))
      .apply(DealEventsPubSubToBigQuery.dedupByEventId(10))
    PAssert.that(rows).satisfies(check[TableRow] { out =>
      expect(out.map(_.get("event_id").toString).sorted, Seq("e-1", "e-2"))
    })
    p.run().waitUntilFinish()
  }

  test("pipeline options validate required parameters") {
    PipelineOptionsFactory.register(classOf[DealStreamOptions])
    an[IllegalArgumentException] should be thrownBy
      PipelineOptionsFactory.fromArgs("--outputTable=p:d.t").withValidation().as(classOf[DealStreamOptions])
    val ok = PipelineOptionsFactory
      .fromArgs("--inputSubscription=projects/p/subscriptions/s", "--outputTable=p:d.t", "--deadLetterTopic=projects/p/topics/dlq")
      .withValidation().as(classOf[DealStreamOptions])
    ok.getDedupMinutes shouldBe 10
  }
}

/**
 * PAssert ships its check functions to the runner, so they must be serializable and must not
 * capture the (non-serializable) test suite - hence plain functions in a top-level object.
 */
object BeamChecks {
  def check[T](f: Seq[T] => Unit): SerializableFunction[JIterable[T], Void] =
    new SerializableFunction[JIterable[T], Void] {
      override def apply(in: JIterable[T]): Void = { f(in.asScala.toList); null }
    }

  def expect(actual: Any, expected: Any): Unit =
    if (actual != expected) throw new AssertionError(s"expected <$expected> but was <$actual>")
}
