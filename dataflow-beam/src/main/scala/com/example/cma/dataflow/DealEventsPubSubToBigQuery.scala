package com.example.cma.dataflow

import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant

import com.example.cma.deals.DealRecordValidator
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.google.api.services.bigquery.model.TableRow
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, Method, WriteDisposition}
import org.apache.beam.sdk.io.gcp.bigquery.{BigQueryIO, BigQueryStorageApiInsertError, TableRowJsonCoder}
import org.apache.beam.sdk.io.gcp.pubsub.{PubsubIO, PubsubMessage, PubsubMessageWithAttributesAndMessageIdCoder, PubsubMessageWithAttributesCoder}
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.options._
import org.apache.beam.sdk.transforms._
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.values._
import org.joda.time.Duration

import scala.collection.JavaConverters._
import scala.util.Try

/** Pipeline options (passed as --name=value, or as Flex Template parameters). */
trait DealStreamOptions extends PipelineOptions with StreamingOptions {
  @Description("Pub/Sub subscription: projects/<project>/subscriptions/<name>")
  @Validation.Required
  def getInputSubscription: String
  def setInputSubscription(value: String): Unit

  @Description("BigQuery table for validated events: <project>:<dataset>.<table>")
  @Validation.Required
  def getOutputTable: String
  def setOutputTable(value: String): Unit

  @Description("Pub/Sub dead-letter topic: projects/<project>/topics/<name>")
  @Validation.Required
  def getDeadLetterTopic: String
  def setDeadLetterTopic(value: String): Unit

  @Description("How long an event_id is remembered for de-duplication, in minutes")
  @Default.Integer(10)
  def getDedupMinutes: Int
  def setDedupMinutes(value: Int): Unit
}

/**
 * Pub/Sub ─► parse + validate (shared deal rules) ─► de-duplicate by event_id ─► BigQuery
 *                     │                                                      │
 *                     └──────────── dead-letter topic (with reasons) ◄──────┘ failed inserts
 *
 * Nothing is dropped: every message ends up in BigQuery or on the dead-letter topic,
 * with the reasons as message attributes so ops can fix and replay it.
 */
object DealEventsPubSubToBigQuery {

  val ValidTag: TupleTag[TableRow]           = new TupleTag[TableRow]("valid")
  val DeadLetterTag: TupleTag[PubsubMessage] = new TupleTag[PubsubMessage]("dead-letter")

  def main(args: Array[String]): Unit = {
    PipelineOptionsFactory.register(classOf[DealStreamOptions])
    val options = PipelineOptionsFactory.fromArgs(args: _*).withValidation().as(classOf[DealStreamOptions])
    options.setStreaming(true)
    val pipeline = Pipeline.create(options)
    build(pipeline, options)
    pipeline.run()
  }

  def build(pipeline: Pipeline, options: DealStreamOptions): Unit = {
    val messages = pipeline.apply(
      "ReadPubSub",
      PubsubIO.readMessagesWithAttributesAndMessageId().fromSubscription(options.getInputSubscription)
    )

    val parsed = messages.apply("ParseAndValidate", new ParseAndValidate(options.getInputSubscription))

    val valid = parsed.get(ValidTag).apply("DedupByEventId", dedupByEventId(options.getDedupMinutes))

    val written = valid.apply(
      "WriteBigQuery",
      BigQueryIO.writeTableRows()
        .to(options.getOutputTable)
        .withMethod(Method.STORAGE_API_AT_LEAST_ONCE)
        .withCreateDisposition(CreateDisposition.CREATE_NEVER) // tables are owned by bigquery/ddl
        .withWriteDisposition(WriteDisposition.WRITE_APPEND)
    )

    val source = options.getInputSubscription // plain String: the lambda below must not capture `options`
    val failedInserts = written.getFailedStorageApiInserts
      .apply("FailedInsertsToDeadLetter", MapElements.via(new SimpleFunction[BigQueryStorageApiInsertError, PubsubMessage]() {
        override def apply(e: BigQueryStorageApiInsertError): PubsubMessage =
          deadLetter(e.getRow.toString.getBytes(UTF_8), Seq(s"bigquery_insert:${e.getErrorMessage}"), source, null)
      }))
      .setCoder(PubsubMessageWithAttributesCoder.of())

    PCollectionList.of(parsed.get(DeadLetterTag)).and(failedInserts)
      .apply("MergeDeadLetters", Flatten.pCollections[PubsubMessage]())
      .apply("WriteDeadLetterTopic", PubsubIO.writeMessages().to(options.getDeadLetterTopic))
  }

  /** Producers retry, so the same event can arrive twice; keep the first within the window. */
  def dedupByEventId(minutes: Int): PTransform[PCollection[TableRow], PCollection[TableRow]] =
    Deduplicate
      .withRepresentativeValueFn[TableRow, String](new SerializableFunction[TableRow, String] {
        override def apply(row: TableRow): String = String.valueOf(row.get("event_id"))
      })
      .withRepresentativeType(TypeDescriptors.strings())
      .withDuration(Duration.standardMinutes(minutes.toLong))

  def deadLetter(payload: Array[Byte], reasons: Seq[String], source: String, messageId: String): PubsubMessage =
    new PubsubMessage(
      payload,
      Map(
        "reject_reasons" -> reasons.mkString("|"),
        "source"         -> String.valueOf(source),
        "source_message_id" -> String.valueOf(messageId),
        "rejected_at"    -> Instant.now().toString
      ).asJava
    )
}

/** Parses JSON deal events and applies the shared deal rules; outputs valid rows and dead letters. */
final class ParseAndValidate(source: String) extends PTransform[PCollection[PubsubMessage], PCollectionTuple] {
  import DealEventsPubSubToBigQuery._

  override def expand(input: PCollection[PubsubMessage]): PCollectionTuple = {
    val out = input.apply(
      ParDo.of(new ParseValidateFn(source)).withOutputTags(ValidTag, TupleTagList.of(DeadLetterTag))
    )
    out.get(ValidTag).setCoder(TableRowJsonCoder.of())
    out.get(DeadLetterTag).setCoder(PubsubMessageWithAttributesCoder.of())
    out
  }
}

final class ParseValidateFn(source: String) extends DoFn[PubsubMessage, TableRow] {
  import DealEventsPubSubToBigQuery._

  @transient private lazy val mapper = new ObjectMapper()
  private val validCount   = Metrics.counter("cma", "valid_events")
  private val invalidCount = Metrics.counter("cma", "dead_lettered_events")

  @ProcessElement
  def process(c: DoFn[PubsubMessage, TableRow]#ProcessContext): Unit = {
    val msg     = c.element()
    val payload = msg.getPayload

    val event: Option[JsonNode] =
      Try(mapper.readTree(payload)).toOption.filter(n => n != null && n.hasNonNull("event_id") && n.path("deal").isObject)

    event match {
      case None =>
        invalidCount.inc()
        c.output(DeadLetterTag, deadLetter(payload, Seq("malformed_record"), source, msg.getMessageId))

      case Some(e) =>
        val deal = e.path("deal")
        val raw = DealRecordValidator.Fields.map { f =>
          f -> (if (deal.hasNonNull(f)) deal.get(f).asText() else null)
        }.toMap
        DealRecordValidator.violations(raw) match {
          case Nil =>
            validCount.inc()
            c.output(toRow(DealRecordValidator.standardise(raw), e, msg))
          case reasons =>
            invalidCount.inc()
            c.output(DeadLetterTag, deadLetter(payload, reasons, source, msg.getMessageId))
        }
    }
  }

  private def toRow(deal: Map[String, String], event: JsonNode, msg: PubsubMessage): TableRow = {
    val row = new TableRow()
    deal.foreach { case (k, v) => if (v != null) row.set(k, v) }
    row
      .set("event_id", event.get("event_id").asText())
      .set("event_type", event.path("event_type").asText(null))
      .set("event_ts", event.path("event_ts").asText(null))
      .set("pubsub_message_id", msg.getMessageId)
      .set("processed_at", Instant.now().toString)
  }
}
