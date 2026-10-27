package com.example.cma.streaming

import com.example.cma.deals.DealSchemas
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Deal events published by the booking systems to Kafka topic `cma.deal.events`:
 *
 *   key   = deal_id
 *   value = {"event_id": "...", "event_type": "DEAL_UPSERT", "event_ts": "...", "deal": { raw deal fields }}
 *
 * Parsing maps each Kafka record onto the raw deal schema used by the batch pipelines,
 * putting the original payload in `_corrupt_record` when the JSON is unreadable, so the
 * shared DealPipeline quarantines it exactly like a malformed CSV line.
 */
object DealEvents {

  val eventSchema: StructType = StructType(Seq(
    StructField("event_id", StringType),
    StructField("event_type", StringType),
    StructField("event_ts", StringType),
    StructField("deal", StructType(DealSchemas.rawDealFields.map(StructField(_, StringType))))
  ))

  /** Kafka source columns -> raw deal rows + event metadata. */
  def parse(kafka: DataFrame): DataFrame = {
    val payload = col("value").cast("string")
    val parsed  = kafka
      .select(
        payload.as("__payload"),
        from_json(payload, eventSchema, Map("mode" -> "PERMISSIVE")).as("__event"),
        col("topic").as("kafka_topic"),
        col("partition").as("kafka_partition"),
        col("offset").as("kafka_offset"),
        col("timestamp").as("kafka_timestamp")
      )

    val unreadable: Column = col("__event").isNull || col("__event.deal").isNull || col("__event.event_id").isNull

    val dealCols = DealSchemas.rawDealFields.map(f => when(unreadable, lit(null).cast(StringType)).otherwise(col(s"__event.deal.$f")).as(f))
    parsed.select(
      dealCols ++ Seq(
        when(unreadable, col("__payload")).as(DealSchemas.CorruptRecordCol),
        col("__event.event_id").as("event_id"),
        col("__event.event_type").as("event_type"),
        to_timestamp(col("__event.event_ts")).as("event_ts"),
        col("kafka_topic"),
        col("kafka_partition"),
        col("kafka_offset"),
        col("kafka_timestamp")
      ): _*
    )
  }

  /** Within a micro-batch keep one record per event_id (producers may retry and duplicate). */
  def dedupeEvents(df: DataFrame): DataFrame = {
    val keyed  = df.filter(col("event_id").isNotNull).dropDuplicates("event_id")
    val nokey  = df.filter(col("event_id").isNull)
    keyed.unionByName(nokey)
  }

  /**
   * Build dead-letter records for the DLQ topic: original deal as JSON value,
   * rejection reasons and source coordinates as Kafka headers for triage and replay.
   */
  def toDeadLetters(rejected: DataFrame, sourceTopic: String): DataFrame = {
    val dealStruct = struct(DealSchemas.rawDealFields.filter(rejected.columns.contains).map(col): _*)
    val value = when(col("_raw_record").isNotNull, col("_raw_record")).otherwise(to_json(dealStruct))
    def header(k: String, v: Column) = struct(lit(k).as("key"), v.cast("binary").as("value"))
    rejected.select(
      coalesce(col("deal_id"), lit("unknown")).cast("binary").as("key"),
      value.cast("binary").as("value"),
      array(
        header("reject_reasons", concat_ws("|", col("_reject_reasons"))),
        header("source_topic", lit(sourceTopic)),
        header("run_id", col("run_id"))
      ).as("headers")
    )
  }
}
