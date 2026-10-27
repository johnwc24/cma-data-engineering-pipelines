package com.example.cma.deals

import java.sql.Date
import java.time.LocalDate

import com.example.cma.common.quality.{DataQuality, DqReport, Quarantine}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

/**
 * Platform-independent deal transformation logic.
 *
 * The Hadoop, Dataproc and Kafka jobs only differ in how they read and write;
 * they all call `process`, so business rules are implemented and tested once.
 *
 * raw ─► corrupt-record split ─► standardise ─► validate/quarantine ─► type ─► latest version
 *     ─► enrich (party, FX) ─► derive measures ─► audit columns
 */
object DealPipeline {

  final case class Result(
      curated: DataFrame,
      rejected: DataFrame,
      validationReport: DqReport,
      inputRows: Long,
      rejectedRows: Long,
      supersededRows: Long
  )

  def process(
      raw: DataFrame,
      parties: DataFrame,
      fxRates: DataFrame,
      businessDate: LocalDate,
      runId: String,
      sourceSystem: String
  ): Result = {
    val input     = raw.cache()
    val inputRows = input.count()

    // 1. rows Spark could not parse at all
    val parsed = Quarantine.corruptRecords(input, DealSchemas.CorruptRecordCol)

    // 2. standardise, then 3. validate and quarantine
    val standardised = standardise(parsed.valid).cache()
    val report       = DataQuality.evaluate("deal_raw", standardised, DealRules.rowValidation)
    val split        = Quarantine.split(standardised, DealRules.rowValidation)

    // keep the original line of a malformed record so it can be fixed and replayed
    val malformed = parsed.rejected.withColumnRenamed(DealSchemas.CorruptRecordCol, "_raw_record")
    val rejected = unionRejects(malformed, split.rejected)
      .withColumn("business_date", lit(Date.valueOf(businessDate)))
      .withColumn("run_id", lit(runId))
      .withColumn("source_system", lit(sourceSystem))
      .cache()
    val rejectedRows = rejected.count()

    // 4. type and keep the latest version of each deal
    val typed  = toTyped(split.valid).cache()
    val latest = latestVersion(typed)
    val supersededRows = typed.count() - latest.count()

    // 5. enrich and derive
    val curated = deriveMeasures(enrich(latest, parties, fxForDate(fxRates, businessDate)))
      .withColumn("source_system", lit(sourceSystem))
      .withColumn("run_id", lit(runId))
      .withColumn("ingested_at", current_timestamp())
      .withColumn("business_date", lit(Date.valueOf(businessDate)))
      .select(DealSchemas.curatedDeal.fieldNames.map(col): _*)

    Result(curated, rejected, report, inputRows, rejectedRows, supersededRows)
  }

  /** Trim everything, blank -> NULL, upper-case code columns. */
  def standardise(df: DataFrame): DataFrame = {
    val codeColumns = Set("product_type", "deal_role", "currency", "deal_status", "booking_system")
    val cols: Seq[Column] = df.schema.fields.toSeq.map { f =>
      if (f.dataType == StringType) {
        val trimmed = trim(col(f.name))
        val cleaned = when(trimmed === "", lit(null).cast(StringType)).otherwise(trimmed)
        (if (codeColumns.contains(f.name)) upper(cleaned) else cleaned).as(f.name)
      } else col(f.name)
    }
    df.select(cols: _*)
  }

  def toTyped(df: DataFrame): DataFrame =
    df.select(
      col("deal_id"),
      col("deal_version").cast("int").as("deal_version"),
      col("deal_name"),
      col("borrower_party_id"),
      col("product_type"),
      col("deal_role"),
      col("currency"),
      col("deal_amount").cast("decimal(20,2)").as("deal_amount"),
      col("bank_share_amount").cast("decimal(20,2)").as("bank_share_amount"),
      to_date(col("trade_date"), "yyyy-MM-dd").as("trade_date"),
      to_date(col("settlement_date"), "yyyy-MM-dd").as("settlement_date"),
      col("booking_system"),
      col("deal_status"),
      to_timestamp(col("last_updated_ts")).as("last_updated_ts")
    )

  /** Deals are amended in place upstream; keep only the latest version per deal_id. */
  def latestVersion(df: DataFrame): DataFrame = {
    val w = Window
      .partitionBy("deal_id")
      .orderBy(col("deal_version").desc_nulls_last, col("last_updated_ts").desc_nulls_last)
    df.withColumn("__rn", row_number().over(w)).filter(col("__rn") === 1).drop("__rn")
  }

  /** Most recent FX rate on or before the business date, per currency. */
  def fxForDate(fx: DataFrame, businessDate: LocalDate): DataFrame = {
    val w = Window.partitionBy("currency").orderBy(col("rate_date").desc)
    fx.filter(col("rate_date") <= lit(Date.valueOf(businessDate)))
      .withColumn("__rn", row_number().over(w))
      .filter(col("__rn") === 1)
      .select(col("currency").as("fx_currency"), col("rate_to_usd").cast("decimal(18,8)").as("fx_rate_to_usd"))
  }

  /** Left joins: an unknown borrower or missing rate is flagged and caught by the DQ gate, never dropped silently. */
  def enrich(deals: DataFrame, parties: DataFrame, fxForDay: DataFrame): DataFrame = {
    val p = parties.select(
      col("party_id").as("__party_id"),
      col("legal_name").as("borrower_name"),
      col("lei").as("borrower_lei"),
      col("country_code").as("borrower_country"),
      col("sector").as("borrower_sector")
    )
    deals
      .join(broadcast(p), deals("borrower_party_id") === p("__party_id"), "left")
      .withColumn("party_resolved", col("__party_id").isNotNull)
      .drop("__party_id")
      .join(broadcast(fxForDay), col("currency") === col("fx_currency"), "left")
      .drop("fx_currency")
  }

  def deriveMeasures(df: DataFrame): DataFrame =
    df.withColumn(
        "bank_share_pct",
        when(col("deal_amount") > 0, round(col("bank_share_amount") / col("deal_amount") * 100, 4))
          .cast("decimal(9,4)")
      )
      .withColumn("deal_amount_usd", round(col("deal_amount") * col("fx_rate_to_usd"), 2).cast("decimal(20,2)"))
      .withColumn("bank_share_usd", round(col("bank_share_amount") * col("fx_rate_to_usd"), 2).cast("decimal(20,2)"))

  private def unionRejects(a: DataFrame, b: DataFrame): DataFrame =
    a.unionByName(b, allowMissingColumns = true)
}
