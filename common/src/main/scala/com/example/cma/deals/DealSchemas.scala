package com.example.cma.deals

import org.apache.spark.sql.types._

/**
 * Schemas for the syndicated-deal use case.
 *
 * Raw feeds are read as strings on purpose: a typed read silently turns a bad
 * value into NULL, whereas reading strings lets us detect, quarantine and report
 * the exact unparseable value back to the data owner.
 */
object DealSchemas {

  val CorruptRecordCol = "_corrupt_record"

  val rawDealFields: Seq[String] = Seq(
    "deal_id",
    "deal_version",
    "deal_name",
    "borrower_party_id",
    "product_type",
    "deal_role",
    "currency",
    "deal_amount",
    "bank_share_amount",
    "trade_date",
    "settlement_date",
    "booking_system",
    "deal_status",
    "last_updated_ts"
  )

  /** Raw CSV/JSON schema, plus the column Spark fills for malformed rows in PERMISSIVE mode. */
  val rawDeal: StructType =
    StructType(rawDealFields.map(StructField(_, StringType, nullable = true)) :+
      StructField(CorruptRecordCol, StringType, nullable = true))

  val party: StructType = StructType(Seq(
    StructField("party_id", StringType, nullable = false),
    StructField("legal_name", StringType),
    StructField("lei", StringType),
    StructField("country_code", StringType),
    StructField("sector", StringType),
    StructField("is_active", BooleanType)
  ))

  val fxRate: StructType = StructType(Seq(
    StructField("currency", StringType, nullable = false),
    StructField("rate_to_usd", DecimalType(18, 8), nullable = false),
    StructField("rate_date", DateType, nullable = false)
  ))

  /** Curated deal table contract (Hive `cma_curated.deal` / BigQuery `cma_curated.deal`). */
  val curatedDeal: StructType = StructType(Seq(
    StructField("deal_id", StringType, nullable = false),
    StructField("deal_version", IntegerType, nullable = false),
    StructField("deal_name", StringType),
    StructField("borrower_party_id", StringType, nullable = false),
    StructField("borrower_name", StringType),
    StructField("borrower_lei", StringType),
    StructField("borrower_country", StringType),
    StructField("borrower_sector", StringType),
    StructField("party_resolved", BooleanType, nullable = false),
    StructField("product_type", StringType, nullable = false),
    StructField("deal_role", StringType),
    StructField("currency", StringType, nullable = false),
    StructField("deal_amount", DecimalType(20, 2), nullable = false),
    StructField("bank_share_amount", DecimalType(20, 2)),
    StructField("bank_share_pct", DecimalType(9, 4)),
    StructField("fx_rate_to_usd", DecimalType(18, 8)),
    StructField("deal_amount_usd", DecimalType(20, 2)),
    StructField("bank_share_usd", DecimalType(20, 2)),
    StructField("trade_date", DateType, nullable = false),
    StructField("settlement_date", DateType),
    StructField("booking_system", StringType),
    StructField("deal_status", StringType),
    StructField("last_updated_ts", TimestampType),
    StructField("source_system", StringType),
    StructField("run_id", StringType),
    StructField("ingested_at", TimestampType),
    StructField("business_date", DateType, nullable = false)
  ))

  val ProductTypes: Seq[String] = Seq("SYNDICATED_LOAN", "BOND", "ECM", "LEVERAGED_LOAN", "PRIVATE_PLACEMENT")
  val DealRoles: Seq[String]    = Seq("BOOKRUNNER", "MANDATED_LEAD_ARRANGER", "ARRANGER", "PARTICIPANT", "AGENT")
  val DealStatuses: Seq[String] = Seq("MANDATED", "LAUNCHED", "PRICED", "SIGNED", "CLOSED", "CANCELLED")
}
