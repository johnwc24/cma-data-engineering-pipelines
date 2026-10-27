package com.example.cma.deals

import com.example.cma.common.quality._
import org.apache.spark.sql.DataFrame

/** Data-quality rules for the deal pipeline, grouped by the stage they protect. */
object DealRules {
  private val Numeric = "^-?[0-9]+(\\.[0-9]+)?$"
  private val IsoDate = "^[0-9]{4}-[0-9]{2}-[0-9]{2}$"

  /**
   * Row-level validation on standardised raw strings. Rows that break any of these
   * are quarantined (not loaded) with the rule names as the rejection reasons.
   */
  val rowValidation: Seq[RowRule] = Seq(
    NotNull("deal_id"),
    NotNull("borrower_party_id"),
    NotNull("product_type"),
    NotNull("currency"),
    NotNull("deal_amount"),
    NotNull("trade_date"),
    MatchesPattern("deal_version", "^[0-9]+$"),
    MatchesPattern("currency", "^[A-Z]{3}$"),
    MatchesPattern("deal_amount", Numeric),
    MatchesPattern("bank_share_amount", Numeric),
    MatchesPattern("trade_date", IsoDate),
    MatchesPattern("settlement_date", IsoDate),
    AllowedValues("product_type", DealSchemas.ProductTypes),
    AllowedValues("deal_role", DealSchemas.DealRoles),
    AllowedValues("deal_status", DealSchemas.DealStatuses),
    Expectation("parseable:trade_date", "to_date(trade_date, 'yyyy-MM-dd') IS NULL", "validity"),
    Expectation("positive:deal_amount", "CAST(deal_amount AS DECIMAL(20,2)) <= 0", "validity"),
    Expectation(
      "share_within_deal:bank_share_amount",
      "CAST(bank_share_amount AS DECIMAL(20,2)) > CAST(deal_amount AS DECIMAL(20,2))"
    ),
    Expectation(
      "settlement_after_trade",
      "to_date(settlement_date, 'yyyy-MM-dd') < to_date(trade_date, 'yyyy-MM-dd')"
    )
  )

  /** Gate on the enriched dataset immediately before it is written. Error-severity rules block the write. */
  def curatedGate(parties: DataFrame, minRows: Long): Seq[DqRule] = Seq(
    MinRowCount(minRows),
    Unique(Seq("deal_id")),
    NotNull("fx_rate_to_usd"),
    // Unknown borrowers are loaded but flagged; more than 2% suggests a broken reference feed.
    ReferentialIntegrity("borrower_party_id", parties, "party_id", severity = Severity.Warn, maxFailureRatio = 0.02),
    InRange("bank_share_pct", min = Some(0.0), max = Some(100.0))
  )
}
