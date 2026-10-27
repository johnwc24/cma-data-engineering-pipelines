package com.example.cma.deals

import java.math.{BigDecimal => JBigDecimal, RoundingMode}
import java.time.LocalDate
import java.util.regex.Pattern

import scala.util.Try

/**
 * Record-at-a-time version of [[DealRules.rowValidation]] with no Spark dependency, for
 * engines that process one message at a time (Apache Beam / Dataflow).
 *
 * Rule names and semantics deliberately match the Spark rules one for one, so a deal is
 * rejected for the same reasons whichever engine sees it. DealRecordValidatorParitySpec
 * runs both on the same inputs and fails the build if they ever drift apart.
 */
object DealRecordValidator {

  val Fields: Seq[String] = Seq(
    "deal_id", "deal_version", "deal_name", "borrower_party_id", "product_type", "deal_role", "currency",
    "deal_amount", "bank_share_amount", "trade_date", "settlement_date", "booking_system", "deal_status",
    "last_updated_ts"
  )

  private val CodeFields   = Set("product_type", "deal_role", "currency", "deal_status", "booking_system")
  private val Numeric      = Pattern.compile("^-?[0-9]+(\\.[0-9]+)?$")
  private val IsoDate      = Pattern.compile("^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
  private val Digits       = Pattern.compile("^[0-9]+$")
  private val CurrencyCode = Pattern.compile("^[A-Z]{3}$")

  private val ProductTypes = Set("SYNDICATED_LOAN", "BOND", "ECM", "LEVERAGED_LOAN", "PRIVATE_PLACEMENT")
  private val DealRoles    = Set("BOOKRUNNER", "MANDATED_LEAD_ARRANGER", "ARRANGER", "PARTICIPANT", "AGENT")
  private val DealStatuses = Set("MANDATED", "LAUNCHED", "PRICED", "SIGNED", "CLOSED", "CANCELLED")

  /** Same as DealPipeline.standardise: trim, blank -> null, upper-case code fields. */
  def standardise(raw: Map[String, String]): Map[String, String] =
    Fields.map { f =>
      val v = raw.get(f).flatMap(Option(_)).map(_.trim).filter(_.nonEmpty)
      f -> v.map(x => if (CodeFields.contains(f)) x.toUpperCase else x).orNull
    }.toMap

  /** Rule names the record breaks, in rule order; empty means valid. */
  def violations(raw: Map[String, String]): Seq[String] = {
    val r = standardise(raw)
    def v(f: String): Option[String] = Option(r(f))
    def matches(f: String, p: Pattern) = v(f).forall(x => p.matcher(x).find())
    def dec(f: String) = v(f).flatMap(x => Try(new JBigDecimal(x).setScale(2, RoundingMode.HALF_UP)).toOption)
    def date(f: String) = v(f).filter(x => IsoDate.matcher(x).find()).flatMap(x => Try(LocalDate.parse(x)).toOption)

    Seq(
      "not_null:deal_id"           -> v("deal_id").isEmpty,
      "not_null:borrower_party_id" -> v("borrower_party_id").isEmpty,
      "not_null:product_type"      -> v("product_type").isEmpty,
      "not_null:currency"          -> v("currency").isEmpty,
      "not_null:deal_amount"       -> v("deal_amount").isEmpty,
      "not_null:trade_date"        -> v("trade_date").isEmpty,
      "pattern:deal_version"       -> !matches("deal_version", Digits),
      "pattern:currency"           -> !matches("currency", CurrencyCode),
      "pattern:deal_amount"        -> !matches("deal_amount", Numeric),
      "pattern:bank_share_amount"  -> !matches("bank_share_amount", Numeric),
      "pattern:trade_date"         -> !matches("trade_date", IsoDate),
      "pattern:settlement_date"    -> !matches("settlement_date", IsoDate),
      "allowed_values:product_type" -> v("product_type").exists(x => !ProductTypes.contains(x)),
      "allowed_values:deal_role"    -> v("deal_role").exists(x => !DealRoles.contains(x)),
      "allowed_values:deal_status"  -> v("deal_status").exists(x => !DealStatuses.contains(x)),
      // like Spark's to_date(...) IS NULL: a missing date also fails
      "parseable:trade_date"       -> date("trade_date").isEmpty,
      "positive:deal_amount"       -> dec("deal_amount").exists(_.signum() <= 0),
      "share_within_deal:bank_share_amount" -> (for (s <- dec("bank_share_amount"); a <- dec("deal_amount")) yield s.compareTo(a) > 0).getOrElse(false),
      "settlement_after_trade"     -> (for (s <- date("settlement_date"); t <- date("trade_date")) yield s.isBefore(t)).getOrElse(false)
    ).collect { case (name, true) => name }
  }
}
