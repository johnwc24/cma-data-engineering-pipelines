package com.example.cma.deals

import java.sql.Date
import java.time.LocalDate

import com.example.cma.common.quality.{DataQuality, Quarantine}
import com.example.cma.testing.SparkTestSession
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class DealPipelineSpec extends AnyFunSuite with Matchers with SparkTestSession {
  import spark.implicits._

  private val bd = LocalDate.parse("2026-10-26")

  private def raw(rows: Seq[Seq[String]]): DataFrame = {
    val padded = rows.map(r => Row.fromSeq(r.padTo(DealSchemas.rawDeal.length, null)))
    spark.createDataFrame(padded.asJava, DealSchemas.rawDeal)
  }

  //           id     ver  name        party   product            role           ccy    amount       share       trade         settle        booking  status    updated
  private def deal(id: String, ver: String = "1", party: String = "P1", ccy: String = "GBP",
                   amount: String = "1000000", share: String = "250000",
                   trade: String = "2026-10-20", settle: String = "2026-10-27",
                   product: String = "SYNDICATED_LOAN"): Seq[String] =
    Seq(id, ver, s"Deal $id", party, product, "BOOKRUNNER", ccy, amount, share, trade, settle, "LOANIQ", "SIGNED",
      s"2026-10-2${ver.take(1)} 10:00:00")

  private lazy val parties = Seq(
    ("P1", "Acme Holdings plc", "LEI000000000000000P1", "GB", "Industrials", true),
    ("P2", "Globex SA", "LEI000000000000000P2", "FR", "Utilities", true)
  ).toDF("party_id", "legal_name", "lei", "country_code", "sector", "is_active")

  private lazy val fx = Seq(
    ("GBP", BigDecimal("1.25"), Date.valueOf("2026-10-25")),
    ("GBP", BigDecimal("1.30"), Date.valueOf("2026-10-27")), // after business date: must be ignored
    ("EUR", BigDecimal("1.08"), Date.valueOf("2026-10-26")),
    ("USD", BigDecimal("1.00"), Date.valueOf("2026-10-26"))
  ).toDF("currency", "rate_to_usd", "rate_date")

  private def run(rows: Seq[Seq[String]]) =
    DealPipeline.process(raw(rows), parties, fx, bd, "run-1", "LOANIQ")

  test("happy path: typed, enriched, FX-converted curated output") {
    val r = run(Seq(deal("D1")))
    r.rejectedRows shouldBe 0
    val out = r.curated.collect()
    out.length shouldBe 1
    val row = out.head
    row.getAs[String]("borrower_name") shouldBe "Acme Holdings plc"
    row.getAs[Boolean]("party_resolved") shouldBe true
    row.getAs[java.math.BigDecimal]("fx_rate_to_usd").doubleValue() shouldBe 1.25
    row.getAs[java.math.BigDecimal]("deal_amount_usd").doubleValue() shouldBe 1250000.0
    row.getAs[java.math.BigDecimal]("bank_share_pct").doubleValue() shouldBe 25.0
    r.curated.schema.fieldNames shouldBe DealSchemas.curatedDeal.fieldNames
  }

  test("standardises whitespace and case before validation") {
    val r = run(Seq(deal("D1", ccy = " gbp ", product = "syndicated_loan")))
    r.rejectedRows shouldBe 0
    r.curated.select("currency", "product_type").as[(String, String)].head() shouldBe (("GBP", "SYNDICATED_LOAN"))
  }

  test("quarantines invalid rows with reasons and loads the rest") {
    val r = run(Seq(
      deal("D1"),
      deal("D2", amount = "abc"),
      deal("D3", ccy = "POUNDS"),
      deal("D4", share = "2000000"),               // share > deal amount
      deal("D5", trade = "2026-10-20", settle = "2026-10-01"),
      deal(null)
    ))
    r.inputRows shouldBe 6
    r.rejectedRows shouldBe 5
    r.curated.count() shouldBe 1

    val reasons = r.rejected
      .select($"deal_id", $"${Quarantine.ReasonsCol}")
      .as[(String, Seq[String])]
      .collect()
      .toMap
    reasons("D2") should contain("pattern:deal_amount")
    reasons("D3") should contain("pattern:currency")
    reasons("D4") should contain("share_within_deal:bank_share_amount")
    reasons("D5") should contain("settlement_after_trade")
    r.validationReport.passed shouldBe false
  }

  test("keeps only the latest version of an amended deal") {
    val r = run(Seq(
      deal("D1", ver = "1", amount = "100", share = "10"),
      deal("D1", ver = "3", amount = "300", share = "10"),
      deal("D1", ver = "2", amount = "200", share = "10")
    ))
    r.supersededRows shouldBe 2
    val row = r.curated.collect().head
    row.getAs[Int]("deal_version") shouldBe 3
    row.getAs[java.math.BigDecimal]("deal_amount").doubleValue() shouldBe 300.0
  }

  test("unknown borrower is loaded but flagged, and the gate warns rather than fails") {
    val r = run(Seq(deal("D1"), deal("D2", party = "P404")))
    val flags = r.curated.select("deal_id", "party_resolved").as[(String, Boolean)].collect().toMap
    flags shouldBe Map("D1" -> true, "D2" -> false)

    val gate = DataQuality.evaluate("deal_curated", r.curated, DealRules.curatedGate(parties, minRows = 1))
    gate.warnings.map(_.rule) should contain("ref_integrity:borrower_party_id")
    gate.passed shouldBe true
  }

  test("missing FX rate blocks the write via the curated gate") {
    val r    = run(Seq(deal("D1", ccy = "JPY")))
    val gate = DataQuality.evaluate("deal_curated", r.curated, DealRules.curatedGate(parties, minRows = 1))
    gate.passed shouldBe false
    gate.blockingFailures.map(_.rule) should contain("not_null:fx_rate_to_usd")
  }

  test("malformed records are quarantined with the raw line preserved") {
    val rows = Seq(deal("D1"), Seq.fill(DealSchemas.rawDealFields.length)(null) :+ "D9,,,garbage")
    val r    = run(rows)
    r.rejectedRows shouldBe 1
    r.rejected.select("_raw_record").as[String].collect().toSeq shouldBe Seq("D9,,,garbage")
    r.curated.count() shouldBe 1
  }
}
