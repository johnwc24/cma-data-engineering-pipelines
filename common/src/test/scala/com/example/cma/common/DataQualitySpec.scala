package com.example.cma.common

import com.example.cma.common.quality._
import com.example.cma.common.recon.{ReconCheck, Reconciliation}
import com.example.cma.testing.SparkTestSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DataQualitySpec extends AnyFunSuite with Matchers with SparkTestSession {
  import spark.implicits._

  private lazy val deals = Seq(
    ("D1", "GBP", 100.0),
    ("D2", "usd", 200.0),
    ("D3", null, 300.0),
    ("D3", "EUR", -5.0)
  ).toDF("deal_id", "currency", "amount")

  test("row rules count violations in one pass and respect thresholds") {
    val report = DataQuality.evaluate(
      "deals",
      deals,
      Seq(
        NotNull("currency"),
        MatchesPattern("currency", "^[A-Z]{3}$"),
        InRange("amount", min = Some(0.0)),
        NotNull("deal_id", maxFailureRatio = 0.5)
      )
    )
    val byRule = report.results.map(r => r.rule -> r).toMap
    byRule("not_null:currency").failed shouldBe 1
    byRule("pattern:currency").failed shouldBe 1 // lower-case usd; null is not double counted
    byRule("range:amount").failed shouldBe 1
    byRule("not_null:deal_id").passed shouldBe true
    report.passed shouldBe false
    report.enforce().isLeft shouldBe true
  }

  test("dataset rules: uniqueness and referential integrity") {
    val ref = Seq("GBP", "USD").toDF("ccy")
    val report = DataQuality.evaluate(
      "deals",
      deals,
      Seq(Unique(Seq("deal_id")), ReferentialIntegrity("currency", ref, "ccy", severity = Severity.Warn))
    )
    val byRule = report.results.map(r => r.rule -> r).toMap
    byRule("unique:deal_id").failed shouldBe 2
    byRule("ref_integrity:currency").failed shouldBe 2 // usd (case) and EUR
    report.blockingFailures.map(_.rule) shouldBe Seq("unique:deal_id")
    report.warnings.map(_.rule) shouldBe Seq("ref_integrity:currency")
  }

  test("warn-only breaches do not block") {
    val report = DataQuality.evaluate("deals", deals, Seq(NotNull("currency", severity = Severity.Warn)))
    report.enforce().isRight shouldBe true
  }

  test("quarantine splits rows and records every broken rule") {
    val split = Quarantine.split(deals, Seq(NotNull("currency"), InRange("amount", min = Some(0.0))))
    split.valid.count() shouldBe 2
    val rejected = split.rejected.select("deal_id", Quarantine.ReasonsCol).as[(String, Seq[String])].collect().toMap
    rejected.keySet shouldBe Set("D3")
    split.rejected.count() shouldBe 2
  }

  test("results can be written to an audit table") {
    val report = DataQuality.evaluate("deals", deals, Seq(NotNull("currency")))
    val df     = report.toDataFrame(spark, "run-1", "2026-10-26")
    df.columns should contain allOf ("rule", "failed", "dataset", "run_id", "business_date")
    df.count() shouldBe 1
  }

  test("reconciliation flags breaks outside tolerance") {
    val ok    = Reconciliation.completeness("rows", input = 10, loaded = 8, rejected = 2)
    val break = ReconCheck("amount", BigDecimal("100.00"), BigDecimal("99.00"), BigDecimal("0.01"))
    Reconciliation.check("deals", Seq(ok)).isRight shouldBe true
    Reconciliation.check("deals", Seq(ok, break)).isLeft shouldBe true
  }

  test("totals computes row count and decimal sum") {
    val t = Reconciliation.totals(deals, Some("amount"))
    t.rows shouldBe 4
    t.amount shouldBe BigDecimal("595.0000")
  }
}
