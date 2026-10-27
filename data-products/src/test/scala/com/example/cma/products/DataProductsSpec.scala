package com.example.cma.products

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardCopyOption}
import java.sql.Date
import java.time.LocalDate

import com.example.cma.common.errors.PipelineError.ExitCodes
import com.example.cma.files.DeltaTestSession
import io.delta.tables.DeltaTable
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DataProductsSpec extends AnyFunSuite with Matchers with DeltaTestSession {
  import spark.implicits._

  private val sample = new File("../data/sample").getCanonicalFile
  private def tmp(p: String) = Files.createTempDirectory(p).toFile

  private def exec(job: ProductJob, bd: String, root: File, out: File, conf: String*): Int =
    job.execute(
      Array("--env", "local", "--business-date", bd, "--run-id", s"test-$bd",
        "--conf", (Seq(s"roots.local=$root", s"outputs.local=$out", "retry.initial-delay-ms=1") ++ conf).mkString(",")),
      Some(spark))

  // ---------------------------------------------------------------- break reconciliation

  /** CMA curated deals as produced by DealIngestionJob for the sample date. */
  private def writeCmaDeals(out: File, bd: String, rows: Seq[(String, String, String, String)]): Unit =
    rows.toDF("deal_id", "deal_amount", "currency", "deal_status")
      .withColumn("deal_amount", col("deal_amount").cast("decimal(20,2)"))
      .withColumn("business_date", lit(Date.valueOf(bd)))
      .write.mode("overwrite").option("partitionOverwriteMode", "dynamic").partitionBy("business_date")
      .parquet(s"$out/curated/deal")

  private val cmaDay1 = Seq(
    ("DL-1001", "800000000", "GBP", "SIGNED"), ("DL-1002", "1000000000", "EUR", "PRICED"),
    ("DL-1003", "420000000", "USD", "PRICED"), ("DL-1004", "350000000", "USD", "LAUNCHED"),
    ("DL-1005", "120000000", "GBP", "CLOSED"))

  test("breaks: classifies each break type, mapping booking statuses first") {
    val out = tmp("breaks")
    writeCmaDeals(out, "2026-10-26", cmaDay1)
    exec(BreakReconciliationJob, "2026-10-26", sample, out) shouldBe ExitCodes.Success

    val breaks = spark.read.format("delta").load(s"$out/curated/recon/deal_breaks")
    breaks.select("deal_id", "break_type").as[(String, String)].collect().toSet shouldBe Set(
      ("DL-1003", "AMOUNT_MISMATCH"), ("DL-1004", "STATUS_MISMATCH"),
      ("DL-1005", "CURRENCY_MISMATCH"), ("DL-1099", "MISSING_IN_CMA"))
    breaks.filter(col("deal_id") === "DL-1003").select("amount_difference").head().getDecimal(0).toPlainString shouldBe "-500000.00"
  }

  test("breaks: age persisting breaks, resolve fixed ones, re-runs are idempotent") {
    val root = tmp("breaks-root")
    val out  = tmp("breaks-out")
    val day1 = new File(root, "landing/booking/business_date=2026-10-26"); day1.mkdirs()
    Files.copy(Paths.get(s"$sample/landing/booking/business_date=2026-10-26/positions.csv"), day1.toPath.resolve("positions.csv"))
    writeCmaDeals(out, "2026-10-26", cmaDay1)
    exec(BreakReconciliationJob, "2026-10-26", root, out) shouldBe ExitCodes.Success
    exec(BreakReconciliationJob, "2026-10-26", root, out) shouldBe ExitCodes.Success // idempotent

    // next day: the DL-1003 amount is fixed in booking, everything else unchanged
    val day2 = new File(root, "landing/booking/business_date=2026-10-27"); day2.mkdirs()
    val fixed = new String(Files.readAllBytes(day1.toPath.resolve("positions.csv")), StandardCharsets.UTF_8).replace("420500000", "420000000")
    Files.write(day2.toPath.resolve("positions.csv"), fixed.getBytes(StandardCharsets.UTF_8))
    writeCmaDeals(out, "2026-10-27", cmaDay1)
    exec(BreakReconciliationJob, "2026-10-27", root, out) shouldBe ExitCodes.Success

    val b = spark.read.format("delta").load(s"$out/curated/recon/deal_breaks")
    b.count() shouldBe 4
    val dl1003 = b.filter(col("deal_id") === "DL-1003").head()
    dl1003.getAs[String]("status") shouldBe "RESOLVED"
    dl1003.getAs[Date]("resolved_on").toString shouldBe "2026-10-27"
    val dl1004 = b.filter(col("deal_id") === "DL-1004").head()
    dl1004.getAs[String]("status") shouldBe "OPEN"
    dl1004.getAs[Date]("first_seen").toString shouldBe "2026-10-26"
    dl1004.getAs[Int]("age_days") shouldBe 1
  }

  test("breaks: a break rate above the limit fails the run (exit 6)") {
    val out = tmp("breaks-limit")
    writeCmaDeals(out, "2026-10-26", cmaDay1)
    exec(BreakReconciliationJob, "2026-10-26", sample, out, "breaks.max-break-rate=0.05") shouldBe ExitCodes.Reconciliation
  }

  // ---------------------------------------------------------------- vendor feed

  private val vendorDir = s"$sample/landing/vendor/ratings/business_date=2026-10-26"

  private def copyVendor(root: File, bd: String): File = {
    val d = new File(root, s"landing/vendor/ratings/business_date=$bd"); d.mkdirs()
    new File(vendorDir).listFiles().foreach(f => Files.copy(f.toPath, d.toPath.resolve(f.getName), StandardCopyOption.REPLACE_EXISTING))
    d
  }

  test("vendor: verified, licensed, typed snapshot with licence tags on the table") {
    val out = tmp("vendor")
    exec(VendorFeedJob, "2026-10-26", sample, out, "feed=agency-ratings") shouldBe ExitCodes.Success
    val path = s"$out/curated/vendor/agency_ratings"
    val snap = spark.read.format("delta").load(path)
    snap.count() shouldBe 6
    snap.select("licence_id").distinct().as[String](Encoders.STRING).collect() shouldBe Array("LIC-RAT-2026-014")
    spark.read.parquet(s"$out/quarantine/vendor/agency_ratings").count() shouldBe 1 // blank rating
    val props = DeltaTable.forPath(spark, path).detail().select("properties").head().getMap[String, String](0)
    props("cma.licence_id") shouldBe "LIC-RAT-2026-014"
    props("cma.redistribution") shouldBe "false"
  }

  test("vendor: expired licence stops the load before parsing (exit 2)") {
    val out = tmp("vendor-lic")
    exec(VendorFeedJob, "2026-10-26", sample, out, "feed=agency-ratings",
      "vendor-feeds.agency-ratings.licence.expires=2026-06-30") shouldBe ExitCodes.Config
    new File(s"$out/curated").exists() shouldBe false
  }

  test("vendor: a tampered file fails the checksum (exit 6); a missing manifest is exit 3") {
    val root = tmp("vendor-tamper")
    val d = copyVendor(root, "2026-10-26")
    Files.write(d.toPath.resolve("ratings_20261026.csv"), "PTY-009,S&P,AAA,Stable,2026-10-25\n".getBytes, java.nio.file.StandardOpenOption.APPEND)
    exec(VendorFeedJob, "2026-10-26", root, tmp("o1"), "feed=agency-ratings") shouldBe ExitCodes.Reconciliation

    new File(d, "manifest.json").delete()
    exec(VendorFeedJob, "2026-10-26", root, tmp("o2"), "feed=agency-ratings") shouldBe ExitCodes.SourceMissing
  }

  test("vendor: daily snapshots are kept side by side and diffed") {
    val root = tmp("vendor-days")
    val out  = tmp("vendor-days-out")
    copyVendor(root, "2026-10-26")
    exec(VendorFeedJob, "2026-10-26", root, out, "feed=agency-ratings") shouldBe ExitCodes.Success
    copyVendor(root, "2026-10-27")
    exec(VendorFeedJob, "2026-10-27", root, out, "feed=agency-ratings") shouldBe ExitCodes.Success
    val snap = spark.read.format("delta").load(s"$out/curated/vendor/agency_ratings")
    snap.select("snapshot_date").distinct().count() shouldBe 2

    val prev = snap.filter(col("snapshot_date") === "2026-10-26")
    val cur  = prev.withColumn("rating", when(col("party_id") === "PTY-003", lit("BB-")).otherwise(col("rating")))
      .filter(col("party_id") =!= "PTY-004")
    VendorFeedJob.diff(cur, prev, Seq("party_id", "agency"), Seq("party_id", "agency", "rating", "outlook", "rated_on")) shouldBe
      Map("added" -> 0L, "removed" -> 1L, "changed" -> 1L)
  }

  // ---------------------------------------------------------------- point-in-time features

  private def d(s: String) = Date.valueOf(s)

  private lazy val history = Seq(
    // D1 amended on 2026-06-01: amount 100 -> 200 (SCD2 versions)
    ("D1", "P1", BigDecimal(100), BigDecimal(10), BigDecimal(10), "BOND", d("2026-01-10"), d("2026-01-10"), d("2026-05-31")),
    ("D1", "P1", BigDecimal(200), BigDecimal(20), BigDecimal(10), "BOND", d("2026-01-10"), d("2026-06-01"), d("9999-12-31")),
    // D2 traded after the second as-of date: must never appear before 2026-09-01
    ("D2", "P1", BigDecimal(500), BigDecimal(50), BigDecimal(10), "SYNDICATED_LOAN", d("2026-09-01"), d("2026-09-01"), d("9999-12-31"))
  ).toDF("deal_id", "borrower_party_id", "deal_amount_usd", "bank_share_usd", "bank_share_pct", "product_type",
    "trade_date", "valid_from", "valid_to")

  private lazy val ratings = Seq(
    ("P1", "S&P", "BBB", d("2026-02-01")),
    ("P1", "S&P", "BB+", d("2026-07-15")),   // downgrade
    ("P1", "Moody's", "Baa1", d("2026-08-01"))
  ).toDF("party_id", "agency", "rating", "rated_on")

  test("features are point-in-time correct: no later amendments, deals or ratings leak backwards") {
    val spine = Seq(("P1", d("2026-03-31")), ("P1", d("2026-07-31")), ("P1", d("2026-09-30")), ("P2", d("2026-09-30")))
      .toDF("party_id", "as_of_date")
    val f = PointInTimeFeaturesJob.features(history, ratings, spine, 365)
      .orderBy("party_id", "as_of_date").collect()
      .map(r => (r.getAs[String]("party_id"), r.getAs[Date]("as_of_date").toString) -> r).toMap

    val mar = f(("P1", "2026-03-31"))
    mar.getAs[Long]("deals_365d") shouldBe 1
    mar.getAs[java.math.BigDecimal]("deal_volume_usd_365d").intValue() shouldBe 100 // pre-amendment version
    mar.getAs[Int]("worst_rating_notch") shouldBe PointInTimeFeaturesJob.RatingNotch("BBB")

    val jul = f(("P1", "2026-07-31"))
    jul.getAs[java.math.BigDecimal]("deal_volume_usd_365d").intValue() shouldBe 200 // amended version
    jul.getAs[Long]("deals_365d") shouldBe 1                                         // D2 not yet traded
    jul.getAs[Int]("worst_rating_notch") shouldBe PointInTimeFeaturesJob.RatingNotch("BB+")
    jul.getAs[Long]("rating_agencies") shouldBe 1                                    // Moody's not yet published

    val sep = f(("P1", "2026-09-30"))
    sep.getAs[Long]("deals_365d") shouldBe 2
    sep.getAs[Long]("rating_agencies") shouldBe 2
    sep.getAs[Int]("days_since_last_deal") shouldBe 29

    val p2 = f(("P2", "2026-09-30"))
    p2.getAs[Long]("deals_365d") shouldBe 0
    p2.isNullAt(p2.fieldIndex("worst_rating_notch")) shouldBe true
  }

  test("features job writes one as-of partition and re-runs idempotently") {
    val out = tmp("pit")
    history.write.parquet(s"$out/curated/deal_history")
    ratings.write.parquet(s"$out/curated/vendor/agency_ratings")
    exec(PointInTimeFeaturesJob, "2026-09-30", sample, out) shouldBe ExitCodes.Success
    exec(PointInTimeFeaturesJob, "2026-09-30", sample, out) shouldBe ExitCodes.Success
    val feats = spark.read.format("delta").load(s"$out/features/borrower_pit")
    feats.count() shouldBe 1
    feats.head().getAs[Long]("deals_365d") shouldBe 2
  }
}
