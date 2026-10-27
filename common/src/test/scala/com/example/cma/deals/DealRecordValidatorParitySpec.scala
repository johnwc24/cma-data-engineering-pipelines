package com.example.cma.deals

import com.example.cma.common.quality.Quarantine
import com.example.cma.testing.SparkTestSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._
import scala.io.Source

/**
 * The Beam/Dataflow pipeline validates with DealRecordValidator; the Spark pipelines use
 * DealRules. This spec runs both on the same records and requires identical rejection
 * reasons, so the two engines can never disagree about what a valid deal is.
 */
class DealRecordValidatorParitySpec extends AnyFunSuite with Matchers with SparkTestSession {

  private def sampleRows: Seq[Map[String, String]] = {
    val src = Source.fromFile("../data/sample/landing/deals/business_date=2026-10-26/deals_20261026.csv", "UTF-8")
    val lines = try src.getLines().toList finally src.close()
    val header = lines.head.split(",", -1).toSeq
    lines.tail.map(l => header.zip(l.split(",", -1).toSeq ++ Seq.fill(header.size)("")).toMap)
  }

  private def base: Map[String, String] = sampleRows.head

  /** Edge cases each aimed at one rule boundary. */
  private def edgeCases: Seq[Map[String, String]] = Seq(
    base,
    base + ("deal_amount" -> "0"),
    base + ("deal_amount" -> "-1.50"),
    base + ("deal_amount" -> "1e6"),
    base + ("bank_share_amount" -> "750000000.004"),          // rounds to equal amount: allowed
    base + ("bank_share_amount" -> ""),
    base + ("currency" -> " gbp "),
    base + ("currency" -> "GB"),
    base + ("trade_date" -> "2026-02-30"),                    // pattern ok, not a real date
    base + ("trade_date" -> ""),
    base + ("settlement_date" -> "2026-10-19"),
    base + ("deal_version" -> "1.0"),
    base + ("product_type" -> "swap"),
    base + ("deal_role" -> ""),
    base + ("deal_status" -> "draft"),
    Map.empty[String, String]
  )

  private def sparkReasons(rows: Seq[Map[String, String]]): Seq[Set[String]] = {
    val fields  = DealSchemas.rawDealFields
    val data    = rows.zipWithIndex.map { case (r, i) => Row.fromSeq(fields.map(f => r.getOrElse(f, null)) :+ i.toString) }
    val schema  = DealSchemas.rawDeal.copy(fields = DealSchemas.rawDeal.fields.filterNot(_.name == DealSchemas.CorruptRecordCol))
      .add("__row", "string")
    val df      = DealPipeline.standardise(spark.createDataFrame(data.asJava, schema))
    val split   = Quarantine.split(df, DealRules.rowValidation)
    val byRow   = split.rejected.select(col("__row"), col(Quarantine.ReasonsCol)).collect()
      .map(r => r.getString(0).toInt -> r.getSeq[String](1).toSet).toMap
    rows.indices.map(i => byRow.getOrElse(i, Set.empty[String]))
  }

  test("Spark rules and the record validator reject the same rows for the same reasons") {
    val rows   = sampleRows ++ edgeCases
    val spark_ = sparkReasons(rows)
    val beam   = rows.map(r => DealRecordValidator.violations(r).toSet)
    rows.indices.foreach { i =>
      withClue(s"row $i ${rows(i)}: ") { beam(i) shouldBe spark_(i) }
    }
    beam.count(_.nonEmpty) should be > 5 // the inputs really do exercise the rules
  }

  test("standardise matches the Spark standardisation") {
    DealRecordValidator.standardise(Map("currency" -> " usd ", "deal_name" -> "  ")) should contain allOf (
      "currency" -> "USD", "deal_name" -> null)
  }
}
