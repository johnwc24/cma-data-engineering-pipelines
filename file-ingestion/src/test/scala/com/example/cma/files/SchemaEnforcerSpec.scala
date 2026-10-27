package com.example.cma.files

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SchemaEnforcerSpec extends AnyFunSuite with Matchers with DeltaTestSession {
  import spark.implicits._

  test("casts real-world file values: thousands separators, UK dates, Excel booleans, blanks") {
    val df = Seq(("1,250,000.50", "26/10/2026", "Y", "  "), ("42", "2026-10-26", "no", "x"))
      .toDF("amount", "day", "flag", "note")
    val out = df.select(
      SchemaEnforcer.castTo(col("amount"), DecimalType(20, 2)).as("amount"),
      SchemaEnforcer.castTo(col("day"), DateType).as("day"),
      SchemaEnforcer.castTo(col("flag"), BooleanType).as("flag"),
      SchemaEnforcer.castTo(col("note"), StringType).as("note")
    ).collect()
    out(0).getDecimal(0).toPlainString shouldBe "1250000.50"
    out(0).getDate(1).toString shouldBe "2026-10-26"
    out(0).getBoolean(2) shouldBe true
    out(0).isNullAt(3) shouldBe true
    out(1).getBoolean(2) shouldBe false
  }

  test("normalises Excel/Avro headers to snake_case") {
    val df = Seq((1, 2, 3)).toDF("Deal ID", "dealAmountUsd", "Trade-Date ")
    FileReaders.normaliseColumnNames(df).columns shouldBe Array("deal_id", "deal_amount_usd", "trade_date")
  }

  test("every rejected row carries all of its reasons; valid rows are typed") {
    val df = Seq(("D1", "10"), ("D2", "abc"), (null, "5"), (null, "x"))
      .toDF("id", "qty")
      .withColumn(FileReaders.SourceFileCol, lit("f.csv"))
    val contract = StructType(Seq(StructField("id", StringType), StructField("qty", IntegerType)))
    val r = SchemaEnforcer.enforce(df, contract, required = Seq("id"))
    r.valid.count() shouldBe 1
    r.valid.schema("qty").dataType shouldBe IntegerType
    val reasons = r.rejected.select(SchemaEnforcer.ReasonsCol).as[Seq[String]].collect().map(_.toSet).toSet
    reasons shouldBe Set(Set("cast:qty:int"), Set("required:id"), Set("required:id", "cast:qty:int"))
  }

  test("missing contract columns are a fatal schema error") {
    val df = Seq("a").toDF("only").withColumn(FileReaders.SourceFileCol, lit("f"))
    val contract = StructType(Seq(StructField("only", StringType), StructField("missing", StringType)))
    an[com.example.cma.common.errors.SchemaValidationError] should be thrownBy SchemaEnforcer.enforce(df, contract, Nil)
  }
}
