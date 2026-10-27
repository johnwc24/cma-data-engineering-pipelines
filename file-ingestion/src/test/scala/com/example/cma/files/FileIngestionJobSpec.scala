package com.example.cma.files

import java.io.{File, PrintWriter}
import java.nio.file.Files

import com.example.cma.common.errors.PipelineError.ExitCodes
import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Every format combination, run end to end on local files (same code paths as hdfs:// and gs://). */
class FileIngestionJobSpec extends AnyFunSuite with Matchers with DeltaTestSession {

  private val sample = new File("../data/sample").getCanonicalFile

  /** A fresh landing root (copy of sample data where needed) and output root per test. */
  private def roots(): (File, File) = {
    val in  = Files.createTempDirectory("landing").toFile
    val out = Files.createTempDirectory("lake").toFile
    (in, out)
  }

  private def run(dataset: String, in: File, out: File, extra: String*): Int =
    FileIngestionJob.execute(
      Array("--env", "local", "--business-date", "2026-10-26", "--run-id", s"test-$dataset",
        "--conf", s"dataset=$dataset,roots.local=${in.getAbsolutePath},outputs.local=${out.getAbsolutePath},retry.initial-delay-ms=1") ++ extra,
      Some(spark))

  // ---------------------------------------------------------------- CSV -> Parquet

  test("CSV -> Parquet: types the contract, quarantines bad rows with reasons, idempotent re-run") {
    val (_, out) = roots()
    run("deals-csv", sample, out) shouldBe ExitCodes.Success
    run("deals-csv", sample, out) shouldBe ExitCodes.Success // re-run replaces the partition

    val curated = spark.read.parquet(s"$out/curated/files/deal_parquet")
    curated.count() shouldBe 9
    curated.schema("deal_amount").dataType shouldBe DecimalType(20, 2)
    curated.schema("trade_date").dataType shouldBe DateType
    curated.columns should contain allOf ("_source_file", "_run_id", "_ingested_at", "business_date")

    val rejects = spark.read.parquet(s"$out/quarantine/files/deals_csv")
    rejects.count() shouldBe 3
    val reasons = rejects.select(explode(col("_reject_reasons"))).as[String](org.apache.spark.sql.Encoders.STRING).collect().toSet
    reasons should contain allOf ("malformed_record", "cast:deal_amount:decimal(20,2)", "required:deal_id")
  }

  // ---------------------------------------------------------------- JSON -> Parquet (append)

  test("JSON -> Parquet append: accepts dd/MM/yyyy dates, rejects unparseable dates and broken JSON") {
    val (_, out) = roots()
    run("ratings-json", sample, out) shouldBe ExitCodes.Success
    val ratings = spark.read.parquet(s"$out/curated/files/rating_parquet")
    ratings.count() shouldBe 3
    ratings.filter(col("party_id") === "PTY-003").select("rated_on").head().getDate(0).toString shouldBe "2026-10-22"
    spark.read.parquet(s"$out/quarantine/files/ratings_json").count() shouldBe 3
  }

  // ---------------------------------------------------------------- Avro -> Delta

  private def writeFxAvro(in: File, rows: Seq[(String, String, String)]): Unit = {
    import spark.implicits._
    rows.toDF("currency", "rate_to_usd", "rate_date")
      .coalesce(1).write.mode("overwrite").format("avro").save(s"$in/landing/fx/business_date=2026-10-26")
  }

  test("Avro -> Delta: partition overwrite via replaceWhere keeps other days and is idempotent") {
    val (in, out) = roots()
    writeFxAvro(in, Seq(("GBP", "1.2791", "2026-10-26"), ("EUR", "1.0812", "2026-10-26"), ("JPY", "n/a", "2026-10-26")))
    run("fx-avro", in, out) shouldBe ExitCodes.Success
    run("fx-avro", in, out) shouldBe ExitCodes.Success

    val fx = spark.read.format("delta").load(s"$out/curated/files/fx_rate_delta")
    fx.count() shouldBe 2
    fx.schema("rate_to_usd").dataType shouldBe DecimalType(18, 8)
    DeltaTable.forPath(spark, s"$out/curated/files/fx_rate_delta").history().count() shouldBe 2 // two ACID commits
    spark.read.parquet(s"$out/quarantine/files/fx_avro").count() shouldBe 1
  }

  test("Avro -> Delta: duplicate business keys fail the unique gate (exit 5) and nothing is written") {
    val (in, out) = roots()
    writeFxAvro(in, Seq(("GBP", "1.27", "2026-10-26"), ("GBP", "1.28", "2026-10-26")))
    run("fx-avro", in, out) shouldBe ExitCodes.DataQuality
    new File(s"$out/curated/files/fx_rate_delta").exists() shouldBe false
  }

  // ---------------------------------------------------------------- Excel -> Delta (merge)

  private val partyHeader = Seq("Party ID", "Legal Name", "LEI", "Country Code", "Sector", "Is Active")

  private def writeParties(in: File, rows: Seq[Seq[String]]): Unit =
    Xlsx.write(new File(s"$in/landing/party/business_date=2026-10-26/parties.xlsx"), "Parties", partyHeader, rows)

  test("Excel -> Delta merge: normalises headers, maps Y/N booleans, upserts on the key") {
    val (in, out) = roots()
    writeParties(in, Seq(
      Seq("PTY-001", "Acme Holdings plc", "LEI1", "GB", "Industrials", "Y"),
      Seq("PTY-002", "Globex SA", "LEI2", "FR", "Utilities", "N"),
      Seq("PTY-003", null, "LEI3", "US", "Technology", "Y"),       // missing required legal name
      Seq("PTY-004", "Umbrella Group", "LEI4", "GB", "Healthcare", "maybe") // bad boolean
    ))
    run("party-excel", in, out) shouldBe ExitCodes.Success
    val path = s"$out/curated/files/party_delta"
    spark.read.format("delta").load(path).count() shouldBe 2
    spark.read.format("delta").load(path).filter(col("party_id") === "PTY-002").select("is_active").head().getBoolean(0) shouldBe false

    // next drop: one update, one new party -> merge, not overwrite
    writeParties(in, Seq(
      Seq("PTY-002", "Globex SE", "LEI2", "FR", "Utilities", "Y"),
      Seq("PTY-005", "Wayne Enterprises", "LEI5", "US", "Industrials", "Y")
    ))
    run("party-excel", in, out) shouldBe ExitCodes.Success
    val parties = spark.read.format("delta").load(path)
    parties.count() shouldBe 3
    parties.filter(col("party_id") === "PTY-002").select("legal_name").head().getString(0) shouldBe "Globex SE"
    spark.read.parquet(s"$out/quarantine/files/party_excel").count() shouldBe 2
  }

  // ---------------------------------------------------------------- error handling

  test("missing landing data -> exit 3; unknown dataset / invalid config -> exit 2") {
    val (in, out) = roots()
    run("deals-csv", in, out) shouldBe ExitCodes.SourceMissing
    run("no-such-dataset", in, out) shouldBe ExitCodes.Config

    val bad = Files.createTempFile("bad", ".conf").toFile
    val w = new PrintWriter(bad)
    try w.write("local.datasets.deals-csv.target { format = parquet, mode = merge, merge-keys = [deal_id] }")
    finally w.close()
    run("deals-csv", sample, out, "--config", bad.getAbsolutePath) shouldBe ExitCodes.Config
  }

  test("a source missing contract columns fails fast with the schema exit code (4)") {
    val (in, out) = roots()
    Xlsx.write(new File(s"$in/landing/party/business_date=2026-10-26/parties.xlsx"), "Parties",
      Seq("Party ID", "Legal Name"), Seq(Seq("PTY-001", "Acme")))
    run("party-excel", in, out) shouldBe ExitCodes.Schema
  }

  test("dry run reads and validates but writes nothing") {
    val (_, out) = roots()
    run("deals-csv", sample, out, "--dry-run") shouldBe ExitCodes.Success
    out.listFiles().toSeq shouldBe empty
  }
}
