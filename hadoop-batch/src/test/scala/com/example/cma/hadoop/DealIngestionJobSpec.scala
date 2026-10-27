package com.example.cma.hadoop

import java.io.File
import java.nio.file.Files

import com.example.cma.common.errors.PipelineError.ExitCodes
import com.example.cma.testing.SparkTestSession
import org.apache.spark.sql.functions.col
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** End-to-end run of the Hadoop job in `local` mode against data/sample. */
class DealIngestionJobSpec extends AnyFunSuite with Matchers with SparkTestSession {

  private val sample = new File("../data/sample").getCanonicalFile

  private def argv(out: File, extra: String*): Array[String] = {
    val o = out.getAbsolutePath
    val overrides = Seq(
      s"paths.landing=${sample}/landing/deals",
      s"paths.parties=${sample}/reference/parties.csv",
      s"paths.fx-rates=${sample}/reference/fx_rates.csv",
      s"paths.curated=$o/curated",
      s"paths.rejects=$o/rejects",
      s"paths.dq-results=$o/dq_results",
      s"paths.run-audit=$o/pipeline_run",
      "retry.initial-delay-ms=1"
    ).mkString(",")
    Array("--env", "local", "--business-date", "2026-10-26", "--run-id", "test-run", "--conf", overrides) ++ extra
  }

  test("loads valid deals, quarantines bad ones, reconciles and audits") {
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    val out  = Files.createTempDirectory("cma-e2e").toFile
    val code = DealIngestionJob.execute(argv(out), Some(spark))
    code shouldBe ExitCodes.Success

    val curated = spark.read.parquet(s"$out/curated")
    curated.count() shouldBe 5
    curated.filter(col("deal_id") === "DL-1001").select("deal_version").head().getInt(0) shouldBe 2
    curated.filter(col("deal_id") === "DL-1005").select("party_resolved").head().getBoolean(0) shouldBe false

    spark.read.parquet(s"$out/rejects").count() shouldBe 6
    spark.read.parquet(s"$out/dq_results").filter(col("passed") === false).count() should be > 0L
    spark.read.parquet(s"$out/pipeline_run").select("status").head().getString(0) shouldBe "SUCCEEDED"
  }

  test("re-running the same business date is idempotent") {
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    val out = Files.createTempDirectory("cma-rerun").toFile
    DealIngestionJob.execute(argv(out), Some(spark)) shouldBe ExitCodes.Success
    DealIngestionJob.execute(argv(out), Some(spark)) shouldBe ExitCodes.Success
    spark.read.parquet(s"$out/curated").count() shouldBe 5
  }

  test("dry run writes nothing") {
    val out = Files.createTempDirectory("cma-dry").toFile
    DealIngestionJob.execute(argv(out, "--dry-run"), Some(spark)) shouldBe ExitCodes.Success
    new File(out, "curated").exists() shouldBe false
  }

  test("missing landing partition fails with the source-missing exit code") {
    val out  = Files.createTempDirectory("cma-missing").toFile
    val args = argv(out).map(a => if (a == "2026-10-26") "2026-01-01" else a)
    DealIngestionJob.execute(args, Some(spark)) shouldBe ExitCodes.SourceMissing
  }

  test("a blocking DQ breach stops the curated write but still records rejects") {
    val out  = Files.createTempDirectory("cma-dq").toFile
    // Demand more rows than the sample has, so the min-row-count gate fails
    val args = argv(out) ++ Array("--conf", "dq.min-rows=1000")
    DealIngestionJob.execute(args, Some(spark)) shouldBe ExitCodes.DataQuality
    new File(out, "curated").exists() shouldBe false
    new File(out, "rejects").exists() shouldBe true
    spark.read.parquet(s"$out/pipeline_run").select("status").head().getString(0) should startWith("FAILED")
  }

  test("invalid arguments return the config exit code") {
    DealIngestionJob.execute(Array("--env", "nowhere"), Some(spark)) shouldBe ExitCodes.Config
  }
}
