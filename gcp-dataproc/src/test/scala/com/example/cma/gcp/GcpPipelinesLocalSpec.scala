package com.example.cma.gcp

import java.io.{File, PrintWriter}
import java.nio.file.Files

import com.example.cma.common.errors.PipelineError.ExitCodes
import com.example.cma.deals.DealSchemas
import com.example.cma.testing.SparkTestSession
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Runs both GCP pipelines end to end on a laptop: GCS is the local data/sample folder
 * and BigQuery is the InMemoryWarehouse. No Google Cloud access needed.
 */
class GcpPipelinesLocalSpec extends AnyFunSuite with Matchers with SparkTestSession {

  private val sample = new File("../data/sample").getCanonicalFile

  private def seededWarehouse(corrupt: Boolean = false): InMemoryWarehouse = {
    val wh = new InMemoryWarehouse(spark, corrupt)
    wh.seed("reference.party",
      spark.read.option("header", "true").schema(DealSchemas.party).csv(s"$sample/reference/parties.csv"))
    wh.seed("reference.fx_rate",
      spark.read.option("header", "true").schema(DealSchemas.fxRate).csv(s"$sample/reference/fx_rates.csv"))
    wh
  }

  private def gcsArgs(extra: String*): Array[String] =
    Array("--env", "dev", "--business-date", "2026-10-26", "--run-id", "local-test",
      "--conf", s"gcs.landing=$sample/landing/deals,retry.initial-delay-ms=1") ++ extra

  // ---------------------------------------------------------------- GCS -> BigQuery

  test("GCS -> BigQuery: stages valid deals, quarantines rejects, records DQ results") {
    val wh  = seededWarehouse()
    val job = new GcsToBigQuery((_, _, _) => wh)
    job.execute(gcsArgs(), Some(spark)) shouldBe ExitCodes.Success

    wh.count("staging.deal_stg") shouldBe 5
    wh.count("quarantine.deal_reject") shouldBe 6
    wh.count("audit.dq_results") should be > 0L
    wh.read("staging.deal_stg", None).filter(col("deal_id") === "DL-1001").select("deal_version").head().getInt(0) shouldBe 2
  }

  test("GCS -> BigQuery: re-running a business date replaces the partition (idempotent)") {
    val wh  = seededWarehouse()
    val job = new GcsToBigQuery((_, _, _) => wh)
    job.execute(gcsArgs(), Some(spark)) shouldBe ExitCodes.Success
    job.execute(gcsArgs(), Some(spark)) shouldBe ExitCodes.Success
    wh.count("staging.deal_stg") shouldBe 5
  }

  test("GCS -> BigQuery: a sink that loses rows is caught by reconciliation (exit 6)") {
    val wh = seededWarehouse(corrupt = true)
    new GcsToBigQuery((_, _, _) => wh).execute(gcsArgs(), Some(spark)) shouldBe ExitCodes.Reconciliation
  }

  test("GCS -> BigQuery: missing reference table is a config error, not a retry loop (exit 2)") {
    val wh = new InMemoryWarehouse(spark) // nothing seeded
    new GcsToBigQuery((_, _, _) => wh).execute(gcsArgs(), Some(spark)) shouldBe ExitCodes.Config
  }

  test("GCS -> BigQuery: dry run writes nothing to BigQuery") {
    val wh = seededWarehouse()
    new GcsToBigQuery((_, _, _) => wh).execute(gcsArgs("--dry-run"), Some(spark)) shouldBe ExitCodes.Success
    wh.writes shouldBe empty
  }

  // ---------------------------------------------------------------- Hive -> BigQuery migration

  private def migrationConfig(): String = {
    val f = Files.createTempFile("migration", ".conf").toFile
    val w = new PrintWriter(f)
    try w.write(
      """dev.migration.tables = [
        |  { source = "deal_source", target = "deal_history_migrated", partition-column = "business_date", amount-column = "deal_amount" }
        |]
        |dev.migration.days-back = 1
        |""".stripMargin)
    finally w.close()
    f.getAbsolutePath
  }

  /** Stands in for the Hive table: two partitions of curated deals. */
  private def registerHiveSource(): Unit = {
    import spark.implicits._
    Seq(
      ("DL-1", BigDecimal("100.00"), java.sql.Date.valueOf("2026-10-26")),
      ("DL-2", BigDecimal("250.50"), java.sql.Date.valueOf("2026-10-26")),
      ("DL-3", BigDecimal("75.25"), java.sql.Date.valueOf("2026-10-25"))
    ).toDF("deal_id", "deal_amount", "business_date").createOrReplaceTempView("deal_source")
  }

  private def migrationArgs: Array[String] =
    Array("--env", "dev", "--business-date", "2026-10-26", "--run-id", "mig-test",
      "--config", migrationConfig(), "--conf", "retry.initial-delay-ms=1")

  test("Hive -> BigQuery migration: moves each partition and reconciles it") {
    registerHiveSource()
    val wh = new InMemoryWarehouse(spark)
    new HiveToBigQueryMigration((_, _, _) => wh).execute(migrationArgs, Some(spark)) shouldBe ExitCodes.Success
    wh.count("curated.deal_history_migrated") shouldBe 3
    wh.read("audit.migration_log", None).filter(col("status") === "OK").count() shouldBe 2
  }

  test("Hive -> BigQuery migration: a lossy write is reported as a reconciliation break") {
    registerHiveSource()
    val wh = new InMemoryWarehouse(spark, corruptWrites = true)
    new HiveToBigQueryMigration((_, _, _) => wh).execute(migrationArgs, Some(spark)) shouldBe ExitCodes.Reconciliation
    wh.read("audit.migration_log", None).filter(col("status") === "RECON_BREAK").count() shouldBe 2
  }
}
