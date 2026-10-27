package com.example.cma.hadoop

import java.io.File

import com.example.cma.common.errors.PipelineError.ExitCodes
import com.example.cma.deals.DealSchemas
import com.example.cma.testing.EmbeddedHiveSession
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Runs the Hadoop pipeline in its real `hive` mode against an embedded metastore:
 * the production DDL is applied, reference tables are loaded, and the job writes
 * Hive partitions exactly as it would on the cluster.
 */
class HiveDealIngestionSpec extends AnyFunSuite with Matchers with EmbeddedHiveSession {

  private val sample = new File("../data/sample").getCanonicalFile
  private val ddl    = new File("src/main/resources/hive/ddl.hql")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    runHql(ddl, Map("env_suffix" -> "_dev"))
    spark.read.option("header", "true").schema(DealSchemas.party).csv(s"$sample/reference/parties.csv")
      .write.mode("overwrite").insertInto("cma_ref_dev.party")
    spark.read.option("header", "true").schema(DealSchemas.fxRate).csv(s"$sample/reference/fx_rates.csv")
      .write.mode("overwrite").insertInto("cma_ref_dev.fx_rate")
  }

  private val args = Array(
    "--env", "dev", "--business-date", "2026-10-26", "--run-id", "hive-test",
    "--conf", s"paths.landing=$sample/landing/deals,retry.initial-delay-ms=1"
  )

  test("production Hive DDL applies cleanly and is idempotent") {
    runHql(ddl, Map("env_suffix" -> "_dev")) // second run must not fail
    spark.catalog.tableExists("cma_curated_dev.deal") shouldBe true
    spark.catalog.tableExists("cma_audit_dev.stream_batch") shouldBe true
  }

  test("job loads Hive partitions, quarantines, audits and is idempotent on re-run") {
    DealIngestionJob.execute(args, Some(spark)) shouldBe ExitCodes.Success
    DealIngestionJob.execute(args, Some(spark)) shouldBe ExitCodes.Success

    val curated = spark.table("cma_curated_dev.deal").filter(col("business_date") === "2026-10-26")
    curated.count() shouldBe 5
    curated.filter(col("deal_id") === "DL-1004").select("currency", "product_type").head().toSeq shouldBe Seq("USD", "LEVERAGED_LOAN")

    val rejects = spark.table("cma_quarantine_dev.deal_reject").filter(col("business_date") === "2026-10-26")
    rejects.count() shouldBe 6
    rejects.filter(col("deal_id") === "DL-1008").select("_reject_reasons").head().getString(0) should include("share_within_deal")

    spark.table("cma_audit_dev.pipeline_run").filter(col("status") === "SUCCEEDED").count() shouldBe 2
  }

  test("a run for a day without landing data fails with exit 3 and leaves other partitions untouched") {
    val before = spark.table("cma_curated_dev.deal").count()
    val noData = args.map(a => if (a == "2026-10-26") "2026-10-27" else a)
    DealIngestionJob.execute(noData, Some(spark)) shouldBe ExitCodes.SourceMissing
    spark.table("cma_curated_dev.deal").count() shouldBe before
  }
}
