package com.example.cma.gcp

import com.example.cma.common.config.{JobArgs, PipelineConfig}
import com.example.cma.testing.SparkTestSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class GcpJobsSpec extends AnyFunSuite with Matchers with SparkTestSession {
  import spark.implicits._

  test("migration fingerprint ignores row order and detects changed values") {
    val a = Seq(("D1", BigDecimal("10.00")), ("D2", BigDecimal("20.00"))).toDF("id", "amt")
    val b = Seq(("D2", BigDecimal("20.00")), ("D1", BigDecimal("10.00"))).toDF("id", "amt")
    val c = Seq(("D1", BigDecimal("10.00")), ("D2", BigDecimal("20.01"))).toDF("id", "amt")

    val fa = HiveToBigQueryMigrationJob.fingerprint(a, "amt")
    fa shouldBe HiveToBigQueryMigrationJob.fingerprint(b, "amt")
    fa.rows shouldBe 2
    fa.amount shouldBe BigDecimal("30.0000")
    HiveToBigQueryMigrationJob.fingerprint(c, "amt").checksum should not be fa.checksum
  }

  test("fingerprint is independent of column order") {
    val a = Seq(("D1", 1)).toDF("id", "n")
    val b = Seq((1, "D1")).toDF("n", "id")
    HiveToBigQueryMigrationJob.fingerprint(a, "n").checksum shouldBe HiveToBigQueryMigrationJob.fingerprint(b, "n").checksum
  }

  test("BigQuery permanent errors are recognised from nested causes") {
    val missing = new RuntimeException("load failed", new RuntimeException("Not found: Table cma-data-dev:cma_staging.deal_stg"))
    BigQueryIO.isMissingTable(missing) shouldBe true
    BigQueryIO.isPermissionDenied(new RuntimeException("Access Denied: Table x: User does not have permission")) shouldBe true
    BigQueryIO.isMissingTable(new RuntimeException("backendError")) shouldBe false
  }

  test("every environment config resolves with the keys the jobs require") {
    Seq("dev", "uat", "prod").foreach { env =>
      val conf = PipelineConfig.load(JobArgs(env = env), "gcp-dataproc")
      conf.isRight shouldBe true
      PipelineConfig
        .requirePaths(conf.toOption.get, (GcsToBigQueryJob.requiredConfig ++ HiveToBigQueryMigrationJob.requiredConfig): _*)
        .isRight shouldBe true
    }
  }
}
