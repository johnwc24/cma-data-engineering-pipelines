package com.example.cma.products

import java.io.{File, PrintWriter}
import java.nio.file.Files
import java.sql.{Connection, DriverManager}
import java.util.UUID

import com.example.cma.common.errors.PipelineError.ExitCodes
import com.example.cma.files.DeltaTestSession
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** PostgreSQL-style incremental loads against an in-memory H2 database in PostgreSQL mode. */
class JdbcIncrementalIngestionJobSpec extends AnyFunSuite with Matchers with DeltaTestSession {

  private def newDb(): (String, Connection) = {
    val url = s"jdbc:h2:mem:party_${UUID.randomUUID().toString.replace("-", "")};MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1"
    val c = DriverManager.getConnection(url, "sa", "")
    c.createStatement().execute(
      "CREATE TABLE party (party_sk INT, party_id VARCHAR(20), legal_name VARCHAR(100), country_code VARCHAR(2), updated_at TIMESTAMP)")
    (url, c)
  }

  private def exec(c: Connection, sql: String): Unit = c.createStatement().execute(sql)

  private def config(url: String, out: File, table: String = "party", lookback: Int = 0): String = {
    val f = Files.createTempFile("jdbc", ".conf").toFile
    val w = new PrintWriter(f)
    try w.write(
      s"""local.jdbc-sources.test-db {
         |  url = "$url"
         |  driver = "org.h2.Driver"
         |  table = "$table"
         |  key-columns = ["party_id"]
         |  watermark-column = "updated_at"
         |  partition-column = "party_sk"
         |  num-partitions = 2
         |  lookback-minutes = $lookback
         |  user = "sa"
         |  password = ""
         |  target-path = "$out/party_delta"
         |  watermark-path = "$out/_wm"
         |  quarantine-path = "$out/quarantine"
         |}
         |""".stripMargin)
    finally w.close()
    f.getAbsolutePath
  }

  private def run(cfg: String): Int =
    JdbcIncrementalIngestionJob.execute(
      Array("--env", "local", "--business-date", "2026-10-26", "--run-id", "jdbc-test", "--config", cfg,
        "--conf", "source=test-db,retry.initial-delay-ms=1"),
      Some(spark))

  private def target(out: File) = spark.read.format("delta").load(s"$out/party_delta")

  test("first load, incremental update and no-op re-run; null keys quarantined") {
    val (url, c) = newDb()
    val out = Files.createTempDirectory("jdbc").toFile
    exec(c, """INSERT INTO party VALUES
      (1, 'PTY-001', 'Acme Holdings plc', 'GB', TIMESTAMP '2026-10-25 09:00:00'),
      (2, 'PTY-002', 'Globex SA', 'FR', TIMESTAMP '2026-10-25 10:00:00'),
      (3, 'PTY-003', 'Initech Inc', 'US', TIMESTAMP '2026-10-25 11:00:00'),
      (4, NULL, 'No key Ltd', 'GB', TIMESTAMP '2026-10-25 11:30:00')""")
    val cfg = config(url, out)

    run(cfg) shouldBe ExitCodes.Success
    target(out).count() shouldBe 3
    spark.read.parquet(s"$out/quarantine").count() shouldBe 1

    // an update and an insert after the watermark
    exec(c, "UPDATE party SET legal_name = 'Globex SE', updated_at = TIMESTAMP '2026-10-26 08:00:00' WHERE party_id = 'PTY-002'")
    exec(c, "INSERT INTO party VALUES (5, 'PTY-005', 'Wayne Enterprises', 'US', TIMESTAMP '2026-10-26 08:30:00')")
    run(cfg) shouldBe ExitCodes.Success
    target(out).count() shouldBe 4
    target(out).filter(col("party_id") === "PTY-002").select("legal_name").head().getString(0) shouldBe "Globex SE"

    // nothing new: target unchanged, watermark history grows by one entry per successful run
    run(cfg) shouldBe ExitCodes.Success
    target(out).count() shouldBe 4
    spark.read.parquet(s"$out/_wm").agg(max("watermark")).head().getTimestamp(0).toString shouldBe "2026-10-26 08:30:00.0"
  }

  test("look-back window catches a late-committing row stamped before the watermark") {
    val (url, c) = newDb()
    val out = Files.createTempDirectory("jdbc-late").toFile
    exec(c, "INSERT INTO party VALUES (1, 'PTY-001', 'Acme', 'GB', TIMESTAMP '2026-10-26 10:00:00')")
    val cfg = config(url, out, lookback = 60)
    run(cfg) shouldBe ExitCodes.Success

    // committed after the first run, but its timestamp is 30 minutes before that run's watermark
    exec(c, "INSERT INTO party VALUES (2, 'PTY-002', 'Late Ltd', 'FR', TIMESTAMP '2026-10-26 09:30:00')")
    run(cfg) shouldBe ExitCodes.Success
    target(out).select("party_id").as[String](org.apache.spark.sql.Encoders.STRING).collect().toSet shouldBe Set("PTY-001", "PTY-002")
  }

  test("a missing source table is a configuration error (exit 2), not a retry loop") {
    val (url, _) = newDb()
    val out = Files.createTempDirectory("jdbc-missing").toFile
    run(config(url, out, table = "no_such_table")) shouldBe ExitCodes.Config
  }

  test("credentials must come from env/file for real sources") {
    val out = Files.createTempDirectory("jdbc-cred").toFile
    val f = Files.createTempFile("nocred", ".conf").toFile
    val w = new PrintWriter(f)
    try w.write(s"""local.jdbc-sources.test-db { url = "jdbc:postgresql://x/y", table = "t", key-columns = ["k"],
                  |  watermark-column = "u", target-path = "$out/t", watermark-path = "$out/w", quarantine-path = "$out/q" }""".stripMargin)
    finally w.close()
    run(f.getAbsolutePath) shouldBe ExitCodes.Config
  }
}
