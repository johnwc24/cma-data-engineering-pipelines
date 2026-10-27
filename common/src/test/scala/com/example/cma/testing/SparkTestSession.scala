package com.example.cma.testing

import java.nio.file.Files

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

/** Shared local SparkSession for unit tests (exported as a test-jar for other modules). */
trait SparkTestSession extends BeforeAndAfterAll { self: Suite =>

  @transient lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local[2]")
    .appName(getClass.getSimpleName)
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.ansi.enabled", "false")
      .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    .config("spark.sql.warehouse.dir", Files.createTempDirectory("spark-warehouse").toString)
    .getOrCreate()

  override protected def afterAll(): Unit = {
    try spark.stop()
    finally super.afterAll()
  }
}
