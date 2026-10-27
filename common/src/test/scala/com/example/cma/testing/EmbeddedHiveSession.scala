package com.example.cma.testing

import java.io.File
import java.nio.file.Files

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.io.Source

/**
 * Local SparkSession with a real Hive catalog (embedded Derby metastore in a temp dir),
 * so Hive DDL, insertInto, dynamic partition overwrite and spark.table run exactly as on
 * the cluster - without a cluster. Requires spark-hive on the test classpath.
 */
trait EmbeddedHiveSession extends BeforeAndAfterAll { self: Suite =>

  private lazy val root = Files.createTempDirectory("embedded-hive").toFile

  @transient lazy val spark: SparkSession = {
    // A plain session from an earlier suite in this JVM would shadow the Hive catalog
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()

    val derbyUrl = s"jdbc:derby:;databaseName=${new File(root, "metastore_db").getAbsolutePath};create=true"
    SparkSession
      .builder()
      .master("local[2]")
      .appName(getClass.getSimpleName)
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.ansi.enabled", "false")
      .config("spark.sql.warehouse.dir", new File(root, "warehouse").getAbsolutePath)
      .config("spark.hadoop.javax.jdo.option.ConnectionURL", derbyUrl)
      .config("spark.hadoop.datanucleus.schema.autoCreateAll", "true")
      .config("spark.hadoop.hive.metastore.schema.verification", "false")
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .enableHiveSupport()
      .getOrCreate()
  }

  /** Run a .hql file statement by statement, substituting ${var} placeholders. */
  def runHql(file: File, vars: Map[String, String]): Unit = {
    val src = Source.fromFile(file, "UTF-8")
    val text =
      try src.getLines().filterNot(_.trim.startsWith("--")).mkString("\n")
      finally src.close()
    val substituted = vars.foldLeft(text) { case (acc, (k, v)) => acc.replace("${" + k + "}", v) }
    substituted.split(";").map(_.trim).filter(_.nonEmpty).foreach(spark.sql)
  }

  override protected def afterAll(): Unit = {
    try {
      spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    } finally super.afterAll()
  }
}
