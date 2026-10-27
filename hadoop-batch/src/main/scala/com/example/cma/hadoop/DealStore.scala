package com.example.cma.hadoop

import java.sql.Date
import java.time.LocalDate

import com.example.cma.common.errors.ConfigError
import com.example.cma.deals.{DealSchemas, RawDealReader}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Storage abstraction so the same job runs against Hive on the cluster
 * and plain files on a laptop / in CI.
 */
trait DealStore {
  def readRawDeals(businessDate: LocalDate): DataFrame
  def readParties(): DataFrame
  def readFxRates(): DataFrame
  def writeCurated(df: DataFrame): Unit
  def readCuratedPartition(businessDate: LocalDate): DataFrame
  def writeRejects(df: DataFrame): Unit
  def appendAudit(name: String, df: DataFrame): Unit
}

object DealStore {
  def apply(spark: SparkSession, conf: Config): DealStore = conf.getString("sink.mode") match {
    case "hive"  => new HiveDealStore(spark, conf)
    case "files" => new FileDealStore(spark, conf)
    case other   => throw ConfigError(s"unknown sink.mode '$other' (expected hive|files)")
  }

  def readRawDeals(spark: SparkSession, conf: Config, businessDate: LocalDate): DataFrame =
    RawDealReader.readCsv(spark, RawDealReader.landingPath(conf.getString("paths.landing"), businessDate))
}

/** Hive-backed store: partitioned tables, dynamic partition overwrite for idempotent re-runs. */
final class HiveDealStore(spark: SparkSession, conf: Config) extends DealStore {
  private def table(key: String): String = conf.getString(s"tables.$key")

  private def requireTable(name: String): Unit =
    if (!spark.catalog.tableExists(name)) throw ConfigError(s"Hive table $name does not exist; run hive/ddl first")

  def readRawDeals(businessDate: LocalDate): DataFrame =
    DealStore.readRawDeals(spark, conf, businessDate)

  def readParties(): DataFrame = {
    requireTable(table("parties"))
    spark.table(table("parties")).filter(col("is_active") === true)
  }

  def readFxRates(): DataFrame = {
    requireTable(table("fx-rates"))
    spark.table(table("fx-rates"))
  }

  /** insertInto is positional, so align columns to the table definition first. */
  private def insertOverwrite(df: DataFrame, target: String): Unit = {
    requireTable(target)
    val ordered = df.select(spark.table(target).columns.map(col): _*)
    ordered.write.mode(SaveMode.Overwrite).insertInto(target)
  }

  def writeCurated(df: DataFrame): Unit = insertOverwrite(df, table("curated"))

  def readCuratedPartition(businessDate: LocalDate): DataFrame =
    spark.table(table("curated")).filter(col("business_date") === lit(Date.valueOf(businessDate)))

  def writeRejects(df: DataFrame): Unit = {
    // array<string> reasons are stored as a delimited string to keep the Hive table simple
    val flat = df.withColumn("_reject_reasons", concat_ws("|", col("_reject_reasons")))
    val target = table("rejects")
    requireTable(target)
    val cols = spark.table(target).columns
    val aligned = cols.foldLeft(flat)((acc, c) => if (acc.columns.contains(c)) acc else acc.withColumn(c, lit(null).cast("string")))
    aligned.select(cols.map(col): _*).write.mode(SaveMode.Overwrite).insertInto(target)
  }

  def appendAudit(name: String, df: DataFrame): Unit = {
    val target = table(name)
    requireTable(target)
    df.select(spark.table(target).columns.map(col): _*).write.mode(SaveMode.Append).insertInto(target)
  }
}

/** Local/CI store: CSV inputs, parquet outputs partitioned like the Hive tables. */
final class FileDealStore(spark: SparkSession, conf: Config) extends DealStore {
  private def path(key: String): String = conf.getString(s"paths.$key")

  def readRawDeals(businessDate: LocalDate): DataFrame =
    DealStore.readRawDeals(spark, conf, businessDate)

  def readParties(): DataFrame =
    spark.read.option("header", "true").schema(DealSchemas.party).csv(path("parties")).filter(col("is_active") === true)

  def readFxRates(): DataFrame =
    spark.read.option("header", "true").schema(DealSchemas.fxRate).csv(path("fx-rates"))

  def writeCurated(df: DataFrame): Unit =
    df.write.mode(SaveMode.Overwrite).partitionBy("business_date").parquet(path("curated"))

  def readCuratedPartition(businessDate: LocalDate): DataFrame =
    spark.read.parquet(path("curated")).filter(col("business_date") === lit(Date.valueOf(businessDate)))

  def writeRejects(df: DataFrame): Unit =
    df.write.mode(SaveMode.Overwrite).partitionBy("business_date").parquet(path("rejects"))

  def appendAudit(name: String, df: DataFrame): Unit =
    df.write.mode(SaveMode.Append).parquet(path(name))
}
