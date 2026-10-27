package com.example.cma.gcp

import java.sql.Date
import java.time.LocalDate

import com.example.cma.common.errors.ConfigError
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable

/**
 * In-memory stand-in for BigQuery used by local tests.
 *
 * Tables are DataFrames keyed by name; partition overwrite and append follow BigQuery
 * semantics, and `read` ignores the SQL filter (tests filter themselves). Writes are
 * materialised with localCheckpoint so later reads see a stable snapshot.
 *
 * `corruptWrites` simulates a sink that silently drops a row, to prove reconciliation catches it.
 */
class InMemoryWarehouse(spark: SparkSession, corruptWrites: Boolean = false) extends WarehouseIO {
  val tables: mutable.Map[String, DataFrame] = mutable.Map.empty
  val writes: mutable.ArrayBuffer[String]    = mutable.ArrayBuffer.empty

  def seed(name: String, df: DataFrame): Unit = tables(name) = df.localCheckpoint()

  def table(datasetKey: String, name: String): String = s"$datasetKey.$name"

  def read(table: String, filter: Option[String]): DataFrame =
    tables.getOrElse(table, throw ConfigError(s"BigQuery table not found: $table (seed it in the test)"))

  def overwritePartition(df: DataFrame, table: String, partitionField: String, day: LocalDate): Unit = {
    val incoming = if (corruptWrites) df.limit(math.max(0, df.count().toInt - 1)) else df
    val kept = tables.get(table).map(_.filter(col(partitionField) =!= lit(Date.valueOf(day))))
    val merged = kept.map(_.unionByName(incoming, allowMissingColumns = true)).getOrElse(incoming)
    tables(table) = merged.localCheckpoint()
    writes += s"overwrite:$table@$day"
  }

  def append(df: DataFrame, table: String): Unit = {
    tables(table) = tables.get(table).map(_.unionByName(df, allowMissingColumns = true)).getOrElse(df).localCheckpoint()
    writes += s"append:$table"
  }

  def count(table: String): Long = tables.get(table).map(_.count()).getOrElse(0L)
}
