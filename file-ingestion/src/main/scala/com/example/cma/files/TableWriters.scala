package com.example.cma.files

import java.sql.Date
import java.time.LocalDate

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Parquet and Delta Lake writers with idempotent re-run semantics:
 *
 *  overwrite-partition  parquet: dynamic partition overwrite; delta: replaceWhere on the business date
 *  append               plain append (e.g. immutable event files)
 *  merge                delta only: upsert on merge keys (latest record wins), ACID
 */
object TableWriters {

  def write(spark: SparkSession, df: DataFrame, target: TargetSpec, businessDate: LocalDate): Unit =
    (target.format, target.mode) match {
      case ("parquet", "overwrite-partition") =>
        df.write.mode(SaveMode.Overwrite)
          .option("partitionOverwriteMode", "dynamic")
          .partitionBy(target.partitionBy: _*)
          .parquet(target.path)

      case ("parquet", "append") =>
        df.write.mode(SaveMode.Append).partitionBy(target.partitionBy: _*).parquet(target.path)

      case ("delta", "overwrite-partition") =>
        df.write.format("delta").mode(SaveMode.Overwrite)
          .option("replaceWhere", s"business_date = '$businessDate'")
          .partitionBy(target.partitionBy: _*)
          .save(target.path)

      case ("delta", "append") =>
        df.write.format("delta").mode(SaveMode.Append).partitionBy(target.partitionBy: _*).save(target.path)

      case ("delta", "merge") =>
        if (!DeltaTable.isDeltaTable(spark, target.path)) {
          // first load creates the table
          df.write.format("delta").partitionBy(target.partitionBy: _*).save(target.path)
        } else {
          val condition = target.mergeKeys.map(k => s"t.`$k` = s.`$k`").mkString(" AND ")
          DeltaTable.forPath(spark, target.path).as("t")
            .merge(df.as("s"), condition)
            .whenMatched().updateAll()
            .whenNotMatched().insertAll()
            .execute()
        }

      case (f, m) => throw new IllegalArgumentException(s"unsupported target $f/$m")
    }

  /** Read back what is now in the target for one business date (for reconciliation). */
  def readPartition(spark: SparkSession, target: TargetSpec, businessDate: LocalDate): DataFrame = {
    val df = if (target.format == "delta") spark.read.format("delta").load(target.path) else spark.read.parquet(target.path)
    if (df.columns.contains("business_date")) df.filter(col("business_date") === lit(Date.valueOf(businessDate))) else df
  }
}
