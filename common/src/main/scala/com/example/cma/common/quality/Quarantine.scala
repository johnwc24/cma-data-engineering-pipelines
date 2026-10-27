package com.example.cma.common.quality

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

/**
 * Splits a dataset into valid rows and quarantined rows.
 *
 * Quarantined rows keep every original column plus:
 *   _reject_reasons  array<string>  names of the rules each row broke
 *   _rejected_at     timestamp
 * so data owners can see exactly why a record was held back, fix it at source and replay it.
 */
object Quarantine {
  val ReasonsCol    = "_reject_reasons"
  val RejectedAtCol = "_rejected_at"

  final case class Split(valid: DataFrame, rejected: DataFrame)

  def split(df: DataFrame, rules: Seq[RowRule]): Split = {
    if (rules.isEmpty) {
      Split(df, emptyRejects(df))
    } else {
      val reasons: Column = filter(
        array(rules.map(r => when(r.violation, lit(r.name))): _*),
        (c: Column) => c.isNotNull
      )
      val tagged = df.withColumn(ReasonsCol, reasons)
      Split(
        valid = tagged.filter(size(col(ReasonsCol)) === 0).drop(ReasonsCol),
        rejected = tagged.filter(size(col(ReasonsCol)) > 0).withColumn(RejectedAtCol, current_timestamp())
      )
    }
  }

  /** Rows Spark could not parse at all (PERMISSIVE mode `_corrupt_record`). */
  def corruptRecords(df: DataFrame, corruptCol: String = "_corrupt_record"): Split =
    if (!df.columns.contains(corruptCol)) Split(df, emptyRejects(df))
    else {
      val bad = df
        .filter(col(corruptCol).isNotNull)
        .withColumn(ReasonsCol, array(lit("malformed_record")))
        .withColumn(RejectedAtCol, current_timestamp())
      Split(df.filter(col(corruptCol).isNull).drop(corruptCol), bad)
    }

  private def emptyRejects(df: DataFrame): DataFrame =
    df.limit(0)
      .withColumn(ReasonsCol, array().cast("array<string>"))
      .withColumn(RejectedAtCol, current_timestamp())
}
