package com.example.cma.common.quality

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

sealed trait Severity
object Severity {
  /** Breach blocks the write. */
  case object Error extends Severity
  /** Breach is recorded and alerted on, but the run continues. */
  case object Warn extends Severity
}

/**
 * A data-quality rule. `maxFailureRatio` is the tolerated share of failing rows
 * (0.0 = zero tolerance) before the rule is considered breached.
 */
sealed trait DqRule {
  def name: String
  def dimension: String
  def severity: Severity
  def maxFailureRatio: Double
}

/** Row-level rule: `violation` is true for each offending row, so it can also drive quarantine. */
sealed trait RowRule extends DqRule {
  def violation: Column
}

/** Dataset-level rule evaluated with its own query. */
sealed trait DatasetRule extends DqRule {
  /** Returns (rows checked, rows failing). */
  def evaluate(df: DataFrame): (Long, Long)
}

// ---------------------------------------------------------------- completeness / validity

final case class NotNull(column: String, severity: Severity = Severity.Error, maxFailureRatio: Double = 0.0)
    extends RowRule {
  val name      = s"not_null:$column"
  val dimension = "completeness"
  def violation: Column = col(column).isNull || (col(column).cast("string") === lit(""))
}

final case class AllowedValues(
    column: String,
    allowed: Seq[String],
    severity: Severity = Severity.Error,
    maxFailureRatio: Double = 0.0
) extends RowRule {
  val name      = s"allowed_values:$column"
  val dimension = "validity"
  def violation: Column = col(column).isNotNull && !col(column).isin(allowed: _*)
}

final case class MatchesPattern(
    column: String,
    regex: String,
    severity: Severity = Severity.Error,
    maxFailureRatio: Double = 0.0
) extends RowRule {
  val name      = s"pattern:$column"
  val dimension = "validity"
  def violation: Column = col(column).isNotNull && !col(column).rlike(regex)
}

final case class InRange(
    column: String,
    min: Option[Double] = None,
    max: Option[Double] = None,
    severity: Severity = Severity.Error,
    maxFailureRatio: Double = 0.0
) extends RowRule {
  val name      = s"range:$column"
  val dimension = "validity"
  def violation: Column = {
    val c       = col(column).cast("double")
    val belowMin = min.map(m => c < lit(m)).getOrElse(lit(false))
    val aboveMax = max.map(m => c > lit(m)).getOrElse(lit(false))
    col(column).isNotNull && (belowMin || aboveMax)
  }
}

/** Free-form SQL predicate that is TRUE for a violating row, e.g. "settlement_date < trade_date". */
final case class Expectation(
    name: String,
    violationSql: String,
    dimension: String = "consistency",
    severity: Severity = Severity.Error,
    maxFailureRatio: Double = 0.0
) extends RowRule {
  def violation: Column = coalesce(expr(violationSql), lit(false))
}

// ---------------------------------------------------------------- dataset-level

final case class Unique(columns: Seq[String], severity: Severity = Severity.Error, maxFailureRatio: Double = 0.0)
    extends DatasetRule {
  val name      = s"unique:${columns.mkString("+")}"
  val dimension = "uniqueness"
  def evaluate(df: DataFrame): (Long, Long) = {
    val total = df.count()
    val dupRows = df
      .groupBy(columns.map(col): _*)
      .count()
      .filter(col("count") > 1)
      .agg(coalesce(sum("count"), lit(0L)))
      .head()
      .getLong(0)
    (total, dupRows)
  }
}

final case class ReferentialIntegrity(
    column: String,
    reference: DataFrame,
    referenceColumn: String,
    severity: Severity = Severity.Error,
    maxFailureRatio: Double = 0.0
) extends DatasetRule {
  val name      = s"ref_integrity:$column"
  val dimension = "integrity"
  def evaluate(df: DataFrame): (Long, Long) = {
    val keyed  = df.filter(col(column).isNotNull)
    val total  = keyed.count()
    val refKeys = reference.select(col(referenceColumn).as("__ref_key")).distinct()
    val orphans = keyed.join(broadcast(refKeys), keyed(column) === refKeys("__ref_key"), "left_anti").count()
    (total, orphans)
  }
}

/** Fails (1 of 1) when the newest record is older than `maxAgeHours` relative to `asOfEpochSeconds`. */
final case class Freshness(
    timestampColumn: String,
    maxAgeHours: Long,
    asOfEpochSeconds: Long,
    severity: Severity = Severity.Warn,
    maxFailureRatio: Double = 0.0
) extends DatasetRule {
  val name      = s"freshness:$timestampColumn"
  val dimension = "timeliness"
  def evaluate(df: DataFrame): (Long, Long) = {
    val latest = df.agg(max(col(timestampColumn).cast("long"))).head()
    if (latest.isNullAt(0)) (1L, 1L)
    else {
      val ageHours = (asOfEpochSeconds - latest.getLong(0)) / 3600.0
      (1L, if (ageHours > maxAgeHours) 1L else 0L)
    }
  }
}

/** Fails when the dataset has fewer than `minRows` rows (catches silently-empty extracts). */
final case class MinRowCount(minRows: Long, severity: Severity = Severity.Error) extends DatasetRule {
  val name            = s"min_row_count:$minRows"
  val dimension       = "completeness"
  val maxFailureRatio = 0.0
  def evaluate(df: DataFrame): (Long, Long) = {
    val n = df.count()
    (1L, if (n < minRows) 1L else 0L)
  }
}
