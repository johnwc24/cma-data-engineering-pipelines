package com.example.cma.common.recon

import com.example.cma.common.errors.ReconciliationError
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

final case class ReconCheck(name: String, source: BigDecimal, target: BigDecimal, tolerance: BigDecimal) {
  def difference: BigDecimal  = (source - target).abs
  def withinTolerance: Boolean = difference <= tolerance
  override def toString: String = s"$name source=$source target=$target diff=$difference tol=$tolerance"
}

/**
 * Control totals between pipeline stages.
 *
 * Two kinds of check are used across the repo:
 *  - completeness: raw rows == loaded rows + rejected rows (nothing silently dropped)
 *  - value: sum(amount) at source == sum(amount) at target (nothing silently changed)
 */
object Reconciliation {
  private val log = LoggerFactory.getLogger(getClass)

  final case class Totals(rows: Long, amount: BigDecimal)

  def totals(df: DataFrame, amountCol: Option[String]): Totals = {
    val aggs = Seq(count(lit(1)).as("rows")) ++
      amountCol.map(c => coalesce(sum(col(c).cast("decimal(38,4)")), lit(0).cast("decimal(38,4)")).as("amount"))
    val row = df.agg(aggs.head, aggs.tail: _*).head()
    Totals(row.getLong(0), if (amountCol.isDefined) BigDecimal(row.getDecimal(1)) else BigDecimal(0))
  }

  def check(dataset: String, checks: Seq[ReconCheck]): Either[ReconciliationError, Seq[ReconCheck]] = {
    checks.foreach(c => if (c.withinTolerance) log.info(s"RECON OK  $c") else log.error(s"RECON BREAK $c"))
    val breaks = checks.filterNot(_.withinTolerance)
    if (breaks.isEmpty) Right(checks) else Left(ReconciliationError(dataset, breaks.map(_.toString)))
  }

  /** Every input row is accounted for as either loaded or rejected. */
  def completeness(name: String, input: Long, loaded: Long, rejected: Long): ReconCheck =
    ReconCheck(name, BigDecimal(input), BigDecimal(loaded + rejected), BigDecimal(0))
}
