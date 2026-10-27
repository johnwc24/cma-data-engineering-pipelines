package com.example.cma.common.quality

import java.sql.Timestamp

import com.example.cma.common.errors.DataQualityError
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

final case class DqResult(
    rule: String,
    dimension: String,
    severity: String,
    checked: Long,
    failed: Long,
    failureRatio: Double,
    threshold: Double,
    passed: Boolean
)

final case class DqReport(dataset: String, results: Seq[DqResult]) {
  def blockingFailures: Seq[DqResult] = results.filter(r => !r.passed && r.severity == Severity.Error.toString)
  def warnings: Seq[DqResult]         = results.filter(r => !r.passed && r.severity == Severity.Warn.toString)
  def passed: Boolean                 = blockingFailures.isEmpty

  /** Right(report) if no blocking rule breached, else a typed DataQualityError. */
  def enforce(): Either[DataQualityError, DqReport] =
    if (passed) Right(this)
    else
      Left(
        DataQualityError(
          dataset,
          blockingFailures.map(r => f"${r.rule} failed ${r.failed}/${r.checked} (${r.failureRatio * 100}%.2f%%)")
        )
      )

  /** Long-format results for the audit table `dq_results`. */
  def toDataFrame(spark: SparkSession, runId: String, businessDate: String): DataFrame = {
    import spark.implicits._
    val now = new Timestamp(System.currentTimeMillis())
    results
      .toDF()
      .withColumn("dataset", lit(dataset))
      .withColumn("run_id", lit(runId))
      .withColumn("business_date", lit(businessDate).cast("date"))
      .withColumn("evaluated_at", lit(now))
  }
}

object DataQuality {
  private val log = LoggerFactory.getLogger(getClass)

  /**
   * Evaluate rules against `df`. Row rules are computed in a single aggregation pass;
   * dataset rules run their own queries. Callers should cache `df` first.
   */
  def evaluate(dataset: String, df: DataFrame, rules: Seq[DqRule]): DqReport = {
    val rowRules     = rules.collect { case r: RowRule => r }
    val datasetRules = rules.collect { case r: DatasetRule => r }

    val rowResults: Seq[DqResult] =
      if (rowRules.isEmpty) Seq.empty
      else {
        val aggs = count(lit(1)).as("__total") +: rowRules.zipWithIndex.map { case (r, i) =>
          sum(when(r.violation, 1L).otherwise(0L)).as(s"__r$i")
        }
        val row   = df.agg(aggs.head, aggs.tail: _*).head()
        val total = row.getLong(0)
        rowRules.zipWithIndex.map { case (r, i) =>
          val failed = if (row.isNullAt(i + 1)) 0L else row.getLong(i + 1)
          toResult(r, total, failed)
        }
      }

    val datasetResults = datasetRules.map { r =>
      val (checked, failed) = r.evaluate(df)
      toResult(r, checked, failed)
    }

    val report = DqReport(dataset, rowResults ++ datasetResults)
    report.results.foreach { r =>
      val msg = s"DQ [$dataset] ${r.rule}: ${r.failed}/${r.checked} failed (threshold ${r.threshold})"
      if (r.passed) log.info(s"PASS $msg") else if (r.severity == "Warn") log.warn(s"WARN $msg") else log.error(s"FAIL $msg")
    }
    report
  }

  private def toResult(r: DqRule, checked: Long, failed: Long): DqResult = {
    val ratio = if (checked == 0) 0.0 else failed.toDouble / checked
    DqResult(
      rule = r.name,
      dimension = r.dimension,
      severity = r.severity.toString,
      checked = checked,
      failed = failed,
      failureRatio = ratio,
      threshold = r.maxFailureRatio,
      passed = ratio <= r.maxFailureRatio
    )
  }
}
