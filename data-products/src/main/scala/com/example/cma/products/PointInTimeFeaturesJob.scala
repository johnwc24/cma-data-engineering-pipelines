package com.example.cma.products

import java.sql.Date
import java.time.LocalDate

import com.example.cma.common.config.JobArgs
import com.example.cma.common.errors.{ConfigError, PipelineError}
import com.example.cma.common.job.JobSummary
import com.example.cma.common.quality.{DataQuality, MinRowCount, NotNull, Unique}
import com.example.cma.common.util.Retry
import com.typesafe.config.Config
import io.delta.tables.DeltaTable
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}

import scala.util.Try

/**
 * Point-in-time correct borrower features for the POD's data scientists.
 *
 * For every (party_id, as_of_date) in the spine, features use only what was known on that date:
 *  - deal attributes from the SCD2 version valid on as_of_date (valid_from <= d <= valid_to),
 *    so later amendments never leak backwards
 *  - deals traded on or before as_of_date, ratings published on or before as_of_date
 *
 * Output: one row per party per as-of date, partitioned by as_of_date (Delta, replaceWhere).
 */
object PointInTimeFeaturesJob extends ProductJob {
  val appName = "cma-pit-features"
  override def requiredConfig: Seq[String] =
    super.requiredConfig ++ Seq("features.deal-history-path", "features.ratings-path", "features.output-path")

  /** Rating scale (S&P/Fitch style; Moody's mapped onto it). Lower is better. */
  val RatingNotch: Map[String, Int] = Seq(
    "AAA", "AA+", "AA", "AA-", "A+", "A", "A-", "BBB+", "BBB", "BBB-",
    "BB+", "BB", "BB-", "B+", "B", "B-", "CCC+", "CCC", "CCC-", "CC", "C", "D"
  ).zipWithIndex.map { case (r, i) => r -> (i + 1) }.toMap ++ Map(
    "Aaa" -> 1, "Aa1" -> 2, "Aa2" -> 3, "Aa3" -> 4, "A1" -> 5, "A2" -> 6, "A3" -> 7,
    "Baa1" -> 8, "Baa2" -> 9, "Baa3" -> 10, "Ba1" -> 11, "Ba2" -> 12, "Ba3" -> 13,
    "B1" -> 14, "B2" -> 15, "B3" -> 16, "Caa1" -> 17, "Caa2" -> 18, "Caa3" -> 19, "Ca" -> 20
  )

  private def notch(c: Column): Column = {
    val m = typedLit(RatingNotch)
    element_at(m, c)
  }

  def run(spark: SparkSession, args: JobArgs, conf: Config): Either[PipelineError, JobSummary] =
    paths(conf).flatMap { resolve =>
      Try(build(spark, args, conf, resolve)).toEither.left.map(PipelineError.classify).flatMap(identity)
    }

  /**
   * Pure feature computation, unit-tested directly.
   *
   * @param dealHistory SCD2 rows: deal_id, borrower_party_id, deal_amount_usd, bank_share_usd, bank_share_pct,
   *                    product_type, trade_date, valid_from, valid_to
   * @param ratings     party_id, agency, rating, rated_on
   * @param spine       party_id, as_of_date
   */
  def features(dealHistory: DataFrame, ratings: DataFrame, spine: DataFrame, lookbackDays: Int): DataFrame = {
    val s = spine.select(col("party_id"), col("as_of_date").cast("date"))

    // deal versions valid on the as-of date, traded within the look-back window
    val asOfDeals = s.join(dealHistory.as("h"),
        col("h.borrower_party_id") === s("party_id") &&
        col("h.valid_from") <= s("as_of_date") && col("h.valid_to") >= s("as_of_date") &&
        col("h.trade_date") <= s("as_of_date") &&
        col("h.trade_date") > date_sub(s("as_of_date"), lookbackDays))
      .groupBy(s("party_id"), s("as_of_date"))
      .agg(
        countDistinct(col("h.deal_id")).as(s"deals_${lookbackDays}d"),
        sum(col("h.bank_share_usd")).cast("decimal(20,2)").as(s"bank_share_usd_${lookbackDays}d"),
        sum(col("h.deal_amount_usd")).cast("decimal(22,2)").as(s"deal_volume_usd_${lookbackDays}d"),
        round(avg(col("h.bank_share_pct")), 4).as(s"avg_bank_share_pct_${lookbackDays}d"),
        countDistinct(col("h.product_type")).as("distinct_products"),
        max(col("h.trade_date")).as("last_trade_date"))

    // ratings known on the as-of date
    val r = ratings.select(col("party_id").as("r_party"), col("agency"), col("rating"), col("rated_on").cast("date"))
      .withColumn("notch", notch(col("rating")))
    val known = s.join(r, r("r_party") === s("party_id") && r("rated_on") <= s("as_of_date"))
    val latestPerAgency = known
      .withColumn("__rn", row_number().over(Window.partitionBy("party_id", "as_of_date", "agency").orderBy(col("rated_on").desc)))
      .filter(col("__rn") === 1)
    val ratingFeatures = latestPerAgency.groupBy("party_id", "as_of_date").agg(
      max("notch").as("worst_rating_notch"),          // most conservative across agencies
      min("notch").as("best_rating_notch"),
      countDistinct("agency").as("rating_agencies"))
    val ratingChanges = known
      .filter(col("rated_on") > date_sub(col("as_of_date"), lookbackDays))
      .groupBy("party_id", "as_of_date").agg(count(lit(1)).as(s"rating_actions_${lookbackDays}d"))

    s.join(asOfDeals, Seq("party_id", "as_of_date"), "left")
      .join(ratingFeatures, Seq("party_id", "as_of_date"), "left")
      .join(ratingChanges, Seq("party_id", "as_of_date"), "left")
      .na.fill(0L, Seq(s"deals_${lookbackDays}d", "distinct_products", "rating_agencies", s"rating_actions_${lookbackDays}d"))
      .withColumn("days_since_last_deal", datediff(col("as_of_date"), col("last_trade_date")))
      .drop("last_trade_date")
  }

  private def build(spark: SparkSession, args: JobArgs, conf: Config, resolve: String => String): Either[PipelineError, JobSummary] = {
    val f     = conf.getConfig("features")
    val retry = retryPolicy(conf)
    val asOf  = args.businessDate
    val lookback = f.getInt("lookback-days")

    def load(path: String): DataFrame = {
      val p = resolve(path)
      if (Try(DeltaTable.isDeltaTable(spark, p)).getOrElse(false)) spark.read.format("delta").load(p) else spark.read.parquet(p)
    }

    val (history, ratings) = Retry.withBackoff("read-inputs", retry)((load(f.getString("deal-history-path")).cache(), load(f.getString("ratings-path")).cache()))
    val needed = Seq("deal_id", "borrower_party_id", "valid_from", "valid_to", "trade_date")
    val missing = needed.filterNot(history.columns.contains)
    if (missing.nonEmpty) return Left(ConfigError(s"deal history is not SCD2-shaped, missing: ${missing.mkString(", ")}"))

    // Spine: every borrower seen in the history, as of the run date
    val spine = history.select(col("borrower_party_id").as("party_id")).distinct().withColumn("as_of_date", lit(Date.valueOf(asOf)))
    val out = features(history, ratings, spine, lookback).cache()

    val gate = DataQuality.evaluate("pit_features", out, Seq(MinRowCount(1), Unique(Seq("party_id", "as_of_date")), NotNull("party_id")))
    gate.enforce() match {
      case Left(e) => return Left(e)
      case Right(_) =>
    }

    if (!args.dryRun)
      Retry.withBackoff("write-features", retry) {
        out.write.format("delta").mode(SaveMode.Overwrite)
          .option("replaceWhere", s"as_of_date = '$asOf'")
          .partitionBy("as_of_date")
          .save(resolve(f.getString("output-path")))
      }

    Right(JobSummary(args.runId, Map("parties" -> out.count(), "lookback_days" -> lookback.toLong)))
  }
}
