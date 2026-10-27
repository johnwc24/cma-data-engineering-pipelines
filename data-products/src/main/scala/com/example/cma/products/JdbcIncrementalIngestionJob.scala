package com.example.cma.products

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.sql.{SQLException, Timestamp}
import java.time.LocalDate

import com.example.cma.common.config.JobArgs
import com.example.cma.common.errors._
import com.example.cma.common.job.JobSummary
import com.example.cma.common.recon.{ReconCheck, Reconciliation}
import com.example.cma.common.util.Retry
import com.example.cma.files.{TableWriters, TargetSpec}
import com.typesafe.config.Config
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.JavaConverters._
import scala.util.Try

/**
 * Incremental ingestion from PostgreSQL (or any JDBC source) using a high-watermark.
 *
 *   last watermark ─► window (wm - lookback, max(updated_at) at start] ─► partitioned JDBC read
 *     ─► latest row per key ─► Delta MERGE ─► verify keys landed ─► advance watermark
 *
 *  - The upper bound is fixed at the start of the run, so rows committed mid-run are picked
 *    up next time rather than half-read now.
 *  - The look-back window re-reads recent rows to catch late-committing transactions;
 *    the MERGE makes those re-reads harmless.
 *  - The watermark only moves after the write is verified: a failed run simply re-reads.
 *  - Credentials come from environment variables or a mounted secret file, never from config.
 */
object JdbcIncrementalIngestionJob extends ProductJob {
  val appName = "cma-jdbc-incremental"
  override def requiredConfig: Seq[String] = super.requiredConfig :+ "source"

  final case class SourceSpec(
      name: String,
      url: String,
      driver: String,
      table: String,
      keyColumns: Seq[String],
      watermarkColumn: String,
      partitionColumn: Option[String],
      numPartitions: Int,
      fetchSize: Int,
      lookbackMinutes: Long,
      initialWatermark: String,
      user: String,
      password: String,
      target: TargetSpec,
      watermarkPath: String,
      quarantinePath: String
  )

  def run(spark: SparkSession, args: JobArgs, conf: Config): Either[PipelineError, JobSummary] =
    for {
      resolve <- paths(conf)
      sc      <- section(conf, "jdbc-sources", conf.getString("source"))
      spec    <- Try(parse(conf.getString("source"), sc, resolve)).toEither.left.map(e => ConfigError(e.getMessage))
      summary <- Try(ingest(spark, args, conf, spec)).toEither.left.map(classifyJdbc).flatMap(identity)
    } yield summary

  private def parse(name: String, c: Config, resolve: String => String): SourceSpec = {
    def secret(key: String): String =
      if (c.hasPath(s"$key-env") && sys.env.contains(c.getString(s"$key-env"))) sys.env(c.getString(s"$key-env"))
      else if (c.hasPath(s"$key-file")) new String(Files.readAllBytes(Paths.get(c.getString(s"$key-file"))), StandardCharsets.UTF_8).trim
      else if (c.hasPath(key)) c.getString(key) // only for local/test databases
      else throw new IllegalArgumentException(s"jdbc source '$name': no $key (set $key-env or $key-file)")
    val strs = (p: String) => if (c.hasPath(p)) c.getStringList(p).asScala.toList else Nil
    SourceSpec(
      name = name,
      url = c.getString("url"),
      driver = if (c.hasPath("driver")) c.getString("driver") else "org.postgresql.Driver",
      table = c.getString("table"),
      keyColumns = strs("key-columns"),
      watermarkColumn = c.getString("watermark-column"),
      partitionColumn = if (c.hasPath("partition-column")) Some(c.getString("partition-column")) else None,
      numPartitions = if (c.hasPath("num-partitions")) c.getInt("num-partitions") else 4,
      fetchSize = if (c.hasPath("fetch-size")) c.getInt("fetch-size") else 10000,
      lookbackMinutes = if (c.hasPath("lookback-minutes")) c.getLong("lookback-minutes") else 60L,
      initialWatermark = if (c.hasPath("initial-watermark")) c.getString("initial-watermark") else "1970-01-01 00:00:00",
      user = secret("user"),
      password = secret("password"),
      target = TargetSpec("delta", resolve(c.getString("target-path")), "merge", strs("partition-by"), strs("key-columns")),
      watermarkPath = resolve(c.getString("watermark-path")),
      quarantinePath = resolve(c.getString("quarantine-path"))
    )
  }

  private def jdbc(spark: SparkSession, s: SourceSpec) =
    spark.read.format("jdbc")
      .option("url", s.url).option("driver", s.driver)
      .option("user", s.user).option("password", s.password)
      .option("fetchsize", s.fetchSize.toString)

  private def ts(t: Timestamp) = s"TIMESTAMP '${t.toString}'"

  private def ingest(spark: SparkSession, args: JobArgs, conf: Config, s: SourceSpec): Either[PipelineError, JobSummary] = {
    val retry = retryPolicy(conf)
    val last  = readWatermark(spark, s)
    val low   = new Timestamp(last.getTime - s.lookbackMinutes * 60000L)

    // Fix the upper bound now (consistent snapshot boundary for this run)
    val high: Option[Timestamp] = Retry.withBackoff(s"jdbc-max-${s.name}", retry) {
      val r = jdbc(spark, s).option("query", s"SELECT MAX(${s.watermarkColumn}) AS hw FROM ${s.table}").load().head()
      if (r.isNullAt(0)) None else Some(r.getTimestamp(0))
    }

    high match {
      case None =>
        Right(JobSummary(args.runId, Map("rows_read" -> 0L), Seq(s"${s.table} is empty")))
      case Some(hw) if !hw.after(last) && s.lookbackMinutes == 0 =>
        Right(JobSummary(args.runId, Map("rows_read" -> 0L), Seq("no new rows since last watermark")))
      case Some(hw) =>
        val window = s"${s.watermarkColumn} > ${ts(low)} AND ${s.watermarkColumn} <= ${ts(hw)}"
        val raw    = Retry.withBackoff(s"jdbc-read-${s.name}", retry)(read(spark, s, window)).cache()
        val rowsRead = raw.count()

        // Rows without a business key cannot be merged: quarantine them with a reason
        val badKey = s.keyColumns.map(k => col(k).isNull).reduceOption(_ || _).getOrElse(lit(false))
        val rejected = raw.filter(badKey).withColumn("_reject_reasons", array(lit("required:key")))
        val latest = raw.filter(!badKey)
          .withColumn("__rn", row_number().over(Window.partitionBy(s.keyColumns.map(col): _*).orderBy(col(s.watermarkColumn).desc)))
          .filter(col("__rn") === 1).drop("__rn")
          .withColumn("_run_id", lit(args.runId))
          .withColumn("_extracted_at", current_timestamp())
          .cache()
        val rowsLatest  = latest.count()
        val distinctKeys = raw.filter(!badKey).select(s.keyColumns.map(col): _*).distinct().count()
        val rowsRejected = rejected.count()

        if (args.dryRun) return Right(JobSummary(args.runId, Map("rows_read" -> rowsRead, "rows_to_merge" -> rowsLatest)))

        if (rowsRejected > 0) rejected.write.mode(SaveMode.Append).parquet(s.quarantinePath)
        if (rowsLatest > 0) Retry.withBackoff(s"merge-${s.name}", retry)(TableWriters.write(spark, latest, s.target, LocalDate.now()))

        // Verify every key from this batch is present in the target at its new watermark value
        val landed = if (rowsLatest == 0) 0L else {
          val target = spark.read.format("delta").load(s.target.path)
          latest.select((s.keyColumns :+ s.watermarkColumn).map(col): _*)
            .join(target.select((s.keyColumns :+ s.watermarkColumn).map(col): _*), s.keyColumns :+ s.watermarkColumn, "left_semi")
            .count()
        }

        for {
          _ <- Reconciliation.check(s.name, Seq(
                 ReconCheck("one_row_per_key", BigDecimal(distinctKeys), BigDecimal(rowsLatest), BigDecimal(0)),
                 ReconCheck("keys_landed_in_target", BigDecimal(rowsLatest), BigDecimal(landed), BigDecimal(0))))
        } yield {
          writeWatermark(spark, s, hw, args.runId) // only after the target is verified
          JobSummary(args.runId, Map(
            "rows_read" -> rowsRead, "rows_merged" -> rowsLatest, "rows_rejected" -> rowsRejected,
            "superseded_in_window" -> (rowsRead - rowsLatest - rowsRejected)),
            Seq(s"watermark ${last} -> ${hw}"))
        }
    }
  }

  /** Partitioned read when a numeric partition column is configured; bounds come from the same window. */
  private def read(spark: SparkSession, s: SourceSpec, window: String): DataFrame = {
    val base = s"(SELECT * FROM ${s.table} WHERE $window) src"
    s.partitionColumn match {
      case Some(pc) =>
        val b = jdbc(spark, s).option("query", s"SELECT MIN($pc) AS lo, MAX($pc) AS hi FROM ${s.table} WHERE $window").load().head()
        if (b.isNullAt(0)) jdbc(spark, s).option("dbtable", base).load()
        else jdbc(spark, s)
          .option("dbtable", base)
          .option("partitionColumn", pc)
          .option("lowerBound", b.getAs[Number](0).longValue().toString)
          .option("upperBound", (b.getAs[Number](1).longValue() + 1).toString)
          .option("numPartitions", s.numPartitions.toString)
          .load()
      case None => jdbc(spark, s).option("dbtable", base).load()
    }
  }

  private def readWatermark(spark: SparkSession, s: SourceSpec): Timestamp = {
    val p = new Path(s.watermarkPath)
    if (p.getFileSystem(spark.sparkContext.hadoopConfiguration).exists(p))
      spark.read.parquet(s.watermarkPath).orderBy(col("updated_at").desc).select("watermark").head().getTimestamp(0)
    else Timestamp.valueOf(s.initialWatermark)
  }

  private def writeWatermark(spark: SparkSession, s: SourceSpec, hw: Timestamp, runId: String): Unit = {
    import spark.implicits._
    Seq((s.name, hw, runId, new Timestamp(System.currentTimeMillis())))
      .toDF("source", "watermark", "run_id", "updated_at")
      .write.mode(SaveMode.Append).parquet(s.watermarkPath) // append keeps the watermark history for audit
  }

  /** JDBC SQLSTATE classes: 08 = connection (retryable), 28 = authorisation (config), 42 = bad SQL/object. */
  private def classifyJdbc(t: Throwable): PipelineError =
    Iterator.iterate(t)(_.getCause).takeWhile(_ != null).collectFirst { case e: SQLException => e } match {
      case Some(e) if Option(e.getSQLState).exists(_.startsWith("08")) => TransientIOError(s"database connection: ${e.getMessage}", e)
      case Some(e) if Option(e.getSQLState).exists(_.startsWith("28")) => ConfigError(s"database authentication failed: ${e.getMessage}")
      case Some(e) if Option(e.getSQLState).exists(_.startsWith("42")) => ConfigError(s"invalid table/column for source: ${e.getMessage}")
      case _ => PipelineError.classify(t)
    }
}
