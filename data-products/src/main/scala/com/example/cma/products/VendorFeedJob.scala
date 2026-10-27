package com.example.cma.products

import java.security.MessageDigest
import java.sql.Date
import java.time.LocalDate

import com.example.cma.common.config.JobArgs
import com.example.cma.common.errors._
import com.example.cma.common.job.JobSummary
import com.example.cma.common.quality.{DataQuality, MinRowCount, Unique}
import com.example.cma.files.{FileReaders, SchemaEnforcer, SourceSpec}
import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.config.Config
import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.JavaConverters._
import scala.util.Try

/**
 * Third-party vendor feed ingestion with integrity and licensing controls.
 *
 *   manifest present? ─► checksum + row count match? ─► licence valid for this date and use?
 *     ─► contract typing + quarantine ─► unique / min-rows gate ─► Delta snapshot (one partition per day)
 *     ─► licence tags on the table ─► change summary vs previous snapshot
 *
 * Every snapshot row carries the vendor file, checksum, licence id and permitted uses, and the
 * Delta table carries the same as table properties, so consumers and data governance can see
 * what the data may be used for. An expired licence or a use outside the licence stops the load.
 */
object VendorFeedJob extends ProductJob {
  val appName = "cma-vendor-feed"
  override def requiredConfig: Seq[String] = super.requiredConfig :+ "feed"

  final case class Manifest(file: String, rows: Long, sha256: String)

  def run(spark: SparkSession, args: JobArgs, conf: Config): Either[PipelineError, JobSummary] =
    for {
      resolve <- paths(conf)
      feed    <- section(conf, "vendor-feeds", conf.getString("feed"))
      summary <- Try(load(spark, args, conf, conf.getString("feed"), feed, resolve)).toEither.left.map(PipelineError.classify).flatMap(identity)
    } yield summary

  def sha256(fs: FileSystem, p: Path): String = {
    val md = MessageDigest.getInstance("SHA-256")
    val in = fs.open(p)
    try {
      val buf = new Array[Byte](1 << 16)
      Iterator.continually(in.read(buf)).takeWhile(_ != -1).foreach(n => md.update(buf, 0, n))
    } finally in.close()
    md.digest().map("%02x".format(_)).mkString
  }

  /** Licence check: expiry and whether every intended use is permitted. */
  def checkLicence(lic: Config, businessDate: LocalDate, intendedUses: Seq[String]): Either[ConfigError, Unit] = {
    val expires   = LocalDate.parse(lic.getString("expires"))
    val permitted = lic.getStringList("permitted-use").asScala.toSet
    val notAllowed = intendedUses.filterNot(permitted.contains)
    if (businessDate.isAfter(expires))
      Left(ConfigError(s"licence ${lic.getString("id")} expired on $expires: data may not be loaded (contact vendor management)"))
    else if (notAllowed.nonEmpty)
      Left(ConfigError(s"licence ${lic.getString("id")} does not permit: ${notAllowed.mkString(", ")} (permitted: ${permitted.mkString(", ")})"))
    else Right(())
  }

  private def load(spark: SparkSession, args: JobArgs, conf: Config, name: String, f: Config, resolve: String => String): Either[PipelineError, JobSummary] = {
    val bd      = args.businessDate
    val landing = resolve(f.getString("landing-path")).replace("{business_date}", bd.toString)
    val target  = resolve(f.getString("target-path"))
    val lic     = f.getConfig("licence")
    val uses    = f.getStringList("intended-use").asScala.toList

    // 1. licence first: an unlicensed file must not even be parsed
    checkLicence(lic, bd, uses) match {
      case Left(e) => return Left(e)
      case Right(_) =>
    }

    // 2. arrival + integrity against the vendor's manifest
    val fs = new Path(landing).getFileSystem(spark.sparkContext.hadoopConfiguration)
    val manifestPath = new Path(landing, f.getString("manifest-name"))
    if (!fs.exists(manifestPath)) return Left(SourceNotFoundError(s"$manifestPath (vendor delivery incomplete or late)"))
    val manifest = {
      val in = fs.open(manifestPath)
      try {
        val n = new ObjectMapper().readTree(in)
        Manifest(n.get("file").asText(), n.get("rows").asLong(), n.get("sha256").asText().toLowerCase)
      } finally in.close()
    }
    val dataPath = new Path(landing, manifest.file)
    if (!fs.exists(dataPath)) return Left(SourceNotFoundError(s"$dataPath listed in manifest but not delivered"))
    val actualSha = sha256(fs, dataPath)
    if (actualSha != manifest.sha256)
      return Left(ReconciliationError(name, Seq(s"checksum mismatch: manifest ${manifest.sha256}, file $actualSha (truncated or altered file)")))

    // 3. parse + contract
    val contract = StructType.fromDDL(f.getString("schema"))
    val src = SourceSpec(f.getString("format"), dataPath.toString, Map.empty, None)
    val raw = FileReaders.read(spark, src, contract).cache()
    val rawRows = raw.count()
    if (rawRows != manifest.rows)
      return Left(ReconciliationError(name, Seq(s"row count: manifest ${manifest.rows}, file $rawRows")))

    val required = f.getStringList("required").asScala.toList
    val keys     = f.getStringList("key-columns").asScala.toList
    val split    = SchemaEnforcer.enforce(raw, contract, required)
    val valid = split.valid
      .withColumn("snapshot_date", lit(Date.valueOf(bd)))
      .withColumn("vendor", lit(f.getString("vendor")))
      .withColumn("vendor_file", lit(manifest.file))
      .withColumn("file_sha256", lit(actualSha))
      .withColumn("licence_id", lit(lic.getString("id")))
      .withColumn("permitted_use", typedLit(lic.getStringList("permitted-use").asScala.toList))
      .withColumn("_run_id", lit(args.runId))
      .cache()

    val gate = DataQuality.evaluate(name, valid, Seq(MinRowCount(f.getLong("min-rows")), Unique(keys)))
    gate.enforce() match {
      case Left(e) => return Left(e)
      case Right(_) =>
    }

    // 4. change summary vs the previous snapshot (before overwriting today's partition)
    val previous = previousSnapshot(spark, target, bd)
    val changes = previous.map { prev => diff(valid, prev, keys, contract.fieldNames.toSeq) }.getOrElse(Map("added" -> valid.count(), "removed" -> 0L, "changed" -> 0L))

    if (!args.dryRun) {
      if (split.rejected.head(1).nonEmpty)
        split.rejected.withColumn("snapshot_date", lit(Date.valueOf(bd)))
          .write.mode(SaveMode.Overwrite).option("partitionOverwriteMode", "dynamic").partitionBy("snapshot_date")
          .parquet(resolve(f.getString("quarantine-path")))
      valid.write.format("delta").mode(SaveMode.Overwrite)
        .option("replaceWhere", s"snapshot_date = '$bd'")
        .partitionBy("snapshot_date")
        .save(target)
      tagTable(spark, target, f.getString("vendor"), lic)
    }

    Right(JobSummary(args.runId,
      Map("rows" -> rawRows, "loaded" -> valid.count(), "rejected" -> split.rejected.count()) ++ changes.map { case (k, v) => s"vs_previous_$k" -> v },
      Seq(s"licence ${lic.getString("id")} valid until ${lic.getString("expires")}")))
  }

  private def previousSnapshot(spark: SparkSession, target: String, bd: LocalDate): Option[DataFrame] =
    if (!Try(DeltaTable.isDeltaTable(spark, target)).getOrElse(false)) None
    else {
      val t = spark.read.format("delta").load(target)
      val prevDate = t.filter(col("snapshot_date") < lit(Date.valueOf(bd))).agg(max("snapshot_date")).head()
      if (prevDate.isNullAt(0)) None else Some(t.filter(col("snapshot_date") === lit(prevDate.getDate(0))))
    }

  /** Counts of keys added, removed and changed (any contract column differs) since the previous snapshot. */
  def diff(current: DataFrame, previous: DataFrame, keys: Seq[String], fields: Seq[String]): Map[String, Long] = {
    val valueCols = fields.filterNot(keys.contains)
    val hash = (df: DataFrame) => df.select(keys.map(col) :+ sha2(to_json(struct(valueCols.map(col): _*)), 256).as("__h"): _*)
    val j = hash(current).as("c").join(hash(previous).as("p"), keys, "full_outer")
    Map(
      "added"   -> j.filter(col("p.__h").isNull).count(),
      "removed" -> j.filter(col("c.__h").isNull).count(),
      "changed" -> j.filter(col("c.__h").isNotNull && col("p.__h").isNotNull && col("c.__h") =!= col("p.__h")).count()
    )
  }

  /** Licence metadata as Delta table properties, visible to catalog and governance tooling. */
  private def tagTable(spark: SparkSession, path: String, vendor: String, lic: Config): Unit = {
    val props = Map(
      "cma.vendor"         -> vendor,
      "cma.licence_id"     -> lic.getString("id"),
      "cma.licence_expires" -> lic.getString("expires"),
      "cma.permitted_use"  -> lic.getStringList("permitted-use").asScala.mkString(","),
      "cma.redistribution" -> lic.getBoolean("redistribution").toString
    ).map { case (k, v) => s"'$k' = '${v.replace("'", "")}'" }.mkString(", ")
    spark.sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ($props)")
  }
}
