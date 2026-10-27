package com.example.cma.products

import com.example.cma.common.config.JobArgs
import com.example.cma.common.errors.ConfigError
import com.example.cma.common.job.SparkJob
import com.example.cma.common.util.RetryPolicy
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

/**
 * Shared set-up for the data-product jobs: Delta-enabled session and storage roots.
 *
 * Paths in data-products.conf use {root} (input root) and {out} (output root); the
 * `platform` setting (hadoop | gcp | local) picks hdfs://, gs:// or local folders.
 */
trait ProductJob extends SparkJob {
  val configResource = "data-products"
  override def requiredConfig: Seq[String] = Seq("platform", "roots")

  override protected def buildSession(args: JobArgs, conf: Config): SparkSession = {
    val b = SparkSession.builder()
      .appName(s"$appName-${args.env}")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .config("spark.sql.ansi.enabled", "false")
    if (args.env == "local") b.master("local[*]").config("spark.sql.shuffle.partitions", "4")
    b.getOrCreate()
  }

  protected def retryPolicy(conf: Config): RetryPolicy =
    RetryPolicy(conf.getInt("retry.max-attempts"), conf.getLong("retry.initial-delay-ms"))

  /** Resolve {root}/{out} placeholders for the configured platform. */
  protected def paths(conf: Config): Either[ConfigError, String => String] = {
    val platform = conf.getString("platform")
    if (!conf.hasPath(s"roots.$platform")) Left(ConfigError(s"platform '$platform' has no storage root (roots.$platform)"))
    else {
      val root = conf.getString(s"roots.$platform")
      val out  = if (conf.hasPath(s"outputs.$platform")) conf.getString(s"outputs.$platform") else root
      Right((p: String) => p.replace("{root}", root).replace("{out}", out))
    }
  }

  protected def section(conf: Config, path: String, name: String): Either[ConfigError, Config] =
    if (conf.hasPath(s"$path.$name")) Right(conf.getConfig(s"$path.$name"))
    else Left(ConfigError(s"unknown $path entry '$name'"))
}
