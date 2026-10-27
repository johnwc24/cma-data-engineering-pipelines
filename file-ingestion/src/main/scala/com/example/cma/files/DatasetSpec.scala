package com.example.cma.files

import com.example.cma.common.errors.ConfigError
import com.typesafe.config.Config
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._
import scala.util.Try

/** Where and how to read one dataset. */
final case class SourceSpec(
    format: String,                 // csv | json | avro | excel | parquet
    path: String,                   // hdfs://, gs:// or local; {business_date} is substituted
    options: Map[String, String],
    sheet: Option[String]           // excel only
)

/** Where and how to write it. */
final case class TargetSpec(
    format: String,                 // parquet | delta
    path: String,
    mode: String,                   // overwrite-partition | append | merge
    partitionBy: Seq[String],
    mergeKeys: Seq[String]          // delta merge (upsert) keys
)

final case class DatasetSpec(
    name: String,
    source: SourceSpec,
    target: TargetSpec,
    quarantinePath: String,
    schema: StructType,             // contract the output must meet
    required: Seq[String],          // must be non-null
    uniqueKeys: Seq[String]
)

object DatasetSpec {
  val SourceFormats: Set[String] = Set("csv", "json", "avro", "excel", "parquet")
  val TargetFormats: Set[String] = Set("parquet", "delta")
  val WriteModes: Set[String]    = Set("overwrite-partition", "append", "merge")

  /**
   * Build and validate a spec from `datasets.<name>`; every problem is reported at start-up.
   * Paths may use {root} (input storage root) and {out} (output root) for the chosen platform.
   */
  def fromConfig(conf: Config, name: String, root: String, out: String): Either[ConfigError, DatasetSpec] = {
    val key = s"datasets.$name"
    if (!conf.hasPath(key)) return Left(ConfigError(s"unknown dataset '$name' (configured: ${datasetNames(conf).mkString(", ")})"))
    val d = conf.getConfig(key)

    def str(p: String)                  = d.getString(p).replace("{root}", root).replace("{out}", out)
    def strs(p: String): Seq[String]    = if (d.hasPath(p)) d.getStringList(p).asScala.toList else Nil
    def opts(p: String): Map[String, String] =
      if (d.hasPath(p)) d.getConfig(p).root().unwrapped().asScala.map { case (k, v) => k -> String.valueOf(v) }.toMap
      else Map.empty

    Try {
      DatasetSpec(
        name = name,
        source = SourceSpec(d.getString("source.format"), str("source.path"), opts("source.options"),
          if (d.hasPath("source.sheet")) Some(d.getString("source.sheet")) else None),
        target = TargetSpec(d.getString("target.format"), str("target.path"),
          if (d.hasPath("target.mode")) d.getString("target.mode") else "overwrite-partition",
          strs("target.partition-by"), strs("target.merge-keys")),
        quarantinePath = str("quarantine-path"),
        schema = StructType.fromDDL(d.getString("schema")),
        required = strs("required"),
        uniqueKeys = strs("unique-keys")
      )
    }.toEither.left.map(e => ConfigError(s"dataset '$name': ${e.getMessage}")).flatMap(validate)
  }

  def datasetNames(conf: Config): Seq[String] =
    if (conf.hasPath("datasets")) conf.getConfig("datasets").root().keySet().asScala.toSeq.sorted else Nil

  private def validate(s: DatasetSpec): Either[ConfigError, DatasetSpec] = {
    val cols = s.schema.fieldNames.toSet
    val problems = Seq(
      (!SourceFormats.contains(s.source.format)) -> s"source.format '${s.source.format}' not in ${SourceFormats.mkString("|")}",
      (!TargetFormats.contains(s.target.format)) -> s"target.format '${s.target.format}' not in ${TargetFormats.mkString("|")}",
      (!WriteModes.contains(s.target.mode))      -> s"target.mode '${s.target.mode}' not in ${WriteModes.mkString("|")}",
      (s.target.mode == "merge" && s.target.format != "delta") -> "target.mode merge requires target.format delta",
      (s.target.mode == "merge" && s.target.mergeKeys.isEmpty) -> "target.mode merge requires target.merge-keys",
      (s.target.mode == "overwrite-partition" && s.target.partitionBy.isEmpty) -> "overwrite-partition requires target.partition-by"
    ).collect { case (true, msg) => msg } ++
      (s.required ++ s.uniqueKeys ++ s.target.mergeKeys).filterNot(c => cols.contains(c) || c == "business_date")
        .map(c => s"column '$c' is not in the schema")

    if (problems.isEmpty) Right(s) else Left(ConfigError(s"dataset '${s.name}': ${problems.mkString("; ")}"))
  }
}
