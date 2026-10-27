package com.example.cma.files

import com.example.cma.common.errors.{SchemaValidationError, SourceNotFoundError}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Reads any supported format and returns every column as it arrived (strings for
 * text formats), plus `_corrupt_record` for unparseable CSV/JSON lines and
 * `_source_file` for lineage. Typing happens afterwards in SchemaEnforcer, so a bad
 * value is quarantined with a reason instead of silently becoming NULL.
 */
object FileReaders {
  val CorruptCol = "_corrupt_record"
  val SourceFileCol = "_source_file"

  def read(spark: SparkSession, src: SourceSpec, contract: StructType): DataFrame = {
    requireExists(spark, src.path)
    val raw = src.format match {
      case "csv"     => text(spark, src, contract).csv(src.path)
      case "json"    => text(spark, src, contract).json(src.path)
      case "avro"    => spark.read.format("avro").options(src.options).load(src.path)
      case "parquet" => spark.read.options(src.options).parquet(src.path)
      case "excel"   => excel(spark, src)
      case other     => throw SchemaValidationError(s"unsupported source format $other")
    }
    normaliseColumnNames(raw).withColumn(SourceFileCol, input_file_name())
  }

  /** CSV/JSON: read as strings in PERMISSIVE mode with a corrupt-record column. */
  private def text(spark: SparkSession, src: SourceSpec, contract: StructType) = {
    val asStrings = StructType(contract.fields.map(f => StructField(f.name, StringType)) :+ StructField(CorruptCol, StringType))
    spark.read
      .option("header", "true")
      .option("mode", "PERMISSIVE")
      .option("columnNameOfCorruptRecord", CorruptCol)
      .options(src.options)
      .schema(asStrings)
  }

  /** Excel via spark-excel: every cell read as text; the contract types it later. */
  private def excel(spark: SparkSession, src: SourceSpec): DataFrame =
    spark.read
      .format("com.crealytics.spark.excel")
      .option("header", "true")
      .option("inferSchema", "false")
      .option("treatEmptyValuesAsNulls", "true")
      .option("dataAddress", s"'${src.sheet.getOrElse("Sheet1")}'!A1")
      .options(src.options)
      .load(src.path)

  /** Excel/Avro headers often arrive as "Deal ID" or "DealId"; normalise to snake_case. */
  def normaliseColumnNames(df: DataFrame): DataFrame = {
    def snake(s: String) = s.trim
      .replaceAll("([a-z0-9])([A-Z])", "$1_$2")
      .replaceAll("[^A-Za-z0-9_]+", "_")
      .replaceAll("_+", "_")
      .stripPrefix("_").stripSuffix("_")
      .toLowerCase
    df.select(df.columns.map(c => col(s"`$c`").as(if (c == CorruptCol) c else snake(c))): _*)
  }

  private def requireExists(spark: SparkSession, path: String): Unit = {
    val p  = new Path(path)
    val fs = p.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val matches = Option(fs.globStatus(p)).getOrElse(Array.empty)
    if (matches.isEmpty) throw SourceNotFoundError(path)
  }
}

/**
 * Applies the dataset contract: required columns must exist (fatal), values must cast to
 * the contract type and required fields must be populated (row-level, quarantined).
 */
object SchemaEnforcer {
  final case class Result(valid: DataFrame, rejected: DataFrame)

  val ReasonsCol = "_reject_reasons"

  def enforce(df: DataFrame, contract: StructType, required: Seq[String]): Result = {
    val missing = contract.fieldNames.filterNot(df.columns.contains)
    if (missing.nonEmpty)
      throw SchemaValidationError(s"source is missing contract columns: ${missing.mkString(", ")} (found: ${df.columns.mkString(", ")})")

    val corrupt: Column =
      if (df.columns.contains(FileReaders.CorruptCol)) col(FileReaders.CorruptCol).isNotNull else lit(false)

    val checks: Seq[Column] =
      when(corrupt, lit("malformed_record")) +:
        contract.fields.toSeq.flatMap { f =>
          val raw   = col(f.name)
          val typed = castTo(raw, f.dataType)
          val castFail = when(!corrupt && raw.isNotNull && trim(raw.cast(StringType)) =!= "" && typed.isNull, lit(s"cast:${f.name}:${f.dataType.simpleString}"))
          val nullFail =
            if (required.contains(f.name)) Some(when(!corrupt && (raw.isNull || trim(raw.cast(StringType)) === ""), lit(s"required:${f.name}")))
            else None
          castFail +: nullFail.toSeq
        }

    val tagged = df.withColumn(ReasonsCol, filter(array(checks: _*), (c: Column) => c.isNotNull))
    val typedCols = contract.fields.map(f => castTo(col(f.name), f.dataType).as(f.name)) :+ col(FileReaders.SourceFileCol)

    Result(
      valid = tagged.filter(size(col(ReasonsCol)) === 0).select(typedCols: _*),
      rejected = tagged.filter(size(col(ReasonsCol)) > 0)
        .select(df.columns.map(c => col(c).cast(StringType).as(c)) :+ col(ReasonsCol): _*)
    )
  }

  /** Cast with the coercions real files need: trimmed text, ISO / dd/MM/yyyy dates, Excel booleans. */
  def castTo(c: Column, t: DataType): Column = {
    val s = trim(c.cast(StringType))
    t match {
      case StringType => when(s === "", lit(null).cast(StringType)).otherwise(s)
      case DateType =>
        coalesce(to_date(s, "yyyy-MM-dd"), to_date(s, "dd/MM/yyyy"), to_date(s, "yyyy/MM/dd"), c.cast(DateType))
      case TimestampType =>
        coalesce(to_timestamp(s, "yyyy-MM-dd HH:mm:ss"), to_timestamp(s, "yyyy-MM-dd'T'HH:mm:ss'Z'"), c.cast(TimestampType))
      case BooleanType =>
        when(lower(s).isin("true", "y", "yes", "1"), lit(true)).when(lower(s).isin("false", "n", "no", "0"), lit(false))
      case _: NumericType => regexp_replace(s, ",", "").cast(t) // "1,250,000.00" from Excel
      case other => c.cast(other)
    }
  }
}
