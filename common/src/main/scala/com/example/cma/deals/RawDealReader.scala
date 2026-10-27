package com.example.cma.deals

import com.example.cma.common.errors.SourceNotFoundError
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Reads the raw deal landing feed from any Hadoop-compatible filesystem
 * (hdfs://, gs:// via the GCS connector on Dataproc, file:// locally).
 */
object RawDealReader {

  def landingPath(landingRoot: String, businessDate: java.time.LocalDate): String =
    s"$landingRoot/business_date=$businessDate"

  def readCsv(spark: SparkSession, path: String): DataFrame = {
    val p  = new Path(path)
    val fs = p.getFileSystem(spark.sparkContext.hadoopConfiguration)
    if (!fs.exists(p)) throw SourceNotFoundError(path)
    val files = fs.listStatus(p).filter(s => s.isFile && s.getLen > 0 && !s.getPath.getName.startsWith("_"))
    if (files.isEmpty) throw SourceNotFoundError(s"$path (folder exists but contains no data files)")

    spark.read
      .option("header", "true")
      .option("mode", "PERMISSIVE")
      .option("columnNameOfCorruptRecord", DealSchemas.CorruptRecordCol)
      .option("escape", "\"")
      .schema(DealSchemas.rawDeal)
      .csv(path)
  }
}
