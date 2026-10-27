package com.example.cma.files

import java.io.{File, FileOutputStream}
import java.nio.file.Files
import java.util.zip.{ZipEntry, ZipOutputStream}

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

/** Local SparkSession with Delta Lake enabled (the static SQL extensions need a fresh session). */
trait DeltaTestSession extends BeforeAndAfterAll { self: Suite =>
  @transient lazy val spark: SparkSession = {
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    SparkSession.builder()
      .master("local[2]")
      .appName(getClass.getSimpleName)
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.ansi.enabled", "false")
      .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .config("spark.sql.warehouse.dir", Files.createTempDirectory("wh").toString)
      .getOrCreate()
  }

  override protected def afterAll(): Unit =
    try {
      spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    } finally super.afterAll()
}

/**
 * Writes a minimal but valid .xlsx (Office Open XML) with inline-string cells, so tests can
 * produce Excel fixtures without committing binaries or depending on POI's API directly.
 */
object Xlsx {
  private def esc(s: String) = s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;")

  private def colName(i: Int): String = {
    @annotation.tailrec def go(n: Int, acc: String): String =
      if (n < 0) acc else go(n / 26 - 1, (('A' + n % 26).toChar).toString + acc)
    go(i, "")
  }

  def write(file: File, sheet: String, header: Seq[String], rows: Seq[Seq[String]]): File = {
    file.getParentFile.mkdirs()
    val sheetRows = (header +: rows).zipWithIndex.map { case (cells, r) =>
      val cs = cells.zipWithIndex.collect { case (v, c) if v != null =>
        s"""<c r="${colName(c)}${r + 1}" t="inlineStr"><is><t>${esc(v)}</t></is></c>"""
      }.mkString
      s"""<row r="${r + 1}">$cs</row>"""
    }.mkString

    val parts = Seq(
      "[Content_Types].xml" ->
        """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
          |<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
          |<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
          |<Default Extension="xml" ContentType="application/xml"/>
          |<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
          |<Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
          |</Types>""".stripMargin,
      "_rels/.rels" ->
        """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
          |<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
          |<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
          |</Relationships>""".stripMargin,
      "xl/workbook.xml" ->
        s"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
          |<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
          |<sheets><sheet name="${esc(sheet)}" sheetId="1" r:id="rId1"/></sheets>
          |</workbook>""".stripMargin,
      "xl/_rels/workbook.xml.rels" ->
        """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
          |<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
          |<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
          |</Relationships>""".stripMargin,
      "xl/worksheets/sheet1.xml" ->
        s"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
          |<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>$sheetRows</sheetData></worksheet>""".stripMargin
    )

    val zip = new ZipOutputStream(new FileOutputStream(file))
    try parts.foreach { case (name, xml) =>
      zip.putNextEntry(new ZipEntry(name)); zip.write(xml.getBytes("UTF-8")); zip.closeEntry()
    } finally zip.close()
    file
  }
}
