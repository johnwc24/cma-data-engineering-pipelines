package com.example.cma.hadoop

import java.sql.{Date, Timestamp}

import com.example.cma.common.config.JobArgs
import org.apache.spark.sql.{DataFrame, SparkSession}

final case class RunAuditRecord(
    run_id: String,
    job_name: String,
    env: String,
    status: String,
    input_rows: Long,
    loaded_rows: Long,
    rejected_rows: Long,
    superseded_rows: Long,
    started_at: Timestamp,
    finished_at: Timestamp,
    business_date: Date
)

/** One row per run in `cma_audit.pipeline_run` — the basis for ops dashboards and SLA reporting. */
object RunAudit {
  def toDF(
      spark: SparkSession,
      job: String,
      args: JobArgs,
      status: String,
      metrics: Map[String, Long],
      started: Timestamp
  ): DataFrame = {
    import spark.implicits._
    Seq(
      RunAuditRecord(
        run_id = args.runId,
        job_name = job,
        env = args.env,
        status = status,
        input_rows = metrics.getOrElse("input_rows", 0L),
        loaded_rows = metrics.getOrElse("loaded_rows", 0L),
        rejected_rows = metrics.getOrElse("rejected_rows", 0L),
        superseded_rows = metrics.getOrElse("superseded_rows", 0L),
        started_at = started,
        finished_at = new Timestamp(System.currentTimeMillis()),
        business_date = Date.valueOf(args.businessDate)
      )
    ).toDF()
  }
}
