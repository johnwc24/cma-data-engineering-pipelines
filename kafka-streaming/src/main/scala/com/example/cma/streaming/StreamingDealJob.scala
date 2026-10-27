package com.example.cma.streaming

import java.time.{LocalDate, ZoneOffset}

import com.example.cma.common.config.JobArgs
import com.example.cma.common.errors.{PipelineError, TransientIOError}
import com.example.cma.common.job.{JobSummary, SparkJob}
import com.example.cma.common.util.{Retry, RetryPolicy}
import com.example.cma.deals.DealPipeline
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryListener, Trigger}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

/**
 * Kafka ─► Spark Structured Streaming ─► platform sink, with a dead-letter topic.
 *
 * Reliability model:
 *  - offsets live in the checkpoint; a crashed or killed query resumes where it stopped
 *  - each micro-batch is processed with foreachBatch and recorded in a batch ledger,
 *    so a batch replayed after a failure is skipped instead of loaded twice
 *  - invalid / unparseable events go to the DLQ topic (+ quarantine table) with reasons
 *    in headers; they never stop the stream
 *  - transient sink failures are retried in-batch; anything else stops the query so it
 *    restarts from the checkpoint (Airflow/YARN/Dataproc restart policy) without data loss
 */
abstract class StreamingDealJob extends SparkJob {

  override def requiredConfig: Seq[String] =
    Seq("kafka.bootstrap-servers", "kafka.topic", "kafka.dlq-topic", "stream.checkpoint", "stream.trigger-seconds")

  /** Static reference data, re-read every batch so intraday updates are picked up. */
  protected def readParties(spark: SparkSession, conf: Config): DataFrame
  protected def readFxRates(spark: SparkSession, conf: Config): DataFrame

  /** Has this micro-batch already been committed to the sink? */
  protected def alreadyCommitted(spark: SparkSession, conf: Config, batchId: Long): Boolean
  protected def writeRawEvents(spark: SparkSession, conf: Config, events: DataFrame): Unit
  protected def writeCurated(spark: SparkSession, conf: Config, curated: DataFrame, businessDate: LocalDate): Unit
  protected def writeQuarantine(spark: SparkSession, conf: Config, rejected: DataFrame, businessDate: LocalDate): Unit
  protected def recordBatch(spark: SparkSession, conf: Config, batchId: Long, metrics: Map[String, Long]): Unit

  /** Kafka client options; security settings come from config so the same code runs on-prem and on GCP. */
  protected def kafkaOptions(conf: Config): Map[String, String] = {
    val extra =
      if (conf.hasPath("kafka.options")) // root().unwrapped() keeps dotted keys such as "security.protocol" intact (entrySet would quote them)
        conf.getConfig("kafka.options").root().unwrapped().asScala.map { case (k, v) => s"kafka.$k" -> String.valueOf(v) }.toMap
      else Map.empty[String, String]
    Map("kafka.bootstrap.servers" -> conf.getString("kafka.bootstrap-servers")) ++ extra
  }

  def run(spark: SparkSession, args: JobArgs, conf: Config): Either[PipelineError, JobSummary] = {
    val retry = RetryPolicy(conf.getInt("retry.max-attempts"), conf.getLong("retry.initial-delay-ms"))
    spark.streams.addListener(new ProgressLogger(appName))

    val source = spark.readStream
      .format("kafka")
      .options(kafkaOptions(conf))
      .option("subscribe", conf.getString("kafka.topic"))
      .option("startingOffsets", conf.getString("kafka.starting-offsets"))
      .option("maxOffsetsPerTrigger", conf.getLong("kafka.max-offsets-per-trigger"))
      // Fail loudly if retention deleted unread data, rather than silently skipping it
      .option("failOnDataLoss", "true")
      .load()

    // Typed function value: Scala 2.12 cannot pick between the Scala and Java foreachBatch overloads for a lambda
    val handleBatch: (DataFrame, Long) => Unit = (batch, batchId) => processBatch(spark, conf, retry, batch, batchId)

    val query: StreamingQuery = DealEvents
      .parse(source)
      .writeStream
      .queryName(appName)
      .option("checkpointLocation", conf.getString("stream.checkpoint"))
      .trigger(
        if (args.dryRun) Trigger.AvailableNow()
        else Trigger.ProcessingTime(s"${conf.getInt("stream.trigger-seconds")} seconds")
      )
      .foreachBatch(handleBatch)
      .start()

    try {
      query.awaitTermination()
      Right(JobSummary(args.runId, Map("last_batch_id" -> Option(query.lastProgress).map(_.batchId).getOrElse(-1L))))
    } catch {
      case NonFatal(e) =>
        log.error(s"Streaming query $appName stopped: ${e.getMessage}", e)
        Left(PipelineError.classify(Option(e.getCause).getOrElse(e)))
    }
  }

  private[streaming] def processBatch(
      spark: SparkSession,
      conf: Config,
      retry: RetryPolicy,
      batch: DataFrame,
      batchId: Long
  ): Unit = {
    if (alreadyCommitted(spark, conf, batchId)) {
      log.warn(s"batch $batchId already committed; skipping replay")
    } else {
      val events = DealEvents.dedupeEvents(batch).cache()
      try {
        val rows = events.count()
        if (rows == 0) recordBatch(spark, conf, batchId, Map("events" -> 0L))
        else {
          val bd = LocalDate.now(ZoneOffset.UTC)
          sinkWithRetry("raw-events", retry)(writeRawEvents(spark, conf, events))

          val result = DealPipeline.process(
            events, readParties(spark, conf), readFxRates(spark, conf), bd, s"$appName-batch-$batchId", "KAFKA"
          )

          if (result.rejectedRows > 0) {
            sinkWithRetry("dlq", retry)(
              DealEvents.toDeadLetters(result.rejected, conf.getString("kafka.topic"))
                .write.format("kafka").options(kafkaOptions(conf)).option("topic", conf.getString("kafka.dlq-topic")).save()
            )
            sinkWithRetry("quarantine", retry)(writeQuarantine(spark, conf, result.rejected, bd))
          }
          sinkWithRetry("curated", retry)(writeCurated(spark, conf, result.curated, bd))

          recordBatch(spark, conf, batchId, Map(
            "events"     -> rows,
            "rejected"   -> result.rejectedRows,
            "superseded" -> result.supersededRows
          ))
          log.info(s"batch $batchId: $rows events, ${result.rejectedRows} to DLQ")
        }
      } finally events.unpersist()
    }
  }

  private def sinkWithRetry(label: String, retry: RetryPolicy)(op: => Unit): Unit =
    try Retry.withBackoff(label, retry)(op)
    catch {
      case e: PipelineError => throw e
      case NonFatal(e) if PipelineError.isTransient(e) => throw TransientIOError(s"$label sink exhausted retries", e)
      case NonFatal(e) => throw PipelineError.classify(e)
    }
}

/** Logs throughput and lag every micro-batch; feeds the log-based alerting described in the runbook. */
final class ProgressLogger(name: String) extends StreamingQueryListener {
  private val log = org.slf4j.LoggerFactory.getLogger(getClass)
  override def onQueryStarted(e: StreamingQueryListener.QueryStartedEvent): Unit =
    log.info(s"[$name] started id=${e.id} runId=${e.runId}")
  override def onQueryProgress(e: StreamingQueryListener.QueryProgressEvent): Unit = {
    val p = e.progress
    log.info(s"[$name] batch=${p.batchId} rows=${p.numInputRows} inRate=${p.inputRowsPerSecond} procRate=${p.processedRowsPerSecond}")
  }
  override def onQueryTerminated(e: StreamingQueryListener.QueryTerminatedEvent): Unit =
    e.exception match {
      case Some(err) => log.error(s"[$name] terminated with error: $err")
      case None      => log.info(s"[$name] terminated cleanly")
    }
}
