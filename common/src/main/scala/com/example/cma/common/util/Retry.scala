package com.example.cma.common.util

import com.example.cma.common.errors.PipelineError
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

/**
 * Retry policy with exponential backoff and jitter-free, capped delays.
 *
 * Only failures classified as retryable are retried; data errors (bad schema,
 * DQ breaches, reconciliation breaks) fail fast so they are not masked.
 */
final case class RetryPolicy(
    maxAttempts: Int = 3,
    initialDelayMs: Long = 2000L,
    multiplier: Double = 2.0,
    maxDelayMs: Long = 60000L,
    isRetryable: Throwable => Boolean = Retry.defaultRetryable
) {
  require(maxAttempts >= 1, "maxAttempts must be >= 1")
  require(initialDelayMs >= 0, "initialDelayMs must be >= 0")
  require(multiplier >= 1.0, "multiplier must be >= 1.0")
}

object Retry {
  private val log = LoggerFactory.getLogger(getClass)

  val defaultRetryable: Throwable => Boolean = {
    case e: PipelineError => e.retryable
    case e                => PipelineError.isTransient(e)
  }

  private val realSleep: Long => Unit = (ms: Long) => Thread.sleep(ms)

  /**
   * Run `op`, retrying retryable failures according to `policy`.
   *
   * @param label   operation name used in log lines
   * @param sleeper injectable for tests
   */
  def withBackoff[T](label: String, policy: RetryPolicy = RetryPolicy(), sleeper: Long => Unit = realSleep)(
      op: => T
  ): T = {
    @tailrec
    def attempt(n: Int, delayMs: Long): T =
      Try(op) match {
        case Success(value) =>
          if (n > 1) log.info(s"[$label] succeeded on attempt $n/${policy.maxAttempts}")
          value
        case Failure(e) if n < policy.maxAttempts && policy.isRetryable(e) =>
          log.warn(
            s"[$label] attempt $n/${policy.maxAttempts} failed (${e.getClass.getSimpleName}: ${e.getMessage}); " +
              s"retrying in ${delayMs}ms"
          )
          sleeper(delayMs)
          attempt(n + 1, math.min((delayMs * policy.multiplier).toLong, policy.maxDelayMs))
        case Failure(e) =>
          log.error(s"[$label] failed after $n attempt(s); not retrying", e)
          throw e
      }

    attempt(1, policy.initialDelayMs)
  }
}
