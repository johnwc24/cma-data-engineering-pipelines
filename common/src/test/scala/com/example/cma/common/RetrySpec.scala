package com.example.cma.common

import java.io.IOException

import com.example.cma.common.errors.{DataQualityError, TransientIOError}
import com.example.cma.common.util.{Retry, RetryPolicy}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable.ArrayBuffer

class RetrySpec extends AnyFunSuite with Matchers {

  private def recordingSleeper(buf: ArrayBuffer[Long]): Long => Unit = (ms: Long) => buf += ms

  test("retries transient failures with exponential backoff, then succeeds") {
    val sleeps   = ArrayBuffer.empty[Long]
    var attempts = 0
    val result = Retry.withBackoff("flaky", RetryPolicy(maxAttempts = 4, initialDelayMs = 100), recordingSleeper(sleeps)) {
      attempts += 1
      if (attempts < 3) throw new IOException("Connection reset")
      "ok"
    }
    result shouldBe "ok"
    attempts shouldBe 3
    sleeps.toSeq shouldBe Seq(100L, 200L)
  }

  test("caps backoff at maxDelayMs") {
    val sleeps = ArrayBuffer.empty[Long]
    an[IOException] should be thrownBy {
      Retry.withBackoff("capped", RetryPolicy(maxAttempts = 4, initialDelayMs = 1000, maxDelayMs = 1500), recordingSleeper(sleeps)) {
        throw new IOException("Read timed out")
      }
    }
    sleeps.toSeq shouldBe Seq(1000L, 1500L, 1500L)
  }

  test("does not retry data errors") {
    var attempts = 0
    a[DataQualityError] should be thrownBy {
      Retry.withBackoff("dq", RetryPolicy(maxAttempts = 5, initialDelayMs = 1), _ => ()) {
        attempts += 1
        throw DataQualityError("deal", Seq("not_null:deal_id"))
      }
    }
    attempts shouldBe 1
  }

  test("retries typed retryable errors and rethrows after maxAttempts") {
    var attempts = 0
    a[TransientIOError] should be thrownBy {
      Retry.withBackoff("typed", RetryPolicy(maxAttempts = 3, initialDelayMs = 1), _ => ()) {
        attempts += 1
        throw TransientIOError("gcs 503", new RuntimeException("503"))
      }
    }
    attempts shouldBe 3
  }
}
