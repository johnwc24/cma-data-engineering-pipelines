package com.example.cma.common

import java.io.IOException

import com.example.cma.common.config.JobArgs
import com.example.cma.common.errors._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class PipelineErrorSpec extends AnyFunSuite with Matchers {

  test("classifies missing paths as non-retryable SourceNotFoundError") {
    val e = PipelineError.classify(new RuntimeException("Path does not exist: hdfs:///landing/deals"))
    e shouldBe a[SourceNotFoundError]
    e.retryable shouldBe false
    e.exitCode shouldBe PipelineError.ExitCodes.SourceMissing
  }

  test("classifies wrapped IO failures as retryable") {
    val e = PipelineError.classify(new RuntimeException("write failed", new IOException("Connection reset by peer")))
    e shouldBe a[TransientIOError]
    e.retryable shouldBe true
  }

  test("classifies BigQuery backend errors as transient") {
    PipelineError.isTransient(new RuntimeException("BigQueryException: backendError")) shouldBe true
  }

  test("leaves typed errors unchanged") {
    val dq = DataQualityError("deal", Seq("unique:deal_id"))
    PipelineError.classify(dq) shouldBe theSameInstanceAs(dq)
  }

  test("unknown failures are unexpected and non-retryable") {
    val e = PipelineError.classify(new IllegalStateException("boom"))
    e shouldBe an[UnexpectedError]
    e.retryable shouldBe false
  }

  test("JobArgs parses a full command line") {
    val args = JobArgs.parse(
      "test",
      Seq("--env", "uat", "--business-date", "2026-10-26", "--conf", "a=1,b=2", "--dry-run", "--run-id", "r1")
    )
    args.isRight shouldBe true
    val a = args.toOption.get
    a.env shouldBe "uat"
    a.businessDate.toString shouldBe "2026-10-26"
    a.overrides shouldBe Map("a" -> "1", "b" -> "2")
    a.dryRun shouldBe true
    a.runId shouldBe "r1"
  }

  test("JobArgs rejects unknown environments and bad dates") {
    JobArgs.parse("test", Seq("--env", "staging")).isLeft shouldBe true
    JobArgs.parse("test", Seq("--env", "dev", "--business-date", "26/10/2026")).isLeft shouldBe true
  }
}
