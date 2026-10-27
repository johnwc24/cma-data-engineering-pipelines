package com.example.cma.common.config

import java.time.LocalDate
import java.util.UUID

import com.example.cma.common.errors.ConfigError
import scopt.OParser

import scala.util.Try

/** Command-line arguments shared by every job. */
final case class JobArgs(
    env: String = "",
    businessDate: LocalDate = LocalDate.now(),
    configFile: Option[String] = None,
    overrides: Map[String, String] = Map.empty,
    dryRun: Boolean = false,
    runId: String = UUID.randomUUID().toString
)

object JobArgs {
  val Environments: Set[String] = Set("local", "dev", "uat", "prod")

  def parse(appName: String, argv: Seq[String]): Either[ConfigError, JobArgs] = {
    val builder = OParser.builder[JobArgs]
    val parser = {
      import builder._
      OParser.sequence(
        programName(appName),
        head(appName),
        opt[String]("env")
          .required()
          .validate(e =>
            if (Environments.contains(e)) success
            else failure(s"--env must be one of ${Environments.mkString(", ")}")
          )
          .action((v, c) => c.copy(env = v))
          .text("target environment"),
        opt[String]("business-date")
          .validate(d => if (Try(LocalDate.parse(d)).isSuccess) success else failure(s"invalid date: $d"))
          .action((v, c) => c.copy(businessDate = LocalDate.parse(v)))
          .text("business date (yyyy-MM-dd); defaults to today"),
        opt[String]("config")
          .action((v, c) => c.copy(configFile = Some(v)))
          .text("optional external HOCON file layered over the bundled config"),
        opt[Map[String, String]]("conf")
          .unbounded() // may be repeated; later values win
          .valueName("k1=v1,k2=v2")
          .action((v, c) => c.copy(overrides = c.overrides ++ v))
          .text("config overrides"),
        opt[String]("run-id")
          .action((v, c) => c.copy(runId = v))
          .text("run identifier (Airflow passes its run_id for lineage)"),
        opt[Unit]("dry-run")
          .action((_, c) => c.copy(dryRun = true))
          .text("run all reads, transforms and checks but skip writes")
      )
    }

    OParser.parse(parser, argv, JobArgs()) match {
      case Some(args) => Right(args)
      case None       => Left(ConfigError(s"invalid arguments: ${argv.mkString(" ")}"))
    }
  }
}
