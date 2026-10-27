package com.example.cma.common.config

import java.io.File

import com.example.cma.common.errors.ConfigError
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.JavaConverters._
import scala.util.Try

/**
 * Layered configuration:
 *   --conf overrides  >  --config file  >  bundled `<resource>.conf` [env]  >  bundled `defaults`
 */
object PipelineConfig {

  def load(args: JobArgs, resource: String): Either[ConfigError, Config] =
    Try {
      val bundled = ConfigFactory.parseResources(s"$resource.conf")
      if (bundled.isEmpty) throw new IllegalStateException(s"bundled config $resource.conf not found on classpath")

      val external = args.configFile match {
        case Some(path) =>
          val f = new File(path)
          if (!f.exists()) throw new IllegalArgumentException(s"config file not found: $path")
          ConfigFactory.parseFile(f)
        case None => ConfigFactory.empty()
      }

      val root     = external.withFallback(bundled).resolve()
      val defaults = if (root.hasPath("defaults")) root.getConfig("defaults") else ConfigFactory.empty()
      if (!root.hasPath(args.env)) throw new IllegalArgumentException(s"no config block for env '${args.env}'")
      val envConf  = root.getConfig(args.env).withFallback(defaults)

      ConfigFactory.parseMap(args.overrides.asJava).withFallback(envConf).resolve()
    }.toEither.left.map(e => ConfigError(e.getMessage))

  /** Fail fast at start-up rather than half-way through a run. */
  def requirePaths(conf: Config, paths: String*): Either[ConfigError, Config] = {
    val missing = paths.filterNot(conf.hasPath)
    if (missing.isEmpty) Right(conf) else Left(ConfigError(s"missing required keys: ${missing.mkString(", ")}"))
  }
}
