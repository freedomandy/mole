package org.freedomandy.mole.monitor

import org.freedomandy.mole.commons.exceptions.BaseException
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.freedomandy.mole.Module

/**
  * @author Andy Huang on 2018/7/24
  */
case class Monitor(module: Module) {
  val supervisor: Supervisor = module.supervisor

  // TODO: Retry mechanism
  // TODO: Exception handling e.g. handle the reload or backup db mechanism in case the sink db is unavailable

  def executeWithMonitor(
      df: DataFrame,
      monitorConfig: Config,
      exceptionConfigs: List[Config],
      action: DataFrame => DataFrame
  ): Option[DataFrame] = {
    def exceptionHandling(exception: Exception, configList: List[Config], dataFrame: DataFrame): Option[DataFrame] = {
      val matchingExceptions = configList.filter { c =>
        exception.toString.split(":")(0) == c.getString("condition")
      }

      if (matchingExceptions.isEmpty) {
        println(s"Failed to execute transformation: ${exception.toString}")
        None
      } else
        supervisor.handleError(exception.toString, dataFrame, matchingExceptions.head.getConfig("errorHandler"))
    }

    try supervisor.supervise(df, monitorConfig).map[DataFrame](action)
    catch { // TODO: Handle exceptions based on the configured strategy
      case e: BaseException =>
        exceptionHandling(e, exceptionConfigs, df)
      case t: Throwable =>
        exceptionHandling(BaseException(s"Failed to finish mole job, due to: ${t.toString}"), exceptionConfigs, df)
    }
  }

  def handle(monitorConfig: Config): Option[DataFrame] = {
    import scala.collection.JavaConversions._
    val exceptionConfigs =
      if (monitorConfig.hasPath("ExceptionHandling"))
        monitorConfig.getConfigList("ExceptionHandling").toList
      else Nil

    for {
      sourceDf <- module.extractor.run()
      transformedDF <- executeWithMonitor(
        sourceDf,
        monitorConfig.getConfig("Transform.preStart"),
        exceptionConfigs,
        module.transformer.run
      )
      result <- executeWithMonitor(
        transformedDF,
        monitorConfig.getConfig("Load.preStart"),
        exceptionConfigs,
        df => { module.loader.run(df); df }
      )
    } yield result
  }
}
