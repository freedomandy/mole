package org.freedomandy.mole.monitor

import org.freedomandy.mole.monitor.checkers.{CountChecker, HttpStatusChecker, UniqueChecker, ValueChecker}
import org.freedomandy.mole.monitor.handlers.{HttpInformer, KafkaInformer}
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.freedomandy.mole.commons.exceptions.BaseException
import org.freedomandy.mole.commons.monitor.checkers.Checker
import org.freedomandy.mole.commons.monitor.handlers.ErrorHandler
import org.freedomandy.mole.commons.utils.PluginLoader

/**
  * @author Andy Huang on 2018/8/7
  */
case class SuperviseData(checkConfig: Config, handlerConfig: Config)

case class Supervisor(config: Config) {
  private val errorHandling: Function[String, ErrorHandler] = {
    def getPF(errorHandler: ErrorHandler): PartialFunction[String, ErrorHandler] = {
      case s: String if s == errorHandler.handlerName => errorHandler
    }

    val baseHandlers = List(HttpInformer, KafkaInformer)
    val endOfPF: PartialFunction[String, ErrorHandler] = {
      case s: String => throw BaseException(s"Unsupported error handler $s")
    }

    (baseHandlers ::: PluginLoader.loadErrorHandlerPlugins(config)).map(getPF).foldRight(endOfPF)(_ orElse _)
  }

  private val conditionChecking: Function[String, Checker] = {
    def getPF(verifier: Checker): PartialFunction[String, Checker] = {
      case s: String if s == verifier.verifierName => verifier
    }

    val baseCheckers = List(CountChecker, HttpStatusChecker, UniqueChecker, ValueChecker)
    val endOfPF: PartialFunction[String, Checker] = {
      case s: String => throw BaseException(s"Unsupported checker $s")
    }

    (baseCheckers ::: PluginLoader.loadCheckerPlugins(config)).map(getPF).foldRight(endOfPF)(_ orElse _)
  }

  def handleError(errMsg: String, dataFrame: DataFrame, config: Config): Option[DataFrame] = {
    val handler: ErrorHandler = errorHandling(config.getString("type"))

    if (handler.isInformer)
      handler.notify(errMsg, config)

    if (handler.isRecover) {
      val result = handler.recover(dataFrame, config)
      if (result.isLeft) None else Some(result.right.get)
    } else
      None
  }

  /**
    * Return the input dataFrame, if all of check statements are satisfied. Otherwise, trigger the corresponding error
    * handling operation and return nothing.
    *
    * @param dataFrame: The data frame which will verified by the check statements.
    * @param config: The supervise data configuration which includes the conditions to verify and the corresponding
    *              error handling strategies.
    * @return an option type of data frame
    * */
  def supervise(dataFrame: DataFrame, config: Config): Option[DataFrame] = {
    def checkStat(dataFrame: DataFrame, config: Config): Either[Throwable, DataFrame] = {
      val checker = conditionChecking(config.getString("type"))

      checker.verify(dataFrame, config)
    }

    def check(dataFrame: DataFrame, statList: List[SuperviseData]): Either[(String, Config), DataFrame] =
      if (statList.isEmpty)
        Right(dataFrame)
      else {
        val verified = checkStat(dataFrame, statList.head.checkConfig)

        if (verified.isRight)
          check(verified.right.get, statList.tail)
        else
          Left((verified.left.get.toString, statList.head.handlerConfig))
      }

    def getSuperviseDataList(config: Config): List[SuperviseData] = {
      import scala.collection.JavaConversions._
      config
        .getConfigList("checkStatements")
        .map(config => SuperviseData(config, config.getConfig("errorHandler")))
        .toList
    }

    val superviseDataList = getSuperviseDataList(config)
    val checkResult       = check(dataFrame, superviseDataList)

    if (checkResult.isRight)
      Some(checkResult.right.get)
    else {
      handleError(checkResult.left.get._1, dataFrame, checkResult.left.get._2)

      None
    }
  }
}
