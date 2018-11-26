package org.freedomandy.mole.commons.monitor.handlers

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.freedomandy.mole.commons.exceptions.BaseException

/**
  * @author Andy Huang on 2018/8/21
  */
trait ErrorHandler {
  def handlerName: String
  def isRecover: Boolean
  def isInformer: Boolean
  def recover(dataFrame: DataFrame, config: Config): Either[BaseException, DataFrame]
  def notify(message: String, config: Config): Unit
}
