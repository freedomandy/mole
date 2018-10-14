package org.freedomandy.mole.commons.monitor.checkers

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame

/**
  * @author Andy Huang on 2018/8/21
  */
trait Checker {
  def verifierName: String
  def verify(dataFrame: DataFrame, config: Config): Either[Throwable, DataFrame]
}
