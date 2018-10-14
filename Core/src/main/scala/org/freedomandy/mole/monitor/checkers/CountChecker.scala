package org.freedomandy.mole.monitor.checkers

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.freedomandy.mole.commons.exceptions.UnsatisfiedPropException
import org.freedomandy.mole.commons.monitor.checkers.Checker

/**
  * @author Andy Huang on 2018/8/21
  */
object CountChecker extends Checker {
  override def verifierName: String = "Count"

  override def verify(dataFrame: DataFrame, config: Config): Either[Throwable, DataFrame] = {
    val number = config.getLong("number")

    val dfCount = dataFrame.count()

    if (dfCount == number)
      Right(dataFrame)
    else
      Left(new UnsatisfiedPropException(s"The count: $dfCount of the dataFrame is not equal $number"))
  }
}
