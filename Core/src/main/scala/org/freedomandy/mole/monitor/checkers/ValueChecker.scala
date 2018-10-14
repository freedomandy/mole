package org.freedomandy.mole.monitor.checkers

import com.typesafe.config.Config
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.freedomandy.mole.commons.exceptions.UnsatisfiedPropException
import org.freedomandy.mole.commons.monitor.checkers.Checker

/**
  * @author Andy Huang on 2018/8/21
  */
object ValueChecker extends Checker {
  override def verifierName: String = "DisallowValues"

  override def verify(dataFrame: DataFrame, config: Config): Either[Throwable, DataFrame] = {
    def checking(invalidValues: Dataset[Row]): Either[Throwable, DataFrame] = {
      if (invalidValues.count() == 0) {
        Right(dataFrame)
      } else {
        Left(new UnsatisfiedPropException(s"values ${invalidValues.head(5).toList}... are not allow in field ${config.getString("column")}"))
      }
    }

    import scala.collection.JavaConversions._

    val disallowValues = config.getAnyRefList("values")
    val column = config.getString("column")
    val df = dataFrame.select(column)

    checking(df.filter(col(column).isin(disallowValues.toList:_*)))
  }
}
