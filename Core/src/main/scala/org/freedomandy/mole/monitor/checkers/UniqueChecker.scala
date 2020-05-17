package org.freedomandy.mole.monitor.checkers

import org.freedomandy.mole.transform.Common
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.freedomandy.mole.commons.exceptions.UnsatisfiedPropException
import org.freedomandy.mole.commons.monitor.checkers.Checker

/**
  * @author Andy Huang on 2018/8/21
  */
object UniqueChecker extends Checker {
  override def verifierName: String = "Unique"

  override def verify(dataFrame: DataFrame, config: Config): Either[Throwable, DataFrame] = {
    import scala.collection.JavaConversions._

    val fields = config.getStringList("fields")

    if (Common.isUnique(dataFrame, fields: _*))
      Right.apply(dataFrame)
    else
      Left(new UnsatisfiedPropException(s"Records in fields ${fields.toList.mkString(",")} are duplicated"))
  }
}
