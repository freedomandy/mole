package org.freedomandy.mole.monitor.checkers

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.freedomandy.mole.commons.exceptions.UnsatisfiedPropException
import org.freedomandy.mole.commons.monitor.checkers.Checker
import org.freedomandy.mole.commons.utils.ApacheHttpClient

/**
  * @author Andy Huang on 2018/8/21
  */
object HttpStatusChecker extends Checker {
  override def verifierName: String = "HttpStatus"

  override def verify(dataFrame: DataFrame, config: Config): Either[Throwable, DataFrame] = {
    val httpClient = new ApacheHttpClient

    try {
      import scala.collection.JavaConversions._

      val headers: Map[String, String] =
        if (config.hasPath("headers"))
          config.getConfigList("headers").map(config => config.getString("key") -> config.getString("value")).toMap
        else Map()

      httpClient.get(config.getString("url"), headers)

      Right.apply(dataFrame)
    } catch {
      case e: Exception =>
        println(s"Failed to check status $config: ${e.toString}")
        Left.apply(new UnsatisfiedPropException(s"Failed to check status $config"))
    }
  }

}
