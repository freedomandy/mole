package org.freedomandy.mole.monitor.handlers

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.freedomandy.mole.commons.exceptions.{BaseException, UnsupportedException}
import org.freedomandy.mole.commons.monitor.handlers.ErrorHandler
import org.freedomandy.mole.commons.utils.ApacheHttpClient

/**
  * @author Andy Huang on 2018/8/20
  */
object HttpInformer extends ErrorHandler {
  override def handlerName: String = "HttpPublish"

  override def isInformer: Boolean = true

  override def isRecover: Boolean = false

  override def notify(message: String, config: Config): Unit = {
    import scala.collection.JavaConversions._
    val headers: Map[String, String] = if (config.hasPath("headers")) config.getConfigList("headers").map(
      config => config.getString("key") -> config.getString("value")).toMap else Map()
    val payload =
      if (config.hasPath("payLoad")) {
        if (config.getString("payLoad").contains("@msg"))

          try {
            Some(config.getString("payLoad").replaceAll("@msg", message))
          } catch { // In case the exception throw from executors
            case e: IndexOutOfBoundsException =>
              Some(config.getString("payLoad").replaceAll("@msg", "exception throw from executors"))
          }
        else
          Some(config.getString("payLoad"))
      } else {
        None
      }

    val httpClient: ApacheHttpClient = new ApacheHttpClient

    httpClient.request(config.getString("httpMethod"), config.getString("url"), headers, payload)
  }

  override def recover(dataFrame: DataFrame, config: Config): Either[BaseException, DataFrame] =
    Left(new UnsupportedException(s"Non support recover mechanism in $handlerName handler"))
}
