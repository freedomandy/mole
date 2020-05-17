package org.freedomandy.mole.monitor.handlers

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.freedomandy.mole.commons.exceptions.{BaseException, UnsupportedException}
import org.freedomandy.mole.commons.monitor.handlers.ErrorHandler
import org.freedomandy.mole.commons.utils.{KafkaPublisher => KPublisher}

/**
  * @author Andy Huang on 2018/8/21
  */
object KafkaInformer extends ErrorHandler {
  override def handlerName: String = "KafkaPublish"

  override def isInformer: Boolean = true

  override def isRecover: Boolean = false

  override def notify(message: String, config: Config): Unit =
    KPublisher
      .apply(config.getString("url"))
      .send(config.getString("topic"), config.getString("key"), config.getString("value"))

  override def recover(dataFrame: DataFrame, config: Config): Either[BaseException, DataFrame] =
    Left(new UnsupportedException(s"Non support recover mechanism in $handlerName handler"))
}
