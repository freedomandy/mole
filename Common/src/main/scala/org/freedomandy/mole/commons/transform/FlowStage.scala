package org.freedomandy.mole.commons.transform

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame

/**
  * @author Andy Huang on 2018/5/30
  */
trait FlowStage {
  protected def getParam[T](config: Config, field: String): Option[T] = {
    try {
      Some(config.getAnyRef(field).asInstanceOf[T])
    } catch {
      case t: Throwable =>
        println(s"Failed to read Filter param: $field, ${t.toString}")
        None
    }
  }

  def actionName: String

  def transform(config: Config)(dataFrame: DataFrame): DataFrame
}
