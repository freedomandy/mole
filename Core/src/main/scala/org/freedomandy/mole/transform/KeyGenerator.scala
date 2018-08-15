package org.freedomandy.mole.transform

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.freedomandy.mole.commons.transform.FlowStage

/**
  * @author Andy Huang on 2018/7/12
  */
object KeyGenerator extends FlowStage {
  override def actionName: String = "KeyGenerating"

  override def transform(config: Config)(dataFrame: DataFrame): DataFrame = {
    val keyParam = getParam[java.util.ArrayList[String]](config, "keyFields")
    val keyName: Option[String] = getParam[String](config, "keyName")

    if (keyParam.isDefined) {
      if (keyParam.get.isEmpty) {
        Common.addIdField(dataFrame, dataFrame.columns.toSet, keyName.getOrElse("_id"))
      } else {
        import scala.collection.JavaConversions._
        Common.addIdField(dataFrame, keyParam.get.toSet, keyName.getOrElse("_id"))
      }
    } else {
      Common.addIdField(dataFrame, dataFrame.columns.toSet, keyName.getOrElse("_id"))
    }
  }
}
