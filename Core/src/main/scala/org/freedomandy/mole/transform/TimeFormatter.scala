package org.freedomandy.mole.transform

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.freedomandy.mole.commons.transform.FlowStage

/**
  * @author Andy Huang on 2018/7/31
  */
object TimeFormatter extends FlowStage {
  override def actionName: String = "Time"

  override def transform(config: Config)(dataFrame: DataFrame): DataFrame =
    Common.addTimeField(
      dataFrame,
      config.getString("field"),
      config.getString("format"),
      config.getString("outputName")
    )
}
