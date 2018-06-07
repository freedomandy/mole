package org.freedomandy.mole.testplugin.transform

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.freedomandy.mole.commons.exceptions.InvalidInputException
import org.freedomandy.mole.commons.transform.FlowStage

/**
  * @author Andy Huang on 2018/5/30
  */
class TestStage extends FlowStage  {
  override def actionName: String = "TEST-ONLY"

  override def transform(config: Config)(dataFrame: DataFrame): DataFrame = {
    // Read params from config
    val fieldName= getParam[String](config, "field")
    val operator = getParam[String](config, "operator")

    if (fieldName.isEmpty || operator.isEmpty)
      throw new InvalidInputException(s"Invalid params: ${config.toString}")
    // transform

    aggregate(dataFrame, fieldName.get, operator.get)
  }

  def aggregate(df: DataFrame, field: String, operator: String): DataFrame = {
    import org.apache.spark.sql.functions._
    operator match {
      case "sum" =>
        df.agg(sum(field))
      case "max" =>
        df.agg(max(field))
      case "min" =>
        df.agg(min(field))
      case "avg" =>
        df.agg(avg(field))
      case _ =>
        throw new InvalidInputException("Unrecognized aggregate operator")
    }
  }
}

