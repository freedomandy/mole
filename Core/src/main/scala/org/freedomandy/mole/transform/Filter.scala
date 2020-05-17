package org.freedomandy.mole.transform

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.freedomandy.mole.commons.exceptions.InvalidInputException
import org.freedomandy.mole.commons.transform.FlowStage

/**
  * @author Andy Huang on 16/03/2018
  */
object Filter extends FlowStage {
  def filter(dataFrame: DataFrame, fieldName: String, operator: String, value: Any): DataFrame = {
    val target = if (value.toString.contains("$")) col(value.toString.tail) else value

    operator match {
      case ">" =>
        dataFrame.filter(col(fieldName) > target)
      case "<" =>
        dataFrame.filter(col(fieldName) < target)
      case ">=" =>
        dataFrame.filter(col(fieldName) >= target)
      case "<=" =>
        dataFrame.filter(col(fieldName) <= target)
      case "==" =>
        dataFrame.filter(col(fieldName) === target)
      case "!=" =>
        dataFrame.filter(col(fieldName) =!= target)
      case _ => throw new InvalidInputException(s"Unrecognized operator: $operator")
    }
  }

  def filter(dataFrame: DataFrame, fieldName: String, regex: String): DataFrame =
    dataFrame.filter(col(fieldName).rlike(regex))

  override def actionName: String = "Filtering"

  override def transform(config: Config)(dataFrame: DataFrame): DataFrame = {
    // TODO: Provide the option to use regex
    val fieldName = getParam[String](config, "field")
    val operator  = getParam[String](config, "operator")
    val value     = getParam[String](config, "value")

    if (fieldName.isEmpty || operator.isEmpty || value.isEmpty)
      throw new InvalidInputException(s"Invalid params: ${config.toString}")

    filter(dataFrame, fieldName.get, operator.get, value.get)
  }
}
