package org.freedomandy.mole.transform

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.freedomandy.mole.commons.exceptions.InvalidInputException
import org.freedomandy.mole.commons.transform.FlowStage

/**
  * @author Andy Huang on 2018/5/29
  */
object FieldConverter extends FlowStage {
  def convert(dataFrame: DataFrame, fieldMap: List[(String, String)]): DataFrame = {
    val renamedDF = fieldMap.foldLeft(dataFrame)(Common.rename)

    renamedDF.columns.foldLeft(renamedDF)(Common.dropFieldsExcept(fieldMap.map(_._2)))
  }

  override def actionName: String = "FieldMapping"

  override def transform(config: Config)(dataFrame: DataFrame): DataFrame = {
    val from = getParam[String](config, "from").map(_.split(","))
    val to = getParam[String](config, "to").map(_.split(","))

    if (from.isEmpty || to.isEmpty)
      throw new InvalidInputException(s"Invalid params: ${config.toString}")

    if (from.get.length != to.get.length)
      throw new InvalidInputException("Invalid field name mapping")

    val params = from.get.zip(to.get).toList

    convert(dataFrame, params)
  }
}
