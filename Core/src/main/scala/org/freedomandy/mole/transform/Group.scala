package org.freedomandy.mole.transform

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.freedomandy.mole.commons.exceptions.InvalidInputException
import org.freedomandy.mole.commons.transform.FlowStage

/**
  * @author Andy Huang on 16/03/2018
  */
object Group extends FlowStage {
  def groupBy(dataFrame: DataFrame, key: Set[String], aggregateOp: String, aggregateFields: Seq[String]): DataFrame =
    aggregateOp match {
      case "sum" =>
        dataFrame.groupBy(key.toSeq.map(col): _*).sum(aggregateFields: _*)
      case "count" =>
        dataFrame.groupBy(key.toSeq.map(col): _*).count
      case "max" =>
        dataFrame.groupBy(key.toSeq.map(col): _*).max(aggregateFields: _*)
      case "min" =>
        dataFrame.groupBy(key.toSeq.map(col): _*).min(aggregateFields: _*)
      case "avg" =>
        dataFrame.groupBy(key.toSeq.map(col): _*).avg(aggregateFields: _*)
      case _ =>
        throw new InvalidInputException("Unrecognized aggregate operator")
    }

  override def actionName: String = "GroupBy"

  override def transform(config: Config)(dataFrame: DataFrame): DataFrame = {
    val key: Option[Set[String]]            = getParam[String](config, "key").map(_.split(",").toSet)
    val aggregateOperator: Option[String]   = getParam[String](config, "aggregate")
    val aggregateField: Option[Seq[String]] = getParam[String](config, "aggregateFields").map(_.split(",").toList)

    if (key.isEmpty || aggregateOperator.isEmpty || aggregateField.isEmpty)
      throw new InvalidInputException(s"Invalid params: ${config.toString}")

    groupBy(dataFrame, key.get, aggregateOperator.get, aggregateField.get)
  }
}
