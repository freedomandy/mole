package org.freedomandy.mole.transform

import com.typesafe.config.Config
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.freedomandy.mole.commons.exceptions.InvalidInputException
import org.freedomandy.mole.commons.transform.FlowStage

object JsonDataConverter extends FlowStage {
  def getNestedDataFromJsonString(df: DataFrame, inputCol: String, outputCol: String): DataFrame = {
    val session = SparkSession.builder.appName("MOLE Job").getOrCreate()
    val schema  = session.read.json(df.select(inputCol).as(org.apache.spark.sql.Encoders.STRING)).schema

    df.withColumn(outputCol, from_json(col(inputCol), schema))
  }

  override def actionName: String = "JsonStrToNestedObj"

  override def transform(config: Config)(dataFrame: DataFrame): DataFrame = {
    val columnOp: Option[String] = getParam[String](config, "column")
    val outputOp: Option[String] = getParam[String](config, "output")

    (for {
      columnName       <- columnOp
      outputColumnName <- outputOp
    } yield getNestedDataFromJsonString(dataFrame, columnName, outputColumnName))
      .getOrElse(throw new InvalidInputException(s"Invalid params: ${config.toString}"))
  }
}
