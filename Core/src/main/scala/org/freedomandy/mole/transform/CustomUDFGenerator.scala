package org.freedomandy.mole.transform

import com.typesafe.config.Config
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.freedomandy.mole.commons.exceptions.InvalidInputException
import org.freedomandy.mole.commons.transform.FlowStage

/**
  * @author Andy Huang on 20/02/2018
  */
object CustomUDFGenerator extends FlowStage {
  def getFunc[T](functionLiteral: String) = {
    new Function1[Seq[Any], T] with Serializable {
      import scala.reflect.runtime.currentMirror
      import scala.tools.reflect.ToolBox

      lazy val toolbox = currentMirror.mkToolBox()
      lazy val func = {
        println("Generating reflected function...")
        toolbox.eval(toolbox.parse(functionLiteral)).asInstanceOf[Seq[Any] => T]
      }

      def apply(s: Seq[Any]): T = func(s)
    }
  }

  def addCustomValue(session: SparkSession, dataFrame: DataFrame, inputCol: Set[String], outputColName: String, columnType: String, functionLiteral: String): DataFrame = {
    def getInputValue(row: Row, inputCol: Set[String]): Seq[Any] = {
      inputCol.toSeq.map(ele => row.getAs[Any](ele))
    }

    val schema = dataFrame.schema
    val (outputSchema, customFunc) =
      if (columnType == "Integer") {
        (schema.add(StructField(outputColName, IntegerType, true)), getFunc[Int](functionLiteral).apply _)
      } else if (columnType == "Double"){
        (schema.add(StructField(outputColName, DoubleType, true)), getFunc[Double](functionLiteral).apply _)
      } else if (columnType == "Boolean"){
        (schema.add(StructField(outputColName, BooleanType, true)), getFunc[Boolean](functionLiteral).apply _)
      } else if (columnType == "Long"){
        (schema.add(StructField(outputColName, LongType, true)), getFunc[Long](functionLiteral).apply _)
      } else if (columnType == "String"){
        (schema.add(StructField(outputColName, StringType, true)), getFunc[String](functionLiteral).apply _)
      } else {
        throw new RuntimeException(s"Unsupported data type $columnType")
      }

    session.createDataFrame(dataFrame.rdd.map(row => Row.fromSeq(row.toSeq :+ customFunc(getInputValue(row, inputCol)))), outputSchema)
  }

  override def actionName: String = "Custom"

  override def transform(config: Config)(dataFrame: DataFrame): DataFrame = {
    val session = SparkSession.builder.appName("MOLE Job").getOrCreate()
    val input = getParam[String](config, "input").map(_.split(",").toSet)
    val output = getParam[String](config, "output")
    val outputType = getParam[String](config, "outputType")
    val function = getParam[String](config, "function")

    if (input.isEmpty || output.isEmpty || outputType.isEmpty || function.isEmpty)
      throw new InvalidInputException(s"Invalid params: ${config.toString}")

    addCustomValue(session, dataFrame, input.get, output.get, outputType.get, function.get)
  }
}
