package org.freedomandy.mole.transform

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.freedomandy.mole.commons.exceptions.InvalidInputException
import org.freedomandy.mole.commons.transform.FlowStage

/**
  * @author Andy Huang on 27/02/2018
  */
object Filler extends FlowStage {
  def fillLastValue(
      lastValue: Double,
      index: Int,
      fieldIndex: Int,
      rddList: List[(Row, Int)],
      numOfSteps: Long
  ): List[(Row, Int)] =
    if (index <= numOfSteps) {
      val indexTuple = rddList.filter(_._2 == index).head._1
      if (indexTuple.isNullAt(fieldIndex)) // Update record with last value
        fillLastValue(
          lastValue,
          index + 1,
          fieldIndex,
          rddList.map {
            case (r: Row, i: Int) =>
              if (i == index)
                (Row.fromSeq(r.toSeq.updated(fieldIndex, lastValue)), i)
              else
                (r, i)
          },
          numOfSteps
        )
      else // Assign new last value
        fillLastValue(indexTuple.getDouble(fieldIndex), index + 1, fieldIndex, rddList, numOfSteps)
    } else
      rddList

  // TODO: Refactor this method to handle the no session key case in a better way
  def fillForward(
      session: SparkSession,
      df: DataFrame,
      sessionKeyFields: Set[String],
      fillField: Int,
      sortField: Int
  ): DataFrame = {
    def getSessionKey(fields: Set[String], row: Row): String =
      fields.foldLeft(List[String]())((list, str) => list ::: List(row.getAs(str).toString)).mkString("_")

    val filler = Filler.fillLastValue _
    val pairRDD =
      if (sessionKeyFields.isEmpty)
        df.rdd.map(t => ("NoSessionKey", t))
      else
        df.rdd.map(t => (getSessionKey(sessionKeyFields, t), t))

    val sortedGroup = pairRDD
      .aggregateByKey(List[Row]())((list, row) => list ::: List(row), (list1, list2) => list1 ::: list2)
      .mapValues(v => v.sortWith((s, t) => s.getLong(sortField) < t.getLong(sortField)).zipWithIndex)
    val result = sortedGroup.flatMapValues(ele => filler(-1, 0, fillField, ele, ele.length - 1)).map(_._2._1)

    session.createDataFrame(result, df.schema)
  }

  override def actionName: String = "Filling"

  override def transform(config: Config)(dataFrame: DataFrame): DataFrame = {
    val session        = SparkSession.builder.appName("MOLE Job").getOrCreate()
    val sessionKey     = getParam[String](config, "sessionKey").map(_.split(",").toSet)
    val fillFieldIndex = getParam[Int](config, "fillFieldIndex")
    val sortFieldIndex = getParam[Int](config, "sortFieldIndex")

    if (sessionKey.isEmpty || fillFieldIndex.isEmpty || sortFieldIndex.isEmpty)
      throw new InvalidInputException(s"Invalid params: ${config.toString}")

    fillForward(session, dataFrame, sessionKey.get, fillFieldIndex.get, sortFieldIndex.get)
  }
}
