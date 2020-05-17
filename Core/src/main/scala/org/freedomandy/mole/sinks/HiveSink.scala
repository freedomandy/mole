package org.freedomandy.mole.sinks

import com.typesafe.config.Config
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.freedomandy.mole.commons.sinks.Sink
import org.freedomandy.mole.transform.Common

/**
  * @author Andy Huang on 29/03/2018
  */
object HiveSink extends Sink {
  case class HiveParams(
      database: String,
      table: String,
      dateCasting: Boolean,
      timeCasting: Boolean,
      decimalCasting: Boolean,
      saveFormat: Option[String]
  )
  object HiveParams {
    def apply(config: Config): HiveParams = {
      val database       = config.getString("database")
      val table          = config.getString("table")
      val dateCasting    = if (config.hasPath("dateCast")) config.getBoolean("dateCast") else false
      val timeCasting    = if (config.hasPath("timeCast")) config.getBoolean("timeCast") else false
      val decimalCasting = if (config.hasPath("decimalCast")) config.getBoolean("decimalCast") else false
      val saveFormat     = if (config.hasPath("saveFormat")) Some(config.getString("saveFormat")) else None

      HiveParams(database, table, dateCasting, timeCasting, decimalCasting, saveFormat)
    }
  }

  override def sinkName: String = "HIVE"

  override def upsert(config: Config)(upsertDF: DataFrame, keyField: String): Unit = {
    val hiveParams = HiveParams(config)
    val session    = SparkSession.builder.appName("MOLE Job").getOrCreate()

    // Load current data frame
    val columns = upsertDF.columns
    val currentDf =
      session.sql(s"SELECT * FROM ${hiveParams.database}.${hiveParams.table}").select(columns.head, columns.tail: _*)

    // Upsert dataframe
    val result = currentDf.rdd
      .map(row => (row.getAs[String](keyField), row))
      .fullOuterJoin[Row](upsertDF.rdd.map(row => (row.getAs[String](keyField), row)))
      .mapValues(pair => if (pair._2.isDefined) pair._2.get else pair._1.get)
      .values
    val resultDF = session.createDataFrame(result, currentDf.schema)

    // Save as temp table
    val tempTableName: String = hiveParams.table + "_" + java.util.UUID.randomUUID().toString.replace("-", "_")

    saveToHive(
      ColsCast(resultDF, hiveParams.dateCasting, hiveParams.timeCasting, hiveParams.decimalCasting),
      hiveParams.database,
      tempTableName,
      hiveParams.saveFormat
    )

    // Synchronize records from temp table to original table
    session.sqlContext
      .table(s"${hiveParams.database}.$tempTableName")
      .write
      .format(hiveParams.saveFormat.getOrElse("parquet"))
      .mode("overwrite")
      .insertInto(s"${hiveParams.database}.${hiveParams.table}")

    // Drop useless temp table
    session.sql(s"DROP TABLE IF EXISTS ${hiveParams.database}.$tempTableName")
  }

  override def deleteData(config: Config)(deletedSet: RDD[String], keyField: String): Unit = {
    val session    = SparkSession.builder.appName("MOLE Job").getOrCreate()
    val hiveParams = HiveParams(config)
    // Load current data frame
    val df = session.sql(s"SELECT * FROM ${hiveParams.database}.${hiveParams.table}")

    // Delete record
    val result =
      df.rdd.map(row => (row.getAs[String](keyField), row)).subtractByKey[Row](deletedSet.map((_, Row.empty))).values
    val resultDF = session.createDataFrame(result, df.schema)

    // Update it
    saveToHive(resultDF, hiveParams.database, hiveParams.table, hiveParams.saveFormat)
  }

  override def overwrite(config: Config)(dataFrame: DataFrame, keyField: String): Unit = {
    val hiveParams = HiveParams(config)

    saveToHive(
      ColsCast(dataFrame, hiveParams.dateCasting, hiveParams.timeCasting, hiveParams.decimalCasting),
      hiveParams.database,
      hiveParams.table,
      hiveParams.saveFormat
    )
  }

  def append(config: Config)(dataFrame: DataFrame, keyField: String = "IGNORED"): Unit = {
    val hiveParams = HiveParams(config)

    ColsCast(dataFrame, hiveParams.dateCasting, hiveParams.timeCasting, hiveParams.decimalCasting).write
      .mode("append")
      .saveAsTable(s"${hiveParams.database}.${hiveParams.table}")
  }

  protected def ColsCast(
      dataFrame: DataFrame,
      dateToString: Boolean = false,
      timestampToLong: Boolean = false,
      decimalToDouble: Boolean = false
  ): DataFrame = {
    val dateDF = if (dateToString) Common.castDateToString(dataFrame) else dataFrame

    val dateTimeDF = if (timestampToLong) Common.castTimestampToLong(dateDF) else dateDF

    if (decimalToDouble) Common.castDecimalToDouble(dateTimeDF) else dateTimeDF
  }

  protected def saveToHive(dataFrame: DataFrame, database: String, table: String, saveFormat: Option[String]): Unit =
    if (saveFormat.isDefined)
      dataFrame.write.format(saveFormat.get).mode("overwrite").saveAsTable(s"$database.$table")
    else
      dataFrame.write.format("parquet").mode("overwrite").saveAsTable(s"$database.$table")
}
