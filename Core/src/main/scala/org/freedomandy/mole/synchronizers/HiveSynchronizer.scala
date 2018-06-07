package org.freedomandy.mole.synchronizers

import com.typesafe.config.Config
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.freedomandy.mole.commons.synchronizers.Synchronizer

/**
  * @author Andy Huang on 29/03/2018
  */
object HiveSynchronizer extends Synchronizer {
  private val DATABASE_CONFIG_PATH = "database"
  private val TABLE_CONFIG_PATH = "table"

  override def sinkName: String = "HIVE"

  override def upsertData(config: Config)(upsertDF: DataFrame, keyField: String): Unit = {
    val database = config.getString(DATABASE_CONFIG_PATH)
    val table = config.getString(TABLE_CONFIG_PATH)
    val session = SparkSession.builder.appName("MOLE Job").getOrCreate()
    // Load current data frame
    val currentDf = session.sql(s"SELECT * FROM $database.$table")

    // Upsert dataframe
    val result =currentDf.rdd.map(row => (row.getAs[String](keyField), row)).
      fullOuterJoin[Row](upsertDF.rdd.map(row => (row.getAs[String](keyField), row))).
      mapValues(pair => if (pair._2.isDefined) pair._2.get else pair._1.get).values
    val resultDF = session.createDataFrame(result, currentDf.schema)

    // Update it
    saveToHive(resultDF, database, table)
  }

  override def deleteData(config: Config)(deletedSet: RDD[String], keyField: String): Unit = {
    val session = SparkSession.builder.appName("MOLE Job").getOrCreate()
    val database = config.getString(DATABASE_CONFIG_PATH)
    val table = config.getString(TABLE_CONFIG_PATH)
    // Load current data frame
    val df = session.sql(s"SELECT * FROM $database.$table")

    // Delete record
    val result= df.rdd.map(row => (row.getAs[String](keyField), row)).
      subtractByKey[Row](deletedSet.map((_, Row.empty))).values
    val resultDF = session.createDataFrame(result, df.schema)

    // Update it
    saveToHive(resultDF, database, table)
  }

  override def sync(config: Config)(dataFrame: DataFrame, keyField: String): Unit = {
    val database = config.getString(DATABASE_CONFIG_PATH)
    val table = config.getString(TABLE_CONFIG_PATH)

    saveToHive(dataFrame, database, table)
  }

  protected def saveToHive(dataFrame: DataFrame, database: String, table: String): Unit = {
    dataFrame.write.mode("overwrite").saveAsTable(s"$database.$table")
  }
}