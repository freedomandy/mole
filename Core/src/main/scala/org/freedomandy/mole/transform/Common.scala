package org.freedomandy.mole.transform

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{array, col, lit, udf}

/**
  * @author Andy Huang on 19/03/2018
  */
object Common {
  def addIdField(df: DataFrame, key: Set[String]): DataFrame = {
    val getHashValue = (fields: Seq[Any], separator: String) => {
      val key = fields.filter(_ != null).mkString(separator)
      val md = java.security.MessageDigest.getInstance("SHA-1")

      md.digest(key.getBytes("UTF-8")).map("%02x".format(_)).mkString
    }
    val getKey = udf(getHashValue)

    if (key.size == 1) {
      df.withColumnRenamed(key.head, "_id")
    } else {
      val keys = array(key.toList.map(col): _*)

      df.withColumn("_id", getKey(keys, lit("_")))
    }
  }

  def addTimeField(df: DataFrame, timeFieldName: String, format: String, outputName: String): DataFrame = {
    if (format == "unix_timestamp") {
      df.withColumn(outputName, col(timeFieldName) * 1000)
    } else {
      val getTimestamp = (time: String) => {
        val df = new java.text.SimpleDateFormat(format)

        df.parse(time).getTime()
      }
      val getTime = udf(getTimestamp)

      df.withColumn(outputName, getTime(col(timeFieldName)))
    }
  }

  def rename(df: DataFrame, mapping: (String, String)): DataFrame = {
    df.withColumnRenamed(mapping._1, mapping._2)
  }

  def dropFieldsExcept(fields: List[String])(df: DataFrame, field: String): DataFrame = {
    if (("_id" + fields.toSet).contains(field))
      df
    else
      df.drop(field)
  }
}
