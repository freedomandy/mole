package org.freedomandy.mole.transform

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, DecimalType, TimestampType}

/**
  * @author Andy Huang on 19/03/2018
  */
object Common {
  def addIdField(df: DataFrame, key: Set[String], keyName: String = "_id"): DataFrame = {
    val tempFieldPrefix = "tmp_"
    val getHashValue = (fields: Seq[Any], separator: String) => {
      val key = fields.filter(_ != null).mkString(separator)
      val md  = java.security.MessageDigest.getInstance("SHA-1")

      md.digest(key.getBytes("UTF-8")).map("%02x".format(_)).mkString
    }
    val getKey       = udf(getHashValue)
    val castToString = (s: Any) => if (s == null) null else s.toString
    val toStringUDF  = udf[String, Any](castToString)

    if (key.size == 1)
      df.withColumnRenamed(key.head, keyName)
    else {
      // Casting all key fields as String type
      val castedDF = key.foldLeft(df) { (df, c) =>
        df.withColumn(tempFieldPrefix + c, toStringUDF(col(c)))
      }

      // Add id field
      val tempFields = key.toList.map(tempFieldPrefix + _)
      val keys       = array(tempFields.map(col): _*)
      val ddf        = castedDF.withColumn(keyName, getKey(keys, lit("_")))

      // Remove temp column
      tempFields.foldLeft(ddf) { (ddf, c) =>
        ddf.drop(c)
      }
    }
  }

  def addTimeField(df: DataFrame, timeFieldName: String, format: String, outputName: String): DataFrame =
    if (format == "unix_timestamp")
      df.withColumn(outputName, col(timeFieldName) * 1000)
    else {
      val getTimestamp = (time: String) => {
        val df = new java.text.SimpleDateFormat(format)

        if (time == null) None else Some(df.parse(time).getTime())
      }
      val getTime = udf(getTimestamp)

      df.withColumn(outputName, getTime(col(timeFieldName)))
    }

  def rename(df: DataFrame, mapping: (String, String)): DataFrame =
    df.withColumnRenamed(mapping._1, mapping._2)

  def dropFieldsExcept(fields: List[String])(df: DataFrame, field: String): DataFrame =
    if (fields.toSet.contains(field))
      df
    else
      df.drop(field)

  def castDateToString(dataFrame: DataFrame): DataFrame = {
    def toString(dataFrame: DataFrame, column: String): DataFrame = {
      val getStr   = udf((date: Date) => if (date == null) None else Some(date.toString))
      val tempName = "$" + column

      dataFrame.withColumn(tempName, getStr(col(column))).drop(column).withColumnRenamed(tempName, column)
    }

    val dateColumns = dataFrame.schema.filter(p => p.dataType.isInstanceOf[DateType]).map(_.name)

    println(dateColumns)

    dateColumns.foldLeft(dataFrame)(toString)
  }

  def castTimestampToLong(dataFrame: DataFrame): DataFrame = {
    def toString(dataFrame: DataFrame, column: String): DataFrame = {
      val getStr   = udf((time: Timestamp) => if (time == null) None else Some(time.getTime))
      val tempName = "$" + column

      dataFrame.withColumn(tempName, getStr(col(column))).drop(column).withColumnRenamed(tempName, column)
    }

    val timeColumns = dataFrame.schema.filter(p => p.dataType.isInstanceOf[TimestampType]).map(_.name)

    timeColumns.foldLeft(dataFrame)(toString)
  }

  def castDecimalToDouble(dataFrame: DataFrame): DataFrame = {
    def toDouble(dataFrame: DataFrame, column: String): DataFrame = {
      val getStr   = udf((decimal: java.math.BigDecimal) => if (decimal == null) None else Some(decimal.doubleValue()))
      val tempName = "$" + column

      dataFrame.withColumn(tempName, getStr(col(column))).drop(column).withColumnRenamed(tempName, column)
    }

    val decimalColumns = dataFrame.schema.filter(p => p.dataType.isInstanceOf[DecimalType]).map(_.name)

    decimalColumns.foldLeft(dataFrame)(toDouble)
  }

  def isUnique(dataFrame: DataFrame, fieldName: String*): Boolean =
    dataFrame.select(fieldName.head, fieldName.tail: _*).distinct().count == dataFrame.count()

  def covertToStruct(fieldName: String, dropCol: Boolean, fields: Map[String, String])(df: DataFrame): DataFrame = {
    val convertDf = df.withColumn(fieldName, struct(fields.toList.map(f => col(f._2).alias(f._1)): _*))
    if (dropCol) {
      val keepCol = convertDf.columns.toList.diff(fields.toList.map(_._2))
      convertDf.columns.foldLeft(convertDf)(Common.dropFieldsExcept(keepCol))
    } else convertDf
  }

  def covertToMap(fieldName: String, dropCol: Boolean, fields: Map[String, String])(df: DataFrame): DataFrame = {
    val convertDf = df.withColumn(fieldName, map(fields.toList.flatMap(f => List(lit(f._1), col(f._2))): _*))
    if (dropCol) {
      val keepCol = convertDf.columns.toList.diff(fields.toList.map(_._2))
      convertDf.columns.foldLeft(convertDf)(Common.dropFieldsExcept(keepCol))
    } else convertDf
  }
}
