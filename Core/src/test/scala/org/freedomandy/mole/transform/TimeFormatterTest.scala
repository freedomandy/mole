package org.freedomandy.mole.transform

import java.sql.Date
import java.util.Calendar

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

/**
  * @author Andy Huang on 2018/7/31
  */
class TimeFormatterTest extends Matchers with FunSpecLike with BeforeAndAfterAll {
  val session: SparkSession = SparkSession.builder.config("spark.sql.warehouse.dir", "./spark-warehouse").appName("Test").master("local[*]").getOrCreate()

  val currentTime: Calendar = Calendar.getInstance()
  val currentDate: java.sql.Date = new Date(currentTime.getTimeInMillis)


  val df: DataFrame = session.createDataFrame(Seq(
    ("Y1", 1000, currentDate, currentTime.getTimeInMillis, java.math.BigDecimal.valueOf(0.19999)),
    ("Y2", 200, null, currentTime.getTimeInMillis, java.math.BigDecimal.valueOf(19999.1))))

  describe("Time Formatter") {
    println(s"Current Time: ${currentTime.getTime}")
    println(s"Current Date: ${currentDate.toLocalDate}")

    it("Sql Date to timestamp casting") {
      val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd")
      val configString =
        """
          |{
          | action = "Time"
          | field = "_3"
          | format = "yyyy-MM-dd"
          | outputName = "time"
          |}
        """.stripMargin

      val result = TimeFormatter.transform(ConfigFactory.parseString(configString))(df)

      assert(result.select("time").first().getAs[Long]("time") == dateFormat.parse(currentDate.toLocalDate.toString).getTime())
    }
  }
}
