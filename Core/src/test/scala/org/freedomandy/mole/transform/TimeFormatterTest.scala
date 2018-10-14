package org.freedomandy.mole.transform

import java.sql.Date

import com.typesafe.config.{ConfigBeanFactory, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

/**
  * @author Andy Huang on 2018/7/31
  */
class TimeFormatterTest extends Matchers with FunSpecLike with BeforeAndAfterAll {
//  def getTimeStamp(time; String) = {
//
//  }

  val session: SparkSession = SparkSession.builder.config("spark.sql.warehouse.dir", "./spark-warehouse").appName("Test").master("local[*]").getOrCreate()
  val date = new java.util.Date
  val df: DataFrame = session.createDataFrame(Seq(
    ("Y1", 1000, Date.valueOf("2018-12-01"), Date.valueOf("2018-12-01").getTime / 1000, java.math.BigDecimal.valueOf(0.19999)),
    ("Y2", 200, null, Date.valueOf("2018-12-01").getTime / 1000, java.math.BigDecimal.valueOf(19999.1))))

  describe("Time Formatter") {
    it("timestamp casting") {
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

      assert(result.select("time").first().getAs[Long]("time") == 1543593600000L)
    }
    it("") {
      val configString2 =
        """
          |{
          | action = "Time"
          | field = "_4"
          | format = "unix_timestamp"
          | outputName = "time"
          |}
        """.stripMargin

      val result = TimeFormatter.transform(ConfigFactory.parseString(configString2))(df)
      assert(result.select("time").first().getAs[Long]("time") == 1543593600000L)
    }
  }

}
