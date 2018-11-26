package org.freedomandy.mole.sinks

import java.sql.Date

import org.freedomandy.mole.sources.HiveSource
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

/**
  * @author Andy Huang on 2018/7/18
  */
class HiveSinkTest extends Matchers with FunSpecLike with BeforeAndAfterAll {
  val session: SparkSession = SparkSession.builder.config("spark.sql.warehouse.dir", "./spark-warehouse").appName("Test").master("local[*]").getOrCreate()
  val date = new java.util.Date
  val df: DataFrame = session.createDataFrame(Seq(
    ("Y1", 1000, Date.valueOf("2018-12-01"), new java.sql.Timestamp(date.getTime), java.math.BigDecimal.valueOf(0.19999)),
    ("Y2", 200, null, new java.sql.Timestamp(date.getTime), java.math.BigDecimal.valueOf(19999.1))))

  override protected def beforeAll(): Unit = {
    session.sql("CREATE DATABASE IF NOT EXISTS test")
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    session.sql("DROP DATABASE IF EXISTS test CASCADE")
    super.afterAll()
  }

  describe("HiveSink") {
    it("overwrite") {
      val configString =
        """
          |{
          |   type = "HIVE"
          |   database = "test"
          |   table = "testTable"
          |}
        """.stripMargin
      val readConfigString =
        """
          |{
          |   type = "HIVE"
          |   query = "SELECT * FROM test.testTable"
          |}
        """.stripMargin
      val config = ConfigFactory.parseString(configString)
      val readConfig = ConfigFactory.parseString(readConfigString)

      HiveSink.overwrite(config)(df, "_1")
      HiveSink.overwrite(config)(df, "_1")

      assert(HiveSource.get(session, readConfig).get.count() == 2)
    }
    it("upsert") {
      val configString =
        """
          |{
          |   type = "HIVE"
          |   database = "test"
          |   table = "testTable"
          |}
        """.stripMargin
      val readConfigString =
        """
          |{
          |   type = "HIVE"
          |   query = "SELECT * FROM test.testTable"
          |}
        """.stripMargin
      val config = ConfigFactory.parseString(configString)
      val readConfig = ConfigFactory.parseString(readConfigString)

      HiveSink.overwrite(config)(df, "_1")

      val upsertDf: DataFrame = session.createDataFrame(Seq(
        ("Y1", 1000, Date.valueOf("2018-12-01"), new java.sql.Timestamp(date.getTime), java.math.BigDecimal.valueOf(0.19999)),
        ("Y2", 200, null, new java.sql.Timestamp(date.getTime), java.math.BigDecimal.valueOf(0.19999)),
        ("Y3", 1000, Date.valueOf("2018-12-01"), new java.sql.Timestamp(date.getTime), java.math.BigDecimal.valueOf(0.19999))))

      HiveSink.upsert(config)(upsertDf, "_1")

      assert(HiveSource.get(session, readConfig).get.count() == 3)
    }
    it("date cast to string") {
      val configString1 =
        """
          |{
          |   saveFormat = "parquet"
          |   type = "HIVE"
          |   database = "test"
          |   table = "testTable"
          |}
        """.stripMargin
      val configString2 =
        """
          |{
          |   saveFormat = "parquet"
          |   type = "HIVE"
          |   database = "test"
          |   table = "testTable"
          |   dateCast = true
          |   timeCast = true
          |   decimalCast = true
          |}
        """.stripMargin
      val readConfigString =
        """
          |{
          |   type = "HIVE"
          |   query = "SELECT * FROM test.testTable"
          |}
        """.stripMargin
      val config1 = ConfigFactory.parseString(configString1)
      val config2 = ConfigFactory.parseString(configString2)
      val readConfig = ConfigFactory.parseString(readConfigString)

      HiveSink.overwrite(config1)(df, "_1")
      assert(HiveSource.get(session, readConfig).get.schema.apply("_3").dataType.simpleString == "date")
      assert(HiveSource.get(session, readConfig).get.schema.apply("_4").dataType.simpleString == "timestamp")
      assert(HiveSource.get(session, readConfig).get.schema.apply("_5").dataType.simpleString.startsWith("decimal"))

      HiveSink.overwrite(config2)(df, "_1")
      assert(HiveSource.get(session, readConfig).get.schema.apply("_3").dataType.simpleString == "string")
      assert(HiveSource.get(session, readConfig).get.schema.apply("_4").dataType.simpleString == "bigint")
      assert(HiveSource.get(session, readConfig).get.schema.apply("_5").dataType.simpleString == "double")
    }
  }
}
