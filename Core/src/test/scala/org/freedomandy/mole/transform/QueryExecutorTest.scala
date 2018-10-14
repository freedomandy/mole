package org.freedomandy.mole.transform

import java.sql.Date

import org.freedomandy.mole.sinks.HiveSink
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

/**
  * @author Andy Huang on 2018/7/31
  */
class QueryExecutorTest extends Matchers with FunSpecLike with BeforeAndAfterAll {
  val session: SparkSession = SparkSession.builder.config("spark.sql.warehouse.dir", "./spark-warehouse").appName("Test").master("local[*]").getOrCreate()
  val date = new java.util.Date
  val df: DataFrame = session.createDataFrame(Seq(
    ("Y1", 1000, Date.valueOf("2018-12-01"), new java.sql.Timestamp(date.getTime), java.math.BigDecimal.valueOf(0.19999)),
    ("Y2", 200, null, new java.sql.Timestamp(date.getTime), java.math.BigDecimal.valueOf(19999.1))))

  override protected def beforeAll(): Unit = {
    session.sql("CREATE DATABASE IF NOT EXISTS query_test")
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    session.sql("DROP DATABASE IF EXISTS query_test CASCADE")
    super.afterAll()
  }

  describe("Query Executor") {
    it("SQL Query inject") {
      val testDF: DataFrame = session.createDataFrame(Seq(
        ("Y1", 1000, "c", "james", "b", Some(0.2), 1L),
        ("Y2", 200, "c", "james", "b", None, 1L),
        ("Y3", 100, "b", "james", "b", None, 2L)))

      val tempConfigString =
        """
          |{
          |   type = "HIVE"
          |   database = "query_test"
          |   table = "testTable"
          |}
        """.stripMargin
      HiveSink.overwrite(ConfigFactory.parseString(tempConfigString))(testDF, "_1")

      val configString =
        """
          |{
          |   action = "Query"
          |   tempViews: [
          |     {
          |       name = "test"
          |       query ="SELECT * FROM query_test.testTable"
          |     }
          |   ]
          |   sourceViewName = "source"
          |   query = "SELECT source.*,test.* FROM source,test WHERE source._1 = test._1"
          |}
        """.stripMargin
      val config = ConfigFactory.parseString(configString)
      val result = QueryExecutor.transform(config)(df)

      assert(result.count() == 2)
      assert(result.columns.length == 12)
    }
  }
}
