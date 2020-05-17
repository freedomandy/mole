package org.freedomandy.mole.postworks

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

/**
  * @author Andy Huang on 2018/9/12
  */
class PostWorkHandlerTest extends Matchers with FunSpecLike with BeforeAndAfterAll {
  val session: SparkSession = SparkSession.builder.appName("Test").master("local[*]").getOrCreate()
  val df: DataFrame = session
    .createDataFrame(
      Seq(
        ("Y1", 100, "c", "james", "b", Some(0.2), 1L),
        ("Y2", 100, "c", "james", "b", None, 1L),
        ("Y3", 100, "b", "james", "b", None, 2L),
        ("Y4", 100, "c", "Andy", "b", Some(5.0), 3L),
        ("Y5", 99, "c", "james", "b", None, 4L)
      )
    )
    .toDF("uid", "credit", "type", "name", "category", "score", "amount")

  override protected def beforeAll(): Unit =
    super.beforeAll()
  override protected def afterAll(): Unit =
    // TODO: Drop test ES index
    super.afterAll()

  describe("Integration Test") {
    ignore("usage") {
      val configString =
        """
          |mole {
          | source {
          |    type = "MONGODB" # such as MONGODB or HIVE
          |    path = "mongodb://127.0.0.1:27017/test.account"
          | }
          | sink: {
          |   primaryKey = "_id"
          |   destinations: [
          |      {
          |        type = "MONGODB"
          |        path = "mongodb://127.0.0.1:27017/test.test_result"
          |      },
          |      {
          |        type = "ELASTIC_SEARCH"
          |        url = "http://127.0.0.1:9200"
          |        resource = "spark/test"
          |      }
          |   ]
          | }
          | postWork {
          |   type = "EsRegistry"
          |   host = "http://52.163.212.183:9200"
          |   category = "test-postwork"
          | }
          |}
        """.stripMargin
      val config = ConfigFactory.parseString(configString)

      val workHandler = new PostWorkHandler(session, config)

      assert(workHandler.run(df, System.currentTimeMillis / 1000).isEmpty)
    }
  }

}
