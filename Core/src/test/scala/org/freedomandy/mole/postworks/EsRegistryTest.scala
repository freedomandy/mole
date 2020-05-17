package org.freedomandy.mole.postworks

import org.freedomandy.mole.sinks.EsSink
import org.freedomandy.mole.transform.Common
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

/**
  * @author Andy Huang on 2018/9/11
  */
class EsRegistryTest  extends Matchers with FunSpecLike with BeforeAndAfterAll {
  val session: SparkSession =  SparkSession.builder.appName("Test").master("local[*]").getOrCreate()
  val df: DataFrame = session.createDataFrame(Seq(("Y1", 100, "c", "james", "b", Some(0.2), 1L),
    ("Y2", 100, "c", "james", "b", None, 1L),
    ("Y3", 100, "b", "james", "b", None, 2L),
    ("Y4", 100, "c", "Andy", "b", Some(5.0), 3L),
    ("Y5", 99, "c", "james", "b", None, 4L))).toDF("uid", "credit", "type", "name", "category", "score", "amount")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }
  override protected def afterAll(): Unit = {
    // TODO: Drop test ES index


    super.afterAll()
  }

  describe("Test for EsRegistry") {
    ignore("insert") {
      val syncConfigString =
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
          |   category = "test-catalog2"
          | }
          |}
        """.stripMargin

      val jobConfig = ConfigFactory.parseString(syncConfigString)

      val startTime = System.currentTimeMillis / 1000

      assert(EsRegistry.execute(session, df, startTime, jobConfig).isEmpty)
    }

    ignore("upsert") {
      // TODO:
    }
  }
}
