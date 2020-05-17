package org.freedomandy.mole.sinks

import org.freedomandy.mole.sources.MongoSource
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

/**
  * @author Andy Huang on 2018/7/13
  */
class MongoSinkTest extends Matchers with FunSpecLike with BeforeAndAfterAll {
  val session: SparkSession = SparkSession.builder.appName("Test").master("local[*]").getOrCreate()
  val df: DataFrame = session.createDataFrame(Seq(("Y1", 1000, "c", "james", "b", Some(0.2), 1L),
    ("Y2", 200, "c", "james", "b", None, 1L),
    ("Y3", 100, "b", "james", "b", None, 2L),
    ("Y4", 100, "c", "Andy", "b", Some(5.0), 3L),
    ("Y5", 99, "c", "james", "b", None, 4L)))

  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }
  override protected def afterAll(): Unit = {
    val configString =
      """
        |{
        |  type = "MONGODB"
        |  path = "mongodb://127.0.0.1:27017/customer_journey.test"
        |}
      """.stripMargin
    val config = ConfigFactory.parseString(configString)

    MongoSink.dropCollection(config)
    super.afterAll()
  }

  describe("Test for Mongo sink") {
    ignore("overwrite") {
      val configString =
        """mole {
          | source {
          |   type = "MONGODB"
          |   path = "mongodb://127.0.0.1:27017/customer_journey.test"
          | }
          | sink: {
          |   primaryKey = "_1"
          |   destinations: [
          |     {
          |       type = "MONGODB"
          |       operation = "OVERWRITE"
          |       path = "mongodb://127.0.0.1:27017/customer_journey.test"
          |     }
          |   ]
          | }
          |}
        """.stripMargin
      val config = ConfigFactory.parseString(configString)

      Loader(session, config).run(df)
      Loader(session, config).run(df.filter(row => row.getInt(1) > 99))

      assert(MongoSource.get(session, config.getConfig("mole.source")).get.count() == 4)
    }

    ignore ("upsert") {
      val configString =
        """mole {
          | source {
          |   type = "MONGODB"
          |   path = "mongodb://127.0.0.1:27017/customer_journey.test"
          | }
          | sink: {
          |   primaryKey = "_1"
          |   destinations: [
          |     {
          |       type = "MONGODB"
          |       operation = "UPSERT"
          |       path = "mongodb://127.0.0.1:27017/customer_journey.test"
          |     }
          |   ]
          |  }
          |}
        """.stripMargin
      val config = ConfigFactory.parseString(configString)

      Loader(session, config).run(df)
      Loader(session, config).run(df.filter(row => row.getInt(1) > 99))
      assert(MongoSource.get(session, config.getConfig("mole.source")).get.count() == 5)
    }
  }
}
