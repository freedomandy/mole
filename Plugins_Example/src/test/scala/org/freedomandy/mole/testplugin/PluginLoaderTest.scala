package org.freedomandy.mole.testplugin

import com.mongodb.client.MongoCollection
import com.mongodb.spark.{MongoConnector, MongoSpark}
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.freedomandy.mole.commons.utils.PluginLoader
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

/**
  * @author Andy Huang on 2018/5/30
  */
class PluginLoaderTest extends Matchers with FunSpecLike with BeforeAndAfterAll {
  val session: SparkSession =  SparkSession.builder.appName("Test").master("local[*]").getOrCreate()
  val df: DataFrame = session.createDataFrame(Seq(("Y1", 100, "c", "james", "b", Some(0.2), 1L),
    ("Y2", 100, "c", "james", "b", None, 1L),
    ("Y3", 100, "c", "james", "b", None, 2L),
    ("Y4", 100, "c", "james", "b", Some(5.0), 3L),
    ("Y5", 99, "c", "james", "b", None, 4L)))


  describe("Test for plugin loader") {
    it("Test for transform plugin") {
      val configString = """plugins {
                             transform = ["org.freedomandy.mole.testplugin.transform.TestStage"]
                           }
                           """
      val config = ConfigFactory.parseString(configString)
      val pluginList = PluginLoader.loadTransformPlugins(config)
      val transformConfigString = """{
                             field = "_2"
                             operator = "sum"
                           }"""
      val transformConfig = ConfigFactory.parseString(transformConfigString)

      assert(pluginList.lengthCompare(1) == 0)
      assert(pluginList.head.actionName == "TEST-ONLY")
      assert(pluginList.head.transform(transformConfig)(df).first.get(0).asInstanceOf[Long] == 499)
    }
  }

  it ("Test for source plugin") {
    val session = SparkSession.builder.appName("MOLE Job").master("local[*]").getOrCreate()
    val configString =
      """
        |plugins {
        |  source = ["org.freedomandy.mole.testplugin.sources.TestSource"]
        |}
        |mole {
        |  source {
        |    type = "WEBHOOK"
        |  }
        |}
      """.stripMargin
    val config = ConfigFactory.parseString(configString)
    val sourceList = PluginLoader.loadSourcePlugins(config)

    assert(sourceList.head.get(session, config).get.count() == 1)
  }

  ignore ("Test for sink plugin") {
    def getNumberOfStoredData(session: SparkSession): Long = {

      val readConfig = ReadConfig(Map("uri" -> "mongodb://127.0.0.1:27017/test.test_result", "readPreference.name" -> "secondaryPreferred"))
      val mongoIdSet = MongoSpark.load(session.sparkContext, readConfig).flatMap(document => Set(document.getString("_1")))

      mongoIdSet.count()
    }
    def cleanMockData(): Unit = {
      val writeConfig = WriteConfig(Map("uri" -> "mongodb://127.0.0.1:27017/test.test_result", "writeConcern.w" -> "majority"))

        try {
          val mongoConnector = MongoConnector(writeConfig.asOptions)

          mongoConnector.withCollectionDo(writeConfig, {
            collection: MongoCollection[org.bson.Document] =>
              collection.drop()
          })
        } catch {
          case t: Throwable =>
            println(s"Failed to drop mock data: ${t.getMessage}")
            throw t
        }
    }

    val session = SparkSession.builder.appName("MOLE Job").master("local[*]").getOrCreate()
    val configString =
      """
        |plugins {
        |  sink = ["org.freedomandy.mole.testplugin.sinks.TestSink"]
        |}
        |mole {
        |  sink: [
        |    {
        |      type = "TEST_SINK"
        |      path = "mongodb://127.0.0.1:27017/test.test_result"
        |    }
        |  ]
        |}
      """.stripMargin
    val config = ConfigFactory.parseString(configString)
    val sinkList = PluginLoader.loadSinkPlugins(config)
    val sinkConfig = config.getObjectList("mole.sink").get(0).toConfig
    val df = session.createDataFrame(Seq(("Y1", 1000, "c", "james", "b", Some(0.2), 1L),
      ("Y2", 200, "c", "james", "b", None, 1L),
      ("Y3", 100, "b", "james", "b", None, 2L),
      ("Y4", 100, "c", "Andy", "b", Some(5.0), 3L),
      ("Y5", 99, "c", "james", "b", None, 4L)))

    sinkList.head.overwrite(sinkConfig)(df, "_1")


    assert(getNumberOfStoredData(session) == 5)
    cleanMockData()
  }
}
