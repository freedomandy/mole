package org.freedomandy.mole.sinks

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

/**
  * @author Andy Huang on 26/03/2018
  */
class EsSinkTest extends Matchers with FunSpecLike with BeforeAndAfterAll {
  describe("Test for Elastic search sink") {
    ignore("upsert") {
      val configString =
        """
          |{
          |  type = "ELASTIC_SEARCH"
          |  url = "http://127.0.0.1:9200"
          |  resource = "spark/test"
          |}
        """.stripMargin
      val config = ConfigFactory.parseString(configString)
      val session = SparkSession.builder.appName("Test").master("local[*]").getOrCreate()
      val df = session.createDataFrame(Seq(("Y1", 1000, "c", "james", "b", Some(0.2), 1L),
        ("Y2", 200, "c", "james", "b", None, 1L),
        ("Y3", 100, "b", "james", "b", None, 2L),
        ("Y4", 100, "c", "Andy", "b", Some(5.0), 3L),
        ("Y5", 99, "c", "james", "b", None, 4L)))
      val esConnector = EsSink

      esConnector.upsert(config)(df, "_1")

      // Checking whether data has been inserted to ElasticSearch
      assert(df.rdd.map(_.getAs[String]("_1")).subtract(esConnector.getData(config, session).map(_._1)).isEmpty())

      // Checking whether data has been updated successfully
      val updatedName = "elon"
      val updatedDF = session.createDataFrame(Seq(("Y4", 100, "c", updatedName, "b", Some(5.0), 3L),
        ("Y5", 99, "c", updatedName, "b", None, 4L)))
      esConnector.upsert(config)(updatedDF, "_1")
      assert(esConnector.getData(config, session).filter(data => (data._1 == "Y4" || data._1 == "Y5") &&
        data._2.values.toList.contains(updatedName)).count() == 2)

      esConnector.deleteData(config)(session.sparkContext.parallelize(List("Y1", "Y2", "Y3", "Y4", "Y5")), "_id")
    }
  }
}
