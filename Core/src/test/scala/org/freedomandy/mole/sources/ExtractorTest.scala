package org.freedomandy.mole.sources

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

/**
  * @author Andy Huang on 2018/6/3
  */
class ExtractorTest extends Matchers with FunSpecLike with BeforeAndAfterAll {
  describe("Test for Extractor") {
    ignore("run") {
      val session = SparkSession.builder.appName("MOLE Job").master("local[*]").getOrCreate()
      val configString =
        """
          |mole {
          |  source {
          |    type = "MONGODB"
          |    path = "mongodb://127.0.0.1:27017/test.test"
          |    key = "_id"
          |  }
          |}
        """.stripMargin
      val config = ConfigFactory.parseString(configString)
      val extractor = Extractor(session, config)

      assert(extractor.run().get.count() == 1)
    }
  }
}
