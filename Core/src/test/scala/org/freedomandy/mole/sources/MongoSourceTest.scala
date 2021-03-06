package org.freedomandy.mole.sources

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

/**
  * @author Andy Huang on 2018/6/1
  */
class MongoSourceTest extends Matchers with FunSpecLike with BeforeAndAfterAll {
  describe("Test for Mongo Source") {
    ignore("get") {
      val session      = SparkSession.builder.appName("MOLE Job").master("local[*]").getOrCreate()
      val configString = """mole {
                           |  source {
                           |    type = "MONGODB"
                           |    path = "mongodb://127.0.0.1:27017/test.test"
                           |  }
                           |}""".stripMargin
      val config       = ConfigFactory.parseString(configString)
      val source       = MongoSource.get(session, config.getConfig("mole.source"))

      assert(source.get.count() == 1)
    }
  }
}
