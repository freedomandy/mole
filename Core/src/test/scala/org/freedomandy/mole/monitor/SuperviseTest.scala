package org.freedomandy.mole.monitor

import com.typesafe.config.ConfigFactory
import java.sql.Date
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.freedomandy.mole.Module
import org.freedomandy.mole.commons.exceptions.BaseException
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

/**
  * @author Andy Huang on 2018/8/7
  */
class SuperviseTest extends Matchers with FunSpecLike with BeforeAndAfterAll {
  val session: SparkSession =  SparkSession.builder.appName("Test").master("local[*]").getOrCreate()
  val date = new java.util.Date
  val time = new java.sql.Timestamp(date.getTime)
  val df: DataFrame = session.createDataFrame(Seq(
    ("Y1", 1000L, Date.valueOf("2018-12-01"), time, java.math.BigDecimal.valueOf(0.19999),10,1.0,true),
    ("Y2", 200L, null, time, java.math.BigDecimal.valueOf(19999.1),11,0.0030,true)))

  describe("Test for Supervisor") {
    it("supervise") {
      val configString1 =
        """
          |checkStatements: [
          |{
          |   type = "Unique"
          |   fields: ["_1"]
          |   errorHandler {
          |     type = "HttpPublish"
          |     payLoad = "{\"status\":\"failed\", \"err\": \"_8 is not Unique field\", \"constraint\":\"Unique Field\"}"
          |     headers: [
          |       {
          |         key = "Content-Type"
          |         value =  "application/json"
          |       }
          |     ]
          |     httpMethod = "PUT"
          |     url = "http://52.163.212.183:9200/mole-jobs-test/job/3"
          |   }
          |}
          |]
        """.stripMargin
      val configString2 =
        """
          |checkStatements: [
          |{
          |   type = "Unique"
          |   fields: ["_8"]
          |   errorHandler {
          |     type = "HttpPublish"
          |     payLoad = "{\"status\":\"failed\", \"err\": \"_8 is not Unique field\", \"constraint\":\"Unique Field\"}"
          |     headers: [
          |       {
          |         key = "Content-Type"
          |         value =  "application/json"
          |       }
          |     ]
          |     httpMethod = "PUT"
          |     url = "http://52.163.212.183:9200/mole-jobs-test/job/3"
          |   }
          |}
          |]
        """.stripMargin
      val configString3 =
        """
          |checkStatements: [
          |{
          |   type = "Unique"
          |   fields: ["_8"]
          |   errorHandler {
          |     type = "FakeHandler"
          |     payLoad = "{\"status\":\"failed\", \"err\": \"_8 is not Unique field\", \"constraint\":\"Unique Field\"}"
          |     headers: [
          |       {
          |         key = "Content-Type"
          |         value =  "application/json"
          |       }
          |     ]
          |     httpMethod = "PUT"
          |     url = "http://52.163.212.183:9200/mole-jobs-test/job/3"
          |   }
          |}
          |]
        """.stripMargin

      val config1 = ConfigFactory.parseString(configString1)
      val config2 = ConfigFactory.parseString(configString2)
      val config3 = ConfigFactory.parseString(configString3)
      val module = Module.apply(session, ConfigFactory.load("job.conf"))

      assert(module.supervisor.supervise(df, config1).isDefined)
      assert(module.supervisor.supervise(df, config2).isEmpty)
      assertThrows[BaseException](module.supervisor.supervise(df, config3).isEmpty)
    }
  }
}
