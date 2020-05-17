package org.freedomandy.mole.monitor

import java.sql.Date

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.freedomandy.mole.Module
import org.freedomandy.mole.commons.exceptions.{BaseException, UnsupportedException}
import org.freedomandy.mole.commons.utils.ApacheHttpClient
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

/**
  * @author Andy Huang on 2018/7/26
  */
class MonitorTest extends Matchers with FunSpecLike with BeforeAndAfterAll {
  val session: SparkSession = SparkSession.builder.appName("Test").master("local[*]").getOrCreate()
  val date                  = new java.util.Date
  val time                  = new java.sql.Timestamp(date.getTime)
  val df: DataFrame = session.createDataFrame(
    Seq(
      ("Y1", 1000L, Date.valueOf("2018-12-01"), time, java.math.BigDecimal.valueOf(0.19999), 10, 1.0, true),
      ("Y2", 200L, null, time, java.math.BigDecimal.valueOf(19999.1), 11, 0.0030, true)
    )
  )
  val jobConfig: Config = ConfigFactory.load("job.conf")
  val httpClient        = new ApacheHttpClient

  override protected def beforeAll(): Unit = super.beforeAll()
  override protected def afterAll(): Unit  =
    // Drop index
    //httpClient.delete("http://52.163.212.183:9200/mole-exception-test", Map(), "")
    super.afterAll()

  describe("Integration Test") {
    ignore("post work") {
      val monitorConfig = ConfigFactory.load("jobMonitor.conf")
      val result        = Monitor(Module.apply(session, jobConfig)).handle(monitorConfig.getConfig("mole.monitor"))

      // TODO: Verify post work
      assert(result.isDefined)
      assert(result.get.schema.fieldNames.contains("mole_id"))
    }

    ignore("exception handling") {
      import scala.collection.JavaConversions._

      def getConfigString(exceptionConfigs: String): String =
        s"""
           |Extract: {
           |  preStart: {
           |    checkStatements: []
           |  }
           |}
           |Transform: {
           |  preStart: {
           |    checkStatements: []
           |  }
           |}
           |Load: {
           |  preStart: {
           |    checkStatements: []
           |  }
           |}
           |$exceptionConfigs
        """.stripMargin

      val baseExConfig =
        """
          |ExceptionHandling: [
          |  {
          |     condition = "com.cathay.mole.commons.exceptions.UnsupportedException"
          |     errorHandler {
          |       type = "HttpPublish"
          |       payLoad = "{\"status\":\"skipped\", \"err\": \"@msg\"}"
          |       httpMethod = "PUT"
          |       url = "http://52.163.212.183:9200/mole-exception-test/job/2"
          |     }
          |  },
          |  {
          |     condition = "com.cathay.mole.commons.exceptions.BaseException"
          |     errorHandler {
          |       type = "HttpPublish"
          |       payLoad = "{\"status\":\"skipped\", \"err\": \"@msg\"}"
          |       httpMethod = "PUT"
          |       url = "http://52.163.212.183:9200/mole-exception-test/job/1"
          |     }
          |  }
          |]
        """.stripMargin
      val unSupExConfig =
        """
          |ExceptionHandling: [
          |  {
          |     condition = "com.cathay.mole.commons.exceptions.UnsupportedException"
          |     errorHandler {
          |       type = "HttpPublish"
          |       payLoad = "{\"status\":\"skipped\", \"err\": \"@msg\"}"
          |       httpMethod = "PUT"
          |       url = "http://52.163.212.183:9200/mole-exception-test/job/2"
          |     }
          |  },
          |  {
          |     condition = "com.cathay.mole.commons.exceptions.BaseException"
          |     errorHandler {
          |       type = "HttpPublish"
          |       payLoad = "{\"status\":\"skipped\", \"err\": \"@msg\"}"
          |       httpMethod = "PUT"
          |       url = "http://52.163.212.183:9200/mole-exception-test/job/2"
          |     }
          |  }
          |]
        """.stripMargin
      val monitorConfig1 = ConfigFactory.parseString(getConfigString(baseExConfig))
      val monitorConfig2 = ConfigFactory.parseString(getConfigString(unSupExConfig))
      val baseExceptionMonitorResult = Monitor(Module.apply(session, jobConfig)).executeWithMonitor(
        df,
        monitorConfig1.getConfig("Transform.preStart"),
        monitorConfig1.getConfigList("ExceptionHandling").toList,
        (df: DataFrame) => {
          throw BaseException("Mock exception")
          df
        }
      )
      val UnSupExceptionMonitorResult = Monitor(Module.apply(session, jobConfig)).executeWithMonitor(
        df,
        monitorConfig2.getConfig("Transform.preStart"),
        monitorConfig2.getConfigList("ExceptionHandling").toList,
        (df: DataFrame) => {
          throw new UnsupportedException("Unsupported transform operation")
          df
        }
      )

      assert(baseExceptionMonitorResult.isEmpty)
      assert(UnSupExceptionMonitorResult.isEmpty)
      assert(httpClient.get("http://52.163.212.183:9200/mole-exception-test/job/1", Map()).contains("\"found\":true"))
      assert(httpClient.get("http://52.163.212.183:9200/mole-exception-test/job/2", Map()).contains("\"found\":true"))
    }
  }
}
