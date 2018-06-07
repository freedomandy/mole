package org.freedomandy.mole.transform

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.freedomandy.mole.commons.transform.FlowStage
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

/**
  * @author Andy Huang on 09/04/2018
  */
class UDFGeneratorTest extends Matchers with FunSpecLike with BeforeAndAfterAll {
  val session =  SparkSession.builder.appName("Test").master("local[*]").getOrCreate()
  val df = session.createDataFrame(Seq(("Y1", 100, "c", "james", "b", Some(0.2), 1L),
    ("Y2", 100, "c", "james", "b", None, 1L),
    ("Y3", 100, "b", "james", "b", None, 2L),
    ("Y4", 100, "c", "Andy", "b", Some(5.0), 3L),
    ("Y5", 99, "c", "james", "b", None, 4L)))

  describe("Test for customized UDF generator ") {
    it("Add column by giving a function literal") {
      val session = SparkSession.builder.appName("Test").master("local[*]").getOrCreate()
      val functionalLiteral =
        """
          (args: Seq[Any]) => {
            args(0).asInstanceOf[Int] * 100 + args(1).asInstanceOf[Long]
          }
          """

      val result = CustomUDFGenerator.addCustomValue(session, df, Set("_2","_7"), "result", "Long", functionalLiteral)

      assert(result.select("result").collect().map(_.getAs[Long](0)).deep == Array(10001L, 10001L, 10002L, 10003L, 9904L).deep)
    }

    it("transform") {
      val flowStage: FlowStage = CustomUDFGenerator
      val functionalLiteral =
        "(args: Seq[Any]) => { args(0).asInstanceOf[Int] * 100 + args(1).asInstanceOf[Long] }"
      val configString =
        s"""
          input = "_2,_7"
          output = "result"
          outputType = "Long"
          function = "$functionalLiteral"
        """
      val config = ConfigFactory.parseString(configString)

      assert(flowStage.transform(config)(df).select("result").collect().map(_.getAs[Long](0)).deep == Array(10001L, 10001L, 10002L, 10003L, 9904L).deep)
    }
  }

}
