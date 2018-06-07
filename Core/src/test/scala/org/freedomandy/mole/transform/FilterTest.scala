package org.freedomandy.mole.transform

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.freedomandy.mole.commons.transform.FlowStage
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

/**
  * @author Andy Huang on 16/03/2018
  */
class FilterTest extends Matchers with FunSpecLike with BeforeAndAfterAll {
  val session =  SparkSession.builder.appName("Test").master("local[*]").getOrCreate()
  val df = session.createDataFrame(Seq(("Y1", 100, "c", "james", "b", Some(0.2), 1L),
    ("Y2", 100, "c", "james", "b", None, 1L),
    ("Y3", 100, "c", "james", "b", None, 2L),
    ("Y4", 100, "c", "james", "b", Some(5.0), 3L),
    ("Y5", 99, "c", "james", "b", None, 4L)))

  override def afterAll(): Unit = {
  }

  describe("Test for Filter") {
    it("constant value") {
      assert(Filter.filter(df, "_2", "==", "100").count() == 4)
      assert(Filter.filter(df, "_2", "==", "$_2").count() == 5)
      assert(Filter.filter(df, "_2", ">", 100).count() == 0)
      assert(Filter.filter(df, "_2", ">=", 100).count() == 4)
      assert(Filter.filter(df, "_2", "<", 100).count() == 1)
      assert(Filter.filter(df, "_2", "<=", 100).count() == 5)
      assert(Filter.filter(df, "_2", "!=", 100).count() == 1)
    }
    it("regex") {
      assert(Filter.filter(df, "_1", "Y.*").count() == 5)
    }
    it ("transform") {
      val flowStage: FlowStage = Filter
      val config = ConfigFactory.parseString("{field=\"_2\",operator=\"==\",value=\"100\"}")

      assert(flowStage.transform(config)(df).count() == 4)
    }
  }
}
