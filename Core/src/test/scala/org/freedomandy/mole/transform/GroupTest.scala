package org.freedomandy.mole.transform

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.freedomandy.mole.commons.transform.FlowStage
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

/**
  * @author Andy Huang on 16/03/2018
  */
class GroupTest extends Matchers with FunSpecLike with BeforeAndAfterAll {
  val session = SparkSession.builder.appName("Test").master("local[*]").getOrCreate()
  val df = session.createDataFrame(
    Seq(
      ("Y1", 100, "c", "james", "b", Some(0.2), 1L),
      ("Y2", 100, "c", "james", "b", None, 1L),
      ("Y3", 100, "b", "james", "b", None, 2L),
      ("Y4", 100, "c", "Andy", "b", Some(5.0), 3L),
      ("Y5", 99, "c", "james", "b", None, 4L)
    )
  )

  override def afterAll(): Unit = {}

  describe("Test for Group") {
    it("aggregate") {
      assert(Group.groupBy(df, Set("_3", "_4"), "sum", Seq("_2", "_7")).count == 3)
      assert(Group.groupBy(df, Set("_3", "_4"), "count", Seq("_2", "_7")).count == 3)
      assert(Group.groupBy(df, Set("_3", "_4"), "max", Seq("_2", "_7")).count == 3)
      assert(Group.groupBy(df, Set("_3", "_4"), "min", Seq("_2", "_7")).count == 3)
      assert(Group.groupBy(df, Set("_3", "_4"), "avg", Seq("_2", "_7")).count == 3)
      assertThrows[RuntimeException](Group.groupBy(df, Set("_3", "_4"), "invalid_operator", Seq("_2", "_7")).count == 3)
    }

    it("transform") {
      val flowStage: FlowStage = Group
      val configString         = """{
                             key = "_3,_4"
                             aggregate = "sum"
                             aggregateFields = "_2,_7"
                           }"""
      val config               = ConfigFactory.parseString(configString)

      assert(flowStage.transform(config)(df).count() == 3)
    }
  }

}
