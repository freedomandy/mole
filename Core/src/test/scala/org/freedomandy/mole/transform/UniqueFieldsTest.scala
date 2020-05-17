package org.freedomandy.mole.transform

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

/**
  * @author Andy Huang on 2018/7/25
  */
class UniqueFieldsTest extends Matchers with FunSpecLike with BeforeAndAfterAll {
  val session = SparkSession.builder.appName("Test").master("local[*]").getOrCreate()
  val df = session.createDataFrame(
    Seq(
      ("Y1", 100, "c", "james", "b", Some(0.2), 1L),
      ("Y2", 100, "c", "james", "b", None, 1L),
      ("Y3", 100, "c", "james", "b", None, 2L),
      ("Y4", 100, "c", "james", "b", Some(5.0), 3L),
      ("Y5", 99, "c", "james", "b", None, 4L)
    )
  )

  describe("Test for unique fields") {
    it("fields") {
      assert(Common.isUnique(df, "_1", "_2"))
      assert(!Common.isUnique(df, "_2", "_3"))
      assert(!Common.isUnique(df, "_2"))
    }
  }
}
