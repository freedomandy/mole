package org.freedomandy.mole.transform

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

/**
  * @author Andy Huang on 2018/7/12
  */
class KeyGeneratorTest extends Matchers with FunSpecLike with BeforeAndAfterAll {
  val session: SparkSession = SparkSession.builder.appName("Test").master("local[*]").getOrCreate()
  val df: DataFrame = session.createDataFrame(
    Seq(
      ("Y1", 100, "c", "james", "b", Some(0.2), 1L),
      ("Y2", 100, "c", "james", "b", None, 1L),
      ("Y3", 100, "b", "james", "b", None, 2L),
      ("Y4", 100, "c", "Andy", "b", Some(5.0), 3L),
      ("Y5", 99, "c", "james", "b", None, 4L)
    )
  )

  override def afterAll(): Unit = {}

  describe("Test for KeyGenerator") {
    it("Add key") {
      val configString1 =
        """
          |{
          |  action = "KeyGenerating"
          |  keyFields: ["_1","_3"]
          |  keyName = "ID"
          |}
        """.stripMargin
      val configString2 =
        """
          |{
          |  action = "KeyGenerating"
          |  keyFields = []
          |  keyName = "ID"
          |}
        """.stripMargin

      val configString3 =
        """
          |{
          |  action = "KeyGenerating"
          |  keyName = "ID"
          |}
        """.stripMargin

      val result1 = KeyGenerator.transform(ConfigFactory.parseString(configString1))(df)
      val result2 = KeyGenerator.transform(ConfigFactory.parseString(configString2))(df)
      val result3 = KeyGenerator.transform(ConfigFactory.parseString(configString3))(df)

      assert(result1.columns.contains("ID"))
      assert(result1.columns.length == 8)
      assert(result2.columns.contains("ID"))
      assert(result2.columns.length == 8)
      assert(result3.columns.contains("ID"))
      assert(result3.columns.length == 8)

      import org.apache.spark.sql.functions.col

      assert(
        result2.filter(col("_1") === "Y1").first().getAs[String](result2.columns.indexOf("ID"))
          == result3.filter(col("_1") === "Y1").first().getAs[String](result3.columns.indexOf("ID"))
      )

      assert(
        result1.filter(col("_1") === "Y1").first().getAs[String](result2.columns.indexOf("ID"))
          != result2.filter(col("_1") === "Y1").first().getAs[String](result3.columns.indexOf("ID"))
      )
    }

    it("Add Nested key") {
      val session: SparkSession = SparkSession.builder.appName("Test").master("local[*]").getOrCreate()
      val df: DataFrame = session.createDataFrame(
        Seq(
          ("Y1", 100, "c", "james", "b", Some(0.2), 1L),
          ("Y2", 100, "c", "james", "b", None, 1L),
          ("Y3", 100, "b", "james", "b", None, 2L),
          ("Y4", 100, "c", "Andy", "b", Some(5.0), 3L),
          ("Y5", 99, "c", "james", "b", None, 4L)
        )
      )

      import session.implicits._
      import org.apache.spark.sql.functions._
      val nestedDF = df.withColumn("value", struct($"_2", $"_6", $"_7")).withColumn("item", map($"_3", $"_5"))

      val configString1 =
        """
          |{
          |  action = "KeyGenerating"
          |  keyFields = ["value","item"]
          |  keyName = "ID"
          |}
        """.stripMargin
      val configString2 =
        """
          |{
          |  action = "KeyGenerating"
          |  keyFields = []
          |  keyName = "ID"
          |}
        """.stripMargin

      val configString3 =
        """
          |{
          |  action = "KeyGenerating"
          |  keyName = "ID"
          |}
        """.stripMargin

      val result1 = KeyGenerator.transform(ConfigFactory.parseString(configString1))(nestedDF)
      val result2 = KeyGenerator.transform(ConfigFactory.parseString(configString2))(nestedDF)
      val result3 = KeyGenerator.transform(ConfigFactory.parseString(configString3))(nestedDF)

      assert(result1.columns.contains("ID"))
      assert(result1.columns.length == 10)
      assert(result2.columns.contains("ID"))
      assert(result2.columns.length == 10)
      assert(result3.columns.contains("ID"))
      assert(result3.columns.length == 10)

      import org.apache.spark.sql.functions.col

      assert(
        result2.filter(col("_1") === "Y1").first().getAs[String](result2.columns.indexOf("ID"))
          == result3.filter(col("_1") === "Y1").first().getAs[String](result3.columns.indexOf("ID"))
      )

      assert(
        result1.filter(col("_1") === "Y1").first().getAs[String](result2.columns.indexOf("ID"))
          != result2.filter(col("_1") === "Y1").first().getAs[String](result3.columns.indexOf("ID"))
      )
    }
  }

}
