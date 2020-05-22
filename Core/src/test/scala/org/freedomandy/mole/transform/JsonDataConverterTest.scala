package org.freedomandy.mole.transform

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

class JsonDataConverterTest extends Matchers with FunSpecLike with BeforeAndAfterAll {
  val session = SparkSession.builder.appName("Test").master("local[*]").getOrCreate()
  val df = session.createDataFrame(
    Seq(
      (1, 100, """{ "bookingId": 1, "currency": "USD", "amount": 90.2 }"""),
      (2, 100, """{ "bookingId": 2, "currency": "USD", "amount": 122 }"""),
      (3, 100, """{ "bookingId": 3 }""")
    )
  )

  override def afterAll(): Unit = {}

  describe("Test for Json Converter") {
    val expectedSchema = new StructType().add(
      "result",
      new StructType()
        .add("amount", DoubleType)
        .add("bookingId", LongType)
        .add("currency", StringType)
    )

    it("json string to nested object") {
      val result = JsonDataConverter.getNestedDataFromJsonString(df, "_3", "result").select("result")

      assert(
        result.select("result.amount").where(col("result.bookingId") === 1).first().getAs[Double]("amount") == 90.2 &&
          result.schema == expectedSchema
      )
    }
    it("transform") {
      val configString =
        """
          |{
          | action = "JsonStrToNestedObj"
          | column = "_3"
          | output = "result"
          |}
        """.stripMargin

      val result = JsonDataConverter.transform(ConfigFactory.parseString(configString))(df).select("result")
      assert(
        result.select("result.amount").where(col("result.bookingId") === 1).first().getAs[Double]("amount") == 90.2 &&
          result.schema == expectedSchema
      )
    }
  }
}
