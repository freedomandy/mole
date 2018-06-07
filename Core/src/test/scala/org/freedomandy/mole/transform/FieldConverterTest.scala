package org.freedomandy.mole.transform

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.freedomandy.mole.commons.transform.FlowStage
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

/**
  * @author Andy Huang on 2018/5/29
  */
class FieldConverterTest extends Matchers with FunSpecLike with BeforeAndAfterAll {
  val session: SparkSession =  SparkSession.builder.appName("Test").master("local[*]").getOrCreate()
  val df: DataFrame = session.createDataFrame(Seq(("Y1", 100, "c", "james", "b", Some(0.2), 1L),
    ("Y2", 100, "c", "james", "b", None, 1L),
    ("Y3", 100, "c", "james", "b", None, 2L),
    ("Y4", 100, "c", "james", "b", Some(5.0), 3L),
    ("Y5", 99, "c", "james", "b", None, 4L)))

  override def afterAll(): Unit = {}

  describe("Test for Field Converter") {
    it("rename") {
      val schema = FieldConverter.convert(df, List(("_1", "id"), ("_2", "category"), ("_3", "group"), ("_4", "name"),
        ("_5", "type"), ("_6", "credit"), ("_7", "amount"))).schema

      assert(schema.lengthCompare(df.schema.size) == 0)
      assert(schema.fieldNames.apply(3) == "name")
    }

    it("drop fields") {
      val schema = FieldConverter.convert(df, List(("_1", "id"), ("_2", "category"), ("_3", "group"), ("_4", "name"),
        ("_5", "type"), ("_6", "credit"))).schema

      assert(schema.lengthCompare(df.schema.size) == -1)
      assert(schema.fieldNames.apply(3) == "name")
    }

    it ("transform") {
      val flowStage: FlowStage = FieldConverter
      val configString = """{
                              from = "_1,_2,_3,_4,_5,_6,_7"
                              to = "id,category,group,name,type,credit,amount"
                            }"""
      val config = ConfigFactory.parseString(configString)
      val resultSchema = flowStage.transform(config)(df).schema

      assert(resultSchema.lengthCompare(df.schema.size) == 0)
      assert(resultSchema.fieldNames.apply(3) == "name")
    }
  }

}
