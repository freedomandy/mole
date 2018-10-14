package org.freedomandy.mole.monitor

import java.sql.Date
import org.freedomandy.mole.monitor.checkers.{CountChecker, UniqueChecker, ValueChecker}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

/**
  * @author Andy Huang on 2018/7/25
  */
class CheckerTest extends Matchers with FunSpecLike with BeforeAndAfterAll {
  val session: SparkSession =  SparkSession.builder.appName("Test").master("local[*]").getOrCreate()
  val date = new java.util.Date
  val time = new java.sql.Timestamp(date.getTime)
  val df: DataFrame = session.createDataFrame(Seq(
    ("Y1", 1000L, Date.valueOf("2018-12-01"), time, java.math.BigDecimal.valueOf(0.19999),10,1.0,true),
    ("Y2", 200L, null, time, java.math.BigDecimal.valueOf(19999.1),11,0.0030,true)))

  describe("Test for Checker") {
    it("Count") {
      val configString1 =
        """{
          |   type = "Count"
          |   number = 2
          |}""".stripMargin

      val configString2 =
        """{
          |   type = "Count"
          |   number = 3
          |}""".stripMargin

      assert(CountChecker.verify(df, ConfigFactory.parseString(configString1)).isRight)
      assert(CountChecker.verify(df, ConfigFactory.parseString(configString2)).isLeft)
    }
    it("Unique") {
      val configString1 =
        """{
          |   type = "Unique"
          |   fields: ["_1"]
          |}""".stripMargin
      val configString2 =
        """{
          |   type = "Unique"
          |   fields: ["_1", "_2"]
          |}""".stripMargin
      val configString3 =
        """{
          |   type = "Unique"
          |   fields: ["_4"]
          |}""".stripMargin
      val configString4 =
        """{
          |   type = "Unique"
          |   fields: ["_4", "_8"]
          |}""".stripMargin


      assert(UniqueChecker.verify(df, ConfigFactory.parseString(configString1)).isRight)
      assert(UniqueChecker.verify(df, ConfigFactory.parseString(configString2)).isRight)
      assert(UniqueChecker.verify(df, ConfigFactory.parseString(configString3)).isLeft)
      assert(UniqueChecker.verify(df, ConfigFactory.parseString(configString4)).isLeft)
    }
    it("Disallow value") {
      val invalidTime = new java.sql.Timestamp(Date.valueOf("1988-12-01").getTime)

      val configString1 =
        """{
          | type = "DisallowValues"
          | values: ["Y6"]
          | column = "_1"
          |}
        """.stripMargin
      val configString2 =
        """{
          | type = "DisallowValues"
          | values: ["Y1"]
          | column = "_1"
          |}
        """.stripMargin
      val configString3 =
        """{
          | type = "DisallowValues"
          | values: ["2000"]
          | column = "_2"
          |}
        """.stripMargin
      val configString4 =
        """{
          | type = "DisallowValues"
          | values: ["1000"]
          | column = "_2"
          |}
        """.stripMargin
      val configString5 =
        """{
          | type = "DisallowValues"
          | values: ["2018-12-02"]
          | column = "_3"
          |}
        """.stripMargin
      val configString6 =
        """{
          | type = "DisallowValues"
          | values: ["2018-12-01"]
          | column = "_3"
          |}
        """.stripMargin
      val configString7 =
        s"""{
           | type = "DisallowValues"
           | values: ["${invalidTime.toString}"]
           | column = "_4"
           |}
        """.stripMargin
      val configString8 =
        s"""{
           | type = "DisallowValues"
           | values: ["${time.toString}"]
           | column = "_4"
           |}
        """.stripMargin
      val configString9 =
        s"""{
           | type = "DisallowValues"
           | values: ["0"]
           | column = "_5"
           |}
        """.stripMargin
      val configString10 =
        s"""{
           | type = "DisallowValues"
           | values: ["19999.100000000000000000"]
           | column = "_5"
           |}
        """.stripMargin
      val configString11 =
        s"""{
           | type = "DisallowValues"
           | values: ["2000"]
           | column = "_6"
           |}
        """.stripMargin
      val configString12 =
        s"""{
           | type = "DisallowValues"
           | values: ["10"]
           | column = "_6"
           |}
        """.stripMargin
      val configString13 =
        s"""{
           | type = "DisallowValues"
           | values: ["2000"]
           | column = "_7"
           |}
        """.stripMargin
      val configString14 =
        s"""{
           | type = "DisallowValues"
           | values: ["0.003"]
           | column = "_7"
           |}
        """.stripMargin
      val configString15 =
        s"""{
           | type = "DisallowValues"
           | values: [false]
           | column = "_8"
           |}
        """.stripMargin
      val configString16 =
        s"""{
           | type = "DisallowValues"
           | values: [true]
           | column = "_8"
           |}
        """.stripMargin


      assert(ValueChecker.verify(df, ConfigFactory.parseString(configString1)).isRight)
      assert(ValueChecker.verify(df, ConfigFactory.parseString(configString2)).isLeft)
      assert(ValueChecker.verify(df, ConfigFactory.parseString(configString3)).isRight)
      assert(ValueChecker.verify(df, ConfigFactory.parseString(configString4)).isLeft)
      assert(ValueChecker.verify(df, ConfigFactory.parseString(configString5)).isRight)
      assert(ValueChecker.verify(df, ConfigFactory.parseString(configString6)).isLeft)
      assert(ValueChecker.verify(df, ConfigFactory.parseString(configString7)).isRight)
      assert(ValueChecker.verify(df, ConfigFactory.parseString(configString8)).isLeft)
      assert(ValueChecker.verify(df, ConfigFactory.parseString(configString9)).isRight)
      assert(ValueChecker.verify(df, ConfigFactory.parseString(configString10)).isLeft)
      assert(ValueChecker.verify(df, ConfigFactory.parseString(configString11)).isRight)
      assert(ValueChecker.verify(df, ConfigFactory.parseString(configString12)).isLeft)
      assert(ValueChecker.verify(df, ConfigFactory.parseString(configString13)).isRight)
      assert(ValueChecker.verify(df, ConfigFactory.parseString(configString14)).isLeft)
      assert(ValueChecker.verify(df, ConfigFactory.parseString(configString15)).isRight)
      assert(ValueChecker.verify(df, ConfigFactory.parseString(configString16)).isLeft)
    }
  }
}
