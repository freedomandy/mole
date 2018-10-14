package org.freedomandy.mole.transform

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

/**
  * @author Andy Huang on 2018/5/31
  */
class TransformerTest extends Matchers with FunSpecLike with BeforeAndAfterAll {
  val session: SparkSession =  SparkSession.builder.appName("Test").master("local[*]").getOrCreate()
  val df: DataFrame = session.createDataFrame(Seq(("Y1", 100, "c", "james", "b", Some(0.2), 1L),
    ("Y2", 100, "c", "james", "b", None, 1L),
    ("Y3", 100, "b", "james", "b", None, 2L),
    ("Y4", 100, "c", "Andy", "b", Some(5.0), 3L),
    ("Y5", 99, "c", "james", "b", None, 4L)))

  override def afterAll(): Unit = {
  }

  describe("Test for Transformer") {
    it("transform") {
      val configString =
        """
          |plugins {
          |  transform = ["org.freedomandy.mole.testplugin.transform.TestStage"]
          |}
          |mole {
          |  source {
          |    type = "HIVE"
          |    query = "SELECT * FROM whatever"
          |  }
          |  sink: [
          |   {
          |     anySink = "fakepath"
          |   }
          |  ]
          |  transform {
          |    flow: [
          |      {
          |        action = "FieldMapping"
          |        from = "_1,_2,_3,_4,_5,_6,_7"
          |        to = "id,quantity,group,name,type,credit,amount"
          |      },
          |      {
          |        action = "Filtering"
          |        field = "name"
          |        value = "james"
          |        operator = "=="
          |      },
          |      {
          |        action = "Custom"
          |        input = "quantity,amount"
          |        output = "result"
          |        outputType = "Long"
          |        function = "(s: Seq[Any]) => { s.head.asInstanceOf[Int] * s(1).asInstanceOf[Long] }"
          |      },
          |      {
          |        action= "TEST-ONLY"
          |        field = "result"
          |        operator = "sum"
          |      }
          |    ]
          |  }
          |}
        """.stripMargin
      val config = ConfigFactory.parseString(configString)
      val transformer: Transformer = Transformer(session, config)

      val result = transformer.transform(df)

      assert(result.first.get(0) == 796)
    }
    it("transform no available plugin") {
      val configString =
        """
          |mole {
          |  source {
          |    query = "SELECT * FROM whatever"
          |    key = "anykey"
          |  }
          |  sink {
          |    anySink = "fakepath"
          |  }
          |  transform {
          |    flow: [
          |      {
          |        action = "FieldMapping"
          |        from = "_1,_2,_3,_4,_5,_6,_7"
          |        to = "id,quantity,group,name,type,credit,amount"
          |      },
          |      {
          |        action = "Filtering"
          |        field = "name"
          |        value = "james"
          |        operator = "=="
          |      },
          |      {
          |        action = "Custom"
          |        input = "quantity,amount"
          |        output = "result"
          |        outputType = "Long"
          |        function = "(s: Seq[Any]) => { s.head.asInstanceOf[Int] * s(1).asInstanceOf[Long] }"
          |      }
          |    ]
          |  }
          |}
        """.stripMargin
      val config = ConfigFactory.parseString(configString)
      val transformer: Transformer = Transformer(session, config)
      val result = transformer.transform(df)

      assert(result.count() == 4)
    }
  }
}
