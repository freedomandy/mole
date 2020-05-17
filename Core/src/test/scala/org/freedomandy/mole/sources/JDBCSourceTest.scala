package org.freedomandy.mole.sources

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

/**
  * @author Andy Huang on 2018/6/26
  */
class JDBCSourceTest extends Matchers with FunSpecLike with BeforeAndAfterAll {
  describe("Test for JDBC synchronizer") {
    ignore("H2") {
      val configString =
        """
          |{
          |  type = "JDBC"
          |  dbType = "H2"
          |  uri = "localhost:53862"
          |  database = "freedomandy"
          |  user = "freedomandy"
          |  password = "freedomandy"
          |  query = "SELECT name,T.amount, T.id FROM ACCOUNT A, TRANSACTION T WHERE A.id = T.customerId"
          |}
        """.stripMargin
      val config  = ConfigFactory.parseString(configString)
      val session = SparkSession.builder.appName("Test").master("local[*]").getOrCreate()
      val df      = JDBCSource.get(session, config).get

      assert(df.count() == 2)
    }
    ignore("Oracle") {
      val configString =
        """
          |{
          |  type = "JDBC"
          |  dbType = "Oracle"
          |  uri = "localhost:1521"
          |  database = "xe"
          |  user = "system"
          |  password = "oracle"
          |  query = "SELECT name,T.amount, T.id FROM ACCOUNT A, TRANSACTION T WHERE A.id = T.customerId"
          |}
        """.stripMargin
      val config  = ConfigFactory.parseString(configString)
      val session = SparkSession.builder.appName("Test").master("local[*]").getOrCreate()
      val df      = JDBCSource.get(session, config).get

      println(df.schema)

      assert(df.count() == 2)
    }
  }
}
