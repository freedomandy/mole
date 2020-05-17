package org.freedomandy.mole.sources

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.freedomandy.mole.commons.sources.Source

/**
  * @author Andy Huang on 2018/6/26
  */
object JDBCSource extends Source {
  override def sourceName: String = "JDBC"

  override def get(session: SparkSession, config: Config): Option[DataFrame] = {
    val dbType   = config.getString("dbType")
    val uri      = config.getString("uri")
    val database = config.getString("database")
    val user     = config.getString("user")
    val password = config.getString("password")
    val query    = s"(${config.getString("query")}) QueryTable"
    val (driverString, connectionString) =
      dbType match {
        case "H2" =>
          Class.forName("org.h2.Driver")
          val driver     = "org.h2.Driver"
          val connection = s"jdbc:h2:tcp://$uri/~/$database;USER=$user;PASSWORD=$password"

          (driver, connection)
        case "Oracle" =>
          Class.forName("oracle.jdbc.driver.OracleDriver")
          val driver     = "oracle.jdbc.driver.OracleDriver"
          val connection = s"jdbc:oracle:thin:$user/$password@$uri:$database"

          (driver, connection)
        case "TeraData" =>
          Class.forName("com.teradata.jdbc.TeraDriver")
          val driver = "com.teradata.jdbc.TeraDriver"
          val connection =
            s"jdbc:teradata://$uri/database=$database, TMODE=ANSI, CHARSET=UTF8, COLUMN_NAME=ON, MAYBENULL=ON, user=$user, password=$password"

          (driver, connection)
      }

    try {
      val session = SparkSession.builder.appName("MOLE Job").getOrCreate()

      Some(
        session.read
          .format("jdbc")
          .option("driver", driverString)
          .option("url", connectionString)
          .option("dbtable", query)
          .load()
      )
    } catch {
      case e: Exception =>
        println(s"Failed to get dataframe from RDB: ${e.toString}")
        None
    }
  }
}
