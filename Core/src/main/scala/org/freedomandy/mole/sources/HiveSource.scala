package org.freedomandy.mole.sources

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.freedomandy.mole.commons.sources.Source

/**
  * @author Andy Huang on 2018/6/1
  */
object HiveSource extends Source {
  override def sourceName: String = "HIVE"

  override def get(session: SparkSession, config: Config): Option[DataFrame] =
    if (config.hasPath("query")) {
      val query = config.getString("query")
      Some(session.sql(query))
    } else None
}
