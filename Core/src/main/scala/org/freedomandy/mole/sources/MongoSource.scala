package org.freedomandy.mole.sources

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.freedomandy.mole.commons.sources.SourceBehaviors

/**
  * @author Andy Huang on 2018/6/1
  */
object MongoSource extends SourceBehaviors {
  override def sourceName: String = "MONGODB"

  override def get(session: SparkSession, config: Config): Option[DataFrame] = {
    if (config.hasPath("path")) {
      val destination = config.getString("path")
      val readConfig = ReadConfig(Map("uri" -> destination, "readPreference.name" -> "secondaryPreferred"))

      Some(MongoSpark.load(session, readConfig))
    } else None
  }
}
