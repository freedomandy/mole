package org.freedomandy.mole.commons.sources

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author Andy Huang on 2018/6/1
  */
trait Source {
  def sourceName: String

  def get(session: SparkSession, config: Config): Option[DataFrame]
}
