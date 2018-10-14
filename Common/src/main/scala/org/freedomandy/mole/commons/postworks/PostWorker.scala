package org.freedomandy.mole.commons.postworks

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author Andy Huang on 2018/9/11
  */
trait PostWorker {
  def workName: String

  def execute(session: SparkSession, dataFrame: DataFrame, startTime: Long, jobConfig: Config): Option[Throwable]
}