package org.freedomandy.mole.commons.sinks

import com.typesafe.config.Config
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
  * @author Andy Huang on 2018/6/4
  */
trait Sink {
  def sinkName: String

  def overwrite(config: Config)(dataFrame: DataFrame, keyField: String): Unit

  def upsert(config: Config)(upsertDF: DataFrame, keyField: String): Unit

  def deleteData(config: Config)(deletedSet: RDD[String], keyField: String): Unit
}
