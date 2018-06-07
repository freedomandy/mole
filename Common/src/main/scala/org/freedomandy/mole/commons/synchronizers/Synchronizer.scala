package org.freedomandy.mole.commons.synchronizers

import com.typesafe.config.Config
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
  * @author Andy Huang on 2018/6/4
  */
trait Synchronizer {
  def sinkName: String

  def sync(config: Config)(dataFrame: DataFrame, keyField: String): Unit

  def deleteData(config: Config)(deletedSet: RDD[String], keyField: String): Unit

  def upsertData(config: Config)(upsertDF: DataFrame, keyField: String): Unit
}
