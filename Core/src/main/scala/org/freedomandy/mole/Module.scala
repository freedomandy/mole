package org.freedomandy.mole

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.freedomandy.mole.monitor.Supervisor
import org.freedomandy.mole.sinks.Loader
import org.freedomandy.mole.sources.Extractor
import org.freedomandy.mole.transform.Transformer

/**
  * @author Andy Huang on 2018/8/21
  */
class Module private (session: SparkSession, config: Config) {
  def getSession: SparkSession = session
  def getConfig: Config        = config

  val extractor   = Extractor(session, config)
  val transformer = Transformer(session, config)
  val loader      = Loader(session, config)
  val supervisor  = Supervisor(config)
}

object Module {
  private val config: Config             = ConfigFactory.load()
  private lazy val session: SparkSession = SparkSession.builder.enableHiveSupport().appName("MOLE Job").getOrCreate()
  private lazy val module                = new Module(session, config)

  def createIfNotExist(): Module = module

  def apply(session: SparkSession, config: Config): Module = {
    println("Create Module...")
    new Module(session, config)
  }
}
