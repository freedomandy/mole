package org.freedomandy.mole.postworks

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.freedomandy.mole.commons.exceptions.InvalidInputException
import org.freedomandy.mole.commons.postworks.PostWorker
import org.freedomandy.mole.commons.utils.PluginLoader

/**
  * @author Andy Huang on 2018/9/12
  */
case class PostWorkHandler(session: SparkSession, config: Config) {
  private type PostWorkFunc = (DataFrame, Long) => Option[Throwable]

  def loadPlugins(): Function[Config, PostWorkFunc] = {
    def getPartialFunction(postWorker: PostWorker): PartialFunction[Config, PostWorkFunc] = {
      case config: Config if config.getString("type") == postWorker.workName =>
        (dataFrame: DataFrame, startTime: Long) => postWorker.execute(session, dataFrame, startTime, config)
    }

    def getWorkerFunction(customWorker: Option[PartialFunction[Config, PostWorkFunc]]): Function[Config, PostWorkFunc] = {
      val basePF: PartialFunction[Config, PostWorkFunc] = {
        case c: Config if c.getString("type") == EsRegistry.workName =>
          (dataFrame: DataFrame, startTime: Long) => EsRegistry.execute(session, dataFrame, startTime, config)
      }
      val finalPF =
        if (customWorker.isDefined)
          basePF orElse customWorker.get
        else
          basePF

      finalPF.orElse({
        case config: Config => throw new InvalidInputException(s"Unsupported post worker type: ${config.getString("type")}")
      })
    }

    // Load plugins by parsing plugin paths
    val pluginTransforms = PluginLoader.loadPostWorkPlugins(config).map(getPartialFunction)

    if (pluginTransforms.isEmpty)
      getWorkerFunction(None)
    else
      getWorkerFunction(Some(pluginTransforms.reduce(_ orElse _)))
  }

  def run(dataFrame: DataFrame, startTime: Long): Option[Throwable] = {
    println("Post work start...")

    if (config.isEmpty)
      Some(new InvalidInputException("Post work config should be provided"))
    else {
      val postWorkFunc = loadPlugins()
      val result = postWorkFunc(config.getConfig("mole.postWork"))(dataFrame, startTime)

      result
    }
  }
}
