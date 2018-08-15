package org.freedomandy.mole.sources

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.freedomandy.mole.commons.exceptions.InvalidInputException
import org.freedomandy.mole.commons.sources.Source
import org.freedomandy.mole.commons.utils.PluginLoader

/**
  * @author Andy Huang on 2018/6/3
  */
case class Extractor(session: SparkSession, config: Config) {
  def loadPlugins(): Function[String,  Option[DataFrame]] = {
    def getPluginSource(sourceConfig: Config)(sourceBehaviors: Source): PartialFunction[String,  Option[DataFrame]] = {
      case s: String if s == sourceBehaviors.sourceName => sourceBehaviors.get(session, sourceConfig)
    }
    def getSourceOptions(sourceConfig: Config)(customSource: Option[PartialFunction[String,  Option[DataFrame]]]): Function[String,  Option[DataFrame]] = {
      val basePF: PartialFunction[String, Option[DataFrame]] = {
        case s: String if s == HiveSource.sourceName =>
          HiveSource.get(session, sourceConfig)
        case s: String if s == MongoSource.sourceName =>
          MongoSource.get(session, sourceConfig)
      }
      val finalPF =
        if (customSource.isDefined)
          basePF orElse customSource.get
        else
          basePF

      finalPF.orElse({
        case s: String => throw new InvalidInputException(s"Unsupported source type: $s")
      })
    }

    val sourceConfig = config.getConfig("mole.source")
    val plugins = PluginLoader.loadSourcePlugins(config)

    if (plugins.isEmpty) {
      getSourceOptions(sourceConfig)(None)
    } else {
      val pluginSources = plugins.map(getPluginSource(sourceConfig)).reduce(_ orElse _)

      getSourceOptions(sourceConfig)(Some(pluginSources))
    }
  }

  def run(): Option[DataFrame] = {
    val sourceFunction = loadPlugins()

    if (config.hasPath("mole.source"))
      sourceFunction(config.getString("mole.source.type"))
    else
      throw new InvalidInputException("Can't get any source info from config file")
  }
}
