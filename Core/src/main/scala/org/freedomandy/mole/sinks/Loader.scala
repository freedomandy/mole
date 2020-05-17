package org.freedomandy.mole.sinks

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.freedomandy.mole.commons.exceptions.InvalidInputException
import org.freedomandy.mole.commons.sinks.Sink
import org.freedomandy.mole.commons.utils.PluginLoader

/**
  * @author Andy Huang on 2018/6/4
  */
case class Loader(session: SparkSession, config: Config) {
  import collection.JavaConversions._
  private val sinkList: List[Config] = config.getObjectList("mole.sink.destinations").toList.map(_.toConfig)

  def loadPlugins(): Function[Config, (DataFrame, String) => Unit] = {
    def getPartialFunction(synchronizer: Sink): PartialFunction[Config, (DataFrame, String) => Unit] = {
      case config: Config => synchronizer.overwrite(config)
    }
    def getSynchronizeFunc(
        customTransform: Option[PartialFunction[Config, (DataFrame, String) => Unit]]
    ): Function[Config, (DataFrame, String) => Unit] = {
      def getOperation(config: Config, sink: Sink): (DataFrame, String) => Unit =
        if (config.getString("operation") == "OVERWRITE")
          sink.overwrite(config)
        else if (config.getString("operation") == "UPSERT")
          sink.upsert(config)
        else
          throw new InvalidInputException("Unsupported sink operation")

      val basePF: PartialFunction[Config, (DataFrame, String) => Unit] = {
        case config: Config if config.getString("type") == EsSink.sinkName =>
          getOperation(config, EsSink)
        case config: Config if config.getString("type") == MongoSink.sinkName =>
          getOperation(config, MongoSink)
        case config: Config if config.getString("type") == HiveSink.sinkName =>
          if (config.getString("operation") == "APPEND")
            HiveSink.append(config)
          else
            getOperation(config, HiveSink)
      }

      val finalPF =
        if (customTransform.isDefined)
          basePF.orElse(customTransform.get)
        else
          basePF

      finalPF.orElse({
        case config: Config => throw new InvalidInputException(s"Unsupported sink ${config.getString("type")}")
      })
    }

    val sinkPlugins = PluginLoader.loadSinkPlugins(config).map(getPartialFunction)

    if (sinkPlugins.isEmpty)
      getSynchronizeFunc(None)
    else
      getSynchronizeFunc(Some(sinkPlugins.reduce(_ orElse _)))
  }

  def run(dataFrame: DataFrame): Unit = {
    val matchRules = loadPlugins()

    val keyField =
      if (config.hasPath("mole.sink.primaryKey"))
        config.getString("mole.sink.primaryKey")
      else "_id"

    sinkList.map(config => matchRules(config)).foreach { func =>
      try func(dataFrame, keyField)
      catch {
        case t: Throwable =>
          println(s"Failed to synchronize data, due to ${t.toString}")
      }
    }
  }
}
