package org.freedomandy.mole.transform

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.freedomandy.mole.commons.exceptions.BaseException
import org.freedomandy.mole.commons.transform.FlowStage
import org.freedomandy.mole.commons.utils.PluginLoader

/**
  * @author Andy Huang on 05/02/2018
  */
case class Transformer(session: SparkSession, config: Config) {
  import collection.JavaConversions._

  val actionList: List[Config] = config.getObjectList("mole.transform.flow").toList.map(_.toConfig)

  def loadPlugins(): Function[Config, DataFrame => DataFrame] = {
    def getPartialFunction(flowStage: FlowStage): PartialFunction[Config, DataFrame => DataFrame] = {
      case config: Config if config.getString("action") == flowStage.actionName =>
        flowStage.transform(config)
    }
    def getTransformFunc(customTransform: Option[PartialFunction[Config, DataFrame => DataFrame]]): Function[Config, DataFrame => DataFrame] = {
      val basePF: PartialFunction[Config, DataFrame => DataFrame] = {
        case config: Config if config.getString("action") == Filler.actionName =>
          Filler.transform(config)
        case config: Config if config.getString("action") == FieldConverter.actionName =>
          FieldConverter.transform(config)
        case config: Config if config.getString("action") == Filter.actionName =>
          Filter.transform(config)
        case config: Config if config.getString("action") == Group.actionName =>
          Group.transform(config)
        case config: Config if config.getString("action") == CustomUDFGenerator.actionName =>
          CustomUDFGenerator.transform(config)
        case config: Config if config.getString("action") == KeyGenerator.actionName =>
          KeyGenerator.transform(config)
        case config: Config if config.getString("action") == QueryExecutor.actionName =>
          QueryExecutor.transform(config)
        case config: Config if config.getString("action") == TimeFormatter.actionName =>
          TimeFormatter.transform(config)
      }
      val finalPF =
        if (customTransform.isDefined)
          basePF.orElse(customTransform.get)
        else
          basePF

      finalPF.orElse({
        case config: Config => throw new BaseException(s"Unsupported transformation ${config.getString("action")}")
      })
    }

    // Load plugins by parsing plugin paths
    val pluginTransforms = PluginLoader.loadTransformPlugins(config).map(getPartialFunction)

    if (pluginTransforms.isEmpty)
      getTransformFunc(None)
    else
      getTransformFunc(Some(pluginTransforms.reduce(_ orElse _)))
  }

  def transform(dataFrame: DataFrame): DataFrame = {
    val transformRules = loadPlugins()
    val transformFlow = actionList.map(config => transformRules(config))

    transformFlow.foldLeft[DataFrame](dataFrame)((df, func) => { func.apply(df) })
  }

  def run(dataFrame: DataFrame): DataFrame = {
    transform(dataFrame)
  }
}

