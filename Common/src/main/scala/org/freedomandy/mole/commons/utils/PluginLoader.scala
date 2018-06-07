package org.freedomandy.mole.commons.utils

import com.typesafe.config.Config
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.freedomandy.mole.commons.sources.SourceBehaviors
import org.freedomandy.mole.commons.synchronizers.Synchronizer
import org.freedomandy.mole.commons.transform.FlowStage

/**
  * @author Andy Huang on 2018/5/30
  */
object PluginLoader {
  def loadSourcePlugins(config: Config): List[SourceBehaviors] = {
    def loadSourceInstance(path: String): SourceBehaviors = {
      new SourceBehaviors with Serializable {
        lazy val sourceBehaviors: SourceBehaviors = Class.forName(path).newInstance().asInstanceOf[SourceBehaviors]

        override val sourceName: String = sourceBehaviors.sourceName

        override def get(session: SparkSession, config: Config): Option[DataFrame] =
          sourceBehaviors.get(session, config)
      }
    }

    import scala.collection.JavaConversions._
    if (config.hasPath("plugins.source")) {
      config.getStringList("plugins.source").toList.map(loadSourceInstance)
    } else Nil
  }

  def loadTransformPlugins(config: Config): List[FlowStage] = {
    def loadFlowInstance(path: String): FlowStage = {
      new FlowStage with Serializable {
        lazy val flowStage: FlowStage = Class.forName(path).newInstance().asInstanceOf[FlowStage]

        override def transform(config: Config)(dataFrame: DataFrame): DataFrame =
          flowStage.transform(config)(dataFrame)

        override def actionName: String =
          flowStage.actionName
      }
    }

    import scala.collection.JavaConversions._
    if (config.hasPath("plugins.transform")) {
      config.getStringList("plugins.transform").toList.map(loadFlowInstance)
    } else Nil
  }

  def loadSinkPlugins(config: Config): List[Synchronizer] = {
    def loadSinkInstance(path: String): Synchronizer = {
      new Synchronizer with Serializable {
        lazy val synchronizer: Synchronizer = Class.forName(path).newInstance().asInstanceOf[Synchronizer]

        override def sinkName: String = synchronizer.sinkName

        override def deleteData(config: Config)(deletedSet: RDD[String], keyField: String): Unit =
          synchronizer.deleteData(config)(deletedSet, keyField)

        override def upsertData(config: Config)(upsertDF: DataFrame, keyField: String): Unit =
          synchronizer.upsertData(config)(upsertDF, keyField)

        override def sync(config: Config)(dataFrame: DataFrame, keyField: String): Unit =
          synchronizer.sync(config)(dataFrame, keyField)
      }
    }

    import scala.collection.JavaConversions._
    if (config.hasPath("plugins.sink")) {
      config.getStringList("plugins.sink").toList.map(loadSinkInstance)
    } else Nil
  }
}
