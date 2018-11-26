package org.freedomandy.mole.commons.utils

import com.typesafe.config.Config
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.freedomandy.mole.commons.exceptions.BaseException
import org.freedomandy.mole.commons.monitor.checkers.Checker
import org.freedomandy.mole.commons.monitor.handlers.ErrorHandler
import org.freedomandy.mole.commons.postworks.PostWorker
import org.freedomandy.mole.commons.sinks.Sink
import org.freedomandy.mole.commons.sources.Source
import org.freedomandy.mole.commons.transform.FlowStage

/**
  * @author Andy Huang on 2018/5/30
  */
object PluginLoader {
  def loadSourcePlugins(config: Config): List[Source] = {
    def loadSourceInstance(path: String): Source = {
      new Source with Serializable {
        val sourceBehaviors: Source = Class.forName(path).newInstance().asInstanceOf[Source]

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
        val flowStage: FlowStage = Class.forName(path).newInstance().asInstanceOf[FlowStage]

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

  def loadSinkPlugins(config: Config): List[Sink] = {
    def loadSinkInstance(path: String): Sink = {
      new Sink with Serializable {
        val synchronizer: Sink = Class.forName(path).newInstance().asInstanceOf[Sink]

        override def sinkName: String = synchronizer.sinkName

        override def deleteData(config: Config)(deletedSet: RDD[String], keyField: String): Unit =
          synchronizer.deleteData(config)(deletedSet, keyField)

        override def upsert(config: Config)(upsertDF: DataFrame, keyField: String): Unit =
          synchronizer.upsert(config)(upsertDF, keyField)

        override def overwrite(config: Config)(dataFrame: DataFrame, keyField: String): Unit =
          synchronizer.overwrite(config)(dataFrame, keyField)
      }
    }

    import scala.collection.JavaConversions._
    if (config.hasPath("plugins.sink")) {
      config.getStringList("plugins.sink").toList.map(loadSinkInstance)
    } else Nil
  }

  def loadCheckerPlugins(config: Config): List[Checker] = {
    def loadCheckerInstance(path: String): Checker = {
      new Checker {
        val verifier: Checker = Class.forName(path).newInstance().asInstanceOf[Checker]

        override def verify(dataFrame: DataFrame, config: Config): Either[Throwable, DataFrame] = verifier.verify(dataFrame, config)

        override def verifierName: String = verifier.verifierName
      }
    }

    println("Load checker plugins...")

    import scala.collection.JavaConversions._
    if (config.hasPath("plugins.verifier")) {
      config.getStringList("plugins.verifier").toList.map(loadCheckerInstance)
    } else Nil
  }

  def loadErrorHandlerPlugins(config: Config): List[ErrorHandler] = {
    def loadErrHandlerInstance(path: String): ErrorHandler = {
      new ErrorHandler with Serializable {
        val errorHandler: ErrorHandler = Class.forName(path).newInstance().asInstanceOf[ErrorHandler]

        override def handlerName: String = errorHandler.handlerName

        override def isInformer: Boolean = errorHandler.isInformer

        override def isRecover: Boolean = errorHandler.isRecover

        override def notify(message: String, config: Config): Unit = errorHandler.notify(message, config)

        override def recover(dataFrame: DataFrame, config: Config): Either[BaseException, DataFrame] = errorHandler.recover(dataFrame, config)
      }
    }

    println("Load error handler plugins...")

    import scala.collection.JavaConversions._
    if (config.hasPath("plugins.errorHandler")) {
      config.getStringList("plugins.errorHandler").toList.map(loadErrHandlerInstance)
    } else Nil
  }

  def loadPostWorkPlugins(config: Config): List[PostWorker] = {
    def loadPostWorkInstance(path: String): PostWorker = {
      new PostWorker {
        val worker: PostWorker = Class.forName(path).newInstance().asInstanceOf[PostWorker]

        override def workName: String = worker.workName

        override def execute(session: SparkSession, dataFrame: DataFrame, startTime: Long, jobConfig: Config):
        Option[Throwable] = worker.execute(session, dataFrame, startTime, jobConfig)
      }
    }

    import scala.collection.JavaConversions._
    if (config.hasPath("plugins.postWorker")) {
      config.getStringList("plugins.postWorker").toList.map(loadPostWorkInstance)
    } else Nil
  }
}
