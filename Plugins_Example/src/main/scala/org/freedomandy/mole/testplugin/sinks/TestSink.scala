package org.freedomandy.mole.testplugin.sinks

import com.mongodb.client.MongoCollection
import com.mongodb.spark.{MongoConnector, MongoSpark}
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import com.typesafe.config.Config
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.freedomandy.mole.commons.sinks.Sink

/**
  * @author Andy Huang on 2018/6/4
  */
class TestSink extends Sink {
  private val MONGO_CONFIG_PATH = "path"

  override def sinkName: String = "TEST_SINK"

  override def overwrite(config: Config)(dataFrame: DataFrame, keyField: String): Unit = {
    val session = SparkSession.builder.appName("MOLE Job").getOrCreate()
    val sc = session.sparkContext
    val sparkIdSet = dataFrame.select(keyField).rdd.map(_.getString(0))
    val readConfig = ReadConfig(Map("uri" -> config.getString(MONGO_CONFIG_PATH), "readPreference.name" -> "secondaryPreferred"))
    val mongoIdSet = MongoSpark.load(sc, readConfig).flatMap(document => Set(document.getString(keyField)))
    val deletedSet = try {
      mongoIdSet.subtract(sparkIdSet)
    } catch {
      case t: org.bson.BsonInvalidOperationException =>
        println(s"Failed to find the data which need to be deleted: ${t.getMessage}")
        sc.parallelize(List[String]())
    }

    // Delete useless records
    deleteData(config)(deletedSet, keyField)

    // Upsert spark data
    upsert(config)(dataFrame, keyField)

  }

  override def deleteData(config: Config)(deletedSet: RDD[String], keyField: String): Unit = {
    val writeConfig = WriteConfig(Map("uri" -> config.getString(MONGO_CONFIG_PATH), "writeConcern.w" -> "majority"))

    if (!deletedSet.isEmpty()) {
      try {
        val mongoConnector = MongoConnector(writeConfig.asOptions)

        deletedSet.foreachPartition(iter => {
          if (iter.nonEmpty) {
            mongoConnector.withCollectionDo(writeConfig, { collection: MongoCollection[org.bson.Document] =>
              iter.grouped(10).foreach(batch => {
                batch.foreach(id => collection.deleteOne(org.bson.Document.parse(s"""{"$keyField":"$id"}""")))
              })
            })
          }
        })
      } catch {
        case t: Throwable =>
          println(s"Failed to delete ${deletedSet.count()} data: ${t.getMessage}")
          throw t
      }
    }
  }

  override def upsert(config: Config)(upsertDF: DataFrame, keyField: String): Unit = {
    val writeConfig = WriteConfig(Map("uri" -> config.getString(MONGO_CONFIG_PATH), "writeConcern.w" -> "majority"))

    try {
      MongoSpark.save(upsertDF, writeConfig)
    } catch {
      case t: Throwable =>
        println(s"Failed to insert data: ${t.getMessage}")
        throw t
    }
  }
}
