package org.freedomandy.mole.synchronizers

import com.typesafe.config.Config
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._
import org.freedomandy.mole.commons.synchronizers.Synchronizer
import org.freedomandy.mole.commons.utils.ApacheHttpClient

/**
  * @author Andy Huang on 26/03/2018
  */
object EsSynchronizer extends Synchronizer {
  private val URL_CONFIG_PATH = "url"
  private val RESOURCE_CONFIG_PATH = "resource"

  override def sinkName: String = "ELASTIC_SEARCH"

  override def sync(config: Config)(dataFrame: DataFrame, keyField: String): Unit = {
    val session = SparkSession.builder.appName("MOLE Job").getOrCreate()
    val sc = session.sparkContext
    val sparkIdSet = dataFrame.select(keyField).rdd.map(_.getString(0))
    val esIdSet = getData(config, session).map(_._1)
    val deletedSet = try {
      esIdSet.subtract(sparkIdSet)
    } catch {
      case t: org.bson.BsonInvalidOperationException =>
        println(s"Failed to find the data which need to be deleted: ${t.getMessage}")
        sc.parallelize(List[String]())
    }

    // Delete useless records
    deleteData(config)(deletedSet, keyField)

    // Upsert spark data
    upsertData(config)(dataFrame, keyField)
  }

  override def deleteData(config: Config)(deletedSet: RDD[String], keyField: String): Unit = {
    def getDeleteString(index: String, docType: String, id: String) =
      s"""{"delete" : { "_index" : "$index", "_type" : "$docType", "$keyField" : "$id" } }\n""".stripMargin

    val resource = config.getString(RESOURCE_CONFIG_PATH)
    val url = config.getString(URL_CONFIG_PATH)
    val bulkUrl = s"$url/_bulk?pretty"
    val temp = resource.split("/")
    val index = temp.head
    val doc = temp(1)

    println(s"url $bulkUrl")

    deletedSet.foreachPartition(partition => {
      if (partition.nonEmpty) {

        val httpClient = new ApacheHttpClient
        val deleteSection =
          partition.grouped(3).flatMap(batch => {
            batch.map(id => getDeleteString(index, doc, id))
          }).mkString

        httpClient.post(bulkUrl, Map("Content-Type" -> "application/x-ndjson"), deleteSection)
      }
    })

  }

  override def upsertData(config: Config)(upsertDF: DataFrame, keyField: String): Unit = {
    val resource = config.getString(RESOURCE_CONFIG_PATH)
    val url = config.getString(URL_CONFIG_PATH)

    upsertDF.saveToEs(resource, Map("es.mapping.id" -> keyField, "es.nodes.discovery" ->"false", "es.nodes" -> url,
      "es.index.auto.create" -> "true"))
  }

  def getData(config: Config, session: SparkSession) = {
    val resource = config.getString(RESOURCE_CONFIG_PATH)
    val url = config.getString(URL_CONFIG_PATH)

    session.sparkContext.esRDD(resource, Map("es.nodes.discovery" ->"false", "es.nodes" -> url,
      "es.index.auto.create" -> "true"))
  }
}
