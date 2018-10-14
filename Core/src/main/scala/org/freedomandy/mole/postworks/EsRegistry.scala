package org.freedomandy.mole.postworks

import org.freedomandy.mole.sinks.EsSink
import org.freedomandy.mole.transform.Common
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.freedomandy.mole.Module
import org.freedomandy.mole.commons.postworks.PostWorker
import org.freedomandy.mole.commons.sinks.Sink

/**
  * @author Andy Huang on 2018/9/11
  */
object EsRegistry extends PostWorker {
  override def workName: String = "EsRegistry"

  override def execute(session: SparkSession, dataFrame: DataFrame, startTime: Long, config: Config = Module.createIfNotExist().getConfig): Option[Throwable] = {
    try {
      import session.implicits._

      // Get Catalog
      val catalog = getRegistryData(config, dataFrame).map(data => (data.source, data.sink,
        data.fields.map(f => Map("fieldName" -> f.fieldName, "fieldType" -> f.fieldType))))
      val catalogDF = session.createDataset(catalog).toDF("source", "sink", "fields")

      // Save catalog
      val host = config.getString("mole.postWork.host")
      val category = config.getString("mole.postWork.category")

      saveCatalog(EsSink, host, category, catalogDF, startTime)

      None
    } catch {
      case t: Throwable =>
        println(s"Failed to save catalog: ${t.toString}")
        Some(t)
    }
  }

  private def saveCatalog(synchronizer: Sink, url: String, catalogName: String, dataFrame: DataFrame, triggerTime: Long): Unit = {
    import org.apache.spark.sql.functions._
    val updatedTime = System.currentTimeMillis / 1000
    val df = Common.addIdField(dataFrame, Set("source", "sink")).
      withColumnRenamed("_id", "id").withColumn("triggerTime", lit(triggerTime)).withColumn("updatedTime", lit(updatedTime))
    val upsertCatalogConfigString =
      s"""
         |{
         |  type = "ELASTIC_SEARCH"
         |  url = "$url"
         |  resource = "$catalogName/catalog"
         |}
      """.stripMargin
    val config = ConfigFactory.parseString(upsertCatalogConfigString)

    synchronizer.upsert(config)(df, "id")
  }

  private def getRegistryData(jobConfig: Config, dataFrame: DataFrame): List[RegistryData] = {
    import collection.JavaConversions._
    val source = jobConfig.getObject("mole.source").unwrapped().toMap.mapValues(_.toString)
    val catalogs = jobConfig.getObjectList("mole.sink.destinations").toList.map(con => {
      val sink = con.unwrapped().toMap.mapValues(_.toString)

      RegistryData(source, sink, dataFrame.schema.map(s => FieldData(s.name, s.dataType.simpleString)).toList, dataFrame.schema)
    })

    catalogs
  }
}

case class FieldData(fieldName: String, fieldType: String)
case class RegistryData(source: Map[String, String], sink: Map[String, String], fields: List[FieldData], schema: StructType)

