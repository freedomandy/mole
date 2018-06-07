package org.freedomandy.mole.commons.utils

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

/**
  * @author Andy Huang on 30/03/2018
  */
class ApacheHttpClientTest extends Matchers with FunSpecLike with BeforeAndAfterAll {
  val session =  SparkSession.builder.appName("Test").master("local[*]").getOrCreate()
  val df = session.createDataFrame(Seq(("Y1", 100, "c", "james", "b", Some(0.2), 1L),
    ("Y2", 100, "c", "james", "b", None, 1L),
    ("Y3", 100, "c", "james", "b", None, 2L),
    ("Y4", 100, "c", "james", "b", Some(5.0), 3L),
    ("Y5", 99, "c", "james", "b", None, 4L)))

  describe("Test for Apache http client") {
    ignore("bulk insert and delete") {
      val host = "http://localhost:9200"
      val bulkURL = s"$host/_bulk?pretty"
      val indexValue = "testhttp"

      def getCreateString(index: String, docType: String, id: String, doc: String) =
        s"""{"create" : { "_index" : "$index", "_type" : "$docType", "_id" : "$id" } }
           |{ "doc" : {"value": "$doc"} }\n""".stripMargin
      def getDeleteString(index: String, docType: String, id: String) =
        s"""{"delete" : { "_index" : "$index", "_type" : "$docType", "_id" : "$id" } }\n""".stripMargin

      df.foreachPartition(partition => {
        val httpClient = new ApacheHttpClient
        val createSection = partition.grouped(3).flatMap(row => {
          row.map(row =>  getCreateString(indexValue, "map", row.getAs[String]("_1"), row.getAs[Int]("_2").toString))
        }).mkString

        httpClient.post(bulkURL, Map("Content-Type" -> "application/x-ndjson"), createSection)
      })

      Thread.sleep(2000)
      val httpClient = new ApacheHttpClient
      assert(httpClient.get(s"$host/$indexValue/_search", Map("Content-Type" -> "application/json")).contains("\"hits\":{\"total\":5"))

      df.foreachPartition(partition => {
        val httpClient = new ApacheHttpClient
        val deleteSection =
          partition.grouped(3).flatMap(batch => {
            batch.map(row => getDeleteString(indexValue, "map", row.getAs[String]("_1")))
          }).mkString

        println(deleteSection)

        httpClient.post(bulkURL, Map("Content-Type" -> "application/x-ndjson"), deleteSection)
      })

      Thread.sleep(2000)
      assert(httpClient.get(s"$host/$indexValue/_search", Map("Content-Type" -> "application/json")).contains("\"hits\":{\"total\":0"))
    }
  }
}
