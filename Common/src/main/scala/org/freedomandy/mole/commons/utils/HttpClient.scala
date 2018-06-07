package org.freedomandy.mole.commons.utils

import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.{CloseableHttpResponse, HttpDelete, HttpGet, HttpPost}
import org.apache.http.entity.ByteArrayEntity
import org.apache.http.impl.client.{BasicResponseHandler, CloseableHttpClient, HttpClients}
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.util.EntityUtils
import org.freedomandy.mole.commons.exceptions.BaseException

/**
  * @author Andy Huang on 2018/5/30
  */
trait HttpClient {
  def get(url: String, headers: Map[String, String]): String
  def post(url: String, headers: Map[String, String], body: String): String
  def update(url: String, headers: Map[String, String], body: String): String
  def delete(url: String, headers: Map[String, String], body: String): String
}

class ApacheHttpClient extends HttpClient {
  private val client: CloseableHttpClient = initHttpClient()
  private val connMgr: PoolingHttpClientConnectionManager = new PoolingHttpClientConnectionManager()

  def initHttpClient(): CloseableHttpClient = {
    val config = RequestConfig.custom().build()
    val client: CloseableHttpClient = HttpClients.custom()
      .setConnectionManager(connMgr)
      .setDefaultRequestConfig(config).build()
    return client
  }

  def close(): Unit = {
    connMgr.close()
  }

  def get(url: String, headers: Map[String, String]): String = {
    val get = new HttpGet(url)

    headers.foreach(pair => get.addHeader(pair._1, pair._2))

    try {
      val response: CloseableHttpResponse = client.execute(get)
      val statusCode: Int = response.getStatusLine.getStatusCode
      val entity = response.getEntity
      val result = if (entity == null) "" else EntityUtils.toString(entity)
      response.close()

      if (statusCode > 200) {
        throw new BaseException(s"Failed to get response: ${result.toString}")
      } else {
        result
      }
    } catch {
      case e: Throwable =>
        throw BaseException(s"Failed to send http request: ${e.toString}")
    }
  }

  def post(url: String, headers: Map[String, String], body: String): String = {
    val post = new HttpPost(url)
    headers.foreach { pair => post.addHeader(pair._1, pair._2) }
    val entity = new ByteArrayEntity(body.getBytes("UTF-8"))
    try {
      post.setEntity(entity)
      val result = client.execute(post, new BasicResponseHandler())

      result
    } catch {
      case e: Throwable =>
        throw BaseException(s"Failed to send http request: ${e.toString}")
    }
  }

  def update(url: String, headers: Map[String, String], body: String): String = ???

  def delete(url: String, headers: Map[String, String], body: String): String = {
    val delete = new HttpDelete(url)

    headers.foreach(pair => delete.addHeader(pair._1, pair._2))

    try {
      val response: CloseableHttpResponse = client.execute(delete)
      val statusCode: Int = response.getStatusLine.getStatusCode
      val entity = response.getEntity
      val result = if (entity == null) "" else EntityUtils.toString(entity)
      response.close()

      if (statusCode > 200) {
        throw new BaseException(s"Failed to get response: ${result.toString}")
      } else {
        result
      }
    } catch {
      case e: Throwable =>
        throw BaseException(s"Failed to send http request: ${e.toString}")
    }
  }
}
