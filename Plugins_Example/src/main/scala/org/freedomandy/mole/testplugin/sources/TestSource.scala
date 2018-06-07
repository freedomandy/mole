package org.freedomandy.mole.testplugin.sources

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.freedomandy.mole.commons.sources.SourceBehaviors
import org.freedomandy.mole.commons.utils.ApacheHttpClient

/**
  * @author Andy Huang on 2018/6/3
  */
class TestSource extends SourceBehaviors {
  override def sourceName: String = "WEBHOOK"
  override def get(session: SparkSession, config: Config): Option[DataFrame] = {
    val httpClient = new ApacheHttpClient

    val result = httpClient.get("https://www.google.com/",  Map[String,String]())

    Some(session.createDataFrame(Seq(("google", result))))
  }
}
