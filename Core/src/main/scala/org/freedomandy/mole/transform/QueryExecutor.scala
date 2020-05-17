package org.freedomandy.mole.transform

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.freedomandy.mole.commons.exceptions.InvalidInputException
import org.freedomandy.mole.commons.transform.FlowStage

object QueryExecutor extends FlowStage {
  override def actionName: String = "Query"
  override def transform(config: Config)(dataFrame: DataFrame): DataFrame = {
    import collection.JavaConversions._
    // Read params from config
    val getTempQuery: Option[List[Config]] =
      if (config.hasPath("tempViews"))
        Some(config.getObjectList("tempViews").toList.map(x => x.toConfig))
      else None
    val sourceView = getParam[String](config, "sourceViewName")
    val query      = getParam[String](config, "query")

    if (query.isEmpty || sourceView.isEmpty)
      throw new InvalidInputException(s"Invalid params: ${config.toString}")

    queryAction(dataFrame, getTempQuery, sourceView.get, query.get)
  }

  def queryAction(df: DataFrame, tmpQuery: Option[List[Config]], sourceView: String, query: String): DataFrame = {
    val session = SparkSession.builder.getOrCreate()

    if (session.catalog.tableExists(sourceView)) {
      session.catalog.dropTempView(sourceView)
      df.createOrReplaceTempView(sourceView)
    } else
      df.createOrReplaceTempView(sourceView)
    if (tmpQuery.isDefined)
      tmpQuery.get.foreach { config =>
        val view  = getParam[String](config, "name")
        val query = getParam[String](config, "query")

        if (view.isEmpty || query.isEmpty)
          throw new InvalidInputException(s"Invalid params: ${config.toString}")

        val tempDF = session.sql(query.get)

        tempDF.createOrReplaceTempView(view.get)
      }

    if (query.contains(sourceView))
      session.sql(query)
    else
      throw new InvalidInputException(s"Invalid query: ${query.toString}")
  }
}
