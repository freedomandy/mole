package org.freedomandy.mole

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.freedomandy.mole.commons.exceptions.InvalidInputException
import org.freedomandy.mole.sources.Extractor
import org.freedomandy.mole.sinks.Loader
import org.freedomandy.mole.transform.Transformer

/**
  * @author Andy Huang on 2018/6/1
  */
object Boot {
  def main(args: Array[String]) {
    val config = ConfigFactory.load()
    val session = SparkSession.builder.enableHiveSupport().appName("MOLE Job").getOrCreate()
    val extractor = Extractor(session, config)
    val transformer: Transformer = Transformer(session, config)
    val loader = Loader(session, config)
    val dfOp = extractor.run()

    if (dfOp.isEmpty)
      throw new InvalidInputException("Failed to load source data frame")

    loader.run(transformer.run(dfOp.get), "_id")
  }
}
