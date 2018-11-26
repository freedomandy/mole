package org.freedomandy.mole

import org.freedomandy.mole.commons.exceptions.{InvalidInputException, UnsatisfiedPropException}
import org.freedomandy.mole.monitor.Monitor
import org.freedomandy.mole.postworks.PostWorkHandler

/**
  * @author Andy Huang on 2018/6/1
  */
object Boot {
  def main(args: Array[String]) {
    val startTime = System.currentTimeMillis / 1000

    val result =
      if (Module.createIfNotExist().getConfig.hasPath("mole.monitor")) {
        val result = Monitor(Module.createIfNotExist()).handle(Module.createIfNotExist().getConfig.getConfig("mole.monitor"))

        if (result.isDefined) {
          Module.createIfNotExist().loader.run(result.get)
          result.get
        } else {
          throw new UnsatisfiedPropException("Failed to pass the verification process")
        }
      } else {
        val extractor = Module.createIfNotExist().extractor
        val transformer = Module.createIfNotExist().transformer
        val loader = Module.createIfNotExist().loader
        val dfOp = extractor.run()

        if (dfOp.isEmpty)
          throw new InvalidInputException("Failed to load source data frame")

        val resultDF = transformer.run(dfOp.get)

        loader.run(resultDF)
        resultDF
      }

    // TODO: trigger post work
    PostWorkHandler(Module.createIfNotExist().getSession, Module.createIfNotExist().getConfig).run(result, startTime)
  }
}
