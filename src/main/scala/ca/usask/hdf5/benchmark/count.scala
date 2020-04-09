package ca.usask.hdf5.benchmark

import java.util.logging.Logger

import ca.usask.hdf5.{P2IRCRDD, Spark}

object count {

  def main(args: Array[String]): Unit = {

    val logger = Logger.getLogger("P2IRC-count-benchmark")

    // force the creation of new context if not already created
    Spark.sc

    if (args.length < 1) {
      logger.severe("Missing arguments! Usage: count <input URI>")
      sys.exit(1)
    }

    val input = args(0)

    logger.info(s"Started read of $input")

    val startTime = System.nanoTime()

    val rdd = P2IRCRDD(input)
    val count = rdd.count()

    val endTime = System.nanoTime()

    rdd.take(10).foreach(println)

    logger.info(s"Count complete. Total items = $count, Time elapsed = ${(endTime - startTime) / 1e9}s")
  }
}
