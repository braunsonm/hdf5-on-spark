package ca.usask.hdf5.ingest

import java.util.logging.Logger

import ca.usask.hdf5.{P2IRCRDD, Spark}

object ingest {

  // takes the input path of the hdf5 directory and the output of the result rdd
  def main(args: Array[String]): Unit = {
    val logger = Logger.getLogger("P2IRC-ingest")

    // force the creation of new context if not already created
    Spark.sc

    if (args.length < 2) {
      logger.severe("Missing arguments! Usage: ingest <input URI> <output URI>")
      sys.exit(1)
    }

    val input = args(0)
    val output = args(1)

    logger.info(s"Started ingest of $input")

    val startTime = System.nanoTime()

    P2IRCRDD(input)
      .optimize
      .save(output)

    val endTime = System.nanoTime()

    logger.info(s"Ingest complete. Time elapsed = ${(endTime - startTime) / 1e9}s")
  }
}
