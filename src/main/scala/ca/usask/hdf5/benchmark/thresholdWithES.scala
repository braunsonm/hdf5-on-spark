package ca.usask.hdf5.benchmark

import java.util.logging.Logger

import ca.usask.hdf5.{P2IRCRDD, Spark}
import ca.usask.hdf5.benchmark.filter.optionMap
import ca.usask.hdf5.benchmark.threshold.checkArgs

object thresholdWithES {
  val usage =
    """
      |Thresholds the data result based on time and/or location. And applies a 16bit mask
      |Usage: [Path to the dataset directory] [Output Directory] (--latitude-gte VALUE) (--latitude-lte VALUE) (--longitude-gte VALUE) (--longitude-lte VALUE) (--time-gte VALUE) (--time-lte VALUE)
      |The time value must be provided as a unix timestamp.
      |The latitude-gte and latitude-lte must be provided together.
      |The longitude-gte and longitude-lte must be provided together.
      |The time-gte and time-lte must be provided together.
      |At lease one of the two pairs must be provided.
      |""".stripMargin

  type OptionMap = Map[String, Any]

  def main(args: Array[String]): Unit = {

    val logger = Logger.getLogger("P2IRC-threshold-benchmark")

    // force the creation of new context if not already created
    Spark.sc

    if (args.length < 3) {
      logger.severe("Missing arguments! Usage: threshold <input URI> <output URI> <options>")
      sys.exit(1)
    }

    val input = args(0)
    val output = args(1)
    val options = optionMap(Map(), args.toList.slice(2, args.toList.size))
    checkArgs(options, usage)

    import P2IRCRDD._

    val startTime = System.nanoTime()

    val counted = P2IRCRDD(input)
      .filterPartitions(options)
      .filter(entry => {
        var cond = true
        if (options.getOrElse("time-gte", null) != null) {
          cond = cond && (entry.timestamp >= options("time-gte").asInstanceOf[Long]) && (entry.timestamp <= options("time-lte").asInstanceOf[Long])
        }

        if (options.getOrElse("latitude-gte", null) != null) {
          cond = cond && (entry.lat >= options("latitude-gte").asInstanceOf[Double]) && (entry.lat <= options("latitude-lte").asInstanceOf[Double])
        }

        if (options.getOrElse("longitude-gte", null) != null) {
          cond = cond && (entry.lon >= options("longitude-gte").asInstanceOf[Double]) && (entry.lon <= options("longitude-lte").asInstanceOf[Double])
        }

        cond
      })
      .map(_.thresholdAndMask16Bit(0))
      .count()

    val endTime = System.nanoTime()

    logger.info(s"Threshold and mask 16-bit: Found ${counted} Time elapsed = ${(endTime - startTime) / 1e9}s")
  }
}
