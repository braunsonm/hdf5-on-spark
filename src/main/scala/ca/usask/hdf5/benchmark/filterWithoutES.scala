package ca.usask.hdf5.benchmark

import java.util.logging.Logger

import ca.usask.hdf5.{P2IRCRDD, Spark}

object filterWithoutES {
  val usage =
    """
      |Filters the RDD result based on time and/or location.
      |Usage: [Path to the dataset directory] (--latitude-gte VALUE) (--latitude-lte VALUE) (--longitude-gte VALUE) (--longitude-lte VALUE) (--time-gte VALUE) (--time-lte VALUE)
      |The time value must be provided as a unix timestamp.
      |The latitude-gte and latitude-lte must be provided together.
      |The longitude-gte and longitude-lte must be provided together.
      |The time-gte and time-lte must be provided together.
      |At lease one of the two pairs must be provided.
      |""".stripMargin

  type OptionMap = Map[String, Any]

  def isSwitch(s: String) = (s(0) == '-')

  def optionMap(map: OptionMap, list: List[String]) : OptionMap = {
    list match {
      case Nil => map
      case "--latitude-gte" :: value :: tail =>
        optionMap(map ++ Map("latitude-gte" -> value.toDouble), tail)
      case "--latitude-lte" :: value :: tail =>
        optionMap(map ++ Map("latitude-lte" -> value.toDouble), tail)
      case "--longitude-lte" :: value :: tail =>
        optionMap(map ++ Map("longitude-lte" -> value.toDouble), tail)
      case "--longitude-gta" :: value :: tail =>
        optionMap(map ++ Map("longitude-gte" -> value.toDouble), tail)
      case "--time-gte" :: value :: tail =>
        optionMap((map ++ Map("time-gte" -> value.toLong)), tail)
      case "--time-lte" :: value :: tail =>
        optionMap((map ++ Map("time-lte" -> value.toLong)), tail)
      case string :: opt2 :: tail if isSwitch(opt2) =>
        optionMap(map ++ Map("input" -> string), list.tail)
      case string :: Nil =>  optionMap(map ++ Map("input" -> string), list.tail)
      case option :: tail => println("Unknown option "+option)
        println(usage)
        System.exit(1)
        map
    }
  }

  def main(args: Array[String]): Unit = {
    val logger = Logger.getLogger("P2IRC-filter-benchmark")
    if (args.length <= 1) {
      println(usage)
    }

    val argsList = args.toList
    val options = optionMap(Map(), argsList)

    var valid = false
    if (options.getOrElse("latitude-gte", null) != null) {
      // lte latitude must be provided
      if (options.getOrElse("latitude-lte", null) == null) {
        println(usage)
        System.exit(2)
        return
      }
      valid = true
    }

    if (options.getOrElse("longitude-gte", null) != null) {
      // lte longitude must be provided
      if (options.getOrElse("longitude-lte", null) == null) {
        println(usage)
        System.exit(2)
        return
      }
      valid = true
    }

    if (options.getOrElse("time-gte", null) != null) {
      // lte time must be provided
      if (options.getOrElse("time-lte", null) == null) {
        println(usage)
        System.exit(2)
        return
      }
      valid = true
    }

    if (!valid) {
      println(usage)
      System.exit(2)
    }

    Spark.sc
    val startTime = System.nanoTime()

    val rdd = P2IRCRDD(options.getOrElse("input", "").toString)
    val filtered = rdd.filter(entry => {
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
    val count = filtered.count()
    val endTime = System.nanoTime()

    filtered.take(10).foreach(println)

    logger.info(s"Count complete. Total items = $count, Time elapsed = ${(endTime - startTime) / 1e9}s")
  }
}
