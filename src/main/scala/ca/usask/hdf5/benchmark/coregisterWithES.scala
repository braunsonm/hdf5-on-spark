package ca.usask.hdf5.benchmark

import java.io.File
import java.util.logging.Logger

import ca.usask.hdf5.benchmark.filter.optionMap
import ca.usask.hdf5.benchmark.threshold.checkArgs
import ca.usask.hdf5.{HyperP2IRCEntry, P2IRCEntry, P2IRCRDD, Spark}
import org.apache.spark.TaskContext
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

object coregisterWithES {
  val usage =
    """
      |Coregisters all data in a specific geo/time range
      |Usage: [Path to the dataset directory] [Output Directory] [ <sub-dir-1> [ <sub-dir-2> [ ... ] ] ] (--latitude-gte VALUE) (--latitude-lte VALUE) (--longitude-gte VALUE) (--longitude-lte VALUE) (--time-gte VALUE) (--time-lte VALUE)
      |The time value must be provided as a unix timestamp.
      |The latitude-gte and latitude-lte must be provided together.
      |The longitude-gte and longitude-lte must be provided together.
      |The time-gte and time-lte must be provided together.
      |At lease one of the two pairs must be provided.
      |""".stripMargin

  def main(args: Array[String]): Unit = {

    val logger = Logger.getLogger("P2IRC-coregister-benchmark")

    // force the creation of new context if not already created
    Spark.sc

    if (args.length < 2) {
      logger.severe("Missing arguments! Usage: coregister <input URI> <output URI> [ <sub-dir-1> [ <sub-dir-2> [ ... ] ] ]")
      sys.exit(1)
    }

    val input = args(0)
    val output = args(1)
    var subdirs = args.drop(2)
    val options = optionMap(Map(), args.toList.slice(2, args.toList.size))
    checkArgs(options, usage)

    // A very basic way to sort out some of the filter parameters, if the subdir begins with a -- or a number, remove it
    subdirs = subdirs.filter(dir => {
      if (dir.charAt(0) == '-')
        false
      else {
        try {
          dir.toLong
          false
        } catch {
          case e: NumberFormatException => true
        }
      }
    })

    import P2IRCRDD._

    val startTime = System.nanoTime()

    val n = subdirs.length

    logger.info("Merging all inputs")

    // prepare RDDs for all instruments, tag each RDD with instrument index as indicated in subdirs
    // and merge all into one RDD for co-registering
    val rdd =
      ( for (i <- 0 until n) yield P2IRCRDD(input + File.separator + subdirs(i)).filterPartitions(options).filter(entry => {
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
      }).map((i, _)) )
        .reduce(_.union(_))

    /*
      Let the set of points of the first instrument be P and the set of points from other instruments be S.
      Ideally for each point p in P, co-registering will output a set of points C subset of S such that the
      Euclidean distance between p and all points in C is minimal.

      Co-registering is done by merging all instruments into one RDD. All data points are then sorted by
      [time, lat, lon] in a lexicographical ordering. For each two consecutive points of P, points from S are
      associated to either point of P based on Euclidean distance.

      There are two assumptions here. The first assumption is that points in P have the lowest capture rate
      and the output is expected to be a set of points from S with respect to a single point from P. The
      second assumption is that a lexicographical ordering of [time, lat, lon] will ensure that points
      from P are always around the points from S that they should be co-registered with.
     */

    logger.info("Computing and persisting partially co-registered data points")

    // co-register with partition local information only and persist the result, it will be used twice
    val partiallyCoregistered = rdd
      .sortBy(e => (e._2.timestamp, e._2.lat, e._2.lon))
      .mapPartitions(it => {
        new Iterator[HyperP2IRCEntry] {

          private var q0: P2IRCEntry = consume
          private val q = ( for (_ <- 0 until n) yield mutable.Queue[P2IRCEntry]() ).toArray

          private def consume = {
            if (it.hasNext) {
              var e = it.next()

              while (e._1 != 0 && it.hasNext) {
                q(e._1).enqueue(e._2)

                e = it.next()
              }

              if (e._1 == 0) e._2 else null
            }
            else null
          }

          override def hasNext: Boolean = q0 != null

          override def next(): HyperP2IRCEntry = {
            val prev = q0
            q0 = consume

            // take from all queues the elements that better match prev than the next q0
            val associatedPts =
              if (q0 != null)
                q.tail.map(_.dequeueAll(e => e.distanceTo(prev) < e.distanceTo(q0)).toArray)
              else
                q.tail.map(_.toArray)

            new HyperP2IRCEntry(prev, associatedPts)
          }
        }
      })
      .persist(StorageLevel.MEMORY_ONLY)

    logger.info("Broadcasting partition edges")

    // get partition edges (first and last point of all partitions) of partiallyCoregistered and broadcast them

    val partitionEdges = Spark.sc.broadcast(
      partiallyCoregistered
        .mapPartitions(it => {
          val part = TaskContext.getPartitionId()

          if (it.hasNext) {
            val first = (it.next(), part, 1)
            val last = if (it.hasNext) (it.reduceLeft((_, b) => b), part, -1) else (first._1, part, -1)
            Seq(first, last).iterator
          }
          else {
            Iterator.empty
          }
        })
        .collect()
    )

    logger.info("Finalizing co-registered data points")

    val numParts = partiallyCoregistered.getNumPartitions

    // Fix the partiallyCoregistered with information from partitionEdges
    val fullyCoregistered = partiallyCoregistered
      .mapPartitions(it => {
        new Iterator[HyperP2IRCEntry] {

          private var first = true
          private val part = TaskContext.getPartitionId()

          override def hasNext: Boolean = it.hasNext

          override def next(): HyperP2IRCEntry = {

            val p = it.next()

            if (first) {
              first = false

              // fix p as a partition's first point. Reconcile with previous partition's last point
              if (part > 0) {
                // get last entry in the previous partition
                val lastOfPrevious = partitionEdges.value.find(x => x._2 == part - 1 && x._3 == -1).get._1

                // filter all instruments for points that are closer to p than lastOfPrevious
                val correctedAssociatedPoints = p.associatedPoints.union(lastOfPrevious.associatedPoints)
                  .map(_.filter(x => x.distanceTo(p) < x.distanceTo(lastOfPrevious)))

                new HyperP2IRCEntry(p.p, correctedAssociatedPoints)
              }
              else
                p
            }
            else if (it.hasNext) {
              // middle points are co-registered correctly
              p
            }
            else {
              // fix p as a partition's last point. Reconcile with next partition's first point
              if (part < numParts - 1) {
                // get first entry in the next partition
                val firstOfNext = partitionEdges.value.find(x => x._2 == part + 1 && x._3 == 1).get._1

                // filter all instruments for points that are closer to p than firstOfNext
                val correctedAssociatedPoints = p.associatedPoints.union(firstOfNext.associatedPoints)
                  .map(_.filter(x => x.distanceTo(p) < x.distanceTo(firstOfNext)))

                new HyperP2IRCEntry(p.p, correctedAssociatedPoints)
              }
              else
                p
            }
          }
        }
      })

    val registered = fullyCoregistered
      .count()

    partiallyCoregistered.unpersist()

    val endTime = System.nanoTime()

    logger.info(s"Co-registering complete. Registered ${registered}, Time elapsed = ${(endTime - startTime) / 1e9}s")
  }
}
