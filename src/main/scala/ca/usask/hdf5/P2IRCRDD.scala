package ca.usask.hdf5

import java.lang.ClassCastException
import java.net.URI
import java.nio.file.{Files, Paths}
import java.util.logging.Logger

import ca.usask.hdf5.input.{HdfsBinaryReader, LocalBinaryReader, LocalHDF5Reader}
import ca.usask.hdf5.output.{HdfsBinaryWriter, LocalBinaryWriter}
import ca.usask.hdf5.types.PartSpec
import com.alibaba.fastjson.JSON
import org.apache.lucene.search.join.ScoreMode
import org.apache.spark.rdd.RDD
import org.apache.spark.{Dependency, OneToOneDependency, Partition, TaskContext}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.{SearchRequest, SearchScrollRequest}
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.common.Strings
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.action.search.ClearScrollRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.builder.SearchSourceBuilder

import scala.collection.mutable
import scala.reflect.io.File

class P2IRCRDD(reader: Reader, conf: RDDConf) extends RDD[P2IRCEntry](Spark.sc, Nil) {

  override def compute(split: Partition, context: TaskContext): Iterator[P2IRCEntry] =
    split.asInstanceOf[P2IRCPartition].iterator

  override protected def getPartitions: Array[Partition] = {

    P2IRCRDD.logger.info(s"Fetching block information for ${getClass.getSimpleName}(${reader.uri})")
    val startTime = System.nanoTime()

    val blocks = reader.getBlockInformation

    val endTime = System.nanoTime()
    P2IRCRDD.logger.info(s"Block information for ${getClass.getSimpleName}(${reader.uri}) fetched in ${(endTime - startTime) / 1e9}s")

    if (blocks.length <= Spark.defaultParallelism) {
      // one block per partition to utilize maximum possible parallelism for this RDD

      blocks.zipWithIndex.map(blockIndexPair => {
        val (block, index) = blockIndexPair
        new P2IRCPartition(index, reader, Seq(block.fileName), block.partitioning.orNull, block.locations.orNull)
      })
    }
    else {
      // group blocks by location and produce partitions that are location & partitioning compatible
      // total size for each partition should not exceed conf.preferredPartitionSize

      val outstanding = mutable.HashSet[Block]()
      blocks.foreach(b => outstanding.add(b))

      val tentative = mutable.HashMap[String, Seq[Block]]()
      blocks.flatMap(_.locations.getOrElse(Seq(""))).distinct.foreach(m => tentative.put(m, Seq[Block]()))

      val finished = mutable.HashMap[String, Seq[Block]]()

      // Throw away "" if it's in tentative
      val removed = tentative.remove("")

      // take all blocks and try to balance the load (by total size) over all machines
      while (outstanding.nonEmpty && tentative.nonEmpty) {
        val m = tentative.minBy(_._2.map(_.size).sum)       // least load machine
        val block = outstanding.find(_.isLocationCompatible(m._1))    // find a block that is local to that machine

        if (block.nonEmpty) {                         // we found a block that is location compatible with the machine
          outstanding.remove(block.get)               // transfer to that machine
          tentative.put(m._1 , m._2 :+ block.get)
        } else {                                      // we didn't find a block for that machine
          finished.put(m._1, m._2)                    // move the machine and its blocks to finished list
          tentative.remove(m._1)                      // so that we don't come back here again
        }
      }

      var index = 0
      def makeIndex = {
        val i = index
        index += 1
        i
      }

      // If we had to remove "" then we have no locations and we need to begin an initial mapping
      if (removed.isDefined) {
        val executors = Spark.sc.statusTracker.getExecutorInfos
        var ii = 0
        blocks.foreach(b => {
          val executorHost = executors(ii % executors.length).host
          val existing = finished.getOrElse(executorHost, Seq())
          finished.put(executorHost, existing :+ b)
          ii += 1
        })
      }

      // machines with their associated blocks are now somewhat-balanced in terms of total load
      // now we need to group together blocks that are partitioning compatible
      val partitions = (finished ++ tentative).flatMap(machine => {
        val (location, blocks) = machine

        var b = blocks.sortBy(_.size)(Ordering.Long.reverse).toList
        val parts = mutable.ListBuffer[P2IRCPartition]()

        while (b.nonEmpty) {
          val first = b.head
          b = b.tail

          val partBlocks = mutable.ListBuffer[Block](first)
          var totalSize = first.size
          var candidateBlock = b.find(bb => bb.isPartitioningCompatible(first) && totalSize + bb.size <= conf.preferredPartitionSize)
          while (candidateBlock.nonEmpty) {
            partBlocks.append(candidateBlock.get)
            b = b.filter(_ != candidateBlock.get)

            totalSize += candidateBlock.size
            candidateBlock = b.find(bb => bb.isPartitioningCompatible(first) && totalSize + bb.size <= conf.preferredPartitionSize)
          }

          // rank locations
          val locations = partBlocks.flatMap(_.locations.getOrElse(Seq()))  // all locations
            .map((_, 1))                                                    // location occurrence frequency
            .groupBy(_._1)                                                  // group by distinct location
            .map(g => (g._1, g._2.map(_._2).sum))                           // sum frequencies within each location
            .toSeq                                                          // map to seq
            .sortBy(_._2)(Ordering.Int.reverse)                             // sort by decreasing frequency
            .map(_._1)                                                      // get locations
            .filter(_ != "")                                                // remove dummy locations

          // make a new partition with the blocks we selected
          parts.append(new P2IRCPartition(
            makeIndex,
            reader,
            partBlocks.map(_.fileName),
            first.partitioning.orNull,
            locations
          ))
        }

        parts
      })

      partitions.toArray
    }
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    try {
      if (split.asInstanceOf[P2IRCPartition].locations != null)
        split.asInstanceOf[P2IRCPartition].locations
      else
        Nil
    } catch {
      // For non-P2IRC partitions we don't give them a preference
      case _: ClassCastException => Nil
    }
  }

  // bring implicits into scope
  import P2IRCRDD._

  /**
   * Saves this RDD using an automatically determined writer for the RDD based on the URI type
   * @param uri location of the saved the RDD
   * @return this RDD
   */
  def save(uri: String): P2IRCRDD = save(writerFromURI(uri))

  /**
   * Saves this RDD using the given writer
   * @param writer the writer to be used
   * @return
   */
  def save(writer: Writer): P2IRCRDD = {
    foreachPartition(it => {
      val partitionIndex = TaskContext.getPartitionId()

      val blocks = writer.write(
        it,
        it match {
          case it: P2IRCPartitionIterator => Option(it.partitioning)
          case _ => Option.empty
        },
        s"part-${"%06d" format partitionIndex}"
      )

      if (Spark.es != null) {
        val partitioning = blocks.partitioning

        // Save this partition to ES
        val builder = XContentFactory.jsonBuilder().startObject().startArray("specs")
        partitioning.partSpec.foreach(spec => {
          builder.startObject().field("key", spec._1)
          if (spec._1 == "timestamp") {
            builder.startObject("date_range").field("gte", spec._2.min) // Millis since epoch
              .field("lte", spec._2.max)
              .endObject()
          } else {
            builder.startObject("geo_range").field("gte", spec._2.min)
              .field("lte", spec._2.max)
              .endObject()
          }
          builder.endObject()
        })

        builder.endArray().field("blocks", blocks.esBlocks).endObject()
        logger.info(Strings.toString(builder))
        // val indexRequest = new IndexRequest("partition").source(builder).id(partitionIndex.toString)
        val indexRequest = new IndexRequest("partition").source(builder)
        Spark.es.index(indexRequest, RequestOptions.DEFAULT)
      }
    })

    this
  }

  private lazy val logger = Logger.getLogger("P2IRCRDD")

  def filterPartitions(q: Map[String, Any]): RDD[P2IRCEntry] = {
    new FilteredP2IRCRDD(reader, conf, q)
  }
  override def filter(f: P2IRCEntry => Boolean): RDD[P2IRCEntry] = super.filter(f)

  def optimize: P2IRCRDD = {
    if (getNumPartitions < Spark.defaultParallelism) {
      P2IRCRDD.logger.info(s"Repartitioning into ${Spark.defaultParallelism} partitions")

      repartition(Spark.defaultParallelism)
    }
    else this
  }
}

object P2IRCRDD {

  protected def logger: Logger = Logger.getLogger("P2IRCRDD")

  def apply(uri: String): P2IRCRDD = {
    val conf = RDDConf()
      .preferredPartitionSize(Spark.BLOCK_SIZE)

    new P2IRCRDD(uri, conf)
  }

  def apply(reader: Reader, conf: RDDConf): P2IRCRDD = new P2IRCRDD(reader, conf)

  import scala.language.implicitConversions

  implicit def fromGenericRdd(rdd: RDD[P2IRCEntry]): P2IRCRDD = ConvertedP2IRCRDD(rdd)

  implicit def fromGenericHyperRdd(rdd: RDD[HyperP2IRCEntry]): P2IRCRDD = ConvertedHyperP2IRCRDD(rdd)

  implicit private def writerFromURI(uri: String): Writer = {
    var u = new URI(uri)

    // Strip the last slash for things like elasticsearch
    if (uri.charAt(uri.length - 1).toString.equals(File.separator)) {
      u = new URI(uri.substring(0, uri.length - 1))
    }

    if (u.getScheme == "file") LocalBinaryWriter(u)
    else if (u.getScheme == "hdfs") HdfsBinaryWriter(u)
    else null
  }

  implicit private def readerFromURI(uri: String): Reader = {

    logger.info(s"Pruning $uri to determine reader")

    var u = new URI(uri)

    // Strip the last slash for things like elasticsearch
    if (uri.charAt(uri.length - 1).toString.equals(File.separator)) {
      u = new URI(uri.substring(0, uri.length - 1))
    }

    if (u.getScheme == "file") {
      val s = Files.newDirectoryStream(Paths.get(u))
      val first = s.iterator().next().toFile.getName.toLowerCase()
      s.close()

      if (first.endsWith(".hdf5")) {
        logger.info(s"Using ${LocalHDF5Reader.getClass}")

        LocalHDF5Reader(u)
      } else if (first.endsWith(".p2irc.bin")) {
        logger.info(s"Using ${LocalBinaryReader.getClass}")

        LocalBinaryReader(u)
      }
      else {
        logger.severe(s"Unable to determine appropriate reader for $uri")
        null
      }
    }
    else if (u.getScheme == "hdfs") {
      logger.info(s"Using ${HdfsBinaryReader.getClass}")
      HdfsBinaryReader(u)
    }
    else {
      logger.severe(s"Unable to determine appropriate reader for $uri")
      null
    }
  }
}

object FilteredP2IRCRDD {
  protected def logger: Logger = Logger.getLogger("FilteredP2IRCRDD")
}

class FilteredP2IRCRDD(reader: Reader, conf: RDDConf, query: Map[String, Any]) extends P2IRCRDD(reader, conf) {
  override protected def getPartitions: Array[Partition] = {
    FilteredP2IRCRDD.logger.info(s"Fetching block information for ${getClass.getSimpleName}(${reader.uri}) using elasticsearch")
    val startTime = System.nanoTime()

    import scala.collection.JavaConverters._
    val boolQuery = QueryBuilders.boolQuery()
    //    val q = QueryBuilders.nestedQuery("specs", QueryBuilders.boolQuery()
    //      .must(QueryBuilders.termQuery("specs.key", "timestamp"))
    //      .must(QueryBuilders.rangeQuery("specs.data_range").gte("1970-01-29T00:41:00")), ScoreMode.None).query()

    if (query.getOrElse("latitude-gte", null) != null) {
      boolQuery.must(QueryBuilders.nestedQuery("specs",
        QueryBuilders.boolQuery().must(QueryBuilders.termQuery("specs.key", "lat"))
          .must(QueryBuilders.rangeQuery("specs.geo_range").gte(query("latitude-gte")).lte(query("latitude-lte"))), ScoreMode.None))
    }

    if (query.getOrElse("longitude-gte", null) != null) {
      boolQuery.must(QueryBuilders.nestedQuery("specs",
        QueryBuilders.boolQuery().must(QueryBuilders.termQuery("specs.key", "lon"))
          .must(QueryBuilders.rangeQuery("specs.geo_range").gte(query("longitude-gte")).lte(query("longitude-lte"))), ScoreMode.None))
    }

    if (query.getOrElse("time-gte", null) != null) {
      boolQuery.must(QueryBuilders.nestedQuery("specs",
        QueryBuilders.boolQuery().must(QueryBuilders.termQuery("specs.key", "timestamp"))
          .must(QueryBuilders.rangeQuery("specs.date_range").gte(query("time-gte")).lte(query("time-lte"))), ScoreMode.None))
    }

    // Only look for files in this directory
    boolQuery.must(QueryBuilders.nestedQuery("blocks",
      QueryBuilders.boolQuery().must(QueryBuilders.prefixQuery("blocks.file_name", reader.uri.getPath)), ScoreMode.None))

    val q = boolQuery

    FilteredP2IRCRDD.logger.info(Strings.toString(q))
    val searchSourceBuilder = new SearchSourceBuilder().query(q).size(100)
    val searchResponse = Spark.es.search(new SearchRequest().source(searchSourceBuilder).scroll(TimeValue.timeValueMinutes(5L)), RequestOptions.DEFAULT)
    var scrollId = searchResponse.getScrollId

    val endTime = System.nanoTime()
    FilteredP2IRCRDD.logger.info(s"Block information for ${this.getClass.getSimpleName}(${reader.uri}) fetched in ${(endTime - startTime) / 1e9}s")

    var results: mutable.MutableList[Partition] = mutable.MutableList[Partition]()
    var hits: List[SearchHit] = searchResponse.getHits.getHits.toList
    while(hits != null && hits.nonEmpty) {
      hits.foreach((hit: SearchHit) => {
        val partition = JSON.parseObject(hit.getSourceAsString, classOf[ESPartition])
        partition.id = hit.getId

        // rank locations
        val locations = partition.blocks.asScala.flatMap(_.locations.asScala)  // all locations
          .map((_, 1))                                                    // location occurrence frequency
          .groupBy(_._1)                                                  // group by distinct location
          .map(g => (g._1, g._2.map(_._2).sum))                           // sum frequencies within each location
          .toSeq                                                          // map to seq
          .sortBy(_._2)(Ordering.Int.reverse)                             // sort by decreasing frequency
          .map(_._1)                                                      // get locations
          .filter(_ != "")                                                // remove dummy locations

        val partSpec: PartSpec = partition.specs.asScala.map(spec => {
          if (spec.key == "timestamp")
            (spec.key, ValueRange(spec.date_range.gte.asInstanceOf[Comparable[Any]], spec.date_range.lte.asInstanceOf[Comparable[Any]]))
          else
            (spec.key, ValueRange(spec.geo_range.gte.asInstanceOf[Comparable[Any]], spec.geo_range.lte.asInstanceOf[Comparable[Any]]))
        })

        results += new P2IRCPartition(results.size, reader, partition.blocks.asScala.map(file => {
          // Strip out everything before the filename
          file.file_name.replace(reader.uri.getPath + File.separator, "")
        }), Partitioning(partSpec), locations)
      })

      // Scroll
      FilteredP2IRCRDD.logger.info("Getting next scroll")
      val scrollRequest = new SearchScrollRequest(scrollId)
      scrollRequest.scroll(TimeValue.timeValueMinutes(5L))
      val searchScrollResponse = Spark.es.scroll(scrollRequest, RequestOptions.DEFAULT)
      scrollId = searchScrollResponse.getScrollId
      hits = searchScrollResponse.getHits.getHits.toList
    }

    // Clean up the scroll
    val clearScrollRequest = new ClearScrollRequest
    clearScrollRequest.addScrollId(scrollId)
    val clearScrollResponse = Spark.es.clearScroll(clearScrollRequest, RequestOptions.DEFAULT)
    val succeeded = clearScrollResponse.isSucceeded
    if (succeeded)
      FilteredP2IRCRDD.logger.info("Cleared ES Scroll")

    results.toArray
  }
}

class P2IRCPartition(
  override val index: Int,
  val reader: Reader,
  val blocks: Seq[String],
  val partitioning: Partitioning,
  val locations: Seq[String]
) extends Partition {

  def iterator: Iterator[P2IRCEntry] = P2IRCPartitionIterator(reader, blocks, partitioning)

  override def toString: String = s"${getClass.getSimpleName}{\n" +
    s"  index = $index\n" +
    s"  reader = ${reader.getClass.getSimpleName}\n" +
    s"  blocks = [${blocks.mkString(", ")}]\n" +
    s"  partitioning = ${if (partitioning != null) partitioning else "None"}\n" +
    s"  locations = [${if (locations != null) locations.mkString(", ") else "None"}]\n" +
    s"}"
}

case class P2IRCPartitionIterator(
  reader: Reader,
  blocks: Seq[String],
  partitioning: Partitioning
) extends Iterator[P2IRCEntry]() {

  var i = 0
  var it: Iterator[P2IRCEntry] = Iterator.empty

  override def hasNext: Boolean = {
    while (i < blocks.length && ! it.hasNext) {
      it = reader.read(blocks(i))
      i += 1
    }

    it.hasNext
  }

  override def next(): P2IRCEntry =
    if (hasNext) it.next()
    else throw new NoSuchElementException("No more elements in iterator")
}

case class ConvertedP2IRCRDD(rdd: RDD[P2IRCEntry]) extends P2IRCRDD(null, null) {

  override def compute(split: Partition, context: TaskContext): Iterator[P2IRCEntry] = rdd.compute(split, context)

  override protected def getPartitions: Array[Partition] = rdd.partitions

  override protected def getDependencies: Seq[Dependency[_]] = Seq(new OneToOneDependency[P2IRCEntry](rdd))
}

case class ConvertedHyperP2IRCRDD(rdd: RDD[HyperP2IRCEntry]) extends P2IRCRDD(null, null) {

  override def compute(split: Partition, context: TaskContext): Iterator[HyperP2IRCEntry] = rdd.compute(split, context)

  override protected def getPartitions: Array[Partition] = rdd.partitions

  override protected def getDependencies: Seq[Dependency[_]] = Seq(new OneToOneDependency[HyperP2IRCEntry](rdd))
}
