package ca.usask.hdf5.output

import java.io.{File, IOException}
import java.net.{InetAddress, URI}
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption
import java.nio.{ByteBuffer, ByteOrder}
import java.util
import java.util.logging.Logger
import java.util.stream.Collectors

import scala.collection.JavaConversions._
import ca.usask.hdf5.util.UnsafeUtil
import ca.usask.hdf5.{P2IRCEntry, Partitioning, Spark, ValueRange, Writer, WriterResponse}
import org.apache.hadoop
import org.apache.spark.TaskContext
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.common.Strings
import org.elasticsearch.common.xcontent.{XContentBuilder, XContentFactory}
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.script.{Script, ScriptType}

import scala.collection.mutable

class HdfsBinaryWriter(
  override val uri: URI,
  override val blockSizeLimit: Long = Spark.BLOCK_SIZE
) extends BinaryWriter {

  private lazy val logger = Logger.getLogger("HdfsBinaryWriter")

  private def fs = {
    val conf = new hadoop.conf.Configuration()
    conf.set("fs.default.name", s"hdfs://${uri.getHost}:${uri.getPort}")
    hadoop.fs.FileSystem.get(conf)
  }

  private def containerDirectory = new hadoop.fs.Path(uri.getPath)
  if (! fs.exists(containerDirectory)) fs.mkdirs(containerDirectory)

  override protected def writeToMedium(buf: ByteBuffer, fileName: String): util.List[String] = {

    logger.info(s"Writing ${uri + hadoop.fs.Path.SEPARATOR + fileName}")

    val p = new hadoop.fs.Path(uri.getPath + hadoop.fs.Path.SEPARATOR + fileName)

    var out: hadoop.fs.FSDataOutputStream = null
    try {
      out = fs.create(p)
      out.write(UnsafeUtil.bufToByteArray(buf))
    } catch {
      case e: IOException => e.printStackTrace()
    } finally {
      if (out != null) {
        try {
          out.close()
        } catch {
          case e: IOException => e.printStackTrace()
        }
      }
    }
    val locations = fs.listLocatedStatus(p)
    if (locations.hasNext) locations.next().getBlockLocations.head.getHosts.toList else List()
  }
}

object HdfsBinaryWriter {

  def apply(uri: URI, blockSizeLimit: Long): HdfsBinaryWriter = new HdfsBinaryWriter(uri, blockSizeLimit)

  def apply(uri: String, blockSizeLimit: Long): HdfsBinaryWriter = new HdfsBinaryWriter(new URI(uri), blockSizeLimit)

  def apply(uri: URI): HdfsBinaryWriter = new HdfsBinaryWriter(uri)

  def apply(uri: String): HdfsBinaryWriter = new HdfsBinaryWriter(new URI(uri))
}

class LocalBinaryWriter(
  override val uri: URI,
  override val blockSizeLimit: Long = Spark.BLOCK_SIZE
) extends BinaryWriter {

  private lazy val logger = Logger.getLogger("LocalBinaryWriter")

  val containerDirectory = new File(uri.getPath)
  if (! containerDirectory.exists()) containerDirectory.mkdir()

  override protected def writeToMedium(buf: ByteBuffer, fileName: String): util.List[String] = {

    logger.info(s"Writing ${uri + File.separator + fileName}")

    val f = new File(uri.getPath + File.separator + fileName)

    var c: FileChannel = null
    try {
      c = FileChannel.open(f.toPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE)
      c.write(buf)
    } catch {
      case e: IOException => e.printStackTrace()
    } finally {
      if (c != null) {
        try {
          c.close()
        } catch {
          case e: IOException => e.printStackTrace()
        }
      }
    }
    // Local writers always return their localhost name
    val lst = new util.LinkedList[String]()
    lst.add(InetAddress.getLocalHost.getHostName)
    lst
  }
}

object LocalBinaryWriter {
  def apply(uri: URI): LocalBinaryWriter = new LocalBinaryWriter(uri)
  def apply(uri: URI, blockSizeLimit: Long): LocalBinaryWriter = new LocalBinaryWriter(uri, blockSizeLimit)
  def apply(uri: String): LocalBinaryWriter = new LocalBinaryWriter(new URI(uri))
  def apply(uri: String, blockSizeLimit: Long): LocalBinaryWriter = new LocalBinaryWriter(new URI(uri), blockSizeLimit)
}

abstract class BinaryWriter extends Writer {

  val EXTENSION = ".p2irc.bin"
  private lazy val logger = Logger.getLogger("BinaryWriter")

  protected def writeToMedium(buf: ByteBuffer, fileName: String): util.List[String]

  override def write(iterator: Iterator[P2IRCEntry], partitioning: Option[Partitioning], blockFileNamePrefix: String): WriterResponse = {
    var createdFiles = Array[String]()
    var buf = ByteBuffer.allocateDirect(blockSizeLimit.toInt).order(ByteOrder.LITTLE_ENDIAN)
    buf.position(4)     // length of file at the beginning
    var size = 4
    val esBlocks = mutable.ListBuffer[util.HashMap[String, Object]]()
    val timestamps = mutable.ListBuffer[Long]()
    val lats = mutable.ListBuffer[Double]()
    val lons = mutable.ListBuffer[Double]()

    def flush = {
      val fileName = s"${blockFileNamePrefix}_block${"%03d" format createdFiles.length}$EXTENSION"
      createdFiles :+= fileName
      buf = buf.limit(size).asInstanceOf[ByteBuffer]
      buf.putInt(0, size)
      buf.position(0)
      val locations = writeToMedium(buf, fileName)

      // TODO: This should actually flush blocks
      // Append this block to ES
      val objectMap = new util.HashMap[String, Object]()
      objectMap.put("file_name", uri.getPath + File.separator + fileName)
      objectMap.put("locations", locations)
      esBlocks.append(objectMap)
    }

    iterator.foreach(element => {
      val elementSize = element.calculateSize()
      timestamps.append(element.timestamp)
      lats.append(element.lat)
      lons.append(element.lon)

      if (size + elementSize > blockSizeLimit) {
        flush

        buf = ByteBuffer.allocateDirect(blockSizeLimit.toInt).order(ByteOrder.LITTLE_ENDIAN)
        buf.position(4)
        size = 4
      }

      element.writeObject(buf)
      size += elementSize
    })

    if (size > 0) flush

    var partitioning = Partitioning(Seq())
    if (timestamps.nonEmpty && lats.nonEmpty && lons.nonEmpty) {
      partitioning = Partitioning(Seq(
        ("timestamp", ValueRange(timestamps.min.asInstanceOf[Comparable[Any]], timestamps.max.asInstanceOf[Comparable[Any]])),
        ("lat", ValueRange(lats.min.asInstanceOf[Comparable[Any]], lats.max.asInstanceOf[Comparable[Any]])),
        ("lon", ValueRange(lons.min.asInstanceOf[Comparable[Any]], lons.max.asInstanceOf[Comparable[Any]]))
      ))
    }

    WriterResponse(createdFiles, esBlocks.toArray, partitioning)
  }
}
