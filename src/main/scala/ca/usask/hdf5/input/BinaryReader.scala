package ca.usask.hdf5.input

import java.io.{File, IOException}
import java.net.URI
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption
import java.nio.{ByteBuffer, ByteOrder}
import java.util.logging.Logger

import ca.usask.hdf5.{Block, P2IRCEntry, Reader}
import org.apache.hadoop

import scala.collection.mutable

class HdfsBinaryReader(override val uri: URI) extends BinaryReader {

  private lazy val logger = Logger.getLogger("HdfsBinaryReader")

  private def fs = {
    val conf = new hadoop.conf.Configuration()
    conf.set("fs.default.name", s"hdfs://${uri.getHost}:${uri.getPort}")
    hadoop.fs.FileSystem.get(conf)
  }

  override protected def readFromMedium(fileName: String): ByteBuffer = {

    logger.info(s"Reading ${uri + hadoop.fs.Path.SEPARATOR + fileName}")

    val p = new hadoop.fs.Path(uri.getPath + hadoop.fs.Path.SEPARATOR + fileName)
    var buf: ByteBuffer = null

    var in: hadoop.fs.FSDataInputStream = null
    try {
      in = fs.open(p)
      buf = ByteBuffer.allocateDirect(blocks(fileName).size.toInt).order(ByteOrder.LITTLE_ENDIAN)
      while (buf.position() < buf.limit()) in.read(buf)
      buf.position(0)
    } catch {
      case e: IOException => e.printStackTrace()
    } finally {
      if (in != null) {
        try {
          in.close()
        } catch {
          case e: IOException => e.printStackTrace()
        }
      }
    }

    buf
  }

  private lazy val blocks: Map[String, Block] = {
    val it = fs.listFiles(new hadoop.fs.Path(uri.getPath), false)
    val blockMap = mutable.Map[String, Block]()

    while (it.hasNext) {
      val fileStatus = it.next()

      val name = fileStatus.getPath.getName
      val len = fileStatus.getLen
      val hosts = fileStatus.getBlockLocations.head.getHosts

      blockMap += name -> Block(name, len, locations = Option(hosts))
    }

    blockMap.toMap
  }

  override def getBlockInformation: Array[Block] = blocks.values.toArray

  // maybe add a parameter to change the ES query
  override def getBlockInformation(query: String): Array[Block] = ??? // TODO
}

object HdfsBinaryReader {

  def apply(uri: URI): HdfsBinaryReader = new HdfsBinaryReader(uri)

  def apply(uri: String): HdfsBinaryReader = new HdfsBinaryReader(new URI(uri))
}

class LocalBinaryReader(override val uri: URI) extends BinaryReader {

  private lazy val logger = Logger.getLogger("LocalBinaryReader")

  override protected def readFromMedium(fileName: String): ByteBuffer = {
    logger.info(s"Reading ${uri + File.separator + fileName}")

    val f = new File(uri.getPath + File.separator + fileName)
    var buf: ByteBuffer = null

    var c: FileChannel = null
    try {
      c = FileChannel.open(f.toPath, StandardOpenOption.READ)
      buf = ByteBuffer.allocateDirect(blocks(fileName).size.toInt).order(ByteOrder.LITTLE_ENDIAN)
      c.read(buf)
      buf.position(0)
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

    buf
  }

  private lazy val blocks: Map[String, Block] = new File(uri.getPath).listFiles()
    .filter(_.getName.endsWith(EXTENSION))
    .map(f => f.getName -> Block(f.getName, f.length))
    .toMap

  override def getBlockInformation: Array[Block] = blocks.values.toArray
}

object LocalBinaryReader {
  def apply(uri: URI): LocalBinaryReader = new LocalBinaryReader(uri)
  def apply(uri: String): LocalBinaryReader = new LocalBinaryReader(new URI(uri))
}

abstract class BinaryReader extends Reader {

  val EXTENSION = ".p2irc.bin"

  protected def readFromMedium(fileName: String): ByteBuffer

  override def read(blockFileName: String): Iterator[P2IRCEntry] = {
    new Iterator[P2IRCEntry] {

      private lazy val buf = {
        val b = readFromMedium(blockFileName)
        b.limit(b.getInt())
        b
      }

      override def hasNext: Boolean = buf.position() < buf.limit()

      override def next(): P2IRCEntry = {
        val e = new P2IRCEntry()
        e.readObject(buf)
        e
      }
    }
  }
}
