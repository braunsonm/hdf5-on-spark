package ca.usask.hdf5.input

import java.io.File
import java.net.URI
import java.text.SimpleDateFormat
import java.util.logging.Logger

import ca.usask.hdf5.{Block, NumericType, P2IRCEntry, Reader, Spark}
import ch.systemsx.cisd.hdf5.{HDF5Factory, IHDF5Reader}

class LocalHDF5Reader(override val uri: URI) extends HDF5Reader(uri) {

  private lazy val logger = Logger.getLogger("LocalHDF5Reader")

  override def read(blockFileName: String): Iterator[P2IRCEntry] = {

    logger.info(s"Reading ${uri + File.separator + blockFileName}")

    val fileNameComponents = blockFileName.split(":")

    val reader = HDF5Factory.openForReading(uri.getPath + File.separator + fileNameComponents(0))

    new HDF5DataIterator(reader, fileNameComponents(1).toLong, fileNameComponents(2).toLong)
  }
}

object LocalHDF5Reader{
  def apply(uri: URI): LocalHDF5Reader = new LocalHDF5Reader(uri)
  def apply(uri: String): LocalHDF5Reader = new LocalHDF5Reader(new URI(uri))
}

class HDF5DataIterator(val reader: IHDF5Reader, val start: Long, val end: Long) extends Iterator[P2IRCEntry] {
  var dataset_num: Long = start

  var closed = false

  override def size: Int = (end - start).toInt

  override def hasNext: Boolean = if (dataset_num < end) {
    true
  } else {
    if (! closed) {
      reader.close()
      closed = true
    }
    false
  }

  private def timestampConverter(timestamp: String): Long = {
    if (timestamp.contains("'T'") || (timestamp.contains("T") && !timestamp.contains("CST") && !timestamp.contains("UTC"))) {
      val dateFormat =
        if (timestamp.contains(".")) new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSS")
        else new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss")
      dateFormat.parse(timestamp).getTime
    }
    else if (timestamp.contains(" ")) {
      val dateFormat =
        if (timestamp.contains(".")) new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS")
        else if (timestamp.contains("-")) new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSSSSS zZ")
        else new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
      dateFormat.parse(timestamp).getTime
    }
    else {
      timestamp.toLong
    }
  }

  override def next(): P2IRCEntry = {
    val dataset_name = s"data/$dataset_num"
    dataset_num += 1

    val datasetInfo = reader.getDataSetInformation(dataset_name)

    new P2IRCEntry(
      timestampConverter(reader.string().getAttr(dataset_name, "timestamp")),
      reader.float64().getAttr(dataset_name, "Latitude"),
      reader.float64().getAttr(dataset_name, "Longitude"),
      datasetInfo.getDimensions.map(_.toInt),
      NumericType.fromHDF5Type(datasetInfo.getTypeInformation),
      reader.readAsByteArray(dataset_name)
    )
  }
}

/**
 * A reader for HDF5 files containing P2IRCElements. The input files should be
 * present on the local file system (or anything accessible through file://)
 * @param uri The path of the directory containing HDF5 files to be read
 */
abstract class HDF5Reader(override val uri: URI) extends Reader {

  val HDF5_EXTENSION = ".hdf5"

  lazy val blocks: Map[String, Block] = new File(uri.getPath).listFiles()
    .filter(_.getName.toLowerCase.endsWith(HDF5_EXTENSION))
    .flatMap(f => {

      val reader = HDF5Factory.openForReading(uri.getPath + File.separator + f.getName)
      val datasetCount = reader.`object`().getGroupMemberInformation("/data", false).size()
      reader.close()

      if (f.length() <= Spark.BLOCK_SIZE) {
        Seq(f.getName -> Block(s"${f.getName}:0:$datasetCount", f.length))
      }
      else {
        val blockCount = math.ceil(f.length().toDouble / Spark.BLOCK_SIZE).toInt
        val entriesPerBlock = datasetCount / blockCount

        for (i <- 0L until blockCount) yield {
          val name = if (i < blockCount - 1)
            s"${f.getName}:${i * entriesPerBlock}:${(i + 1) * entriesPerBlock}"
          else
            s"${f.getName}:${i * entriesPerBlock}:$datasetCount"

          name -> Block(name, f.length)
        }
      }
    })
    .toMap

  /**
   * Lists all the block files and partitioning information. Executes on
   * driver node.
   *
   * @return An array of Block metadata.
   */
  override def getBlockInformation: Array[Block] = blocks.values.toArray

  override def getBlockInformation(query: String): Array[Block] = ??? // TODO
}
