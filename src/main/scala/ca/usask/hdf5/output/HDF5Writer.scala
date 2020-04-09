package ca.usask.hdf5.output

import java.io.File
import java.net.URI
import java.nio.{ByteBuffer, LongBuffer}
import java.util
import java.util.logging.Logger

import ca.usask.hdf5.{P2IRCEntry, Partitioning, Spark, Writer, WriterResponse}
import ch.systemsx.cisd.hdf5.{HDF5Factory, HDF5FactoryProvider, IHDF5Writer}

import scala.collection.mutable

/**
 * A writer that produces HDF5 files on the local file system (or anything
 * accessible through file://)
 * @param uri The path of the directory to be created
 * @param blockSizeLimit The preferred upper bound on the file size in bytes.
 */
class LocalHDF5Writer(
  override val uri: URI,
  override val blockSizeLimit: Long = Spark.BLOCK_SIZE
  ) extends Writer {

  private lazy val logger = Logger.getLogger("LocalHDF5Writer")
  val EXTENSION = ".hdf5"

  /**
   * Creates a number of block files inside the base location using the given
   * file name prefix. Executes on executor nodes.
   *
   * @param iterator            An iterator over the entire partition's elements
   * @param partitioning        Optional partitioning information
   * @param blockFileNamePrefix The prefix of the files created
   * @return An array containing the names of the actual files created.
   */
  override def write(iterator: Iterator[P2IRCEntry], partitioning: Option[Partitioning], blockFileNamePrefix: String): WriterResponse = {
    var entryCount = 0
    var partSize = 0l
    var partFile: IHDF5Writer = null
    var partFiles = Array[String]()

    /**
     * Create a partition file for the output
     * @param fileName: The file name to create under the URI directory
     * @return The part file writer
     */
    def createNewPartFile(fileName: String): IHDF5Writer = {
      logger.info(s"Writing ${uri + File.separator + fileName}")
      val partFile = HDF5FactoryProvider.get().open(new File(uri.getPath + File.separator + fileName))
      partFile.`object`().createGroup("/data")
      partFile
    }

    iterator.foreach(entry => {
      if (partSize >= blockSizeLimit || partFiles.length == 0) {
        // Create a new part file
        if (partFiles.length != 0) {
          partFile.close()
          partSize = 0
          entryCount = 0
        }

        val fileName = s"${blockFileNamePrefix}_block${"%03d" format partFiles.length}$EXTENSION"
        partFile = createNewPartFile(fileName)
        partFiles :+= (uri + fileName)
      }

      val entryName = s"/data/$entryCount"
      partFile.writeLongArray(entryName, entry.getPayloadAsLongs(new Array[Long](entry.getPayload.limit() / 8)))

      // Metadata
      partFile.string().setAttr(entryName, "timestamp", entry.timestamp.toString)
      partFile.float64().setAttr(entryName, "Latitude", entry.lat)
      partFile.float64().setAttr(entryName, "Longitude", entry.lon)
      //FIXME
//      partFile.int64().setAttr(entryName, "Height", entry.height)
      entryCount += 1

      // TODO: estimate actual size
      partSize += entry.calculateSize()
    })

    partFile.close()

    WriterResponse(partFiles, null, null)
  }
}

object LocalHDF5Writer{
  def apply(uri: URI): LocalHDF5Writer = new LocalHDF5Writer(uri)
  def apply(uri: URI, blockSizeLimit: Long): LocalHDF5Writer = new LocalHDF5Writer(uri, blockSizeLimit)
  def apply(uri: String): LocalHDF5Writer = new LocalHDF5Writer(new URI(uri))
  def apply(uri: String, blockSizeLimit: Long): LocalHDF5Writer = new LocalHDF5Writer(new URI(uri), blockSizeLimit)
}
