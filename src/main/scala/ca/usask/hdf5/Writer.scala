package ca.usask.hdf5

import java.net.URI

/**
 * A writer for HDF5RDD partitions
 */
trait Writer extends Serializable {

  /**
   * The base URI indicating the write location
   */
  val uri: URI

  /**
   * The preferred size of each block file created by this writer. This ensures
   * an upper bound on the file size only. Actual file size depends on the
   * the partition size (could be multiple block files, or a single file less
   * than the limit)
   */
  val blockSizeLimit: Long

  /**
   * Creates a number of block files inside the base location using the given
   * file name prefix. Executes on executor nodes.
   * @param iterator An iterator over the entire partition's elements
   * @param partitioning Optional partitioning information
   * @param blockFileNamePrefix The prefix of the files created
   * @return An array containing the names of the actual files created.
   */
  def write(
             iterator: Iterator[P2IRCEntry],
             partitioning: Option[Partitioning],
             blockFileNamePrefix: String): WriterResponse
}
