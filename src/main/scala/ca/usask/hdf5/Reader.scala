package ca.usask.hdf5

import java.net.URI

/**
 * A reader for files containing P2IRCElements
 */
trait Reader extends Serializable {

  /**
   * The base URI of the resource to be read.
   */
  val uri: URI

  /**
   * Lists all the block files and partitioning information. Executes on
   * driver node.
   * @return An array of Block metadata.
   */
  def getBlockInformation: Array[Block]

  /**
   * List all block files and partitioning information using a query to filter
   * the blocks returned. Executes on the driver.
   * @param query The ES query to filter by
   * @return An array of Block metadata
   */
  def getBlockInformation(query: String): Array[Block] = ??? // TODO

  /**
   * Reads the block file containing the P2IRCElements.
   * A lazy read implementation is advised. Executes on executor nodes.
   * @return An Iterator over the P2IRCElements
   */
  def read(blockFileName: String): Iterator[P2IRCEntry]
}
