package ca.usask.hdf5

import ca.usask.hdf5.types.PartSpec

/**
 * Represents a range of values
 *
 * @param min          the minimum value
 * @param max          the maximum value
 * @param minInclusive indicates whether this range inclusive of the minimum
 *                     value or not
 * @param maxInclusive indicates whether this range inclusive of the maximum
 *                     value or not
 */
case class ValueRange(
  min: Comparable[Any],
  max: Comparable[Any],
  minInclusive: Boolean = true,
  maxInclusive: Boolean = true
) {

  def contains(other: ValueRange): Boolean = {
    val minComp = min.compareTo(other.min)
    val maxComp = max.compareTo(other.max)

    (
      (minComp < 0 && other.minInclusive) || ( minComp == 0 && (minInclusive || !other.minInclusive) )
    ) &&
    (
      (maxComp > 0 && other.maxInclusive) || ( maxComp == 0 && (maxInclusive || !other.maxInclusive) )
    )
  }

  override def toString: String =
    s"${if (minInclusive) "[" else "("} $min, $max ${if (maxInclusive) "]" else ")"}"
}

/**
 * Represents a block file
 * @param fileName The name of the block file
 * @param size The size of the block file
 * @param partitioning Optional partitioning information
 * @param locations A set of hostnames that have the block file
 */
case class Block(
  fileName: String,
  size: Long,
  partitioning: Option[Partitioning] = Option.empty,
  locations: Option[Seq[String]] = Option.empty
) {

  override def equals(o: Any): Boolean = o match {
    case b: Block => b.fileName == fileName
    case _ => false
  }

  override def hashCode(): Int = fileName.hashCode

  def isPartitioningCompatible(other: Block): Boolean =
    if (partitioning.nonEmpty) {
      other.partitioning.nonEmpty &&
        (partitioning.get.isSubsetOf(other.partitioning.get) || other.partitioning.get.isSubsetOf(partitioning.get))
    }
    else {
      other.partitioning.isEmpty
    }

  def isLocationCompatible(other: Block): Boolean =
    if (locations.nonEmpty) {
      other.locations.nonEmpty && locations.get.exists(l => other.locations.get.contains(l))
    }
    else {
      other.locations.isEmpty
    }

  def isLocationCompatible(location: String): Boolean =
    if (locations.nonEmpty) {
      locations.get.contains(location)
    }
    else {
      false
    }
}

/**
 * The partitioning of an HDF5RDD represented as sequence of tuples of keys
 * and ValueRanges. The keys must be valid keys in the metadata of
 * P2IRCElements.
 */
case class Partitioning(partSpec: PartSpec) extends PartSpec {
  override def length: Int = partSpec.length

  override def apply(idx: Int): (String, ValueRange) = partSpec.apply(idx)

  override def iterator: Iterator[(String, ValueRange)] = partSpec.iterator

  def isSubsetOf(other: Partitioning): Boolean = {
    partSpec.length == other.partSpec.length &&
    partSpec.zip(other.partSpec).forall(pair => {
      val (first, second) = pair
      first._1 == second._1 && second._2.contains(first._2)
    })
  }

  def combine(other: Partitioning): Option[Partitioning] =
    if (other isSubsetOf this) Option(this)
    else if (this isSubsetOf other) Option(other)
    else Option.empty

  override def toString(): String =
    s"(${partSpec.map(pair => s"${pair._1} => ${pair._2}").mkString(", ")})"
}

case class RDDConf(params: Map[Int, Any]) {

  private val PREFERRED_PARTITION_SIZE = 1

  def preferredPartitionSize: Long = params(PREFERRED_PARTITION_SIZE).asInstanceOf[Long]

  def preferredPartitionSize(size: Long) = RDDConf(params + (PREFERRED_PARTITION_SIZE -> size))
}

object RDDConf {

  def apply(): RDDConf = new RDDConf(Map[Int, Any]())
}

object types {

  type PartSpec = Seq[(String, ValueRange)]
}

case class WriterResponse(files: Array[String], esBlocks: Array[java.util.HashMap[String, Object]], partitioning: Partitioning) {

}
