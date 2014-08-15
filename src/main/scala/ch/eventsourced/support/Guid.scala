package ch.eventsourced.support

import java.nio.ByteBuffer
import java.util.UUID
import scala.util.Try
import ch.eventsourced.api.StringSerialize

/** Globally unique identifier. */
final class Guid private(private val upper: Long, private val lower: Long) {

  def serializeToString = {
    serializeUint(upper >> 32) + serializeUint(upper) +
      serializeUint(lower >> 32) + serializeUint(lower)
  }
  private def serializeUint(value: Long) = {
    val r = (value & 0xFFFFFFFF).toHexString.takeRight(8)
    "0" * (8 - r.length) + r
  }

  def serializeToBytes: Array[Byte] = {
    val bb = ByteBuffer.allocate(16)
    bb.putLong(upper)
    bb.putLong(lower)
    bb.array()
  }

  override def equals(o: Any) = o match {
    case o: Guid => o.lower == lower && o.upper == upper
    case _ => false
  }
  override def hashCode = lower.toInt
  override def toString = serializeToString
}

object Guid {
  def generate = {
    val uuid = UUID.randomUUID()
    new Guid(uuid.getMostSignificantBits, uuid.getLeastSignificantBits)
  }

  /** Opposite of Guid.serializeToString. */
  def parseFromString(s: String): Option[Guid] = {
    if (s.length == 32) {
      val parts = s.grouped(8).flatMap(parseUint).toSeq
      if (parts.length == 4) {
        val upper = parts(0) << 32 | parts(1)
        val lower = parts(2) << 32 | parts(3)
        new Some(new Guid(upper, lower))
      } else None
    }
    else None
  }
  private def parseUint(s: String) = Try(java.lang.Long.parseLong(s, 16) & 0xFFFFFFFF).toOption

  /** Opposite of Guid.serializeToBytes. */
  def parseFromBytes(bytes: Array[Byte]): Option[Guid] = {
    if (bytes.length == 16) {
      val bb = ByteBuffer.wrap(bytes)
      val upper = bb.getLong()
      val lower = bb.getLong()
      Some(new Guid(upper, lower))
    } else None
  }

  implicit val stringSerializer = new StringSerialize[Guid] {
    def serialize(value: Guid) = value.serializeToString
    def parse(serialized: String) = parseFromString(serialized)
  }
}