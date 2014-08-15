package ch.eventsourced.api

/** Serialize a class into a string and parse it back from the serialized representation. */
trait StringSerialize[A] {
  def serialize(value: A): String
  def parse(serialized: String): Option[A]
}

object StringSerialize {
  def serialize[A](value: A)(implicit s: StringSerialize[A]) = s.serialize(value)
  def parse[A](serialized: String)(implicit s: StringSerialize[A]) = s.parse(serialized)
}