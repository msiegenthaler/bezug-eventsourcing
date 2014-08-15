package ch.eventsourced.support

import ch.eventsourced.api.StringSerialize

/** Identifier, that is derived from another identifier. */
trait DerivedId[Base] {
  class Id private[DerivedId](val base: Base) {
    override def equals(o: Any) = o match {
      case o: Id => o.base == base
      case _ => false
    }
    override def hashCode = base.hashCode ^ DerivedId.this.hashCode
    override def toString = s"Ref-to-$base"
  }

  protected def generateId(base: Base) = new Id(base)

  implicit def stringSerializeId(implicit s: StringSerialize[Base]) = new StringSerialize[Id] {
    def serialize(value: Id) = StringSerialize.serialize(value.base)
    def parse(serialized: String) = StringSerialize.parse(serialized).map(new Id(_))
  }
}