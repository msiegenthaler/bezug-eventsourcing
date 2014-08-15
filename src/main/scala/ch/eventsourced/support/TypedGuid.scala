package ch.eventsourced.support

import ch.eventsourced.api.StringSerialize

/** Guid with a specific class.
  * Use i.e. for the id of entities to give a compile error if entity type A is used with an id of the entity type B. */
trait TypedGuid {
  class Id private[TypedGuid](private[TypedGuid] val guid: Guid) {
    override def equals(o: Any) = o match {
      case o: Id => o.guid == guid
      case _ => false
    }
    override def hashCode = guid.hashCode
    override def toString = guid.toString
  }

  protected def generateId = new Id(Guid.generate)

  implicit val stringSerialize = new StringSerialize[Id] {
    def serialize(value: Id) = value.guid.serializeToString
    def parse(serialized: String) = Guid.parseFromString(serialized).map(new Id(_))
  }
}