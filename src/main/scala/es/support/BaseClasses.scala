package es.support

import es.api.{ProcessManagerType, AggregateType}

/** Aggregate that uses a Guid as its id. */
trait GuidAggregateType extends AggregateType {
  case class Id(guid: Guid)

  protected def generateId = Id(Guid.generate)
  override def parseId(serialized: String) = Guid.parseFromString(serialized).map(Id(_))
  override def serializeId(id: Id) = id.guid.serializeToString
}

/** ProcessManger that uses a Guid as its id. */
trait GuidProcessManagerType extends ProcessManagerType {
  case class Id(guid: Guid)

  protected def generateId = Id(Guid.generate)
  override def parseId(serialized: String) = Guid.parseFromString(serialized).map(Id(_))
  override def serializeId(id: Id) = id.guid.serializeToString
}