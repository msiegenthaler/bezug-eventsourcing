package es.support

import es.api.{ProcessManagerType, AggregateType}

/** Aggregate that uses a Guid as its id. */
trait GuidAggregateType extends AggregateType {
  type Id = Guid

  override def parseId(serialized: String) = Guid.parseFromString(serialized)
  override def serializeId(id: Id) = id.serializeToString
}

/** ProcessManger that uses a Guid as its id. */
trait GuidProcessManagerType extends ProcessManagerType {
  type Id = Guid

  override def parseId(serialized: String) = Guid.parseFromString(serialized)
  override def serializeId(id: Id) = id.serializeToString
}