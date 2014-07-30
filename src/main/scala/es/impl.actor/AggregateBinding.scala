package es.impl.actor

import es.api.AggregateType

trait AggregateBinding[A <: AggregateType] {
  val aggregateType: A
  import aggregateType._

  def name: String
  def commandToId(cmd: Command): String
  def seed(id: String): Root
}