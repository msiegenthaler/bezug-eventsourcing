package es.api

import ProcessManager._

/**
 * Contains process related business logic.
 * Reacts to events (from one or more aggregate) and issues commands.
 *
 * The infrastructure ensures, that the process manager reliably receives all events it is subscribed to. Subscriptions
 * are retroactive (past events are also received when a subscription is made).
 */
trait ProcessManager[Self <: ProcessManager[Self, Id, Command], Id, Command] {
  def id: Id

  type Next = Either[Completed.type, Self]
  type Result = (Seq[Command], Traversable[Subscribe], Next)
  def handle: PartialFunction[Event, Result]
}
object ProcessManager {
  type Event = Any

  case object Completed

  sealed trait Subscribe
  case class SubscribeToAggregateType(aggregateType: AggregateType) extends Subscribe
  case class SubscribeToAggregate[A <: AggregateType](aggregateType: A, id: A#Id) extends Subscribe
}

trait ProcessManagerType {
  def name: String

  type Id
  type Command
  type Manager <: ProcessManager[Manager, Id, Command]
  trait BaseManager extends ProcessManager[Manager, Id, Command]

  /** Subscribe to events needed by #initiate. */
  def triggeredBy: Traversable[Subscribe]

  /**
   * Responsible for starting an process manager instance.
   * If it returns an id then the event is forwarded to the respective process manager instance. If the instance
   * already exists, then it is loaded, else it is created.
   * The Id can either be derived from an event (i.e. using the aggregates id) or be created (i.e. using UUID.randomUUID).
   * Take care to not instantiate two process managers if the Id is created and not derived.
   */
  def initiate: PartialFunction[Event, Id]
}
