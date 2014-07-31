package es.api

import ProcessManager._

/**
 * Contains process related business logic.
 * Reacts to events (from one or more aggregate) and issues commands.
 *
 * The infrastructure ensures, that the process manager reliably receives all events it is subscribed to. Subscriptions
 * are retroactive (past events are also received when a subscription is made). The events of one aggregate are received
 * in sequence, event ordering between aggregates is arbitrary, but is stable (does not change when the process manager
 * is loaded from the store).
 */
trait ProcessManager[Self <: ProcessManager[Self, Id, Command], Id, Command] {
  def id: Id

  type Next = Either[Completed.type, Self]
  type Result = (Seq[Command], Seq[SubscriptionAction], Next)
  def handle: PartialFunction[EventData, Result]
}
object ProcessManager {
  case object Completed

  sealed trait SubscriptionAction

  sealed trait Subscribe extends SubscriptionAction
  case class SubscribeToAggregateType(aggregateType: AggregateType) extends Subscribe
  case class SubscribeToAggregate[A <: AggregateType](aggregateType: A, id: A#Id) extends Subscribe
  sealed trait Unsubscribe extends SubscriptionAction
  case class UnsubscribeFromAggregateType(aggregateType: AggregateType) extends Unsubscribe
  case class UnsubscribeFromAggregate[A <: AggregateType](aggregateType: A, id: A#Id) extends Unsubscribe
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
  def initiate: PartialFunction[EventData, Id]

  def seed(id: Id): Manager
}
