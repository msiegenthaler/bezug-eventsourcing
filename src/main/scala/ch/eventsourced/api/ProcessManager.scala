package ch.eventsourced.api

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
trait ProcessManager[Self <: ProcessManager[Self, Id, Command, Next], Id, Command, Next] {
  def id: Id
  def handle: PartialFunction[EventData, Next]
}
object ProcessManager {
  case object Completed

  sealed trait SubscriptionAction
  case class Subscribe(id: AggregateKey) extends SubscriptionAction
  case class Unsubscribe(id: AggregateKey) extends SubscriptionAction
}

trait ProcessManagerType {
  def name: String

  type Id
  type Command
  type Error
  type Manager <: ProcessManager[Manager, Id, Command, Next]
  trait BaseManager extends ProcessManager[Manager, Id, Command, Next]

  sealed trait Next
  case class Completed(commands: Seq[Command] = Seq.empty) extends Next {
    def +(cmd: Command) = copy(commands = commands :+ cmd)
  }
  case class Continue(state: Manager,
    commands: Seq[Command] = Seq.empty,
    subscriptionActions: Seq[SubscriptionAction] = Seq.empty) extends Next {
    def +(cmd: Command) = copy(commands = commands :+ cmd)
    def +(action: SubscriptionAction) = copy(subscriptionActions = subscriptionActions :+ action)
  }


  /** Subscribe to events needed by #initiate. */
  def triggeredBy: Set[AggregateType]

  /**
   * Responsible for starting an process manager instance.
   * Return an Id to start a process manager instance. If the instance with the id is not already started, then it is
   * created and the event is forwarded to it. It will also be auto-subscribed to all events of the origin aggregate,
   * starting with the triggering event.
   * If the instance already exists then nothing will be done, the event will not be forwarded.
   *
   * The Id can either be derived from an event (i.e. using the aggregates id) or be created (i.e. using UUID.randomUUID).
   * Take care to not instantiate two process managers if the Id is created and not derived.
   */
  def initiate: PartialFunction[EventData, Id]

  def seed(id: Id): Manager
  def serializeId(id: Id): String
  def parseId(serialized: String): Option[Id]

  override def toString = name
}
