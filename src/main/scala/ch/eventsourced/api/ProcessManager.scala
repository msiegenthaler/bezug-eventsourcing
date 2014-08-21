package ch.eventsourced.api

import scala.language.implicitConversions
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
trait ProcessManager[Self <: ProcessManager[Self, Id, Command, Next, Transition], Id, Command, Next, Transition] {
  def id: Id
  def handle: PartialFunction[EventData, Next]
  def applyTransition: PartialFunction[Transition, (Self, Seq[SubscriptionAction])]
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
  type Transition
  type Manager <: ProcessManager[Manager, Id, Command, Next, Transition]
  protected trait BaseManager extends ProcessManager[Manager, Id, Command, Next, Transition] {
    protected implicit def noSubscriptionAction(m: Manager): (Manager, Seq[SubscriptionAction]) = (m, Nil)
    protected implicit class RichManager(m: Manager) {
      def +(s: SubscriptionAction): (Manager, Seq[SubscriptionAction]) = (m, Seq(s))
    }
    protected implicit class RichResult(x: (Manager, Seq[SubscriptionAction])) {
      def +(s: SubscriptionAction): (Manager, Seq[SubscriptionAction]) = (x._1, x._2 :+ s)
    }
  }

  sealed trait Next
  case class Completed(commands: Seq[Command] = Seq.empty) extends Next {
    def +(cmd: Command) = copy(commands = commands :+ cmd)
    def ++(cmd: Seq[Command]) = copy(commands = commands ++ cmd)
  }
  case class Continue(transition: Transition,
    commands: Seq[Command] = Seq.empty) extends Next {
    def +(cmd: Command) = copy(commands = commands :+ cmd)
    def ++(cmd: Seq[Command]) = copy(commands = commands ++ cmd)
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

  object Id {
    def serialize(id: Id) = types.serialize(id)
    def parse(string: String) = types.parse(string)
    implicit def stringSerialize: StringSerialize[Id] = types
  }

  protected def types: StringSerialize[Id]
  protected def typeInfo[I >: Id : StringSerialize]: StringSerialize[I] = implicitly

  override def toString = name
}
