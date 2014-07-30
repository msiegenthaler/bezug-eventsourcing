package es.api

import shapeless._
import shapeless.syntax.typeable._

import scala.concurrent.Future
import scala.language.implicitConversions
import scalaz.Scalaz._
import scalaz._

/** Root of an aggregate (see DDD) */
trait AggregateRoot[Self <: AggregateRoot[Self, Id, Command, Event, Error], Id, Command, Event, Error] {
  def id: Id
  def execute(c: Command): Validation[Error, Seq[Event]]
  def applyEvent(e: Event): Self

  // Helper methods for more convenience when writing execute() implementations
  protected implicit def eventToEvents[E](event: Validation[E, Event]): Validation[E, Seq[Event]] = event.map(Seq(_))
  protected implicit def errorToValidation(error: Error): Validation[Error, Nothing] = error.fail
  protected implicit def eventToValidation(event: Event): Validation[Nothing, Seq[Event]] = Seq(event).success
  protected implicit def eventsToValidation(events: Seq[Event]): Validation[Nothing, Seq[Event]] = events.success
}

/** A type of aggregate. Implement using an object. */
trait AggregateType {
  type Id
  type Command
  type Event
  type Error
  type Root <: AggregateRoot[Root, Id, Command, Event, Error]
  protected trait RootBase extends AggregateRoot[Root, Id, Command, Event, Error]

  /**
   * Wraps an event.
   * @param aggregate the aggregate affected by the event
   * @param sequence sequence number of the event within the aggregate (0 is first, 1 is second, ...)
   * @param event the event itself
   */
  case class EventData(aggregate: Id, sequence: Int, event: Event)

  trait CommandHandler {
    def execute(c: Command): Future[Validation[Error, Unit]]
  }

  object Command {
    private implicit def commandTypeable: Typeable[Command] = types._1
    def unapply(a: Any): Option[Command] = a.cast[Command]
  }
  object Event {
    private implicit def eventTypeable: Typeable[Event] = types._2
    def unapply(a: Any): Option[Event] = a.cast[Event]
  }
  object Error {
    private implicit def errorTypeable: Typeable[Error] = types._3
    def unapply(a: Any): Option[Error] = a.cast[Error]
  }

  protected def typeInfo[Cmd <: Command : Typeable, Ev <: Event : Typeable, Err <: Error : Typeable]: (Typeable[Cmd], Typeable[Ev], Typeable[Err]) = {
    (implicitly, implicitly, implicitly)
  }
  protected def types: (Typeable[Command], Typeable[Event], Typeable[Error])
}
