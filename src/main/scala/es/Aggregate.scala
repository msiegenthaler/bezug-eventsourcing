package es

import scala.language.implicitConversions
import scala.concurrent.Future
import scalaz._
import Scalaz._

trait AggregateRoot[Self <: AggregateRoot[Self, Id, Command, Event, Error], Id, Command, Event, Error] {
  def id: Id
  def execute(c: Command): Validation[Error, Seq[Event]]
  def applyEvent(e: Event): Self

  protected implicit def eventToEvents[E](event: Validation[E, Event]): Validation[E, Seq[Event]] = event.map(Seq(_))
  protected implicit def errorToValidation(error: Error): Validation[Error, Nothing] = error.fail
  protected implicit def eventToValidation(event: Event): Validation[Nothing, Seq[Event]] = Seq(event).success
  protected implicit def eventsToValidation(events: Seq[Event]): Validation[Nothing, Seq[Event]] = events.success
}

trait AggregateType {
  type Id
  type Command
  type Event
  type Error
  type Root <: AggregateRoot[Root, Id, Command, Event, Error]
  protected trait RootBase extends AggregateRoot[Root, Id, Command, Event, Error]

  case class EventData(aggregate: Id, event: Event)

  trait CommandHandler {
    def execute(c: Command): Future[Validation[Error, Unit]]
  }

  object Command {
    def unapply(a: Any): Option[Command] = commandMatcher.lift(a)
  }
  object Event {
    def unapply(a: Any): Option[Event] = eventMatcher.lift(a)
  }
  object Error {
    def unapply(a: Any): Option[Error] = errorMatcher.lift(a)
  }
  protected def commandMatcher: PartialFunction[Any, Command]
  protected def eventMatcher: PartialFunction[Any, Event]
  protected def errorMatcher: PartialFunction[Any, Error]
}