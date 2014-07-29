package es

import scala.concurrent.Future

trait AggregateRoot[Self <: AggregateRoot[Self, Id, Command, Event, Error], Id, Command, Event, Error] {
  def id: Id
  def execute(c: Command): Either[Error, Seq[Event]]
  def applyEvent(e: Event): Self
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
    def execute(c: Command): Future[Either[Error, Unit]]
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