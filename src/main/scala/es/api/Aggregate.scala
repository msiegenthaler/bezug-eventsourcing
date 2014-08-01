package es.api

import shapeless._
import shapeless.syntax.typeable._

import scala.concurrent.Future
import scala.language.implicitConversions
import scalaz.Scalaz._
import scalaz._

/** Root of an aggregate. */
trait AggregateRoot[Self <: AggregateRoot[Self, Id, Command, Event, Error], Id, Command, Event, Error] extends Entity[Id] {
  def execute(c: Command): Validation[Error, Seq[Event]]
  def applyEvent(e: Event): Self
}

/** A type of aggregate. Implement using an object. */
trait AggregateType {
  def name: String

  type Id
  type Command
  type Event
  type Error
  type Root <: AggregateRoot[Root, Id, Command, Event, Error]
  protected trait RootBase extends AggregateRoot[Root, Id, Command, Event, Error] {
    // Helper methods for more convenience when writing execute() implementations
    protected implicit def eventToEvents[E](event: Validation[E, Event]): Validation[E, Seq[Event]] = event.map(Seq(_))
    protected implicit def errorToValidation(error: Error): Validation[Error, Nothing] = error.fail
    protected implicit def eventToValidation(event: Event): Validation[Nothing, Seq[Event]] = Seq(event).success
    protected implicit def eventsToValidation(events: Seq[Event]): Validation[Nothing, Seq[Event]] = events.success
    protected implicit def unitToNoEvents(r: Unit): Validation[Nothing, Seq[Event]] = Seq.empty.success
  }

  def seed(id: Id): Root
  def serializeId(id: Id): String
  def parseId(serialized: String): Option[Id]
  def aggregateIdForCommand(command: Command): Option[Id]
  protected def types: (Typeable[Command], Typeable[Event], Typeable[Error])

  object Command {
    private implicit def commandTypeable: Typeable[Command] = types._1
    def unapply(a: Any): Option[(Id, Command)] =
      a.cast[Command].flatMap(c => aggregateIdForCommand(c).map((_, c)))
  }
  object Event {
    private implicit def eventTypeable: Typeable[Event] = types._2
    def unapply(a: Any): Option[Event] = a.cast[Event]

    object Data {
      def apply(aggregate: Id, sequence: Long, event: Event) =
        es.api.EventData(AggregateType.this)(aggregate, sequence, event)
      def unapply(eventData: es.api.EventData): Option[EventData] =
        if (eventData.aggregateType == AggregateType.this) Some(eventData) else None
    }
  }
  object Error {
    private implicit def errorTypeable: Typeable[Error] = types._3
    def unapply(a: Any): Option[Error] = a.cast[Error]
  }

  def AggregateKey(aggregateId: Id): AggregateKey = new AggregateKey {
    val aggregateType = AggregateType.this
    val id = aggregateId.asInstanceOf[aggregateType.Id]
    override def hashCode = aggregateType.hashCode ^ id.hashCode
    override def equals(o: Any) = o match {
      case o: AggregateKey => aggregateType == o.aggregateType && id == o.id
      case _ => false
    }
    override def toString = s"AggregateKey($aggregateType, $id)"
  }

  object EventData {
    def apply(aggregate: Id, sequence: Long, event: Event) = {
      es.api.EventData(AggregateType.this)(aggregate, sequence, event)
    }
    def unapply(e: es.api.EventData): Option[(Id, Long, Event)] = {
      if (e.aggregateType == AggregateType.this) {
        val id = e.aggregate.asInstanceOf[Id]
        val event = Event.unapply(e.event)
          .getOrElse(throw new IllegalArgumentException(s"Wrong event type for aggregate $name: ${e.event.getClass}"))
        Some(id, e.sequence, event)
      } else None
    }
  }

  protected def typeInfo[Cmd <: Command : Typeable, Ev <: Event : Typeable, Err <: Error : Typeable]: (Typeable[Cmd], Typeable[Ev], Typeable[Err]) = {
    (implicitly, implicitly, implicitly)
  }
  override def toString = name
}

sealed trait AggregateKey {
  val aggregateType: AggregateType
  def id: aggregateType.Id
}