package ch.eventsourced.infrastructure.akka

import ch.eventsourced.api.AggregateType
import ch.eventsourced.support.TypedGuid

object counter extends AggregateType with TypedGuid {
  def name = "Counter"

  sealed trait Command {
    val counter: Id
  }
  case class Initialize() extends Command {
    val counter = generateId
  }
  case class Increment(counter: Id) extends Command
  case class Set(counter: Id, value: Int) extends Command
  case class Kill(counter: Id) extends Command
  case class BadCommand(counter: Id) extends Command

  sealed trait Event
  case class Incremented(newValue: Int) extends Event
  case class WasSet(toValue: Int) extends Event

  sealed trait Error
  case object Unhandled extends Error

  type Root = Counter
  case class Counter(id: Id, value: Int) extends RootBase {
    def execute(c: Command) = c match {
      case Initialize() => Seq.empty
      case Increment(`id`) => Incremented(value + 1)
      case Set(`id`, to) => WasSet(to)
      case Kill(`id`) => throw new RuntimeException("Got a 'Kill' command (expected)")
      case _ => Unhandled
    }
    def applyEvent = {
      case Incremented(value) => copy(value = value)
      case WasSet(value) => copy(value = value)
    }
  }

  def seed(id: Id) = Counter(id, 0)
  def aggregateIdForCommand(command: Command) = Some(command.counter)
  protected def types = typeInfo
}