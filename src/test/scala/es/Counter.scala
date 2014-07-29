package es

import java.util.UUID

object counter extends AggregateType {
  type Id = UUID

  sealed trait Command {
    val counter: Id
  }
  case class Initialize() extends Command {
    val counter = UUID.randomUUID
  }
  case class Increment(counter: Id) extends Command
  case class Set(counter: Id, value: Int) extends Command
  case class Kill(counter: Id) extends Command

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
    def applyEvent(e: Event) = e match {
      case Incremented(value) => copy(value = value)
      case WasSet(value) => copy(value = value)
    }
  }

  def seed(id: Id) = Counter(id, 0)
  protected val types = typeInfo[Command, Event, Error]
}
object CounterActorBinding extends AggregateActorBinding[counter.type] {
  val aggregateType = counter
  import counter._
  def name = "counter"
  def commandToId(cmd: Command) = cmd.counter.toString
  def seed(id: String) = {
    val parsedId = UUID.fromString(id)
    counter.seed(parsedId)
  }
}