package es.infrastructure.akka

import java.util.UUID

import es.api.ProcessManagerType
import es.infrastructure.akka.counter.Incremented

import scala.util.Try

class CounterProcessManager extends ProcessManagerType {
  def name = "Counter"
  type Id = UUID

  type Command = PairThem
  case class Manager(id: UUID, round: Int) extends BaseManager {
    def handle = {
      case counter.EventData(_, _, Incremented(i)) if i % 2 == 0 =>
        Continue(addRound) + PairThem(i % 2, round)
    }
    private def addRound = copy(round = round + 1)
  }

  def triggeredBy = Set(counter)
  def initiate = {
    //one process manager per counter
    case counter.EventData(id, _, _) => id
  }

  def serializeId(id: Id) = id.toString
  def parseId(serialized: String) = Try(UUID.fromString(serialized)).toOption
  def seed(id: Id) = Manager(id, 1)
}

case class PairThem(numberOfPairs: Int, round: Int)