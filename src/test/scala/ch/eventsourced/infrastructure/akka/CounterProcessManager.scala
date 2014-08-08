package ch.eventsourced.infrastructure.akka

import ch.eventsourced.infrastructure.akka.counter.Incremented
import ch.eventsourced.support.GuidProcessManagerType

object CounterProcessManager extends GuidProcessManagerType {
  def name = "Counter"

  type Command = PairThem
  case class Manager(id: Id, round: Int) extends BaseManager {
    def handle = {
      case counter.EventData(_, _, Incremented(i)) if i % 2 == 0 =>
        Continue(addRound) + PairThem(i % 2, round)
    }
    private def addRound = copy(round = round + 1)
  }

  def triggeredBy = Set(counter)
  def initiate = {
    //one process manager per counter
    case counter.EventData(id, _, _) => Id(id.guid)
  }

  def seed(id: Id) = Manager(id, 1)
}

case class PairThem(numberOfPairs: Int, round: Int)