package ch.eventsourced.infrastructure.akka.localsharding

import akka.actor.{ActorLogging, Props}
import akka.persistence.{RecoveryCompleted, PersistentActor}
import ch.eventsourced.infrastructure.akka.CompositeName

/** Keeps (persistent) track of Ids that are marked as 'unclean'.
  * When the actor starts it sends NeedsCleaning(id) messages to its parent for all the ids that were last marked
  * unclean.
  */
class CleannessTracker[Id](name: CompositeName) {
  case class MarkUnclean(id: Id)
  case class MarkClean(id: Id)
  case class NeedsCleaning(id: Id)

  def props: Props = Props(new TrackerActor)

  private class TrackerActor extends PersistentActor with ActorLogging {
    val persistenceId = (CompositeName("cleannessTracker") / name).serialize
    var dirty = Set.empty[Id]

    def receiveCommand = {
      case MarkClean(id) if dirty(id) =>
        persist(MarkedClean(id))(processEvent)
      case MarkedUnclean(id) if !dirty(id) =>
        persist(MarkedUnclean(id))(processEvent)
    }
    def receiveRecover = {
      case event: Event => processEvent(event)
      case RecoveryCompleted =>
        log.info(s"Completed recovery, ${dirty.size} unclean.")
        dirty foreach { id =>
          context.parent ! NeedsCleaning(id)
        }
    }

    def processEvent(event: Event) = event match {
      case MarkedClean(id) => dirty -= id
      case MarkedUnclean(id) => dirty += id
    }
  }
  private sealed trait Event
  private case class MarkedClean(id: Id) extends Event
  private case class MarkedUnclean(id: Id) extends Event
}
