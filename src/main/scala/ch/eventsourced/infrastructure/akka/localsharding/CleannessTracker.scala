package ch.eventsourced.infrastructure.akka.localsharding

import akka.actor.{ActorLogging, Props}
import akka.persistence.{SnapshotOffer, RecoveryCompleted, PersistentActor}
import ch.eventsourced.infrastructure.akka.CompositeName

/** Keeps (persistent) track of Ids that are marked as 'unclean'.
  * When the actor starts it sends NeedsCleaning(id) messages to its parent for all the ids that were last marked
  * unclean.
  */
class CleannessTracker[Id](name: CompositeName) {
  case class MarkUnclean(id: Id, ack: Any)
  case class MarkClean(id: Id)
  case class NeedsCleaning(id: Id)

  def props: Props = Props(new TrackerActor)


  private class TrackerActor extends PersistentActor with ActorLogging {
    val persistenceId = (CompositeName("cleannessTracker") / name).serialize
    def config = context.system.settings.config.getConfig("ch.eventsourced.sharding.local.cleanness-tracker")
    val snapshotInterval = config.getLong("snapshot-interval")

    var eventCount = 0L
    var dirty = Set.empty[Id]

    def receiveCommand = {
      case MarkClean(id) if dirty(id) =>
        persist(MarkedClean(id)) { event =>
          processEvent(event)
          maybeSnapshot()
        }
      case MarkUnclean(id, ack) if !dirty(id) =>
        persist(MarkedUnclean(id)) { event =>
          processEvent(event)
          maybeSnapshot()
          sender() ! ack
        }
    }

    def receiveRecover = {
      case event: Event => processEvent(event)
      case SnapshotOffer(_, s: Set[Id]) =>
        eventCount = s.size
        dirty = s
      case RecoveryCompleted =>
        log.info(s"Completed recovery, ${dirty.size} unclean.")
        dirty foreach { id =>
          context.parent ! NeedsCleaning(id)
        }
    }

    def processEvent(event: Event) = {
      event match {
        case MarkedClean(id) => dirty -= id
        case MarkedUnclean(id) => dirty += id
      }
      eventCount += 1
    }

    def maybeSnapshot() = if (eventCount % snapshotInterval == 0) saveSnapshot(dirty)
  }
  private sealed trait Event
  private case class MarkedClean(id: Id) extends Event
  private case class MarkedUnclean(id: Id) extends Event
}
