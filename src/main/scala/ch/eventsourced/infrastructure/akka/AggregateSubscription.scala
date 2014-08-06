package ch.eventsourced.infrastructure.akka

import scala.collection.SortedSet
import scala.concurrent.duration._
import akka.actor._
import akka.actor.SupervisorStrategy.Escalate
import akka.persistence.{RecoveryCompleted, PersistentActor}
import ch.eventsourced.api.EventData
import ch.eventsourced.infrastructure.akka.AggregateManager.AggregateEvent

/**
 * Handles a persistent single subscription for events of a single aggregate.
 * Needs to be started with the Start() message.
 * Loads old events from the journal if needed and afterwards follows a life-event stream of OnEvent messages (must be
 * sent to this actor by the creator).
 * The events are sent to the target as EventBus.AggregateEvent, acks are noted persistently.
 */
object AggregateSubscription {
  /** Send to start the subscription. `liveEventFrom` is the sequence of the first event that will be received live. */
  case class Start(lifeEventsFrom: Long)

  /** Event generated by the aggregate. Reply with `ack`. */
  case class OnEvent(event: EventData, ack: Any)

  /** Close the subscription. Events pending ack will be discarded. */
  case class Close(whenDone: Any)

  def props(id: String, partition: String, target: ActorRef, journalReplay: (Long, Long) => Props, startAtEventSequence: Long = 0): Props = {
    Props(new SubscriptionActor(id, partition, journalReplay, target, startAtEventSequence))
  }

  private implicit object EventDataOrdering extends Ordering[EventData] {
    def compare(x: EventData, y: EventData) = x.sequence compare y.sequence
  }
  private class SubscriptionActor(id: String, partition: String, journalReplay: (Long, Long) => Props,
    _target: ActorRef, start: Long = 0) extends PersistentActor with ActorLogging with Stash {
    def persistenceId = s"AggregateSubscription/$id/$partition"
    private var pos = start - 1
    private var buffer = SortedSet.empty[EventData]

    private object Config {
      private def config = context.system.settings.config.getConfig("ch.eventsourced.aggregate-subscription")
      def retries = config.getInt("retries-until-restart")
      def retryTimeout = config.getDuration("retry-interval", MILLISECONDS).millis
      val maxBufferSize = config.getLong("max-buffered-messages")
    }

    override val supervisorStrategy = OneForOneStrategy() {
      case _ => Escalate // restart this actor if the journal fails.
    }
    val target = context actorOf OrderPreservingAck.props(_target,
      retryAfter = Config.retryTimeout, retryLimit = Config.retries, maxInFlight = 2) {
      case AggregateEvent(_, _, ack) => _ == ack
    }

    //TODO use snapshots
    //TODO add overflow protection using aggregate journal backpressure and discarding of live events (reread from journal)
    //TODO retries

    def receiveCommand = {
      case Start(liveFrom) =>
        if (liveFrom > pos + 1) {
          //load "missing" events from journal
          context actorOf journalReplay(pos + 1, liveFrom - 1)
          log.debug(s"Started with journal replay (next=${pos + 1}, live events start at $liveFrom)")
        } else {
          log.debug(s"Started with live-events only (next=${pos + 1}, live events start at $liveFrom)")
        }
        context become handleSubscription(liveFrom)
        unstashAll()

      case others => stash()
    }

    def handleSubscription(liveEventsFrom: Long): Receive = {
      case event: EventData if event.sequence > pos && event.sequence < liveEventsFrom =>
        //from the journal
        handleEvent(event)

      case OnEvent(event, ack) =>
        //from the "live-stream"
        if (event.sequence > pos && event.sequence >= liveEventsFrom) {
          handleEvent(event)
        }
        sender ! ack

      case AckEvent(seq) if seq == pos + 1 =>
        persist(EventAcknowledged(seq)) { _ =>
          pos = seq
          //send next to target
          buffer.headOption.filter(_.sequence == pos + 1).foreach { event =>
            target ! AggregateEvent(id, event, AckEvent(event.sequence))
            buffer -= event
          }
        }
      case AckEvent(seq) if seq > pos + 1 =>
        log.warning(s"out of sequence ack $pos => $seq. Ignoring")

      case Close(d) =>
        persist(Closed(d)) {
          case Closed(whenDone) =>
            context.parent ! whenDone
            context stop self
        }
    }

    def receiveRecover = {
      case EventAcknowledged(seq) => pos = seq max pos
      case Closed(whenDone) =>
        context.parent ! whenDone
        context stop self
      case RecoveryCompleted =>
        () //wait for Start message
    }

    def handleEvent(event: EventData) = {
      if (event.sequence == pos + 1) {
        //no outstanding ack, send it to target
        target ! AggregateEvent(id, event, AckEvent(event.sequence))
      } else {
        //enqueue it
        buffer += event
        if (buffer.size > Config.maxBufferSize) throw new RuntimeException(s"Event buffer exceeded max size of ${Config.maxBufferSize}")
      }
    }

  }
  private case class EventAcknowledged(sequence: Long)
  private case class Closed(whenDone: Any)
  private case class AckEvent(sequence: Long)
}