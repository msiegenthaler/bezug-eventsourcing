package es.infrastructure.akka

import scala.collection.immutable.Queue
import akka.actor._
import akka.actor.SupervisorStrategy.{Escalate, Restart}
import akka.persistence.{RecoveryCompleted, PersistentActor}
import es.api.EventData
import es.infrastructure.akka.AggregateActor.OnEvent
import es.infrastructure.akka.EventBus.AggregateEvent

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
  /** Close the subscription. Events pending ack will be discarded. */
  case class Close(whenDone: Any)

  def props(id: String, target: ActorRef, journalReplay: (Long, Long) => Props, startAtEventSequence: Long = 0): Props = {
    Props(new SubscriptionActor(id, journalReplay, target, startAtEventSequence))
  }

  private class SubscriptionActor(id: String, journalReplay: (Long, Long) => Props,
    target: ActorRef, start: Long = 0, maxBufferSize: Long = 1000) extends PersistentActor with ActorLogging with Stash {
    def persistenceId = s"AggregateSubscription/$id"
    private var pos = start - 1
    private var buffer = Queue.empty[EventData]

    override val supervisorStrategy = OneForOneStrategy() {
      case _ => Escalate // restart this actor if the journal fails.
    }

    //TODO use snapshots
    //TODO add overflow protection using aggregate journal backpressure and discarding of live events (reread from journal)

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
          buffer.dequeueOption foreach {
            case (event, b2) =>
              target ! AggregateEvent(id, event, AckEvent(event.sequence))
              buffer = b2
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
        buffer = buffer enqueue event
        if (buffer.size > maxBufferSize) throw new RuntimeException(s"Event buffer exceeded max size of $maxBufferSize")
      }
    }

  }
  private case class EventAcknowledged(sequence: Long)
  private case class Closed(whenDone: Any)
  private case class AckEvent(sequence: Long)
}
