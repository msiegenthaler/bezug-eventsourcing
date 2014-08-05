package es.infrastructure.akka

import akka.actor.{ActorRef, Props}
import akka.persistence.{RecoveryCompleted, PersistentActor}
import es.api.{AggregateType, EventData}
import es.infrastructure.akka.AggregateActor.{OnEvent, EventEmitted}
import es.infrastructure.akka.EventBus.AggregateEvent

import scala.collection.immutable.Queue

/**
 * Handles a persistent single subscription for events of a single aggregate.
 * Needs to be started with the Start() message.
 * Loads old events from the journal if needed and afterwards follows a life-event stream of OnEvent messages (must be
 * sent to this actor by the creator).
 * The events are sent to the target as EventBus.AggregateEvent, acks are noted.
 */
object AggregateSubscription {
  /** Send to start the subscription. */
  case class Start(lifeEventsFrom: Long)

  def props(id: String, target: ActorRef, journalReplay: (Long, Long) => Props, startAtEventSequence: Long = 0): Props = {
    Props(new SubscriptionActor(id, journalReplay, target, startAtEventSequence))
  }

  private class SubscriptionActor(id: String, journalReplay: (Long, Long) => Props,
    target: ActorRef, start: Long, maxBufferSize: Long = 1000) extends PersistentActor {
    def persistenceId = s"AggregateSubscription/$id"
    private var pos = start
    private var buffer = Queue.empty[EventData]

    //TODO use snapshots

    def receiveCommand = {
      case Start(liveFrom) =>
        if (liveFrom > start) {
          //load "missing" events from journal
          context actorOf journalReplay(start, liveFrom)
        }
        context become handleSubscription(liveFrom)
    }

    def handleSubscription(liveEventsFrom: Long): Receive = {
      case event: EventData if event.sequence > pos && event.sequence <= liveEventsFrom =>
        //from the journal
        handleEvent(event)

      case OnEvent(event, ack) if event.sequence > pos && event.sequence > liveEventsFrom =>
        //from the "live-stream"
        handleEvent(event)
        sender ! ack

      case AckEvent(seq) if seq > pos =>
        assert(seq == pos + 1, s"out of sequence ack ($pos => $seq)")
        persist(EventAcknowledged(seq)) { _ =>
          pos = seq
          //send next to target
          buffer.dequeueOption foreach {
            case (event, b2) =>
              target ! AggregateEvent(id, event, AckEvent(event.sequence))
              buffer = b2
          }
        }
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

    def receiveRecover = {
      case EventAcknowledged(seq) => pos = seq max pos
      case RecoveryCompleted => () //wait for Start message
    }
  }
  private case class EventAcknowledged(sequence: Long)
  private case class AckEvent(sequence: Long)
}
