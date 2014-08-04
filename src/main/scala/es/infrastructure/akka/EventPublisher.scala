package es.infrastructure.akka

import akka.actor._
import akka.persistence.{PersistentView, RecoveryCompleted, PersistentActor}
import es.api.EventData
import es.infrastructure.akka.AggregateActor.EventEmitted
import es.infrastructure.akka.EventBus.{AggregateEvent, UnsubscribeFromAggregate, SubscribeToAggregate}
import es.support.Guid
import scala.collection.immutable.Queue

/**
 * Responsible to handle events emitted by aggregates.
 *
 * - Forward to pubSub infrastructure
 * - Start process manager if it is an initiation event
 * - Manages (persistent) subscriptions (EventBus.SubscribeToAggregate messages) for its aggregate.
 */
object EventPublisher {

  def props(parentId: String): Props = Props(new Publisher(parentId))

  private type SubscriptionId = String
  private class Publisher(aggregateId: String) extends PersistentActor with ActorLogging {
    def persistenceId = s"$aggregateId\EventPublisher"

    //TODO use snapshots to improve performance

    private var subscribers = Map.empty[SubscriptionId, ActorSelection]
    private var positions = Map.empty[SubscriptionId, Long]
    private var currentSeq = -1L

    object PendingEvents {
      private var pendingEvents = Queue.empty[EventData]
      private var offset = 0
    }

    def receiveCommand = {
      case AggregateActor.OnEvent(event, ack) =>
        //TODO don't persist the complete event..
        persist(event) { _ =>
          positions.filter(_._2 + 1 == event.sequence).keys.foreach { id =>
            val ack = AggregateEventAcknowledged(id, event.sequence)
            subscribers(id) ! AggregateEvent(id, event, ack)
          }
          sender() ! ack
        }

      case e@AggregateEventAcknowledged(id, seq) =>
        positions.get(id).foreach { old =>
          if (old + 1 == seq) persist(e) { _ =>
            positions += id -> seq
            sendNextFor(id)
          }
          else ???
          // TODO log.warning(s"Out of order ack received on subscription $id ($seq received, current is $pos")
        }

      case _ => ()
    }
    def receiveRecover = {
      case SubscribeToAggregate(id, _, path, start) =>
        subscribers += id -> context.actorSelection(path)
        positions += id -> start
      case AggregateEventAcknowledged(id, pos) =>
        positions.get(id) foreach { old =>
          assert(old + 1 == pos, s"invalid ack sequence: $old => $pos")
          positions += id -> pos
        }
      case UnsubscribeFromAggregate(id) =>
        subscribers -= id
        positions -= id

      case event: EventData =>
        assert(event.sequence == currentSeq + 1)
        currentSeq = event.sequence

      case RecoveryCompleted =>
        //TODO ?
        ???
    }

    def sendNextFor(id: SubscriptionId) = {
      val pos = positions(id)
      if (pos <= currentSeq) ()
      ???
      //
    }
  }
  private case class AggregateEventAcknowledged(subscriptionId: SubscriptionId, seq: Long)


  /**
   * Handles a single subscription for aggregate events. Loads old events from the journal if needed.
   *
   * @param start first event (sequence number) to send
   * @param positionWhenStarted the number of the next event that will be emitted (this seq and onwards will be
   *                            sent to this actor as EventData messages.
   */
  private class SubscriptionHandler(id: SubscriptionId, aggregateId: String, start: Long, positionWhenStarted: Long)
    extends Actor with Stash with ActorLogging {
    private var pos = start

    if (start < positionWhenStarted) {
      context actorOf AggregateJournalReplay.props(aggregateId, start, positionWhenStarted - 1)
    }

    def receive = {
      //from journal
      case EventEmitted(seq, event) if (seq == pos) =>
        sendEvent(seq, event)
      //new event
      case EventData(_, _, seq, event) if (seq == pos) =>
        sendEvent(seq, event)

      case AggregateEventAcknowledged(`id`, seq) =>


      case outOfSequence: EventEmitted[_] => stash
      case outOfSequence: EventData => stash
    }

    def sendEvent(seq: Long, event: Any) = {
      pos = pos + 1
      ??? //TODO send
      unstashAll()
    }
  }


}
