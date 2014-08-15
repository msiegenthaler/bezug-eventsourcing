package ch.eventsourced.infrastructure.akka

import scala.collection.immutable.Queue
import scala.concurrent.duration._
import akka.actor._
import akka.persistence.{PersistentActor, RecoveryCompleted}
import scalaz._
import ch.eventsourced.api.{EventData, AggregateKey, AggregateType}
import ch.eventsourced.support.CompositeName
import ch.eventsourced.infrastructure.akka.AggregateSubscription.OnEvent
import ch.eventsourced.infrastructure.akka.AggregateSubscriptionManager.{Start, AddManualSubscription}
import ch.eventsourced.infrastructure.akka.AggregateActor._

object AggregateActor {
  sealed trait Command
  sealed trait Event
  /** Execute the command and the reply with onSuccess or onFailed to the sender of the message. */
  case class Execute[Cmd, Err](command: Cmd, onSuccess: Any, onFailed: Err => Any) extends Command
  /**
   * Set up a persistent subscription to the aggregate. The subscriber will continue to receive events after a system
   * restart. So it's important to unsubscribe.
   * The subscription will start at the event with the sequenceId `startEventSeq` (first event delivered will have this seq).
   */
  case class SubscribeToAggregate(subscriptionId: SubscriptionId, aggregate: AggregateKey,
    subscriber: ActorPath, startEventSeq: Long = 0, ack: Any) extends Command
  /** Event delivered a part of the persistent subscription. Reply with `ack`. */
  case class AggregateEvent(subscriptionId: SubscriptionId, event: EventData, ack: Any) extends Event
  /** Remove the (persistent) subscription. */
  case class UnsubscribeFromAggregate(subscriptionId: SubscriptionId, aggregate: AggregateKey, ack: Any) extends Command


  /** Events that are saved to the journal. */
  sealed trait JournalEvent
  /** Emitted events in the journal (for use in persistent view). */
  case class EventEmitted[+Event](sequence: Long, event: Event) extends JournalEvent

  type SubscriptionId = CompositeName
}

/**
 * Runs an aggregate type as an akka actor.
 * The individual aggregate roots can be distributed across the cluster using cluster sharding. Akka persistence
 * is used for persistence of the events. All persisted events are sent to the event handler as OnEvent
 * messages. This messages must be acknowledged.
 *
 * The aggregate also handles subscriptions. There are two ways:
 * - Aggregate subscriptions: Set up by sending a SubscribeToAggregate to the aggregate. It will survive system
 * restarts, only events that were not ack'd before will be sent.
 * - Aggregate type subscriptions: Use the 'eventSubscriptions' parameter. Will receive events from all aggregates
 * of this type. The state is persistent (which events were already handled).
 * In both cases the events will be sent as AggregateEvent.
 */
class AggregateActor[I, C, E](contextName: String,
  val aggregateType: AggregateType {type Id = I; type Command = C; type Error = E},
  eventSubscriptions: Map[SubscriptionId, ActorRef]) extends ShardedActor[I] {
  import aggregateType._

  def name = CompositeName(contextName) / "aggregate" / aggregateType.name
  def serializeId(id: I) = aggregateType.serializeId(id)
  def parseId(value: String) = aggregateType.parseId(value)

  def messageSelector = {
    case Execute(Command(id, cmd), suc, fail) => id
    case SubscribeToAggregate(_, AggregateKey(id), _, _, _) => id
    case UnsubscribeFromAggregate(_, AggregateKey(id), _) => id
  }
  def props(publicRef: ActorRef, id: Id, name: CompositeName) = Props(new AggregateInstance(id, name))

  private val journalReplay = new AggregateJournalReplay(aggregateType)
  private case class EventDelivered(id: Long) extends JournalEvent
  private case class EventAck(id: Long) extends JournalEvent
  private case object PassivateAggregateRoot

  private class AggregateInstance(id: Id, name: CompositeName) extends PersistentActor with ActorLogging {
    val persistenceId = name.serialize
    private var eventSeq: Long = 0
    private var state = seed(id)

    val subscriptionManager = {
      def journalProps(from: Long, until: Long) = journalReplay.props(id, persistenceId, from, until)
      val props = AggregateSubscriptionManager.props(name, journalProps)
      val mgr = context.actorOf(props, "SubscriptionManager")
      eventSubscriptions foreach {
        case (subId, target) => mgr ! AddManualSubscription(subId, persistenceId, target)
      }
      mgr
    }
    def eventTarget = context actorOf OrderPreservingAck.props(subscriptionManager,
      retryAfter = 1.second, retryLimit = 120, maxInFlight = 2000) {
      case OnEvent(_, ack) => _ == ack
    }

    log.debug(s"Starting aggregator actor for ${aggregateType.name} with id $persistenceId")

    def receiveCommand = {
      case Execute(Command(`id`, cmd), success, fail) =>
        state.execute(cmd) match {
          case Success(events) if events.isEmpty =>
            sender() ! success
          case Success(events) =>
            val toEmit = events.map { e =>
              val ee = EventEmitted[Event](eventSeq, e)
              eventSeq = eventSeq + 1
              ee
            }
            toEmit.dropRight(1).foreach(persist(_)(handleEvent))
            persist(toEmit.last) {
              event =>
                handleEvent(event)
                sender() ! success
            }
          case Failure(Error(error)) =>
            sender() ! fail(error.asInstanceOf)
        }

      case EventAck(seq) if (seq > deliveryConfirmedUpTo) =>
        if (seq == deliveryConfirmedUpTo + 1) {
          persist(EventDelivered(seq)) { _ => confirmDelivery(seq)}
        } else {
          log.warning(s"Ignoring out-of-order delivery ($seq does not follow $deliveryConfirmedUpTo).")
        }

      case s: SubscribeToAggregate => subscriptionManager ! s
      case s: UnsubscribeFromAggregate => subscriptionManager ! s

      case RequestPassivation(yes, no) =>
        if (deliveryConfirmedUpTo == eventSeq - 1)
          sender() ! yes
        else
          sender() ! no
      case Passivate =>
        context stop self
    }

    def receiveRecover = {
      case e@EventEmitted(seq, Event(event)) =>
        handleEvent(EventEmitted(seq, event))
        eventSeq = seq + 1
      case EventDelivered(id) => confirmDelivery(id)
      case RecoveryCompleted =>
        log.info(s"Events successfully applied, ${toSendOnRecovery.size} left to deliver")
        subscriptionManager ! Start(eventSeq - toSendOnRecovery.size)
        sendUnconfirmedEvents()
    }

    def handleEvent(emit: EventEmitted[Event]) = {
      val event = emit.event
      state = state.applyEvent.lift(event).
        getOrElse(throw new IllegalStateException(s"Cannot apply event $event to $state. Not handled."))
      //at-least-once trait replays it if needed (no ack received)
      deliver(Event.Data(state.id, emit.sequence, event))
    }

    private var deliveryConfirmedUpTo: Long = -1
    private var toSendOnRecovery = Queue.empty[EventData]
    def confirmDelivery(seq: Long) = {
      if (seq != deliveryConfirmedUpTo + 1)
        throw new IllegalStateException(s"out-of-order ack: $eventSeq does not follow $deliveryConfirmedUpTo")
      deliveryConfirmedUpTo = seq
      if (recoveryRunning) toSendOnRecovery = toSendOnRecovery.dequeue._2
    }
    def deliver(event: EventData) = {
      if (recoveryRunning) toSendOnRecovery = toSendOnRecovery enqueue event
      else eventTarget ! OnEvent(event, EventAck(event.sequence))
    }
    def sendUnconfirmedEvents() = {
      toSendOnRecovery.foreach { event =>
        eventTarget ! OnEvent(event, EventAck(event.sequence))
      }
      toSendOnRecovery = Queue.empty
    }
  }
}
