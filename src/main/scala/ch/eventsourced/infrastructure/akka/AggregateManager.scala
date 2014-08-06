package ch.eventsourced.infrastructure.akka

import ch.eventsourced.infrastructure.akka.AggregateSubscription.OnEvent
import scala.collection.immutable.Queue
import scala.concurrent.duration._
import akka.actor._
import akka.contrib.pattern.ClusterSharding
import akka.contrib.pattern.ShardRegion._
import akka.persistence.{PersistentActor, RecoveryCompleted}
import scalaz._
import ch.eventsourced.api.{AggregateKey, EventData, AggregateType}
import ch.eventsourced.infrastructure.akka.AggregateSubscriptionManager.{Start, AddManualSubscription}
import AggregateManager._


object AggregateManager {
  sealed trait Command
  sealed trait Event
  /** Execute the command and the reply with onSuccess or onFailed to the sender of the message. */
  case class Execute[Cmd, Err](command: Cmd, onSuccess: Any, onFailed: Err => Any) extends Command
  /**
   * Set up a persistent subscription to the aggregate. The subscriber will continue to receive events after a system
   * restart. So it's important to unsubscribe.
   * The subscription will start at the event with the sequenceId `startEventSeq` (first event delivered will have this seq).
   */
  case class SubscribeToAggregate(subscriptionId: String, aggregate: AggregateKey,
    subscriber: ActorPath, startEventSeq: Long = 0, ack: Any) extends Command

  /** Event delivered a part of the persistent subscription. Reply with `ack`. */
  case class AggregateEvent(subscriptionId: String, event: EventData, ack: Any) extends Event
  /** Remove the (persistent) subscription. */
  case class UnsubscribeFromAggregate(subscriptionId: String, aggregate: AggregateKey, ack: Any) extends Command


  /** Events that are saved to the journal. */
  sealed trait JournalEvent
  /** Emitted events in the journal (for use in persistent view). */
  case class EventEmitted[+Event](sequence: Long, event: Event) extends JournalEvent
}

/**
 * Runs an aggregate type as an akka actor.
 * The individual aggregate roots are distributed across the cluster using cluster sharding. Akka persistence
 * is used for persistence of the events. All persisted events are sent to the event handler as OnEvent
 * messages. This messages must be acknowledged.
 *
 * The aggregate also handles subscriptions. There are two ways:
 * - Aggregate subscriptions: Set up by sending a SubscribeToAggregate to the aggregate. It will survive system
 * restarts, only events that were not ack'd before will be sent.
 * - Aggregate type subscriptions: Use the 'eventSubscriptions' parameter. Will receive events from all aggregates
 * of this type. The state is persistent (which events were already handled).
 * In both cases the events will be sent as AggregateEvent.
 *
 * When an aggregate has not received commands for some time it is removed from memory. At the next command
 * it is again constructed from the persistent events in the event store. This is transparent to the user.
 */
class AggregateManager[I, C, E](contextName: String,
  val aggregateType: AggregateType {type Id = I; type Command = C; type Error = E},
  eventSubscriptions: Map[String, ActorRef])
  (system: ActorSystem, shardCount: Int = 100, inMemoryTimeout: Duration = 5.minutes) {
  import aggregateType._

  /** Execute the command and the reply with onSuccess or onFailed to the sender of the message. */
  type Execute = AggregateManager.Execute[Command, Error]

  /** Handles Command messages. */
  def ref: ActorRef = region


  private val idExtractor: IdExtractor = {
    case msg@Execute(Command(id, cmd), suc, fail) => (serializeId(id), msg)
    case msg@SubscribeToAggregate(_, AggregateKey(id), _, _, _) => (serializeId(id), msg)
    case msg@UnsubscribeFromAggregate(_, AggregateKey(id), _) => (serializeId(id), msg)
  }
  private val shardResolver: ShardResolver =
    idExtractor.andThen(_._1.hashCode % shardCount).andThen(_.toString)

  private val regionName = s"$contextName-aggregate-$name"
  private val region = {
    ClusterSharding(system).start(regionName, Some(Props(new AggregateRootActor)), idExtractor, shardResolver)
  }

  private val journalReplay = new AggregateJournalReplay(aggregateType)

  //TODO command deduplication
  private class AggregateRootActor extends PersistentActor with ActorLogging {
    override val persistenceId = s"$contextName/Aggregate/$name/${self.path.name}"
    private val id = {
      parseId(self.path.name)
        .getOrElse(throw new IllegalArgumentException(s"$persistenceId is not a valid id for aggregate $name"))
    }

    private var eventSeq: Long = 0
    private var state = seed(id)

    val subscriptionManager = {
      def journalProps(from: Long, until: Long) = journalReplay.props(id, persistenceId, from, until)
      val props = AggregateSubscriptionManager.props(persistenceId, journalProps)
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

    log.debug(s"Starting aggregator actor for $name with id $persistenceId")
    // evict from memory if not used for some time
    context.setReceiveTimeout(inMemoryTimeout)

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

      case ReceiveTimeout =>
        //ensure that we have no pending messages
        log.debug(s"Passivation initiated (due to timeout)")
        context.parent ! Passivate(PassivateAggregateRoot)
      case PassivateAggregateRoot =>
        log.debug(s"Passivation completed, actor will stop")
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

  private case class EventDelivered(id: Long) extends JournalEvent
  private case class EventAck(id: Long) extends JournalEvent
  private case object PassivateAggregateRoot
}