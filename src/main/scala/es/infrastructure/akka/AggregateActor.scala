package es.infrastructure.akka

import scala.collection.immutable.Queue
import scala.concurrent.duration._
import akka.actor._
import akka.contrib.pattern.ClusterSharding
import akka.contrib.pattern.ShardRegion._
import akka.persistence.{PersistentActor, RecoveryCompleted}
import scalaz._
import es.api.{EventData, AggregateType}


case class OnEvent(event: EventData, ack: Any)

/**
 * Runs an aggregate type as an akka actor.
 * The individual aggregate roots are distributed across the cluster using cluster sharding. Akka persistence
 * is used for persistence of the events. All persisted events are sent to the event handler as OnEvent
 * messages. This messages must be acknowledged.
 *
 * When an aggregate has not received commands for some time it is removed from memory. At the next command
 * it is again constructed from the persistent events in the event store. This is transparent to the user.
 *
 * @tparam A the aggregate type
 */
class AggregateActorManager[A <: AggregateType](val aggregateType: A, eventHandler: ActorRef)
  (system: ActorSystem, shardCount: Int = 100, inMemoryTimeout: Duration = 5.minutes) {
  import aggregateType._

  sealed trait CommandResult
  case class CommandExecuted(command: Command) extends CommandResult
  case class CommandFailed(command: Command, error: Error) extends CommandResult

  /** Handles Command messages. */
  def ref: ActorRef = region


  private val idExtractor: IdExtractor = {
    case Command(id, cmd) => (serializeId(id), cmd)
    case any => ("", any)
  }
  private val shardResolver: ShardResolver =
    idExtractor.andThen(_._1.hashCode % shardCount).andThen(_.toString)

  private val regionName = s"$name-aggregate"
  private val region = {
    ClusterSharding(system).start(regionName, Some(Props(new AggregateRootActor)), idExtractor, shardResolver)
  }

  //TODO command deduplication
  private class AggregateRootActor extends PersistentActor with ActorLogging {
    override def persistenceId = self.path.name
    private val id = {
      parseId(persistenceId)
        .getOrElse(throw new IllegalArgumentException(s"$persistenceId is not a valid id for aggregate $name"))
    }

    private var eventSeq: Long = 0
    private var state = seed(id)

    //ensures the correct ordering of events and retries sending
    val eventTarget = context actorOf OrderPreservingAck.props(eventHandler) {
      case msg: OnEvent => msg.ack
    }

    log.debug(s"Starting aggregator actor for $name with id $persistenceId")
    // evict from memory if not used for some time
    context.setReceiveTimeout(inMemoryTimeout)

    def receiveCommand = {
      case Command(`id`, cmd) =>
        state.execute(cmd) match {
          case Success(events) if events.isEmpty =>
            sender() ! CommandExecuted(cmd)
          case Success(events) =>
            events.dropRight(1).foreach(persist(_)(handleEvent))
            persist(events.last) { event =>
              handleEvent(event)
              sender() ! CommandExecuted(cmd)
            }
          case Failure(Error(error)) =>
            sender() ! CommandFailed(cmd, error)
        }

      case EventAck(id) =>
        persist(EventDelivered(id)) { _ => confirmDelivery(id)}

      case ReceiveTimeout =>
        //ensure that we have no pending messages
        log.debug(s"Passivation initiated (due to timeout)")
        context.parent ! Passivate(PassivateAggregateRoot)
      case PassivateAggregateRoot =>
        log.debug(s"Passivation completed, actor will stop")
        context stop self
    }

    def receiveRecover = {
      case Event(event) => handleEvent(event)
      case EventDelivered(id) => confirmDelivery(id)
      case RecoveryCompleted =>
        log.info(s"Events successfully applied")
        sendOutstandingEvents()
    }

    def handleEvent(event: Event) = {
      state = state.applyEvent.lift(event).
        getOrElse(throw new IllegalStateException(s"Cannot apply event $event to $state. Not handled."))
      //at-least-once trait replays it if needed (no ack received)
      deliver(Event.Data(state.id, eventSeq, event))
      eventSeq = eventSeq + 1
    }

    private var deliveryConfirmedUpTo: Long = -1
    private var toSendOnRecovery = Queue.empty[EventData]
    def confirmDelivery(eventSeq: Long) = if (eventSeq > deliveryConfirmedUpTo) {
      assert(eventSeq == deliveryConfirmedUpTo + 1, s"out-of-order ack received")
      deliveryConfirmedUpTo = eventSeq
      if (recoveryRunning) toSendOnRecovery = toSendOnRecovery.dequeue._2
    }
    def deliver(event: EventData) = {
      if (recoveryRunning) toSendOnRecovery = toSendOnRecovery enqueue event
      else eventTarget ! OnEvent(event, EventAck(event.sequence))
    }
    def sendOutstandingEvents() = {
      toSendOnRecovery.foreach { event =>
        eventTarget ! OnEvent(event, EventAck(event.sequence))
      }
      toSendOnRecovery = Queue.empty
    }
  }

  private case class EventDelivered(id: Long)
  private case class EventAck(id: Long)
  private case object PassivateAggregateRoot
}