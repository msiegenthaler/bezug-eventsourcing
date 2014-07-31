package es.impl.actor

import akka.actor._
import akka.contrib.pattern.ClusterSharding
import akka.contrib.pattern.ShardRegion._
import akka.pattern.ask
import akka.persistence.{AtLeastOnceDelivery, PersistentActor, RecoveryCompleted}
import akka.util.Timeout
import es.api.{EventData, AggregateType}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scalaz.Scalaz._
import scalaz._

/**
 * Runs an aggregate type as an akka actor.
 * The individual aggregate roots are distributed across the cluster using cluster sharding. Akka persistence
 * is used for persistence of the events. All persisted events are published to the event bus.
 * After an aggregate has not received commands for some time it is removed
 * from memory. At the next command it is again constructed from the persistent events in the event store.
 *
 * @tparam A the aggregate type
 */
class AggregateActorManager[A <: AggregateType](binding: AggregateBinding[A])
  (system: ActorSystem, pubSub: ActorRef, eventBusConfig: EventBusConfig,
    shardCount: Int = 100, inMemoryTimeout: Duration = 5.minutes) {
  import binding.aggregateType._

  private val idExtractor: IdExtractor = {
    case Command(cmd) => (binding.commandToId(cmd), cmd)
    case any => ("", any)
  }
  private val shardResolver: ShardResolver =
    idExtractor.andThen(_._1.hashCode % shardCount).andThen(_.toString)

  private val regionName = s"${binding.aggregateType.name}-aggregate"
  private val region = {
    ClusterSharding(system).start(regionName, Some(Props(new AggregateRootActor)), idExtractor, shardResolver)
  }

  //TODO at least once delivery for pub/sub
  //TODO command deduplication
  private class AggregateRootActor extends PersistentActor with AtLeastOnceDelivery with ActorLogging {
    override def persistenceId = self.path.name
    private var eventSeq: Long = 0
    private var state = binding.seed(persistenceId)

    log.debug(s"Starting aggregator actor for ${binding.aggregateType.name} with id $persistenceId")

    // evict from memory if not used for some time
    context.setReceiveTimeout(inMemoryTimeout)

    def receiveCommand = {
      case Command(cmd) =>
        state.execute(cmd) match {
          case Success(events) if events.isEmpty =>
            sender() ! ().success
          case Success(events) =>
            events.dropRight(1).foreach(persist(_)(handleEvent))
            persist(events.last) { event =>
              handleEvent(event)
              sender() ! ().success
            }
          case Failure(Error(error)) =>
            sender() ! error.fail
        }

      case PubSubAck(id) =>
        persist(DeliveredToPubSub(id)) { _ => confirmDelivery(id)}

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
      case DeliveredToPubSub(id) => confirmDelivery(id)
      case RecoveryCompleted => log.info(s"Events successfully applied")
    }

    def handleEvent(event: Event) = {
      state = state applyEvent event
      //at-least-once trait replays it if needed (no ack received)
      publishEvent(binding.aggregateType.Event.Data(state.id, eventSeq, event))
      eventSeq = eventSeq + 1
    }
    def publishEvent(event: EventData) = {
      //TODO id serialization
      val topic = eventBusConfig.topicFor(binding.aggregateType, state.id.toString)
      deliver(pubSub.path, id => PubSub.Producer.Publish(topic, event, PubSubAck(id)))
    }
  }

  private case class PubSubAck(id: Long)
  private case class DeliveredToPubSub(id: Long)
  private case object PassivateAggregateRoot

  def execute(cmd: Command)(implicit timeout: Timeout, ec: ExecutionContext): Future[Validation[A#Error, Unit]] = {
    region ? cmd map {
      case Success(_) => ().success
      case Failure(binding.aggregateType.Error(e)) => e.failure
    }
  }
}