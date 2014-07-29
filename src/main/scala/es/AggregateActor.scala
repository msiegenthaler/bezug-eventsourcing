package es

import akka.actor.{ActorLogging, ReceiveTimeout, Props, ActorSystem}
import akka.contrib.pattern.ClusterSharding
import akka.contrib.pattern.ShardRegion._
import akka.event.EventBus
import akka.persistence.{RecoveryCompleted, PersistentActor}
import akka.util.Timeout
import akka.pattern.ask

import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.duration._
import scalaz._
import Scalaz._


trait AggregateActorBinding[A <: AggregateType] {
  val aggregateType: A

  import aggregateType._

  def name: String
  def commandToId(cmd: Command): String
  def seed(id: String): Root
}

class AggregateActorManager[A <: AggregateType](binding: AggregateActorBinding[A])
  (system: ActorSystem, eventBus: EventBus {type Event >: A#EventData},
    maxNodes: Int = 10, inMemoryTimeout: Duration = 5.minutes) {

  import binding.aggregateType._

  protected def regionName = s"${binding.name}-aggregate"
  protected val shardCount = maxNodes * 10

  private val idExtractor: IdExtractor = {
    case Command(cmd) => (binding.commandToId(cmd), cmd)
    case any => ("", any)
  }
  private val shardResolver: ShardResolver =
    idExtractor.andThen(_._1.hashCode % shardCount).andThen(_.toString)

  private val region = {
    ClusterSharding(system).start(regionName, Some(Props(new AggregateRootActor)), idExtractor, shardResolver)
  }

  private class AggregateRootActor extends PersistentActor with ActorLogging {
    def persistenceId = self.path.name
    private var state = binding.seed(persistenceId)

    log.debug(s"Starting aggregator actor for ${binding.name} with id $persistenceId")

    // evict from memory if not used for some time
    context.setReceiveTimeout(inMemoryTimeout)

    def receiveCommand = {
      case Command(cmd) =>
        state.execute(cmd) match {
          case Success(events) if events.isEmpty =>
            sender() ! ().success
          case Success(events) =>
            def eventHandler(event: Event) = {
              handleEvent(event)
              publishEvent(EventData(state.id, event))
            }
            events.dropRight(1).foreach(persist(_)(eventHandler))
            persist(events.last) { event =>
              eventHandler(event)
              sender() ! ().success
            }
          case Failure(Error(error)) =>
            sender() ! error.fail
        }

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
      case RecoveryCompleted => log.info(s"Events successfully applied")
    }

    def handleEvent(event: Event) =
      state = state applyEvent event
    def publishEvent(event: EventData) = eventBus publish event
  }

  private case object PassivateAggregateRoot

  def execute(cmd: Command)(implicit timeout: Timeout, ec: ExecutionContext): Future[Validation[A#Error, Unit]] = {
    region ? cmd map {
      case Success(_) => ().success
      case Failure(binding.aggregateType.Error(e)) => e.failure
    }
  }
}