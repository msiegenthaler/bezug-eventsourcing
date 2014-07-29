package es

import akka.actor.{ReceiveTimeout, Props, ActorSystem}
import akka.contrib.pattern.ClusterSharding
import akka.contrib.pattern.ShardRegion._
import akka.event.EventBus
import akka.persistence.PersistentActor
import akka.util.Timeout
import akka.pattern.ask

import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.duration._
import scalaz.{Failure, Success}


trait AggregateActorBinding[A <: AggregateType] {
  val aggregateType: A

  import aggregateType._

  def name: String
  def commandToId(cmd: Command): String
  def seed(id: String): Root
}

class AggregateActorManager[A <: AggregateType](binding: AggregateActorBinding[A])
  (system: ActorSystem, eventBus: EventBus {type Event >: A#EventData}, maxNodes: Int = 10, inMemoryTimeout: Duration = 5.minutes) {

  import binding.aggregateType._

  protected def regionName = s"aggregate/${binding.name}"
  protected val shardCount = maxNodes * 10

  private val idExtractor: IdExtractor = {
    case Command(cmd) => (binding.commandToId(cmd), cmd)
  }
  private val shardResolver: ShardResolver =
    idExtractor.andThen(_._1.hashCode % shardCount).andThen(_.toString)

  private val region = ClusterSharding(system).start(regionName, Some(Props(new AggregateRootActor)), idExtractor, shardResolver)

  private class AggregateRootActor extends PersistentActor {
    def persistenceId = self.path.name
    private var state = binding.seed(persistenceId)

    // evict from memory if not used for some time
    context.setReceiveTimeout(inMemoryTimeout)

    def receiveCommand = {
      case Command(cmd) =>
        state.execute(cmd) match {
          case Success(events) =>
            persist(events) { events =>
              events.foreach(handleEvent)
              events map (e => EventData(state.id, e)) foreach publishEvent
            }
          case Failure(Error(error)) =>
            sender ! error
        }

      case ReceiveTimeout =>
        //ensure that we have no pending messages
        context.parent ! Passivate(PassivateAggregateRoot)
      case PassivateAggregateRoot =>
        context stop self
    }

    def receiveRecover = {
      case Event(event) => handleEvent(event)
    }

    def handleEvent(event: Event) = state = state.applyEvent(event)
    def publishEvent(event: EventData) = eventBus.publish(event)
  }

  private case object PassivateAggregateRoot

  def execute(cmd: Command)(implicit timeout: Timeout, ec: ExecutionContext): Future[Either[A#Error, Unit]] = {
    region ? cmd map {
      case Right(_) => Right(())
      case Left(Error(e)) => Left(e)
    }
  }
}