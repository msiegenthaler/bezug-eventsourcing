package es

import akka.actor.{ReceiveTimeout, Props, ActorSystem}
import akka.contrib.pattern.ClusterSharding
import akka.contrib.pattern.ShardRegion._
import akka.persistence.PersistentActor
import akka.util.Timeout
import akka.pattern.ask

import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.duration.Duration

trait AggregateActorManager[A <: AggregateType] {
  val aggregateType: A

  import aggregateType._

  def name: String
  def commandToId(cmd: Command): String
  def seed(id: String): Root
  def publishEvent(event: EventData): Unit

  def system: ActorSystem
  def maxNodes: Int
  def inMemoryTimeout: Duration


  protected def regionName = s"aggregate/$name"
  protected val shardCount = maxNodes * 10

  private val idExtractor: IdExtractor = {
    case Command(cmd) => (commandToId(cmd), cmd)
  }
  private val shardResolver: ShardResolver =
    idExtractor.andThen(_._1.hashCode % shardCount).andThen(_.toString)

  private val region = ClusterSharding(system).start(regionName, Some(Props(new AggregateRootActor)), idExtractor, shardResolver)

  private class AggregateRootActor extends PersistentActor {
    def persistenceId = self.path.name
    private var state = seed(persistenceId)

    // evict from memory if not used for some time
    context.setReceiveTimeout(inMemoryTimeout)

    def receiveCommand = {
      case Command(cmd) =>
        state.execute(cmd) match {
          case Right(events) =>
            persist(events) { events =>
              events.foreach(handleEvent)
              events map (e => EventData(state.id, e)) foreach publishEvent
            }
          case Left(Error(error)) =>
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
  }
  private case object PassivateAggregateRoot

  def execute(cmd: Command)(implicit timeout: Timeout, ec: ExecutionContext): Future[Either[A#Error, Unit]] = {
    region ? cmd map {
      case Right(_) => Right(())
      case Left(Error(e)) => Left(e)
    }
  }
}