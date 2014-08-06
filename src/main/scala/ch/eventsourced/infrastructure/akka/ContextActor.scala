package ch.eventsourced.infrastructure.akka

import java.net.URLEncoder
import scala.concurrent.duration._
import akka.actor.{Props, Actor, ActorRef}
import ch.eventsourced.api.{AggregateType, BoundedContextBackendType}
import pubsub.Topic
import ContextActor._

/** Actor responsible for the bounded context. */
class ContextActor(val definition: BoundedContextBackendType, pubSub: ActorRef, config: Config) extends Actor {
  def createPubSubPublisher(aggregate: AggregateType) = {
    val topic = config.topicFor(definition.name, aggregate)
    val props = new AggregateEventToPubSubPublisher(aggregate).props(pubSub, Set(topic))
    context.actorOf(props, URLEncoder.encode(s"pubSub-publisher-for-${aggregate.name}", "UTF-8"))
  }

  val processMgrs = definition.processManagers.map { pmt =>
    val manager = new ProcessManagerActor[pmt.Command, pmt.Error](definition.name, pmt,
      commandDistributor)(context.system, config.processManagerPartitions)
    manager
  }

  def inititorsFor(aggregate: AggregateType) = {
    processMgrs.
      filter(_.registerOn.contains(aggregate)).
      map { pm =>
      val name = s"ProcessManager/${pm.processManagerType.name}/Initiator"
      (name, pm.initiator)
    }
  }

  val aggregateMgrs = definition.aggregates.map { aggregateType =>
    val publisher = createPubSubPublisher(aggregateType)
    val subscriptions = Map("pubSub" -> publisher) ++
      inititorsFor(aggregateType)

    new AggregateManager(definition.name, aggregateType, subscriptions)(context.system,
      shardCount = config.shardCount, inMemoryTimeout = config.aggregateTimeout)
  }

  val commandDistributor: ActorRef = {
    //TODO the unknown error.. from the definition..
    val props = AggregateCommandDistributor.props[definition.Command, definition.Error](aggregateMgrs, ???)
    context.actorOf(props, "command-distributor")
  }

  //TODO read models

  def receive = {
    case Shutdown =>
      //TODO stop shards?
      context stop self
    case msg: AggregateManager.Command => commandDistributor forward msg
  }
}

object ContextActor {
  def props(definition: BoundedContextBackendType, pubSub: ActorRef, config: Config) =
    Props(new ContextActor(definition, pubSub, config))

  case object Shutdown

  trait Config {
    def topicFor(contextName: String, aggregate: AggregateType) = {
      Topic.root \ contextName \ "Aggregate" \ aggregate.name
    }

    def shardCount: Int
    def aggregateTimeout: Duration
    def processManagerPartitions: Int
  }
  object DefaultConfig extends Config {
    def shardCount = processManagerPartitions
    def aggregateTimeout = 5.minutes
    def processManagerPartitions = 100
  }
}