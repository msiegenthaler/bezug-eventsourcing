package ch.eventsourced.infrastructure.akka

import java.net.URLEncoder
import scala.concurrent.duration._
import akka.actor.{Props, Actor, ActorRef}
import ch.eventsourced.api.{AggregateType, BoundedContextBackendType}
import ch.eventsourced.support.CompositeName
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
    val manager = new ProcessManagerActor[pmt.Id, pmt.Command, pmt.Error](definition.name, pmt,
      commandDistributor)
    val actor: ActorRef = ??? // TODO start with sharding
  val initiator: ActorRef = ??? // TODO start with sharding
    (manager, actor, initiator)
  }

  def inititorsFor(aggregate: AggregateType) = {
    processMgrs.
      filter(_._1.registerOn.contains(aggregate)).
      map {
      case (pm, _, initiator) =>
        val id = pm.name / "initiator"
        (id, initiator)
    }
  }

  val aggregateMgrs = definition.aggregates.map { aggregateType =>
    val publisher = createPubSubPublisher(aggregateType)
    val subscriptions = Map(CompositeName("pubSub") -> publisher) ++
      inititorsFor(aggregateType)

    val manager = new AggregateActor(definition.name, aggregateType, subscriptions)
    val actor: ActorRef = ??? // TODO start with sharding
    (manager, actor)
  }

  val commandDistributor: ActorRef = {
    //TODO the unknown error.. from the definition..
    val props = AggregateCommandDistributor.props[definition.Command, definition.Error](aggregateMgrs.toMap, ???)
    context.actorOf(props, "command-distributor")
  }

  //TODO read models

  def receive = {
    case Shutdown =>
      //TODO stop shards?
      context stop self
    case msg: AggregateActor.Command => commandDistributor forward msg
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