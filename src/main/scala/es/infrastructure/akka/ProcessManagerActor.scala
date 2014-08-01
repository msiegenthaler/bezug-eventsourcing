package es.infrastructure.akka

import scala.concurrent.duration._
import akka.actor._
import akka.contrib.pattern.{ClusterSingletonManager, ClusterSharding}
import akka.contrib.pattern.ShardRegion._
import akka.persistence.{AtLeastOnceDelivery, RecoveryCompleted, PersistentActor}
import pubsub._
import es.api.{ProcessManager, EventData, ProcessManagerType}
import es.api.ProcessManager.SubscriptionAction
import es.infrastructure.akka.ProcessInitiator._

/**
 * Runs the process manager instance as actors and starts them as needed.
 */
class ProcessManagerActorManager[T <: ProcessManagerType](managerType: T)
  (system: ActorSystem, pubSub: ActorRef, eventBusConfig: EventBusConfig,
    shardCount: Int = 100, inMemoryTimeout: Duration = 5.minutes) {
  import managerType._

  private val idExtractor: IdExtractor = {
    case msg@StartProcessManager(pm, _, _) => (pm, msg)
  }
  private val shardResolver: ShardResolver =
    idExtractor.andThen(_._1.hashCode % shardCount).andThen(_.toString)
  private val regionName = s"$name-processManager"
  private val region = {
    ClusterSharding(system).start(regionName, Some(ProcessManagerActor.props), idExtractor, shardResolver)
  }

  private val initiator = {
    val fun = initiate.andThen(serializeId)
    val props = ProcessInitiator.props(pubSub, region, eventBusConfig)(s"$regionName-initiator", triggeredBy, fun)
    system.actorOf(ClusterSingletonManager.props(props, regionName, PoisonPill, None))
  }

  val commandTopics = Set(eventBusConfig.commandTopic)

  /** Manages a ProcessManager instance. */
  object ProcessManagerActor {
    def props = Props(new ProcessManagerActor)

    sealed trait ProcessManagerActorEvent
    private case class SubscriptionAdded(id: String, request: ProcessManager.Subscribe) extends ProcessManagerActorEvent
    private case class SubscriptionRemoved(id: String) extends ProcessManagerActorEvent
    private case class CommandEmitted(command: Command) extends ProcessManagerActorEvent
    private case class CommandDeliveredToPubSub(id: Long) extends ProcessManagerActorEvent
    private case object Finished extends ProcessManagerActorEvent

    private case class SubscriptionState(position: Position, request: ProcessManager.Subscribe)
    private case class PubSubAck(id: Long)
    private case object PassivateProcessManager

    //TODO handle initiate
    //TODO deduplication of events?
    private class ProcessManagerActor extends PersistentActor with AtLeastOnceDelivery with ActorLogging {
      import Consumer._
      override def persistenceId = self.path.name
      private val id = {
        parseId(persistenceId)
          .getOrElse(throw new IllegalArgumentException(s"$persistenceId is not a valid id for process manager {$managerType}"))
      }
      private var state: Manager = seed(id)

      context.setReceiveTimeout(inMemoryTimeout)

      def receiveCommand = {
        case message: Message =>
          persist(message) {
            case msg: Message =>
              persist(msg) { msg => pubSub ! msg.ack}

              (msg.data match {
                case event: EventData => state.handle.lift(event)
                case _ => None
              }) foreach {
                case Continue(next, commands, actions) =>
                  handleCommands(commands)
                  handleSubscriptionActions(actions)
                  state = next
                case Completed(commands) =>
                  handleCommands(commands)
                  persist(Finished)(_ => shutdown)
              }
          }

        case PubSubAck(id) =>
          persist(CommandDeliveredToPubSub(id)) { _ => confirmDelivery(id)}

        case ReceiveTimeout =>
          //ensure that we have no pending messages
          log.debug(s"Passivation initiated (due to timeout)")
          context.parent ! Passivate(PassivateProcessManager)
        case PassivateProcessManager =>
          log.debug(s"Passivation completed, actor will stop")
          context stop self
      }

      private def handleCommands(commands: Seq[Command]) = {
        commands foreach (persist(_)(publishCommand))
      }

      private def handleSubscriptionActions(actions: Seq[SubscriptionAction]) = actions.foreach {
        case s: ProcessManager.Subscribe =>
          persist(SubscriptionAdded(nextSubscriptionId(), s)) { event =>
            subscriptions += (event.id -> SubscriptionState(Position.start, s))
            startSubscription(event.id, event.request, Position.start)
          }
        case s: ProcessManager.Unsubscribe =>
          subscriptions.find(_._2.request == subscriptionFor(s)).foreach {
            case (id, _) =>
              persist(SubscriptionRemoved(id)) { event =>
                subscriptions -= id
                stopSubscription(id)
              }
          }
      }

      def receiveRecover = {
        case SubscriptionAdded(id, request) =>
          subscriptions += (id -> SubscriptionState(Position.start, request))
        case SubscriptionRemoved(id: String) =>
          subscriptions -= id

        case Message(sub, event: EventData, pos) =>
          updateSubscription(sub, pos)
          state.handle.lift(event) foreach {
            case Continue(next, _, _) => state = next
            case Completed(_) => ()
          }
        case CommandEmitted(command) =>
          publishCommand(command)
        case CommandDeliveredToPubSub(id) =>
          confirmDelivery(id)
        case Finished =>
          log.warning("process already finished.")
          shutdown()

        case RecoveryCompleted =>
          log.debug(s"Loaded from event store.")
          subscriptions.foreach {
            case (id, SubscriptionState(pos, request)) => startSubscription(id, request, pos)
          }
          log.debug(s"set up ${subscriptions.size} subscriptions, now ready.")
      }

      def updateSubscription(subscriptionId: String, update: PositionUpdate) = {
        val old = subscriptions(subscriptionId)
        val nv = old.copy(position = update(old.position))
        subscriptions += (subscriptionId -> nv)
      }
      private var subscriptions: Map[String, SubscriptionState] = Map.empty
      def nextSubscriptionId() = {
        val id = _nextSubscriptionId
        _nextSubscriptionId = _nextSubscriptionId + 1
        s"$persistenceId#subscription-$id"
      }
      private var _nextSubscriptionId = 0

      def startSubscription(id: String, request: ProcessManager.Subscribe, position: Position) = {
        val msg = Subscribe(id, Set(eventBusConfig.topicFor(request)), position)
        val props = SubscriptionManager.props(pubSub, msg)
        val actor = context actorOf props
        subscriptionManagers += id -> actor
      }
      def stopSubscription(id: String) = {
        subscriptionManagers.get(id) foreach { actor =>
          actor ! Unsubscribe
          subscriptionManagers -= id
        }
      }
      private var subscriptionManagers: Map[String, ActorRef] = Map.empty

      def publishCommand(command: Command) = {
        deliver(pubSub.path, delivery => Producer.Publish(commandTopics, command, PubSubAck(delivery)))
      }

      def shutdown() = {
        log.info(s"Process $name ($id) finished, shutting down..")
        subscriptions.keys.foreach(sub => pubSub ! Unsubscribe(sub))
        context stop self
      }
    }
  }

  private def subscriptionFor(unsub: ProcessManager.Unsubscribe) = unsub match {
    case ProcessManager.UnsubscribeFromAggregate(aggregate) =>
      ProcessManager.SubscribeToAggregate(aggregate)
    case ProcessManager.UnsubscribeFromAggregateType(aggregateType) =>
      ProcessManager.SubscribeToAggregateType(aggregateType)
  }
}