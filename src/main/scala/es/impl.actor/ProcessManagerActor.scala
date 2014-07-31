package es.impl.actor

import akka.actor.{ActorLogging, ActorRef, ActorSystem}
import akka.persistence.{AtLeastOnceDelivery, RecoveryCompleted, PersistentActor}
import es.api.{ProcessManager, EventData, ProcessManagerType}
import PubSub._
import es.impl.actor.PubSub.Producer.Publish

class ProcessManagerActorManager[T <: ProcessManagerType](managerType: T)
  (system: ActorSystem, pubSub: ActorRef, eventBus: EventBusConfig) {
  import managerType._

  private def subscriptionFor(unsub: ProcessManager.Unsubscribe) = unsub match {
    case ProcessManager.UnsubscribeFromAggregate(aggregate) =>
      val subscription = ProcessManager.SubscribeToAggregate(aggregate)
    case ProcessManager.UnsubscribeFromAggregateType(aggregateType) =>
      val subscription = ProcessManager.SubscribeToAggregateType(aggregateType)
  }
  private def topicFor(subscription: ProcessManager.Subscribe) = subscription match {
    case ProcessManager.SubscribeToAggregate(id) => eventBus.topicFor(id)
    case ProcessManager.SubscribeToAggregateType(at) => eventBus.topicFor(at)
  }

  private class ProcessManagerActor extends PersistentActor with AtLeastOnceDelivery with ActorLogging {
    import PubSub.Consumer._
    override def persistenceId = self.path.name
    private val id = {
      parseId(persistenceId)
        .getOrElse(throw new IllegalArgumentException(s"$persistenceId is not a valid id for process manager {$managerType}"))
    }
    private var state: Manager = seed(id)
    private var subscriptions: Map[String, SubscriptionState] = Map.empty

    def nextSubscriptionId() = {
      val id = _nextSubscriptionId
      _nextSubscriptionId = _nextSubscriptionId + 1
      s"$persistenceId#subscription-$id"
    }
    private var _nextSubscriptionId = 0


    def receiveCommand = {
      case message: Message =>
        persist(message) {
          case msg: Message =>
            persist(msg) { msg => pubSub ! Next(msg.subscription)}

            (msg.data match {
              case event: EventData => state.handle.lift(event)
              case _ => None
            }) foreach {
              case (commands, actions, next) =>
                commands foreach (persist(_)(publishCommand))

                actions.foreach {
                  case s: ProcessManager.Subscribe =>
                    persist(SubscriptionAdded(nextSubscriptionId(), s)) { event =>
                      startSubscription(event.id, event.request, Position.start)
                    }
                  case s: ProcessManager.Unsubscribe =>
                    subscriptions.find(_._2.request == subscriptionFor(s)).foreach {
                      case (id, _) =>
                        persist(SubscriptionRemoved(id)) { event =>
                          stopSubscription(id)
                        }
                    }
                }

                next match {
                  case Left(ProcessManager.Completed) => shutdown()
                    persist(Finished)(_ => shutdown)
                  case Right(newState) =>
                    state = newState
                }
            }
        }

      case PubSubAck(id) =>
        persist(CommandDeliveredToPubSub(id)) { _ => confirmDelivery(id)}
    }

    def receiveRecover = {
      case SubscriptionAdded(id, request) =>
        subscriptions += (id -> SubscriptionState(Position.start, request))
      case SubscriptionRemoved(id: String) =>
        subscriptions -= id

      case Message(sub, event: EventData, pos) =>
        updateSubscription(sub, pos)
        state.handle.lift(event) foreach {
          case (_, _, Right(next)) => state = next
          case _ => ()
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
    private def updateSubscription(subscriptionId: String, pos: Position) = {
      val nv = subscriptions(subscriptionId).copy(position = pos)
      subscriptions += (subscriptionId -> nv)
    }

    def startSubscription(id: String, request: ProcessManager.Subscribe, position: Position) = {
      //TODO need to await confirmation and retry if it did not work..
      // possibly need to use a child actor to do that
      pubSub ! Subscribe(id, topicFor(request), position)
    }

    def stopSubscription(id: String) = {
      //TODO need to await confirmation and retry if it did not work..
      // possibly need to use a child actor to do that
      pubSub ! Unsubscribe(id)
    }

    def publishCommand(command: Command) = {
      deliver(pubSub.path, delivery => Publish(eventBus.commandTopic, command, PubSubAck(delivery)))
    }

    def shutdown() = {
      log.info(s"Process $name ($id) finished, shutting down..")
      subscriptions.keys.foreach(sub => pubSub ! Unsubscribe(sub))
      context stop self
    }
  }

  //events
  private case class SubscriptionAdded(id: String, request: ProcessManager.Subscribe)
  private case class SubscriptionRemoved(id: String)
  private case class CommandEmitted(command: Command)
  private case class CommandDeliveredToPubSub(id: Long)
  private case object Finished

  private case class SubscriptionState(position: Position, request: ProcessManager.Subscribe)
  private case class PubSubAck(id: Long)
}