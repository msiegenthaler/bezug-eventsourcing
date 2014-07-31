package es.impl.actor

import akka.actor.{ActorLogging, ActorRef, ActorSystem}
import akka.persistence.{RecoveryCompleted, PersistentActor}
import es.api.{ProcessManager, EventData, ProcessManagerType}
import PubSub._

class ProcessManagerActorManager[T <: ProcessManagerType](managerType: T)
  (system: ActorSystem, pubSub: ActorRef, eventBus: EventBusConfig) {
  import managerType._

  //TODO use at least once delivery for commands
  private class ProcessManagerActor extends PersistentActor with ActorLogging {
    import PubSub.Consumer._
    def persistenceId = self.path.name
    private val id = {
      parseId(persistenceId)
        .getOrElse(throw new IllegalArgumentException(s"$persistenceId is not a valid id for process manager {$managerType}"))
    }
    private var state: Manager = seed(id)
    private var subscriptions: Map[String, SubscriptionState] = Map.empty

    def receiveCommand = {
      case message: Message =>
        persist(message) {
          case msg: Message =>
            persist(msg) { msg => pubSub ! Next(msg.subscription)}

            val result = msg.data match {
              case event: EventData => state.handle.lift(event)
              case _ => None
            }
            result.foreach {
              case (commands, actions, next) =>
                commands foreach { cmd =>
                  //TODO send reliably (at least once?)
                }

                //TODO process actions for subscriptions
                actions

                next match {
                  case Left(ProcessManager.Completed) => shutdown()
                    persist(Finished)(_ => shutdown)
                  case Right(newState) =>
                    // TODO handle
                    state = newState
                }
            }
        }
    }

    def shutdown() = {
      log.info(s"Process $name ($id) finished, shutting down..")
      subscriptions.keys.foreach(sub => pubSub ! Unsubscribe(sub))
      context stop self
    }


    def receiveRecover = {
      case SetupSubscription(id, request) =>
        subscriptions += (id -> SubscriptionState(Position.start, request))
      case DeleteSubscription(id: String) =>
        subscriptions -= id

      case Message(sub, event: EventData, pos) =>
        updateSubscription(sub, pos)
        state.handle.lift(event) foreach {
          case (_, _, Right(next)) => state = next
          case _ => ()
        }

      case Finished =>
        log.warning("Trying to load an already finished process $name ($id)")
        shutdown()

      case RecoveryCompleted =>
        log.debug(s"Loaded from event store.")
        subscriptions.foreach {
          case (id, SubscriptionState(pos, request)) => setupSubscription(id, request, pos)
        }
        log.debug(s"set up ${subscriptions.size} subscriptions, now ready.")
    }

    def updateSubscription(subscriptionId: String, pos: Position) = {
      val nv = subscriptions(subscriptionId).copy(position = pos)
      subscriptions += (subscriptionId -> nv)
    }

    def setupSubscription(id: String, request: ProcessManager.Subscribe, position: Position) = {
      val topic = request match {
        case ProcessManager.SubscribeToAggregate(id) => eventBus.topicFor(id)
        case ProcessManager.SubscribeToAggregateType(at) => eventBus.topicFor(at)
      }
      pubSub ! Subscribe(id, topic, position)
    }

    private var _nextSubscriptionId = 0
    def nextSubscriptionId() = {
      val id = _nextSubscriptionId
      _nextSubscriptionId = _nextSubscriptionId + 1
      s"$persistenceId#subscription-$id"
    }
  }
  private case class SubscriptionState(position: Position, request: ProcessManager.Subscribe)
  private case class SetupSubscription(id: String, request: ProcessManager.Subscribe)
  private case class DeleteSubscription(id: String)
  private case object Finished
}
