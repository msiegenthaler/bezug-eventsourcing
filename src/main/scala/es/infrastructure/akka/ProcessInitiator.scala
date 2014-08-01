package es.infrastructure.akka

import akka.actor.{ActorRef, Props}
import akka.persistence.{RecoveryCompleted, AtLeastOnceDelivery, PersistentActor}
import es.api.{EventData, ProcessManager}
import pubsub.{SubscriptionManager, PositionUpdate, Consumer, Position}

/** Sets up a subscription and checks if a initiated new process managers. */
private object ProcessInitiator {
  sealed trait Command
  sealed trait Event

  /** Is sent to the receiver, must be answered with ProcessStarted, as soon as it is persisted. */
  case class StartProcessManager(processManagerId: String, withEvent: EventData, deliveryId: Long) extends Event
  case class ProcessStarted(deliveryId: Long) extends Command

  def props(pubSub: ActorRef, receiver: ActorRef, eventBusConfig: EventBusConfig)
    (name: String, subscriptions: Traversable[ProcessManager.Subscribe], initiate: PartialFunction[EventData, String]) = {
    Props(new InitiatorActor(name, subscriptions, receiver, initiate, pubSub, eventBusConfig))
  }

  //TODO add snapshotting (big performance improvement)
  private class InitiatorActor(
    name: String, subscriptions: Traversable[ProcessManager.Subscribe],
    receiver: ActorRef, initiate: PartialFunction[EventData, String],
    pubSub: ActorRef, eventBusConfig: EventBusConfig)
    extends PersistentActor with AtLeastOnceDelivery {
    import Consumer._
    override val persistenceId = s"$name-processManager-initiator"
    val topics = subscriptions.map(eventBusConfig.topicFor).toSet

    private var position = Position.start

    def receiveCommand = {
      case m@Message(_, event: EventData, _) =>
        persist(PositionUpdated(m.positionUpdate)) { updateEvent =>
          position = updateEvent.update(position)
          pubSub ! m.ack
        }

        initiate.lift(event).foreach { responsible =>
          persist(InitiationRequested(responsible, event))(handleInitiation)
        }

      case ProcessStarted(deliveryId) =>
        confirmDelivery(deliveryId)
    }

    def receiveRecover = {
      case u: PositionUpdated =>
        position = u.update(position)

      case i: InitiationRequested => handleInitiation(i)
      case Initiated(deliveryId) => confirmDelivery(deliveryId)

      case RecoveryCompleted =>
        startSubscription()
    }

    def startSubscription() = {
      val props = SubscriptionManager.props(pubSub, Subscribe(persistenceId, topics, position))
      context actorOf props
    }

    private def handleInitiation(req: InitiationRequested) = {
      deliver(receiver.path, i => StartProcessManager(req.processManagerId, req.event, i))
    }
  }

  sealed trait InitiatorActorEvent
  private case class PositionUpdated(update: PositionUpdate) extends InitiatorActorEvent
  private case class InitiationRequested(processManagerId: String, event: EventData) extends InitiatorActorEvent
  private case class Initiated(id: Long) extends InitiatorActorEvent
}