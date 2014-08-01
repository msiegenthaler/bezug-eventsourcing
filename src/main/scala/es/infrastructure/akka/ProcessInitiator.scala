package es.infrastructure.akka

import akka.actor.{ActorRef, Props}
import akka.persistence.{RecoveryCompleted, AtLeastOnceDelivery, PersistentActor}
import es.api.{EventData, ProcessManager}
import pubsub.{Consumer, Position}

object ProcessInitiator {
  sealed trait Command
  sealed trait Event

  /** Is sent to the receiver, must be answered with ProcessStarted, as soon as it is persisted. */
  case class StartProcessManager(processManagerId: String, withEvent: EventData, deliveryId: Long) extends Event
  case class ProcessStarted(deliveryId: Long) extends Command

  def props(pubSub: ActorRef, receiver: ActorRef)(name: String, subscriptions: Traversable[ProcessManager.Subscribe], initiate: PartialFunction[EventData, String]) =
    Props(new InitiatorActor(name, subscriptions, receiver, initiate, pubSub))

  private case class UpdatePosition(subscription: String, position: Position)
  private case class InitiationRequested(processManagerId: String, event: EventData)
  private case class Initiated(id: Long)

  //TODO add snapshoting
  private class InitiatorActor(
    name: String, subscriptions: Traversable[ProcessManager.Subscribe],
    receiver: ActorRef, initiate: PartialFunction[EventData, String],
    pubSub: ActorRef)
    extends PersistentActor with AtLeastOnceDelivery {
    import Consumer._
    override val persistenceId = s"$name-processManager-initiator"

    //TODO subscribe as requested

    private var positions: Map[String, Position] = Map.empty
    private def updatePosition(event: UpdatePosition) = {
      positions += (event.subscription -> event.position)
    }

    def receiveCommand = {
      case m@Message(_, event: EventData, _) =>
        persist(UpdatePosition(m.subscription, m.position)) { updateEvent =>
          updatePosition(updateEvent)
          pubSub ! Next(updateEvent.subscription)
        }

        initiate.lift(event).foreach { responsible =>
          persist(InitiationRequested(responsible, event))(handleInitiation)
        }

      case ProcessStarted(deliveryId) =>
        confirmDelivery(deliveryId)
    }

    def receiveRecover = {
      case u: UpdatePosition => updatePosition(u)
      case i: InitiationRequested => handleInitiation(i)
      case Initiated(deliveryId) => confirmDelivery(deliveryId)
      case RecoveryCompleted =>
      //TODO
      //SubscriptionManager.props(pubSub, Subscribe(id, topic, start))
    }

    private def handleInitiation(req: InitiationRequested) = {
      deliver(receiver.path, i => StartProcessManager(req.processManagerId, req.event, i))
    }
  }
}