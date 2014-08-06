package es.infrastructure.akka

import akka.util.Timeout
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import akka.actor._
import akka.persistence.{RecoveryCompleted, PersistentActor}
import akka.pattern.ask
import es.infrastructure.akka.AggregateActor.OnEvent
import es.infrastructure.akka.AggregateSubscription.Close
import es.infrastructure.akka.EventBus.{UnsubscribeFromAggregate, SubscribeToAggregate}

/**
 * Manages subscriptions to aggregate events. This includes retroactive subscriptions.
 * It must be started with a single Start() message, containing the current event sequence number of the aggregate.
 *
 * Handles:
 * - Start
 * - SubscribeToAggregate
 * - UnsubscribeFromAggregate
 * - OnEvent: Forwards to all subscribers
  */
object AggregateSubscriptionManager {
  case class Start(at: Long)

  def props(namePrefix: String, journalReplay: (Long, Long) => Props): Props = Props(new Publisher(namePrefix, journalReplay))

  private type SubscriptionId = String
  private class Publisher(namePrefix: String, journalReplay: (Long, Long) => Props) extends PersistentActor with ActorLogging {
    def persistenceId = s"$namePrefix/SubscriptionManager"

    //TODO use snapshots to improve performance
    //TODO think through supervisor role (probably individual restart)

    private var pos = -1L
    private var subscriptionActors = Map.empty[SubscriptionId, ActorRef]

    def receiveCommand = waitingForStart
    def waitingForStart: Receive = {
      case Start(at) =>
        log.debug(s"Event handling started, expecting live events starting at $at")
        pos = at
        subscriptionActors.values.foreach(_ ! AggregateSubscription.Start(at))
        context become running
    }
    def running: Receive = {
      case OnEvent(event, ack) =>
        val origin = sender()
        implicit val timeout = Timeout(5.seconds)
        val ackFutures = subscriptionActors.values.map { actor =>
          (actor ? OnEvent(event, EventAck)).map { case EventAck => ()}
        }
        //When we got a successful ack from all aggregateSubscription actor then we ack the event
        // Note: this does not mean, that all subscribers have received the message, only that we can make
        // sure they eventually will do.
        Future.sequence(ackFutures).onSuccess {
          case _ => origin ! ack
        }

      case SubscribeToAggregate(id, _, subscriber, start, ack) if !subscriptionActors.contains(id) =>
        persist(Subscribed(id, subscriber, start)) {
          case Subscribed(id, path, start) =>
            log.info(s"New subscription added: $id from $subscriber, starting at $start")
            val selection = context.actorSelection(path)
            val actor = startSubscription(id, selection, start)
            actor ! Start(pos)
            sender() ! ack
        }
      case UnsubscribeFromAggregate(id, _, ack) =>
        log.debug(s"Unsubscribing $id")
        if (subscriptionActors.isDefinedAt(id)) {
          subscriptionActors(id) ! Close(SubscriptionHandlerClosed(id, sender(), ack))
        } else sender() ! ack

      case SubscriptionHandlerClosed(id, origin, ack) =>
        persist(Unsubscribed(id)) { _ =>
          log.info(s"Subscription $id terminated")
          subscriptionActors -= id
          origin ! ack
        }
    }

    def receiveRecover = {
      case Subscribed(id, path, start) =>
        subscribersToStart += id ->(context.actorSelection(path), start)
      case Unsubscribed(id) =>
        subscribersToStart -= id

      case RecoveryCompleted =>
        log.debug(s"RecoveryCompleted: Starting ${subscribersToStart.size} subscriptions")
        subscribersToStart.foreach {
          case (id, (target, start)) => startSubscription(id, target, start)
        }
        subscribersToStart = Map.empty
    }
    private var subscribersToStart = Map.empty[SubscriptionId, (ActorSelection, Long)]

    def startSubscription(id: SubscriptionId, target: ActorSelection, start: Long) = {
      val props = AggregateSubscription.props(id, target, journalReplay, start)
      val actor = context actorOf props
      subscriptionActors += id -> actor
      actor
    }
  }

  private case class SubscriptionHandlerClosed(id: SubscriptionId, origin: ActorRef, originAck: Any)
  private case class Subscribed(id: SubscriptionId, subscriber: ActorPath, start: Long)
  private case class Unsubscribed(id: SubscriptionId)

  private case object EventAck
}
