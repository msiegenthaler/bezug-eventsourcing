package ch.eventsourced.infrastructure.akka

import akka.util.Timeout
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import akka.actor._
import akka.persistence.{RecoveryCompleted, PersistentActor}
import akka.pattern.ask
import ch.eventsourced.support.CompositeIdentifier
import ch.eventsourced.infrastructure.akka.AggregateActor.{UnsubscribeFromAggregate, SubscribeToAggregate, SubscriptionId}
import ch.eventsourced.infrastructure.akka.AggregateSubscription.{OnEvent, Close}

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
  case class AddManualSubscription(subscriptionId: SubscriptionId, partitionId: String, target: ActorRef)

  def props(baseName: CompositeIdentifier, journalReplay: (Long, Long) => Props): Props = Props(new Publisher(baseName, journalReplay))

  private class Publisher(baseName: CompositeIdentifier, journalReplay: (Long, Long) => Props)
    extends PersistentActor with ActorLogging with Stash {
    def persistenceId = (baseName / "SubscriptionManager").serialize

    //TODO use snapshots to improve performance
    //TODO think through supervisor role (probably individual restart)

    private var pos = -1L
    private var subscriptionActors = Map.empty[SubscriptionId, ActorRef]

    def receiveCommand = waitingForStart
    def waitingForStart: Receive = {
      case Start(at) =>
        log.debug(s"Event handling started, expecting live events starting at $at")
        pos = at
        context become running
        unstashAll()
      case other => stash()
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
            resolveSubscription(id, selection, start)
            sender() ! ack
        }
      case UnsubscribeFromAggregate(id, _, ack) =>
        log.debug(s"Unsubscribing $id")
        if (subscriptionActors.isDefinedAt(id)) {
          subscriptionActors(id) ! Close(SubscriptionHandlerClosed(id, sender(), ack))
        } else sender() ! ack

      case SubscriptionRefResolved(id, ref, start) =>
        startSubscription(id, "0", ref, start)

      case AddManualSubscription(id, part, target) =>
        log.debug(s"Adding new manual subscription: $id from $target")
        startSubscription(id, part, target, 0)

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
          case (id, (target, start)) => resolveSubscription(id, target, start)
        }
        subscribersToStart = Map.empty
    }
    private var subscribersToStart = Map.empty[SubscriptionId, (ActorSelection, Long)]

    def resolveSubscription(id: SubscriptionId, target: ActorSelection, start: Long): Unit = {
      val f = target.resolveOne(10.minutes)
      f.onSuccess {
        case ref => context.self ! SubscriptionRefResolved(id, ref, start)
      }
      f.onFailure {
        case error => log.warning(s"Cannot set up subscription $id: could not resolve actor selection $target")
      }
    }
    def startSubscription(id: SubscriptionId, partition: String, target: ActorRef, start: Long): Unit = {
      val props = AggregateSubscription.props(id, partition, target, journalReplay, start)
      val actor = context actorOf props
      subscriptionActors += id -> actor
      actor ! AggregateSubscription.Start(pos)
    }
  }

  private case class SubscriptionHandlerClosed(id: SubscriptionId, origin: ActorRef, originAck: Any)
  private case class Subscribed(id: SubscriptionId, subscriber: ActorPath, start: Long)
  private case class Unsubscribed(id: SubscriptionId)

  private case class SubscriptionRefResolved(id: SubscriptionId, ref: ActorRef, start: Long)
  private case object EventAck
}
