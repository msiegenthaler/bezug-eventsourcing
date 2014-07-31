package es.infrastructure.akka

import scala.concurrent.duration._
import akka.actor._
import akka.util.ByteString

object PubSub {
  /** Topic. Is hierarchical, subscribing to the parent topic includes all subtopics. */
  case class Topic private(path: List[String]) {
    def parent = Topic(path.dropRight(1))
    def \(child: String) = {
      require(child.nonEmpty)
      Topic(path :+ child)
    }
    override def toString = path.mkString("/")
  }
  object Topic {
    val root = Topic(Nil)
  }

  /** Position with a subscription (opaque for users). */
  case class Position(serialized: ByteString)
  object Position {
    def start = Position(ByteString.empty)
  }

  type SubscriptionId = String

  sealed trait Command
  sealed trait Event
  object Producer {
    /** Publishes data on the topic. After successful (persistent) completion, it replies with 'onPublished'. */
    case class Publish(topic: Topic, data: Any, onPublished: Any) extends Command
  }
  object Consumer {
    /** Subscribe to messages on a topic. Is answered with subscribed, then the sender starts receiving Messages. */
    case class Subscribe(id: SubscriptionId, topic: Topic, startAt: Position = Position.start) extends Command
    case class Subscribed(id: SubscriptionId) extends Event
    /** Could not set up a subscription, because the provided position is invalid. The subscription might have been deleted. */
    case class InvalidPosition(id: SubscriptionId, pos: Position) extends Event

    /** Will be sent to the subscriber, answer with Next (backpressure). */
    case class Message(subscription: SubscriptionId, data: Any, position: Position) extends Event
    case class Next(id: SubscriptionId) extends Command

    /** Cancel a subscription. The subscription state will still be available. */
    case class Unsubscribe(id: SubscriptionId) extends Command
    case class Unsubscribed(id: SubscriptionId) extends Event

    /** Permanently deletes a subscription. */
    case class Delete(id: SubscriptionId) extends Command
    case class Deleted(id: SubscriptionId) extends Event
  }


  /** Helper to setup subscriptions. Start as a child actor, it will forward the messages and
    * take care of the subscription handling. */
  object SubscriptionManager {
    import Consumer._

    def props(pubSub: ActorRef, s: Subscribe) = Props(new SubscriptionManagerActor(pubSub, s))

    private sealed trait State
    private case object Subscribing extends State
    private case object Active extends State
    private case object Unsubscribing extends State

    private val retryTimeout = 2.seconds
    private class SubscriptionManagerActor(pubSub: ActorRef, subscribe: Subscribe)
      extends FSM[SubscriptionManager.State, Position] {
      val id = subscribe.id

      context watch pubSub
      startWith(Subscribing, subscribe.startAt)
      pubSub ! subscribe

      when(Subscribing, stateTimeout = retryTimeout) {
        case Event(s@Subscribed(`id`), _) =>
          context.parent ! s
          goto(Active)
        case Event(p@InvalidPosition(`id`, _), _) =>
          context.parent ! p
          stop()
        case Event(Unsubscribe(`id`), _) =>
          context.parent ! Unsubscribed(id)
          stop()
        case Event(StateTimeout | Terminated(`pubSub`), pos) =>
          setupSubscription(pos)
      }

      when(Active) {
        case Event(m: Message, _) if m.subscription == id =>
          context.parent ! m
          stay() using (m.position)
        case Event(Unsubscribe(`id`), _) =>
          context.parent ! Unsubscribe(id)
          goto(Unsubscribing)
        case Event(Terminated(`pubSub`), pos) =>
          setupSubscription(pos)
      }

      when(Unsubscribing) {
        case Event(Unsubscribed(`id`) | Terminated(`pubSub`), _) =>
          context.parent ! Unsubscribed(id)
          stop()
      }

      private def setupSubscription(pos: Position) = {
        pubSub ! subscribe.copy(startAt = pos)
        goto(Subscribing)
      }

      initialize()
    }
  }
}