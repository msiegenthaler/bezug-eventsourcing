package es.impl.actor

import akka.util.ByteString

object PubSub {
  /** Topic. Is hierarchical, subscribing to the parent topic includes all subtopics. */
  case class Topic private(path: List[String]) {
    def parent = Topic(path.dropRight(1))
    def \(child: String) = {
      require(child.nonEmpty)
      Topic(path :+ child)
    }
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
}
