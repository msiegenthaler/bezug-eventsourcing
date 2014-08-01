package pubsub

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
  case class Message(subscription: SubscriptionId, data: Any, positionUpdate: PositionUpdate) extends Event {
    def ack = AckMessage(subscription, positionUpdate)
  }
  case class AckMessage(id: SubscriptionId, positionUpdate: PositionUpdate) extends Command

  /** Cancel a subscription. The subscription state will still be available. */
  case class Unsubscribe(id: SubscriptionId) extends Command
  case class Unsubscribed(id: SubscriptionId) extends Event

  /** Permanently deletes a subscription. */
  case class Delete(id: SubscriptionId) extends Command
  case class Deleted(id: SubscriptionId) extends Event
}