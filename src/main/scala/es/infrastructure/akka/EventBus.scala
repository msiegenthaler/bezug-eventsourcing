package es.infrastructure.akka

import akka.actor.ActorPath
import es.api.{EventData, AggregateKey}

object EventBus {
  sealed trait EventBusCommand
  sealed trait EventBusEvent


  /**
   * Set up a persistent subscription to the aggregate. The subscriber will continue to receive events after a system
   * restart. So it's important to unsubscribe.
   * The subscription will start at the event with the sequenceId `startEventSeq`.
   */
  case class SubscribeToAggregate(subscriptionId: String, aggregate: AggregateKey,
    subscriber: ActorPath, startEventSeq: Long = 0) extends EventBusCommand

  /** Event delivered a part of the persistent subscription. Reply with `ack`. */
  case class AggregateEvent(subscriptionId: String, event: EventData, ack: Any) extends EventBusEvent

  /** Remove the (persistent) subscription. */
  case class UnsubscribeFromAggregate(subscriptionId: String) extends EventBusCommand
}
