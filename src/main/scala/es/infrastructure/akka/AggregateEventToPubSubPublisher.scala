package es.infrastructure.akka

import akka.actor.{ActorRef, Actor, Props}
import es.api.AggregateType
import es.infrastructure.akka.EventBus.AggregateEvent
import pubsub.Producer.Publish
import pubsub.Topic

/** Publishes AggregateEvents to the PubSub. */
class AggregateEventToPubSubPublisher[A <: AggregateType](aggregateType: A) {
  import aggregateType._

  def props(pubSub: ActorRef, topics: Set[Topic]): Props = {
    Props(new Publisher(pubSub, topics))
  }

  private class Publisher(pubSub: ActorRef, topics: Set[Topic]) extends Actor {
    def receive = {
      case AggregateEvent(_, event@EventData(id, seq, _), ack) =>
        //pubSub will directly ack to sender
        pubSub forward Publish(topics, serializeId(id), event, ack)
    }
  }
}
