package ch.eventsourced.infrastructure.akka

import akka.actor.{ActorRef, Actor, Props}
import ch.eventsourced.api.AggregateType
import ch.eventsourced.infrastructure.akka.AggregateActor.AggregateEvent
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
