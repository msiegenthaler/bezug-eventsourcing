package es.impl.actor

import es.api.{AggregateKey, AggregateType}
import es.impl.actor.PubSub.Topic

case class EventBusConfig(baseTopic: Topic) {
  val aggregateEventTopic =
    baseTopic \ "aggregate"

  def topicFor(aggregateType: AggregateType): Topic =
    aggregateEventTopic \ aggregateType.name

  def topicFor[A <: AggregateType](id: AggregateKey): Topic =
    topicFor(id.aggregateType) \ id.aggregateType.serializeId(id.id)
}