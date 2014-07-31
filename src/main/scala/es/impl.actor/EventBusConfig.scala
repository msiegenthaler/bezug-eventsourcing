package es.impl.actor

import es.api.AggregateType
import es.impl.actor.PubSub.Topic

class EventBusConfig(baseTopic: Topic) {
  val aggregateEventTopic =
    baseTopic \ "aggregate"

  def topicFor(aggregateType: AggregateType): Topic =
    aggregateEventTopic \ aggregateType.name

  //TODO handle string serialization..
  def topicFor(aggregateType: AggregateType, id: String): Topic =
    topicFor(aggregateType) \ id
}
