package es.infrastructure.akka

import es.api.{ProcessManager, AggregateKey, AggregateType}
import pubsub.Topic

case class EventBusConfig(baseTopic: Topic) {
  val aggregateEventTopic =
    baseTopic \ "aggregate"

  def topicFor(aggregateType: AggregateType): Topic =
    aggregateEventTopic \ aggregateType.name
  def topicFor[A <: AggregateType](id: AggregateKey): Topic =
    topicFor(id.aggregateType) \ id.aggregateType.serializeId(id.id)

  def topicFor(subscription: ProcessManager.Subscribe): Topic = subscription match {
    case ProcessManager.SubscribeToAggregate(id) => topicFor(id)
    case ProcessManager.SubscribeToAggregateType(at) => topicFor(at)
  }

  val commandTopic =
    baseTopic \ "command"
}