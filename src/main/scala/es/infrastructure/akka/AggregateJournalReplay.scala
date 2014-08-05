package es.infrastructure.akka

import akka.actor.Props
import akka.persistence.PersistentView
import es.api.AggregateType
import es.infrastructure.akka.AggregateActor.EventEmitted
import es.support.Guid

/** Sends all events from the actors journal in the boundary as EventData messages to its parent. */
class AggregateJournalReplay[A <: AggregateType](val aggregateType: A) {
  import aggregateType._

  //TODO add some throtteling (backpressure)

  /**
   * @param from inclusive
   * @param until inclusive
   */
  def props(aggregateId: Id, aggregatePersistenceId: String, from: Long, until: Long) = {
    Props(new ReplayActor(aggregateId, aggregatePersistenceId, from, until))
  }

  private class ReplayActor(aggregateId: Id, val persistenceId: String, from: Long, until: Long) extends PersistentView {
    def viewId = Guid.generate.serializeToString

    def receive = {
      case e@EventEmitted(seq, Event(event)) if seq >= from =>
        if (seq <= until) {
          val eventData = EventData(aggregateId, seq, event)
          context.parent ! e
        } else context stop self
    }
  }
}