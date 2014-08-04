package es.infrastructure.akka

import akka.actor.Props
import akka.persistence.PersistentView
import es.infrastructure.akka.AggregateActor.EventEmitted
import es.support.Guid

/** Sends all events from the actors journal in the boundary as EventEmitted messages to its parent. */
object AggregateJournalReplay {
  /**
   * @param from inclusive
   * @param until inclusive
   */
  def props(aggregateId: String, from: Long, until: Long) = {
    Props(new ReplayActor(aggregateId, from, until))
  }

  private class ReplayActor(aggregateId: String, from: Long, until: Long) extends PersistentView {
    override def persistenceId = aggregateId
    def viewId = Guid.generate.serializeToString

    def receive = {
      case e@EventEmitted(seq, event) if seq >= from =>
        if (seq <= until) context.parent ! e
        else context stop self
    }
  }
}