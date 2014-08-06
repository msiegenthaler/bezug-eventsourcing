package ch.eventsourced.infrastructure.akka

import akka.actor.{ActorRef, Actor, Props}
import ch.eventsourced.api.{EventData, ProcessManagerType}
import ch.eventsourced.infrastructure.akka.AggregateManager.AggregateEvent

/**
 * Responsible to start a process manager instance if an event triggers it.
 * Sends: messages created by makeMessage(eventData, ack)
 * Receives: AggregateEvent
 */
class ProcessInitator[I](processManagerType: ProcessManagerType {type Id <: I}, makeMassage: (I, EventData, Any) => Any) {

  def props(target: ActorRef) = Props(new Initiator(target))

  private class Initiator(target: ActorRef) extends Actor {
    def receive = {
      case AggregateEvent(_, event, ack) =>
        processManagerType.initiate.lift(event) match {
          case Some(id) =>
            val msg = makeMassage(id, event, ack)
            target forward msg

          case None =>
            //does not start a process manager, just ack it
            sender() ! ack
        }
    }
  }
}
