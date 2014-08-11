package ch.eventsourced.infrastructure.akka

import akka.actor.{ActorRef, Props, Actor}
import ch.eventsourced.infrastructure.akka.AggregateActor._

/** Distribute commands to aggregate actors based on the types. */
object AggregateCommandDistributor {
  def props[Cmd, Err](
    aggregates: Map[AggregateActor[_, _ <: Cmd, _ <: Err], ActorRef],
    unknown: Err): Props = {
    Props(new ActorImpl[Cmd, Err](aggregates, unknown))
  }

  private class ActorImpl[Cmd, Err](aggregates: Map[AggregateActor[_, _ <: Cmd, _ <: Err], ActorRef],
    unknown: Err) extends Actor {
    def receive = {
      case msg: Execute[Cmd, Err] =>
        aggregates.find { a =>
          msg.command match {
            case a._1.aggregateType.Command(id, cmd) =>
              a._2 forward msg
              true
            case _ => false
          }
        } getOrElse {
          sender() ! unknown
        }

      case msg: SubscribeToAggregate =>
        aggregates.find(_._1.aggregateType == msg.aggregate.aggregateType).foreach { a =>
          a._2 forward msg
        }

      case msg: UnsubscribeFromAggregate =>
        aggregates.find(_._1.aggregateType == msg.aggregate.aggregateType).foreach { a =>
          a._2 forward msg
        }
    }
  }
}