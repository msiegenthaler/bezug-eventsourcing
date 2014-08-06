package ch.eventsourced.infrastructure.akka

import akka.actor.{Props, Actor}
import ch.eventsourced.infrastructure.akka.AggregateManager._

/** Distribute commands to aggregate actors based on the types. */
object CommandDistributor {
  def props[Cmd, Err](
    aggregates: Traversable[AggregateManager[_, _ <: Cmd, _ <: Err]],
    unknown: Err): Props = {
    Props(new ActorImpl[Cmd, Err](aggregates, unknown))
  }

  private class ActorImpl[Cmd, Err](aggregates: Traversable[AggregateManager[_, _ <: Cmd, _ <: Err]],
    unknown: Err) extends Actor {
    def receive = {
      case msg: Execute[Cmd, Err] =>
        aggregates.find { a =>
          msg.command match {
            case a.aggregateType.Command(id, cmd) =>
              a.ref forward msg
              true
            case _ => false
          }
        } getOrElse {
          sender() ! unknown
        }

      case msg: SubscribeToAggregate =>
        aggregates.find(_.aggregateType == msg.aggregate.aggregateType).foreach { a =>
          a.ref forward msg
        }

      case msg: UnsubscribeFromAggregate =>
        aggregates.find(_.aggregateType == msg.aggregate.aggregateType).foreach { a =>
          a.ref forward msg
        }
    }
  }
}