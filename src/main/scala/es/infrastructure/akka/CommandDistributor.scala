package es.infrastructure.akka

import akka.actor.{Props, Actor}

/** Distribute commands to aggregate actors based on the types. */
object CommandDistributor {
  def props[Cmd, Err](
    aggregates: Traversable[AggregateActorManager[_, _ <: Cmd, _ <: Err]],
    unknown: Err): Props = {
    Props(new ActorImpl[Cmd, Err](aggregates, unknown))
  }

  private class ActorImpl[Cmd, Err](aggregates: Traversable[AggregateActorManager[_, _ <: Cmd, _ <: Err]],
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
    }
  }
}