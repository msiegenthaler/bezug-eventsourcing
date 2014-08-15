package ch.eventsourced.infrastructure.akka

import akka.actor.{ActorRef, ActorLogging, Actor, Props}

object LocalSharder {
  def props(sharded: ShardedActor[_]): Props = Props(new LocalSharder(sharded))

  //TODO handle passivation

  private class LocalSharder[Id](sharded: ShardedActor[Id]) extends Actor with ActorLogging {
    var children = Map.empty[Id, ActorRef]

    def receive = {
      case msg =>
        sharded.messageSelector.lift(msg).map { id =>
          log.info(s"forwarding $msg to $id")
          children.getOrElse(id, startChild(id)) forward msg
        }.getOrElse(log.info(s"discarding $msg"))
    }

    def startChild(id: Id) = {
      log.info(s"starting $id")
      val name = sharded.name / sharded.serializeId(id)
      val child = context.actorOf(sharded.props(context.self, id, name), sharded.serializeId(id))
      children += id -> child
      child
    }
  }
}