package ch.eventsourced.infrastructure.akka

import akka.actor._
import akka.persistence.PersistentActor
import ch.eventsourced.support.CompositeIdentifier

trait ShardedActor[Id] {
  def name: CompositeIdentifier

  /** Props to create a new actor instance.
    * @param publicRef reference the actor should give out to pears instead of 'context.self' if redirection of messages
    *                  over the sharding infrastructure is desired.
    */
  def props(publicRef: ActorRef): Props

  def messageSelector: PartialFunction[Any, Id]
  def serializeId(id: Id): String
  def parseId(value: String): Option[Id]


  trait ActorBase extends PersistentActor {
    final val id = {
      parseId(self.path.name).getOrElse(throw new IllegalStateException(s"id ${self.path.name} cannot be parsed"))
    }
    final override val persistenceId = (name / serializeId(id)).serialize
  }
}

object LocalSharder {
  def props(sharded: ShardedActor[_]): Props = Props(new LocalSharder(sharded))

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
      val child = context.actorOf(sharded.props(context.self), sharded.serializeId(id))
      children += id -> child
      child
    }
  }
}