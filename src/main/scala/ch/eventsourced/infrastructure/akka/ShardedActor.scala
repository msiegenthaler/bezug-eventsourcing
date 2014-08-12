package ch.eventsourced.infrastructure.akka

import akka.actor._
import ch.eventsourced.support.CompositeIdentifier

/**
 * (Persitent) actor that can be sharded across multiple machines.
 * Sharding takes care of:
 * - Starting of an actor instances when a message for it arrives
 * - Passivation (stopping) of running actor instances when they have not received messages for some time (to free the memory)
 * - Making sure that the actors that did not passivate normally are restarted (without a message) at the next system start
 */
trait ShardedActor[Id] {
  def name: CompositeIdentifier

  /** Props to create a new actor instance.
    * @param publicRef reference the actor should give out to pears instead of 'context.self' if redirection of messages
    *                  over the sharding infrastructure is desired.
    */
  def props(publicRef: ActorRef, id: Id, name: CompositeIdentifier): Props

  def messageSelector: PartialFunction[Any, Id]
  def serializeId(id: Id): String
  def parseId(value: String): Option[Id]
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
      val name = sharded.name / sharded.serializeId(id)
      val child = context.actorOf(sharded.props(context.self, id, name), sharded.serializeId(id))
      children += id -> child
      child
    }
  }
}