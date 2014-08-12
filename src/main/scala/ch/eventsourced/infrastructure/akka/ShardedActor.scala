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

  /** The infrastructure requests the passivation of the receiving actor.
    * Respond to the sender with either `ok` or `notNow`. No response will be considered (after a timeout) as not now.
    * When passivated the actor will not be started again until the next message for it arrives. So be sure to not
    * have any outstanding outgoing messages (notifications).
    */
  case class RequestPassivation(ok: Any, notNow: Any)

  /** Force passivation. Sent after a positive passivation request was made before, no messages received in the meantime. */
  case object Passivate

  /** Can be sent from the actor to its parent to signal that it'd be a good time to passivate it.
    * The infrastructure will consider the offer and send a RequestPassivation message if it decides to passivate the
    * actor. The actor can still deny the passivation then, but normally it should not.
    */
  case object OfferPassivation
}

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