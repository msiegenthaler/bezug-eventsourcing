package ch.eventsourced.infrastructure.akka.localsharding

import java.net.URLEncoder
import scala.concurrent.duration._
import akka.actor._
import ch.eventsourced.infrastructure.akka.ShardedActor

/** Sharder that only uses a single actor as the entry point.
  * Only use in very small project or for tests. */
class LocalSharder(
  passivateAfter: Duration = 5.minutes,
  passivationWaitTime: Duration = 5.seconds,
  stopWaitTime: Duration = 30.seconds) {

  def props(sharded: ShardedActor[_]): Props = Props(new LocalSharder(sharded))

  private class LocalSharder[Id](sharded: ShardedActor[Id]) extends Actor with ActorLogging {
    val passivationManager = new PassivationManager(sharded, passivateAfter = passivateAfter, passivationWaitTime = passivationWaitTime, stopWaitTime = stopWaitTime)
    val cleannessTracker = new CleannessTracker[Id](sharded.name)
    val elementProps = passivationManager.props(context.self)
    override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries = -1)(super.supervisorStrategy.decider)
    import cleannessTracker.{MarkClean, MarkUnclean, NeedsCleaning}
    import passivationManager.{RequestCleanStop, Passivated, PassivateIfPossible}

    val tracker = context.actorOf(cleannessTracker.props, "cleanness-tracker")
    var children = Map.empty[Id, ActorRef]


    def receive = {
      case LocalSharder.Shutdown =>
        context stop self

      case RequestCleanStop(from, ack) if children.contains(from) =>
        children -= from
        sender() ! ack

      case Passivated(id) =>
        if (!children.contains(id)) {
          tracker ! MarkClean(id)
        } else {
          //the child was already restarted because of a new message, so it's not clean
        }

      case NeedsCleaning(id) if !children.contains(id) =>
        // Start possibly dirty child and try to stop it cleanly
        startChild(id) ! PassivateIfPossible

      case ForwardMsg(id, sender, msg) =>
        children.getOrElse(id, startChild(id)).tell(msg, sender)

      case msg =>
        sharded.messageSelector.lift(msg).map { id =>
          tracker ! MarkUnclean(id, ForwardMsg(id, sender(), msg))
        }.getOrElse(log.info(s"discarding $msg"))
    }

    def startChild(id: Id) = {
      log.info(s"activating $id")
      val name = sharded.name / sharded.serializeId(id)
      val child = context.actorOf(elementProps, URLEncoder.encode(sharded.serializeId(id), "UTF-8"))
      children += id -> child
      child
    }

    private case class ForwardMsg(to: Id, sender: ActorRef, msg: Any)
  }
}

object LocalSharder {
  object Shutdown
}