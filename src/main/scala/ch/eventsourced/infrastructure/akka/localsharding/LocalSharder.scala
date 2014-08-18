package ch.eventsourced.infrastructure.akka.localsharding

import akka.actor.SupervisorStrategy.Escalate
import akka.actor._
import akka.persistence.{PersistentActor, RecoveryCompleted}
import ch.eventsourced.infrastructure.akka.{CompositeName, ShardedActor}
import scala.concurrent.duration._

class LocalSharder(
  passivateAfter: Duration = 5.minutes,
  passivationWaitTime: Duration = 5.seconds,
  stopWaitTime: Duration = 30.seconds) {

  def props(sharded: ShardedActor[_]): Props = Props(new LocalSharder(sharded))

  object Shutdown

  //TODO keep track of non-nicely passivated actors.

  private class LocalSharder[Id](sharded: ShardedActor[Id]) extends Actor with ActorLogging {
    val passivationManager = new PassivationManager(sharded, passivateAfter = passivateAfter, passivationWaitTime = passivationWaitTime, stopWaitTime = stopWaitTime)
    val elementProps = passivationManager.props(context.self)
    override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries = -1)(super.supervisorStrategy.decider)

    var children = Map.empty[Id, ActorRef]

    def receive = {
      case Shutdown =>
        //TODO nicer shutdown
        context stop self

      case msg =>
        sharded.messageSelector.lift(msg).map { id =>
          log.debug(s"forwarding $msg to $id")
          children.getOrElse(id, startChild(id)) forward msg
        }.getOrElse(log.info(s"discarding $msg"))
    }

    def startChild(id: Id) = {
      log.info(s"activating $id")
      val name = sharded.name / sharded.serializeId(id)
      val child = context.actorOf(elementProps, sharded.serializeId(id))
      children += id -> child
      child
    }
  }
}