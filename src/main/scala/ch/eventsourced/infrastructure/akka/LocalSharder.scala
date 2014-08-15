package ch.eventsourced.infrastructure.akka

import akka.actor.SupervisorStrategy.Escalate
import scala.concurrent.duration._
import akka.actor._

class LocalSharder(
  passivateAfter: Duration = 5.minutes,
  passivationWaitTime: Duration = 5.seconds,
  stopWaitTime: Duration = 30.seconds) {

  def props(sharded: ShardedActor[_]): Props = Props(new LocalSharder(sharded))

  object Shutdown

  //TODO keep track of non-nicely passivated actors.

  private class LocalSharder[Id](sharded: ShardedActor[Id]) extends Actor with ActorLogging {
    val elementProps = Props(new PassivationManager(context.self, sharded))
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


  private class PassivationManager[Id](publicRef: ActorRef, sharded: ShardedActor[Id]) extends Actor with ActorLogging {
    import sharded._

    val id = parseId(context.self.path.name).
      getOrElse(throw new IllegalStateException(s"cannot parse id  from ${context.self.path.name}"))
    def name = sharded.name / serializeId(id)

    val element = context actorOf sharded.props(publicRef, id, name)
    context watch element
    override val supervisorStrategy = AllForOneStrategy() {
      case _ => Escalate
    }

    def resumeNormalOperation() = {
      context.setReceiveTimeout(passivateAfter)
      context become receive
    }
    def receive = {
      case OfferPassivation | ReceiveTimeout =>
        requestPassivation()

      case msg =>
        element forward msg
    }


    def requestPassivation() = {
      element ! RequestPassivation(PassivationOk, PassivationNotOk)
      context setReceiveTimeout passivationWaitTime
      context become passivating
    }
    def passivating: Receive = {
      case PassivationOk =>
        passivate()

      case ReceiveTimeout | PassivationNotOk =>
        resumeNormalOperation()

      case msg if sender() != element =>
        //abort passivation since we have another message to process
        element forward msg
        resumeNormalOperation()
    }


    def passivate() = {
      log.debug("passivation started")
      element ! Passivate
      context setReceiveTimeout stopWaitTime
      context become waitingForStop
    }
    def waitingForStop: Receive = {
      case Terminated if sender() == element =>
        log.debug("stopped")
        context stop self

      case ReceiveTimeout =>
        log.info(s"sharded actor did not stop within $stopWaitTime, terminating it.")
        context stop self
    }
  }
  private case object PassivationOk
  private case object PassivationNotOk
}