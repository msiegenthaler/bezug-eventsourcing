package ch.eventsourced.infrastructure.akka.localsharding

import java.net.URLDecoder
import scala.concurrent.duration._
import akka.actor._
import akka.actor.SupervisorStrategy.Escalate
import ch.eventsourced.infrastructure.akka.ShardedActor

/** Passivates actors that were not active for some time. */
class PassivationManager[Id](sharded: ShardedActor[Id], passivateAfter: Duration = 5.minutes,
  passivationWaitTime: Duration = 5.seconds,
  stopWaitTime: Duration = 30.seconds) {

  def props(publicRef: ActorRef) = Props(new PassivationActor(publicRef))

  sealed trait Command
  sealed trait Event
  /** Send to the actor to ask for a clean passivation if the actors state is stable. Might result in a RequestCleanStop. */
  case object PassivateIfPossible extends Command
  /** Sent to the parent when a controlled stop is requested. The parent must not forward any new messages after the ack. */
  case class RequestCleanStop(id: Id, ack: Any) extends Event
  /** Sent to the parent when it has been cleanly stopped with passivation (no pending tasks). */
  case class Passivated(id: Id) extends Event

  private class PassivationActor(publicRef: ActorRef) extends Actor with ActorLogging {
    import sharded._

    val id = parseId(URLDecoder.decode(context.self.path.name, "UTF-8")).
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
        askParentForStop()

      case ReceiveTimeout | PassivationNotOk =>
        resumeNormalOperation()

      case msg if sender() != element =>
        //abort passivation since we have another message to process
        element forward msg
        resumeNormalOperation()
    }

    def askParentForStop() = {
      log.debug("passivation ok, asking the parent")
      context.parent ! RequestCleanStop(id, StopOk)
      context become waitForParentsOk
    }
    def waitForParentsOk: Receive = {
      case StopOk => passivate()

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
        context.parent ! Passivated(id)
        context stop self

      case ReceiveTimeout =>
        log.info(s"sharded actor did not stop within $stopWaitTime, terminating it.")
        context stop self
    }
  }
  private case object PassivationOk
  private case object PassivationNotOk
  private case object StopOk
}
