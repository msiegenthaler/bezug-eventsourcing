package ch.eventsourced.infrastructure.akka.localsharding

import scala.concurrent.duration._
import akka.actor._
import akka.actor.SupervisorStrategy.Escalate
import ch.eventsourced.infrastructure.akka.ShardedActor

/** Passivates actors that were not active for some time. */
class PassivationManager[Id](sharded: ShardedActor[Id], passivateAfter: Duration = 5.minutes,
  passivationWaitTime: Duration = 5.seconds,
  stopWaitTime: Duration = 30.seconds) {

  def props(publicRef: ActorRef) = Props(new PassivationActor(publicRef))

  private class PassivationActor(publicRef: ActorRef) extends Actor with ActorLogging {
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
