package es.infrastructure.akka

import akka.actor._

import scala.collection.immutable.Queue
import scala.concurrent.duration._

/**
 * Preserves the order for messages with ack that are sent to a destination.
 * The next message is not forwarded to the destination until the previous one
 * has been acknowledged. When a message is not acknowledged for some time it will be resent.
 * If there are too many hold back messages the actor will crash with TooManyInFlightMessages,
 * if a message is resent too often the actor will crash with RetryLimitExceeded.
 */
object OrderPreservingAck {
  def props(destination: ActorRef, retryAfter: FiniteDuration = 1.second, retryLimit: Int = 20, maxInFlight: Int = 100)
    (msgToAck: PartialFunction[Any, Any]) = {
    Props(new ActorImpl(destination, msgToAck, maxInFlight, retryAfter, retryLimit))
  }

  case class TooManyInFlightMessages(count: Long) extends RuntimeException(s"Too many messages are in the buffer: $count.")
  case class RetryLimitExceeded(limit: Long) extends RuntimeException(s"Retry limit of $limit exceeded")

  private class ActorImpl(target: ActorRef, ackFor: PartialFunction[Any, Any], maxInFlight: Int,
    retryAfter: FiniteDuration, retryLimit: Int) extends Actor {
    private var expectedAck = Option.empty[InFlightMessage]
    private var queue = Queue.empty[(ActorRef, Any)]

    private val redeliverTask = {
      import context.dispatcher
      context.system.scheduler.schedule(retryAfter, retryAfter / 2, self, Tick)
    }
    override def postStop() = {
      redeliverTask.cancel()
      super.postStop()
    }
    override def preRestart(reason: Throwable, message: Option[Any]) = {
      redeliverTask.cancel()
      super.preRestart(reason, message)
    }

    def receive = {
      case Tick =>
        expectedAck.filter(_.timeSinceSent > retryAfter).foreach { inFlight =>
          if (inFlight.retries >= retryLimit) throw RetryLimitExceeded(retryLimit)
          else {
            target ! inFlight.msg
            expectedAck = Some(inFlight.retry)
          }
        }

      case ack if expectedAck.exists(_.expectedAck == ack) =>
        ackToSource(expectedAck.get, ack)
        queue.dequeueOption.foreach {
          case ((s, msg), q2) =>
            sendMessage(s, msg)
            queue = q2
        }

      case msg if sender() != target && ackFor.isDefinedAt(msg) =>
        if (expectedAck.isEmpty) sendMessage(sender(), msg)
        else if (queue.length > maxInFlight) throw TooManyInFlightMessages(maxInFlight)
        else queue = queue.enqueue(sender(), msg)
    }

    def ackToSource(inFlight: InFlightMessage, ack: Any) = {
      inFlight.sender ! ack
      expectedAck = None
    }
    def sendMessage(source: ActorRef, msg: Any) = {
      expectedAck = Some(InFlightMessage(msg, source, ackFor(msg)))
      target ! msg
    }
  }
  private object Tick
  private case class InFlightMessage(msg: Any, sender: ActorRef, expectedAck: Any,
    retries: Int = 0, sent: Long = System.currentTimeMillis) {
    def timeSinceSent = (System.currentTimeMillis - sent).millis
    def retry = copy(retries = retries + 1, sent = System.currentTimeMillis)
  }
}
