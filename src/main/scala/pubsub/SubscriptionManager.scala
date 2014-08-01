package pubsub

import scala.concurrent.duration._
import akka.actor.{Terminated, FSM, Props, ActorRef}

/** Helper to setup subscriptions. Start as a child actor, it will forward the messages and
  * take care of the subscription handling. */
object SubscriptionManager {
  import Consumer._

  def props(pubSub: ActorRef, s: Subscribe) = Props(new SubscriptionManagerActor(pubSub, s))

  private sealed trait State
  private case object Subscribing extends State
  private case object Active extends State
  private case object Unsubscribing extends State

  private val retryTimeout = 2.seconds
  private class SubscriptionManagerActor(pubSub: ActorRef, subscribe: Subscribe)
    extends FSM[SubscriptionManager.State, Position] {
    val id = subscribe.id

    context watch pubSub
    startWith(Subscribing, subscribe.startAt)
    pubSub ! subscribe

    when(Subscribing, stateTimeout = retryTimeout) {
      case Event(s@Subscribed(`id`), _) =>
        context.parent ! s
        goto(Active)
      case Event(p@InvalidPosition(`id`, _), _) =>
        context.parent ! p
        stop()
      case Event(Unsubscribe(`id`), _) =>
        context.parent ! Unsubscribed(id)
        stop()
      case Event(StateTimeout | Terminated(`pubSub`), pos) =>
        setupSubscription(pos)
    }

    when(Active) {
      case Event(m: Message, _) if m.subscription == id =>
        context.parent ! m
        stay() using (m.position)
      case Event(Unsubscribe(`id`), _) =>
        context.parent ! Unsubscribe(id)
        goto(Unsubscribing)
      case Event(Terminated(`pubSub`), pos) =>
        setupSubscription(pos)
    }

    when(Unsubscribing) {
      case Event(Unsubscribed(`id`) | Terminated(`pubSub`), _) =>
        context.parent ! Unsubscribed(id)
        stop()
    }

    private def setupSubscription(pos: Position) = {
      pubSub ! subscribe.copy(startAt = pos)
      goto(Subscribing)
    }

    initialize()
  }
}