package es.infrastructure.akka

import akka.actor.{ActorLogging, Props, ActorRef}
import akka.persistence.{RecoveryCompleted, PersistentActor}
import es.api._
import es.infrastructure.akka.EventBus.{AggregateEvent, UnsubscribeFromAggregate, SubscribeToAggregate}

/**
 * Represents a running instance of a process manager.
 *
 * - Handle the events and send the commands
 * - Subscribe/unsubscribe to/from aggregate events
 */
class ProcessManagerInstance[I, C, E](contextName: String,
  val processManagerType: ProcessManagerType {type Id = I; type Command <: C; type Error <: E},
  commandDistributor: ActorRef) {
  import processManagerType._

  case class InitiateProcess(process: Id, event: EventData, ack: Any)
  case class ProcessCompleted(id: Id)

  def props(id: Id): Props = Props(new Process(id))


  /**
   * Implementation notes:
   * - Subscriptions are regenerated on loading, because the impl might change and depend on this new subscriptions
   * - The actually setup subscriptions (acks) are persistent, because they need to be removed
   * - The commands are persistent, because we don't want to send out commands from a new impl when replaying
   * - process is done only if it has no active subscriptions and no pending commands
   */
  private class Process(id: Id) extends PersistentActor with ActorLogging {
    def persistenceId = s"$contextName/ProcessManager/$name/Instance/$id"
    val commandTarget = context actorOf OrderPreservingAck.props(commandDistributor) {
      case Execute(_, ok, fail) => msg => msg == ok || msg == fail
      case s: SubscribeToAggregate => _ == s.ack
      case s: UnsubscribeFromAggregate => _ == s.ack
    }

    private var state = seed(id)
    private var done = false
    private var activeSubscriptions = Map.empty[String, AggregateKey]

    //Live messages
    def receiveCommand = {
      case InitiateProcess(`id`, event, ack) =>
        persist(Started(event.aggregateKey, event.sequence)) { e =>
          addSubscription(e.from, event.sequence)
          sender() ! ack
        }
        receiveCommand(event) //handle the initiating event

      case CommandAck(cmdId) =>
        commandConfirmed(cmdId)
      case CommandFailed(cmdId, cmd, error) =>
        log.error(s"Emitted command $cmd (sequence=$cmdId) failed: $error")
        commandConfirmed(cmdId)

      case s@SubscriptionAdded(id, to) =>
        persist(s) { _ => activeSubscriptions += id -> to}

      case s@SubscriptionRemoved(id) =>
        activeSubscriptions -= id
        persist(s) { _ => ()}
        terminateIfDone()

      case AggregateEvent(subId, event, ack) if !done && activeSubscriptions.contains(subId) =>
        state.handle.lift(event).foreach {
          case Continue(next, cmds, subscriptionActions) =>
            state = next
            cmds.foreach(emitCommand)
            subscriptionActions foreach {
              case ProcessManager.Subscribe(to) => addSubscription(to, 0)
              case ProcessManager.Unsubscribe(from) => removeSubscription(from)
            }
            persist(event)(_ => sender() ! ack)

          case Completed(cmds) if cmds.isEmpty =>
            done = true
            cmds.foreach(emitCommand)
            persist(event)(_ => sender() ! ack)

          case Completed(_) =>
            done = true
            persist(event)(_ => sender() ! ack)
            terminateIfDone()
        }

      case AggregateEvent(subId, event, ack) if !activeSubscriptions.contains(subId) =>
        //unrequested event, try to remove the subscription
        removeSubscription(subId)
    }

    // From Journal
    def receiveRecover = {
      case Started(from, seq) =>
        expectedSubscriptionsAfterRecovery += from -> seq

      case event: EventData if !done =>
        state.handle.lift(event) foreach {
          case Completed(_) =>
            done = true
            expectedSubscriptionsAfterRecovery = Map.empty
          case Continue(next, _, subscriptionActions) =>
            state = next
            subscriptionActions foreach {
              case ProcessManager.Subscribe(to) =>
                expectedSubscriptionsAfterRecovery += to -> 0L
              case ProcessManager.Unsubscribe(from) =>
                expectedSubscriptionsAfterRecovery -= from
            }
        }

      case SubscriptionAdded(id, to) => activeSubscriptions += id -> to
      case SubscriptionRemoved(id) => activeSubscriptions -= id

      case CommandEmitted(id, cmd) =>
        commandsToResend += id -> cmd
        nextCommandId += 1
      case CommandDelivered(id) =>
        commandsToResend -= id

      case ProcessCompleted =>
        completeProcess() // will terminate the actor

      case RecoveryCompleted =>
        log.debug(s"Loaded from event store")

        //Set up subscriptions
        val pendingSubscribes = expectedSubscriptionsAfterRecovery -- activeSubscriptions.values
        val pendingUnsubscribes = activeSubscriptions.filterNot(e => expectedSubscriptionsAfterRecovery.contains(e._2))
        pendingSubscribes.foreach((addSubscription _).tupled)
        pendingUnsubscribes.keys.foreach(removeSubscription)
        expectedSubscriptionsAfterRecovery = Map.empty

        //Resend unacknowledged commands
        commandsToResend.map(e => CommandEmitted(e._1, e._2)).foreach(sendCommand _)
        commandsToResend = Map.empty

        terminateIfDone
    }
    private var expectedSubscriptionsAfterRecovery = Map.empty[AggregateKey, Long]
    private var commandsToResend = Map.empty[Long, C]


    //Command handling
    def emitCommand(cmd: Command) = {
      val commandId = nextCommandId
      nextCommandId += 1
      persist(CommandEmitted(commandId, cmd))(sendCommand)
    }
    def sendCommand(cmd: CommandEmitted) = {
      unconfirmedCommands += cmd.id
      commandTarget ! Execute(cmd.command, CommandAck(cmd.id), (e: Error) => CommandFailed(cmd.id, cmd.command, e))
    }
    def commandConfirmed(id: Long) = {
      unconfirmedCommands -= id
      persist(CommandDelivered(id)) { _ => ()}
      terminateIfDone()
    }
    private var nextCommandId = 0L
    private var unconfirmedCommands = Set.empty[Long]


    //Subscription handling
    def addSubscription(to: AggregateKey, fromSequence: Long) = {
      val subscriptionId = s"$persistenceId/${to.aggregateType}/${to.aggregateType.serializeId(to.id)}"
      val ack = SubscriptionAdded(subscriptionId, to)
      commandDistributor ! SubscribeToAggregate(subscriptionId, to, context.self.path, fromSequence, ack)
    }
    def removeSubscription(to: AggregateKey): Unit = {
      activeSubscriptions.filter(_._2 == to).map(_._1).foreach(removeSubscription)
    }
    def removeSubscription(id: String): Unit = {
      commandDistributor ! UnsubscribeFromAggregate(id, SubscriptionRemoved(id))
    }

    //Termination
    def terminateIfDone() = {
      if (done && activeSubscriptions.isEmpty && unconfirmedCommands.isEmpty)
        persist(ProcessCompleted)(_ => completeProcess())
    }
    def completeProcess() = {
      //we don't ack this message because it will be just sent again if the process is waken up again
      context.parent ! ProcessCompleted(id)
      context stop self
    }
  }

  private sealed trait Event
  private case class Started(from: AggregateKey, atSequence: Long) extends Event
  private case class CommandEmitted(id: Long, command: C) extends Event
  private case class CommandDelivered(id: Long) extends Event
  private case class SubscriptionAdded(id: String, to: AggregateKey) extends Event
  private case class SubscriptionRemoved(id: String) extends Event

  private sealed trait CommandResponse {
    def id: Long
  }
  private case class CommandAck(id: Long) extends CommandResponse
  private case class CommandFailed(id: Long, cmd: C, error: E) extends CommandResponse
}
