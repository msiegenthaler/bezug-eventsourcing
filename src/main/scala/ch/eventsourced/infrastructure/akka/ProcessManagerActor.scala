package ch.eventsourced.infrastructure.akka

import akka.actor.{Props, ActorLogging, ActorRef}
import akka.persistence.{RecoveryCompleted, PersistentActor}
import ch.eventsourced.api.{ProcessManager, EventData, AggregateKey, ProcessManagerType}
import ch.eventsourced.infrastructure.akka.AggregateActor._
import ch.eventsourced.support.CompositeName

/**
 * Represents a running instance of a process manager.
 * To use instantiate a x.initiator actor and shard the x.props actors.
 *
 * Responsibilities of the ProcessManagerActor:
 * - setup the initial subscription (at InitiateProcess message).
 * - process the events received from subscriptions
 * and in result
 * - reliably send the commands
 * - subscribe/unsubscribe to/from aggregate events
 */
class ProcessManagerActor[I, C, E](contextName: String,
  val processManagerType: ProcessManagerType {type Id = I; type Command <: C; type Error <: E},
  commandDistributor: ActorRef) extends ShardedActor[I] {
  import processManagerType._

  def props(publicRef: ActorRef, id: I, name: CompositeName) = Props(new Process(publicRef, id, name))

  /** Handles EventData messages and starts process instances as needed. */
  def initiator = new ProcessInitator(processManagerType, initiateMessage)
  def registerOn = processManagerType.triggeredBy

  def name = CompositeName(contextName) / "processManager" / processManagerType.name
  def serializeId(id: Id) = processManagerType.serializeId(id)
  def parseId(value: String) = processManagerType.parseId(value)

  def messageSelector = {
    case msg@InitiateProcess(processId, _, _) => processId
    case AggregateEvent(SubscriptionId(processId), _, _) => processId
  }

  /** Message that starts a process if it is not already started. */
  case class InitiateProcess(process: Id, event: EventData, ack: Any)
  object InitiateProcess {
    def apply(event: EventData, ack: Any): InitiateProcess =
      apply(processManagerType.initiate(event), event, ack)
  }
  private def initiateMessage(process: Id, event: EventData, ack: Any) = InitiateProcess(process, event, ack)


  private[akka] object SubscriptionId {
    def apply(id: Id, key: AggregateKey) = {
      val pm = ProcessManagerActor.this.name / "instance" / serializeId(id)
      pm / "aggregate" / key.aggregateType.name / key.aggregateType.serializeId(key.id)
    }
    def unapply(id: CompositeName): Option[Id] = id match {
      case CompositeName(`contextName`, "processManager", `name`, "instance", id, "aggregate", _, _) =>
        parseId(id)
      case _ => None
    }
    private val name = processManagerType.name
  }

  /**
   * Implementation notes:
   * - Subscriptions are regenerated on loading, because the impl might change and depend on this new subscriptions
   * - The actually setup subscriptions (acks) are persistent, because they need to be removed
   * - The commands are persistent, because we don't want to send out commands from a new impl when replaying
   * - process is done only if it has no active subscriptions and no pending commands
   */
  private class Process(publicRef: ActorRef, id: Id, name: CompositeName) extends PersistentActor with ActorLogging {
    val persistenceId = name.serialize
    val commandTarget = context actorOf OrderPreservingAck.props(commandDistributor) {
      case Execute(_, ok, fail) => msg => msg == ok || msg == fail
      case s: SubscribeToAggregate => _ == s.ack
      case s: UnsubscribeFromAggregate => _ == s.ack
    }

    private var state = seed(id)
    private var done = false
    private var activeSubscriptions = Map.empty[SubscriptionId, AggregateKey]
    private var unconfirmedSubscribes = Set.empty[SubscriptionId]
    private var unconfirmedUnsubscribes = Set.empty[SubscriptionId]

    //TODO event deduplication

    //Live messages
    def receiveCommand = {
      case InitiateProcess(`id`, event, ack) =>
        persist(Started(event.aggregateKey, event.sequence)) { e =>
          addSubscription(e.from, event.sequence)
          //subscription will send us the event again
          sender() ! ack
        }

      case CommandAck(cmdId) =>
        commandConfirmed(cmdId)
      case CommandFailed(cmdId, cmd, error) =>
        log.error(s"Emitted command $cmd (sequence=$cmdId) failed: $error")
        commandConfirmed(cmdId)

      case s@SubscriptionAdded(id, to) =>
        persist(s) { _ =>
          unconfirmedSubscribes -= id
        }

      case s@SubscriptionRemoved(id) =>
        persist(s) { _ =>
          unconfirmedUnsubscribes -= id
        }
        terminateIfDone()

      case AggregateEvent(subId, event, ack) if !done && activeSubscriptions.contains(subId) =>
        state.handle.lift(event).map {
          case Continue(transition, cmds) =>
            cmds.foreach(emitCommand)
            persist(StateTransition(transition)) { t =>
              handleTransition(t)
              sender() ! ack
            }

          case Completed(cmds) =>
            log.debug(s"process execution completed, unsubscribing from ${activeSubscriptions.size} aggregates")
            done = true
            cmds.foreach(emitCommand)
            persist(CompletionStarted) { c =>
              activeSubscriptions.foreach(s => removeSubscription(s._1, s._2))
              terminateIfDone()
              sender() ! ack
            }
        }.getOrElse(sender() ! ack)

      case AggregateEvent(subId, event, ack) if !activeSubscriptions.contains(subId) =>
        //unrequested event, try to remove the subscription
        removeSubscription(subId, event.aggregateKey)

      case RequestPassivation(yes, no) =>
        if (unconfirmedCommands.isEmpty && unconfirmedSubscribes.isEmpty && unconfirmedUnsubscribes.isEmpty)
          sender() ! yes
        else
          sender() ! no

      case Passivate =>
        log.debug("passivate the process manager")
        context stop self
    }

    // From Journal
    def receiveRecover = {
      case Started(from, seq) =>
        expectedSubscriptionsAfterRecovery += from -> seq

      case StateTransition(transition) if !done =>
        val (newState, subscriptionActions) = state.applyTransition.lift(transition).
          getOrElse(throw new IllegalStateException(s"Transition $transition not expected in state $state"))
        state = newState
        subscriptionActions foreach {
          case ProcessManager.Subscribe(to) =>
            expectedSubscriptionsAfterRecovery += to -> 0L
          case ProcessManager.Unsubscribe(from) =>
            expectedSubscriptionsAfterRecovery -= from
        }

      case CompletionStarted =>
        done = true
        expectedSubscriptionsAfterRecovery = Map.empty
        ()

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

        //Resend unacknowledged commands
        commandsToResend.map(e => CommandEmitted(e._1, e._2)).foreach(sendCommand _)
        commandsToResend = Map.empty

        //Set up subscriptions
        val pendingSubscribes = expectedSubscriptionsAfterRecovery -- activeSubscriptions.values
        val pendingUnsubscribes = activeSubscriptions
          .filterNot(e => expectedSubscriptionsAfterRecovery.contains(e._2))
        pendingSubscribes.foreach((addSubscription _).tupled)
        pendingUnsubscribes.foreach(e => removeSubscription(e._1, e._2))
        expectedSubscriptionsAfterRecovery = Map.empty

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

    //Transition Handling
    def handleTransition(transition: StateTransition) = {
      val (newState, subscriptionActions) = state.applyTransition(transition.transition)
      state = newState
      subscriptionActions foreach {
        case ProcessManager.Subscribe(to) => addSubscription(to, 0)
        case ProcessManager.Unsubscribe(from) => removeSubscription(from)
      }
    }

    //Subscription handling
    def addSubscription(to: AggregateKey, fromSequence: Long) = {
      val subscriptionId = SubscriptionId(id, to)
      val ack = SubscriptionAdded(subscriptionId, to)
      activeSubscriptions += subscriptionId -> to
      unconfirmedSubscribes += subscriptionId
      commandTarget ! SubscribeToAggregate(subscriptionId, to, publicRef.path, fromSequence, ack)
    }
    def removeSubscription(to: AggregateKey): Unit = {
      activeSubscriptions.filter(_._2 == to).map(_._1).foreach(removeSubscription(_, to))
    }
    def removeSubscription(subscriptionId: SubscriptionId, to: AggregateKey): Unit = {
      activeSubscriptions -= subscriptionId
      unconfirmedUnsubscribes += subscriptionId
      commandTarget ! UnsubscribeFromAggregate(subscriptionId, to, SubscriptionRemoved(subscriptionId))
    }

    //Termination
    def terminateIfDone() = {
      if (done && activeSubscriptions.isEmpty && unconfirmedCommands.isEmpty)
        persist(ProcessCompleted)(_ => completeProcess())
    }
    def completeProcess() = {
      log.debug("process is completed")
      context stop self
    }
  }

  private sealed trait PersitentEvent
  private case class Started(from: AggregateKey, atSequence: Long) extends PersitentEvent
  private case class CommandEmitted(id: Long, command: C) extends PersitentEvent
  private case class CommandDelivered(id: Long) extends PersitentEvent
  private case class StateTransition(transition: Transition) extends PersitentEvent
  private case class SubscriptionAdded(id: SubscriptionId, to: AggregateKey) extends PersitentEvent
  private case class SubscriptionRemoved(id: SubscriptionId) extends PersitentEvent
  private case object CompletionStarted extends PersitentEvent
  private case object ProcessCompleted extends PersitentEvent


  private sealed trait CommandResponse {
    def id: Long
  }
  private case class CommandAck(id: Long) extends CommandResponse
  private case class CommandFailed(id: Long, cmd: C, error: E) extends CommandResponse
}
