package es.infrastructure.akka

import akka.actor.{ActorLogging, Props, ActorRef}
import akka.persistence.{RecoveryCompleted, PersistentActor}
import es.api._

/**
 * Represents a running instance of a process manager.
 *
 * - Handle the events and send the commands
 * - Subscribe/unsubscribe to/from aggregate events
 */
class ProcessManagerInstance[I, C, E](contextName: String,
  val processManagerType: ProcessManagerType {type Id = I; type Command <: C; type Error <: E},
  commandBus: ActorRef) {
  import processManagerType._

  case class InitiateProcess(process: Id, event: EventData, ack: Any)
  case class ProcessCompleted(id: Id)

  def props(id: Id): Props = Props(new Process(id))


  private class Process(id: Id) extends PersistentActor with ActorLogging {
    def persistenceId = s"$contextName/ProcessManager/$name/$id"
    val commandTarget = context actorOf OrderPreservingAck.props(commandBus) {
      case Execute(_, ok, fail) => msg => msg == ok || msg == fail
    }

    private var state = seed(id)
    private var done = false

    def receiveCommand = {
      case InitiateProcess(`id`, event, ack) =>
        persist(Started(event.aggregateKey)) { e =>
          addSubscription(e.from)
          sender() ! ack
        }
        receiveCommand(event) //handle the initiating event

      case CommandAck(cmdId) =>
        commandConfirmed(cmdId)

      case CommandFailed(cmdId, cmd, error) =>
        log.error(s"Emitted command $cmd (sequence=$cmdId) failed: $error")
        commandConfirmed(cmdId)

      //TODO is it wrapped? depends on subscription implementation
      case event: EventData if !done =>
        def ack(event: EventData) = ??? //TODO how?
        state.handle.lift(event).foreach {
          case Continue(next, cmds, subscriptionActions) =>
            state = next
            cmds.foreach(emitCommand)
            subscriptionActions foreach {
              case ProcessManager.Subscribe(to) => addSubscription(to)
              case ProcessManager.Unsubscribe(from) => removeSubscription(from)
            }
            persist(event)(ack)

          case Completed(cmds) if cmds.isEmpty =>
            done = true
            cmds.foreach(emitCommand)
            persist(event)(ack)

          case Completed(_) =>
            done = true
            persist(event)(ack)
            persist(Completed)(_ => processDone())
        }
    }

    def receiveRecover = {
      case Started(from) =>
        subscriptionsToRecover += from

      case event: EventData =>
        state.handle.lift(event) foreach {
          case Completed(_) => processDone()
          case Continue(next, _, subscriptionActions) =>
            state = next
            subscriptionActions foreach {
              case ProcessManager.Subscribe(to) =>
                subscriptionsToRecover += to
              case ProcessManager.Unsubscribe(from) =>
                subscriptionsToRecover -= from
            }
        }

      case CommandEmitted(id, cmd) =>
        commandsToResend += id -> cmd
        nextCommandId += 1
      case CommandDelivered(id) =>
        commandsToResend -= id

      case RecoveryCompleted =>
        log.debug(s"Loaded from event store, setting up ${subscriptionsToRecover.size} subscriptions")
        //Set up subscriptions
        subscriptionsToRecover.foreach(addSubscription)
        subscriptionsToRecover = Set.empty
        //Resend unacknowledged commands
        commandsToResend.map(e => CommandEmitted(e._1, e._2)).foreach(sendCommand _)
        commandsToResend = Map.empty
    }
    private var subscriptionsToRecover = Set.empty[AggregateKey]
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
      if (done && unconfirmedCommands.isEmpty) {
        persist(Completed)(_ => processDone())
      }
    }
    private var nextCommandId = 0L
    private var unconfirmedCommands = Set.empty[Long]


    //Subscription handling
    def addSubscription(to: AggregateKey) = {
      ??? //TODO
    }
    def removeSubscription(to: AggregateKey) = {
      ??? //TODO
    }

    //Termination
    def processDone() = {
      //we don't ack this message because it will be just sent again if the process is waken up again
      context.parent ! ProcessCompleted(id)
      context stop self
    }
  }

  private sealed trait Event
  private case class Started(from: AggregateKey) extends Event
  private case class CommandEmitted(id: Long, command: C) extends Event
  private case class CommandDelivered(id: Long) extends Event

  private sealed trait CommandResponse {
    def id: Long
  }
  private case class CommandAck(id: Long) extends CommandResponse
  private case class CommandFailed(id: Long, cmd: C, error: E) extends CommandResponse
}
