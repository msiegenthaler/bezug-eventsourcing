package ch.eventsourced.infrastructure.akka

import java.net.URLEncoder
import scala.util.hashing.MurmurHash3
import akka.actor.{ActorSystem, Props, ActorRef}
import akka.contrib.pattern.ClusterSharding
import akka.contrib.pattern.ShardRegion._
import akka.persistence.{RecoveryCompleted, PersistentActor}
import ch.eventsourced.api.{EventData, ProcessManagerType}

/**
 * Handles the running instances of a process manager type.
 * - Keeps track of running processes and instantiates them on startup.
 * - Starts new processes in response to ProcessInitationMessage.
 * - Distributes running processes across the nodes.
 *
 * Do not change the manager count after the first start of the application, the pending ProcessManagers will
 * not work anymore after the change. Terminated and new instances are not affected.
 */
class ProcessManagerActor[C, E](contextName: String, val processManagerType: ProcessManagerType {type Command <: C; type Error <: E},
  commandDistributor: ActorRef)(system: ActorSystem, managerCount: Int = 1000) {
  import processManagerType._
  private val fullName = s"$contextName/ProcessManager/$name/Manager"

  /** Message that starts a process if it is not already started. */
  case class ProcessInitationMessage(process: Id, event: EventData, ack: Any)

  /** Handles ProcessInitationMessage messages. */
  def ref: ActorRef = region

  /** Handles EventData messages and starts process instances as needed. */
  val initiator = {
    val i = new ProcessInitator(processManagerType, initiatorMessage)
    val name = URLEncoder.encode(s"$fullName/Initiator", "UTF-8")
    system.actorOf(i.props(ref), name)
  }

  /** Aggregate types to register the initiator to. */
  def registerOn = processManagerType.triggeredBy


  private val idExtractor: IdExtractor = {
    case msg@ProcessInitationMessage(id, _, _) =>
      val sid = serializeId(id)
      val managerId = MurmurHash3.stringHash(sid) % managerCount
      (managerId.toString, msg)
    case any => ("", any)
  }
  private val shardResolver: ShardResolver = idExtractor.andThen(_._1)
  private def initiatorMessage(id: Id, event: EventData, ack: Any) =
    ProcessInitationMessage(id, event, ack)

  private val region = {
    ClusterSharding(system).start(fullName, Some(Props(new ManagerActor)), idExtractor, shardResolver)
  }

  private val instance = new ProcessManagerInstance(contextName, processManagerType, commandDistributor)


  private class ManagerActor extends PersistentActor {
    val persistenceId = s"$contextName/ProcessesManager/$name/${self.path.name}"

    //TODO use snapshots to speed up loading?
    private var runningProcesses = Map.empty[Id, ActorRef]
    //TODO this might get a bit memory intensive... maybe better use a bloom filter and an external storage?
    private var terminatedProcesses = Set.empty[Id]

    def receiveCommand = {
      case ProcessInitationMessage(id, event, ack)
        if !runningProcesses.contains(id) && !terminatedProcesses.contains(id) =>
        val pia = ProcessInitAck(id, sender(), ack)
        startProcess(id) ! instance.InitiateProcess(event, pia)

      case ProcessInitAck(id, origin, ack) =>
        persist(ProcessStarted(id)) { e =>
          origin ! ack
        }

      case instance.ProcessCompleted(id) if runningProcesses.contains(id) =>
        persist(ProcessEnded) { event =>
          runningProcesses -= id
          terminatedProcesses += id
        }
    }

    def receiveRecover = {
      case ProcessStarted(id) =>
        processesToStart += id
      case ProcessEnded(id) =>
        processesToStart -= id
        terminatedProcesses += id
      case RecoveryCompleted =>
        processesToStart foreach startProcess
        processesToStart = Set.empty
    }
    private var processesToStart = Set.empty[Id]

    def startProcess(id: Id): ActorRef = {
      val ref = context actorOf instance.props(id)
      runningProcesses += id -> ref
      ref
    }
  }
  private sealed trait Event
  private case class ProcessStarted(id: Id) extends Event
  private case class ProcessEnded(id: Id) extends Event

  private case class ProcessInitAck(id: Id, origin: ActorRef, ack: Any)
}
