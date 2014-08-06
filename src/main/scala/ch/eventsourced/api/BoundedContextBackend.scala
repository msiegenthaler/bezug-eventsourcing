package ch.eventsourced.api

import akka.actor.ActorRef
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scalaz.Validation


/** Running instance of the backend of a bounded context. Do not directly expose to consumers, use a facade. */
trait BoundedContextBackend[Command, Event, Error] {
  def execute(c: Command)(implicit timeout: FiniteDuration): Future[Validation[Error, Unit]]
  def shutdown()(implicit t: FiniteDuration): Future[Unit]
}

/** Definition of a bounded context. */
trait BoundedContextBackendType {
  type Command
  type Event
  type Error
  type Backend = BoundedContextBackend[Command, Event, Error]

  def name: String
  def aggregates: Traversable[CommonAggregateType]
  def processManagers: Traversable[CommonProcessManagerType]
  def readModels: Traversable[ReadModelRegistration]

  // Needed to prove that the Command/Event type is actually a supertype of the types used inside aggregates/process managers
  private type Cmd = Command
  private type Evt = Event
  private type Err = Error
  private type CommonAggregateType = AggregateType {
    type Command <: Cmd
    type Event <: Evt
    type Error <: Err
  }
  private type CommonProcessManagerType = ProcessManagerType {
    type Command <: Cmd
    type Error <: Err
  }
}

/** Is implemented by an SPI that offers an implementation of the persistence and the event bus. */
trait Infrastructure {
  def startContext(definition: BoundedContextBackendType, pubSub: ActorRef): BoundedContextBackend[definition.Command, definition.Event, definition.Error]
}