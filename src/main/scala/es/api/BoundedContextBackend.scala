package es.api

import scala.concurrent.Future
import scalaz.Validation


/** Running instance of the backend of a bounded context. Do not directly expose to consumers, use a facade. */
trait BoundedContextBackend[Command, Event] {
  def execute(c: Command): Future[Validation[Error, Unit]]
  //TODO expose events (resp. event subscription)

  def shutdown: Future[Unit]
}

/** Definition of a bounded context. */
trait BoundedContextBackendType {
  type Command
  type Event
  type Backend = BoundedContextBackend[Command, Event]

  def aggregates: Traversable[CommonAggregateType]
  def processManagers: Traversable[CommonProcessManagerType]
  def readModels: Traversable[ReadModelRegistration]

  // Needed to prove that the Command/Event type is actually a supertype of the types used inside aggregates/process managers
  private type Cmd = Command
  private type Evt = Event
  private type CommonAggregateType = AggregateType {
    type Command <: Cmd
    type Event <: Evt
  }
  private type CommonProcessManagerType = ProcessManagerType {
    type Command <: Cmd
  }
}

/** Is implemented by an SPI that offers an implementation of the persistence and the event bus. */
trait Infrastructure {
  def startContext(definition: BoundedContextBackendType): BoundedContextBackend[definition.Command, definition.Event]
}