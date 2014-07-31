package es.api

/**
 * Read-only representation of the data from one or more aggregate type.
 * The infrastructure provides capability to restart reading at a specific position in the event stream. The event-ordering
 * is stable (will not change, if the same read model (same name) is read twice). Events are in-order per aggregate, event
 * ordering between aggregate is arbitrary.
 */
trait ReadModel {
  /**
   * Apply the event to the read model, save it including the position and then call ack to get the receive the event.
   * @param event
   * @param position of the current event in the stream. Persist along the the data to be able to recover from there
   *                 (instead of from the beginning).
   * @param ack call if the event has been handled and the read model is ready for the next event (backpressure).
   */
  def handleEvent(event: EventData, position: ReadModelPosition, ack: () => Unit): Unit
}

trait ReadModelConnector {
  /**
   * Register the read model to start receiving events.
   * @param name unique name of the read model
   * @param model read model
   * @param on aggregate types to subscribe on. If a type is added (on a already established
   * @param at last position handled by the ReadModel. The event stream will start with the next event after that.
   */
  def registerReadModel(name: String, model: ReadModel, on: Traversable[AggregateType])(at: ReadModelPosition): Unit
}

/** Position within the event stream. */
case class ReadModelPosition(serialized: String)
object ReadModelPosition {
  val fromStart = ReadModelPosition("")
}