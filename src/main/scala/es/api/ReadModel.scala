package es.api

/**
 * Read-only representation of the data from one or more aggregate type.
 * The infrastructure provides capability to restart reading at a specific position in the event stream. Events are
 * in-order per aggregate, event ordering between aggregate is arbitrary.
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

trait ReadModelRegistration {
  def name: String
  def model: ReadModel

  /** Aggregate types to subscribe on.
    * If an aggregate type is added (on a already known) then all events from the new type will be sent as well (retroactive).
    * If an aggregate type is removed then the read model might still receive events of this type for some time.
    */
  def subscribeTo: Traversable[AggregateType]

  /** The last position handled by the ReadModel. The event stream will start with the next event after that. */
  def startFrom: ReadModelPosition
}

/** Position within the event stream. */
case class ReadModelPosition(serialized: String)
object ReadModelPosition {
  val fromStart = ReadModelPosition("")
}