package es.api


/** Event with metadata. Used to preserve an events context. */
sealed trait EventData {
  val aggregateType: AggregateType
  /** the aggregate affected by the event. */
  def aggregate: aggregateType.Id
  /** sequence number of the event within the aggregate (0 is first, 1 is second, ...). */
  def sequence: Long
  def event: aggregateType.Event

  def aggregateKey = aggregateType.AggregateKey(aggregate)
}
object EventData {
  def apply[A <: AggregateType](aggregateType: A)(aggregate: aggregateType.Id, sequence: Long, event: aggregateType.Event): EventData = {
    val at = aggregateType
    def a = aggregate
    def s = sequence
    def e = event
    new EventData {
      val aggregateType = at
      def aggregate = a.asInstanceOf[aggregateType.Id]
      def event = e.asInstanceOf[aggregateType.Event]
      def sequence = s
      override def hashCode = aggregate.hashCode ^ sequence.hashCode
      override def equals(o: Any) = o match {
        case e: EventData =>
          aggregateType == e.aggregateType &&
            aggregate == e.aggregate &&
            sequence == e.sequence &&
            event == e.event
        case _ => false
      }
      override def toString = s"EventData($aggregateType, $sequence, $event)"
    }
  }
  def unapply(e: Any): Option[(AggregateType, Any, Long, Any)] = e match {
    case e: EventData => Some((e.aggregateType, e.aggregate, e.sequence, e.event))
    case _ => None
  }
}