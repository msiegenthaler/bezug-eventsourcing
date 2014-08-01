package pubsub

/** Position within a subscription (opaque for users). */
trait Position
object Position {
  val start: Position = StartPosition
}

case object StartPosition extends Position

/** Update to a position. */
trait PositionUpdate {
  def apply(pos: Position): Position
}
