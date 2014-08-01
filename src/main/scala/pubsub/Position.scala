package pubsub

import akka.util.ByteString

/** Position with a subscription (opaque for users). */
case class Position(serialized: ByteString)
object Position {
  def start = Position(ByteString.empty)
}
