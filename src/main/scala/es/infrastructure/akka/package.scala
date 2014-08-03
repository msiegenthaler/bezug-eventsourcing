package es.infrastructure

package object akka {
  
  /** Execute the command and the reply with onSuccess or onFailed to the sender of the message. */
  case class Execute[Cmd, Err](command: Cmd, onSuccess: Any, onFailed: Err => Any)
}
