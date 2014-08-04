package es.sample

import es.support.{Guid, GuidAggregateType}

object veranlagung extends GuidAggregateType {
  def name = "Veranlagung"

  //Commands
  sealed trait Command {
    val veranlagung: Id
  }
  case class Request() extends Command {
    val veranlagung = generateId
  }
  case class FillOut(veranlagung: Id, einkommen: Long) extends Command
  case class SubmitToAuthority(veranlagung: Id) extends Command
  //Events
  sealed trait Event
  case class FilledOut(einkommen: Long) extends Event
  case object Submitted extends Event
  //Errors
  sealed trait Error
  case object Incomplete extends Error
  case object AlreadySubmitted extends Error
  case object Unhandled extends Error

  type Root = Veranlagung
  case class Veranlagung(id: Id, einkommen: Option[Long], finished: Boolean) extends RootBase {
    def execute(c: Command) = c match {
      case Request() =>
        Seq.empty
      case FillOut(`id`, einkommen: Long) =>
        if (finished) AlreadySubmitted
        else FilledOut(einkommen)
      case SubmitToAuthority(`id`) =>
        if (finished) AlreadySubmitted
        if (einkommen.isEmpty) Incomplete
        else Submitted
      case _ => Unhandled
    }

    def applyEvent = {
      case FilledOut(einkommen) => copy(einkommen = Some(einkommen))
      case Submitted => copy(finished = true)
    }
  }

  def seed(id: Id) = Veranlagung(id, None, false)
  def aggregateIdForCommand(command: Command) = Some(command.veranlagung)
  protected def types = typeInfo
}