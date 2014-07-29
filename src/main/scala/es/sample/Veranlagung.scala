package es.sample

import java.util.UUID
import es.{AggregateActorBinding, AggregateType}

object veranlagung extends AggregateType {
  type Id = UUID

  //Commands
  sealed trait Command {
    val veranlagung: Id
  }
  case class Request() extends Command {
    val veranlagung = UUID.randomUUID
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

    def applyEvent(e: Event) = e match {
      case FilledOut(einkommen) => copy(einkommen = Some(einkommen))
      case Submitted => copy(finished = true)
    }
  }

  protected def commandMatcher = {
    case c: Command => c
  }
  protected def eventMatcher = {
    case e: Event => e
  }
  protected def errorMatcher = {
    case e: Error => e
  }

  private[sample] def seed(id: Id) = Veranlagung(id, None, false)
}