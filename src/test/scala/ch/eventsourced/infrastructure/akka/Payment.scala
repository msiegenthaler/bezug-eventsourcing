package ch.eventsourced.infrastructure.akka

import ch.eventsourced.api.AggregateType
import ch.eventsourced.support.TypedGuid

object Payment extends AggregateType with TypedGuid {
  def name = "Payment"

  sealed trait Event
  case class PaymentRequested(amount: Money, reference: String) extends Event
  case object PaymentConfirmed extends Event
  case class PaymentFailed(reason: String) extends Event

  sealed trait Command {
    def payment: Id
  }
  case class RequestPayment(amount: Money, reference: String, payment: Id = generateId) extends Command
  case class ConfirmPayment(payment: Id, amount: Money) extends Command
  case class DenyPayment(payment: Id, reason: String) extends Command
  def aggregateIdForCommand(command: Command) = Some(command.payment)

  sealed trait Error
  case object NotRequested extends Error
  case object CannotRequestAgain extends Error
  case object WrongAmount extends Error
  case object AlreadyConfirmed extends Error
  case object AlreadyFailed extends Error

  type Money = Int

  type Root = Payment
  sealed trait Payment extends RootBase
  case class EmptyPayment(id: Id) extends Payment {
    def execute(c: Command) = c match {
      case RequestPayment(amount, ref, `id`) => PaymentRequested(amount, ref)
      case _ => NotRequested
    }
    def applyEvent = {
      case PaymentRequested(amount, ref) => RequestedPayment(id, amount, ref, false, false)
    }
  }
  case class RequestedPayment(id: Id, amount: Money, ref: String, confirmed: Boolean, failed: Boolean) extends Payment {
    def execute(c: Command) = c match {
      case ConfirmPayment(`id`, a) if !failed =>
        if (confirmed) ()
        else if (a != amount) WrongAmount
        else PaymentConfirmed
      case ConfirmPayment(`id`, _) => AlreadyFailed

      case DenyPayment(`id`, reason) if !confirmed =>
        if (!failed) PaymentFailed(reason) else ()
      case DenyPayment(`id`, reason) => AlreadyConfirmed

      case RequestPayment(`amount`, `ref`, `id`) => ()
      case _ => CannotRequestAgain
    }
    def applyEvent = {
      case PaymentConfirmed => copy(confirmed = true)
    }
  }
  def seed(id: Id) = EmptyPayment(id)

  protected def types = typeInfo
}
