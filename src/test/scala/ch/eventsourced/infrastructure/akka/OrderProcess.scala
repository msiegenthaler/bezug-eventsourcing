package ch.eventsourced.infrastructure.akka


import ch.eventsourced.api.ProcessManager.{Unsubscribe, Subscribe}
import ch.eventsourced.support.GuidProcessManagerType

object OrderProcess extends GuidProcessManagerType {
  def name = "OrderProcess"

  type Command = Any

  def triggeredBy = Set(Order)
  def initiate = {
    case Order.EventData(id, _, _: Order.OrderPlaced) => Id(id.guid)
  }

  sealed trait Transition
  case class PaymentRequested(payment: Payment.Id) extends Transition

  case class Manager(id: Id, order: Order.Id, payment: Option[Payment.Id], placed: Boolean) extends BaseManager {
    def handle = {
      case Order.Event(Order.OrderPlaced(items, total, billRef)) if payment.isEmpty =>
        val paymentRequest = Payment.RequestPayment(total, billRef)
        Continue(PaymentRequested(paymentRequest.payment)) + paymentRequest
      case Payment.Event(Payment.PaymentConfirmed) =>
        Completed() + Order.CompleteOrder(order)
      case Payment.Event(Payment.PaymentFailed(_)) =>
        Completed() + Order.CancelOrder(order)
    }
    def applyTransition = {
      case PaymentRequested(payment) =>
        copy(payment = Some(payment)) +
          Subscribe(Payment.AggregateKey(payment)) +
          Unsubscribe(Order.AggregateKey(order))
    }
  }
  def seed(id: Id) = Manager(id, Order.Id(id.guid), None, false)
}
