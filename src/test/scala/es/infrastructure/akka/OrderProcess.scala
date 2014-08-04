package es.infrastructure.akka


import es.api.ProcessManager.{Unsubscribe, Subscribe}
import es.support.GuidProcessManagerType

class OrderProcess extends GuidProcessManagerType {
  def name = "OrderProcess"

  type Command = Any

  def triggeredBy = Set(Order)
  def initiate = {
    case Order.EventData(id, _, _: Order.OrderPlaced) => Id(id.guid)
  }

  case class Manager(id: Id, order: Order.Id, payment: Option[Payment.Id], placed: Boolean) extends BaseManager {
    def handle = {
      case Order.Event(Order.OrderPlaced(items, total, billRef)) if payment.isEmpty =>
        val paymentRequest = Payment.RequestPayment(total, billRef)
        Continue(copy(placed = true, payment = Some(paymentRequest.payment))) +
          paymentRequest +
          Subscribe(Payment.AggregateKey(paymentRequest.payment)) +
          Unsubscribe(Order.AggregateKey(order))

      case Payment.Event(Payment.PaymentConfirmed) =>
        Completed() + Order.CompleteOrder(order)
      case Payment.Event(Payment.PaymentFailed(_)) =>
        Completed() + Order.CancelOrder(order)
    }
  }
  def seed(id: Id) = Manager(id, Order.Id(id.guid), None, false)
}
