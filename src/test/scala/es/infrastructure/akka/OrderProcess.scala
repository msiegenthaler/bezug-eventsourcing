package es.infrastructure.akka

import java.util.UUID

import es.api.ProcessManager.Subscribe
import es.api.{EventData, ProcessManagerType}

import scala.util.Try

class OrderProcess extends ProcessManagerType {
  def name = "OrderProcess"

  type Id = UUID
  def serializeId(id: Id) = id.toString
  def parseId(serialized: String) = Try(UUID.fromString(serialized)).toOption

  type Command = Any

  def triggeredBy = Set(Order)
  def initiate = {
    case Order.EventData(id, _, _: Order.OrderPlaced) => id
  }

  case class Manager(id: Id, order: Order.Id, payment: Option[Payment.Id], placed: Boolean) extends BaseManager {
    def handle = {
      case Order.Event(Order.OrderPlaced(items, total, billRef)) if payment.isEmpty =>
        val paymentRequest = Payment.RequestPayment(total, billRef)
        Continue(copy(placed = true, payment = Some(paymentRequest.payment))) +
          paymentRequest +
          Subscribe(Payment.AggregateKey(paymentRequest.payment))

      case Payment.Event(Payment.PaymentConfirmed) =>
        Completed() + Order.CompleteOrder(order)
      case Payment.Event(Payment.PaymentFailed(_)) =>
        Completed() + Order.CancelOrder(order)
    }
  }
  def seed(id: Id) = Manager(id, id, None, false)
}
