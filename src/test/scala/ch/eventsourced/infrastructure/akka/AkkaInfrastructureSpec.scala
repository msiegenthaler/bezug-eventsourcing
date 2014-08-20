package ch.eventsourced.infrastructure.akka

import ch.eventsourced.api.EventData
import scala.concurrent.duration._
import akka.testkit.ImplicitSender
import org.scalatest.Matchers
import ch.eventsourced.shop._

class AkkaInfrastructureSpec extends ContextBackendTestKit with ImplicitSender with Matchers {
  val context = ShopContext

  implicit val timeout = 5.second

  "example context with akka infrastructure" must {
    "start" in {
      pubSub.expectNoEvents
    }

    "must execute commands and publish to pubSub" in {
      val start = Order.StartOrder()
      def order = start.order
      execute(start)
      execute(Order.AddItem(order, "test", 10))
      pubSub.expectEvent() { case Order.ItemAdded("test", 10) => ()}
    }

    "return errors on non-ok commands" in {
      val start = Order.StartOrder()
      def order = start.order
      execute(start)
      expectCommandFailure(Order.CompleteOrder(order), Order.OrderIsOpen)
    }

    "must execute processes" in {
      val start = Order.StartOrder()
      def order = start.order
      execute(start)
      execute(Order.AddItem(order, "test", 10))
      execute(Order.PlaceOrder(order))

      val ref = pubSub.expectEvent() { case Order.OrderPlaced(("test", 10) :: Nil, 10, ref) => ref}
      pubSub.expectEvent() { case Payment.PaymentRequested(10, `ref`) => ()}
    }

    "must execute multi-step processes " in {
      val start = Order.StartOrder()
      def order = start.order
      execute(start)
      execute(Order.AddItem(order, "test", 10))
      execute(Order.PlaceOrder(order))

      val ref = pubSub.expectEvent() { case Order.OrderPlaced(("test", 10) :: Nil, 10, ref) => ref}
      val paymentId = pubSub.expect() { case EventData(_, id: Payment.Id, _, Payment.PaymentRequested(10, `ref`)) => id}
      pubSub.reset()

      execute(Payment.ConfirmPayment(paymentId, 10))
      pubSub.expectEvent() { case Payment.PaymentConfirmed => ()}
      pubSub.expectEvent() { case Order.OrderCompleted => ()}
    }
  }
}

