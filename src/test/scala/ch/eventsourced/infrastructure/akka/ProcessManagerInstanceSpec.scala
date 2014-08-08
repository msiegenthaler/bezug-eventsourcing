package ch.eventsourced.infrastructure.akka

import akka.actor.{PoisonPill, ActorRef}
import akka.testkit.TestProbe
import ch.eventsourced.api.{AggregateKey, EventData}
import ch.eventsourced.infrastructure.akka.AggregateManager.{UnsubscribeFromAggregate, Execute, AggregateEvent, SubscribeToAggregate}
import ch.eventsourced.infrastructure.akka.Payment.PaymentConfirmed
import ch.eventsourced.support.Guid

class ProcessManagerInstanceSpec extends AbstractSpec {

  val orderPmi = new ProcessManagerInstance("test", OrderProcess)
  def startPmi(orderId: Order.Id, cmdDist: ActorRef) = {
    val props = orderPmi.props(OrderProcess.Id(orderId.guid), cmdDist)
    system actorOf props
  }

  class TestOrder {
    val id = Order.StartOrder().order
    def key = Order.AggregateKey(id)
    def placed = {
      val items = ("Hat", 20) ::("Shirt", 40) ::("Pants", 20) ::("Shoes", 20) :: Nil
      val event = Order.OrderPlaced(items, 100, "test-bill")
      Order.EventData(id, 2, event)
    }
  }

  implicit class RichCommandProbe(t: TestProbe) {
    def expectSubscribe() = {
      t.expectMsgPF() {
        case SubscribeToAggregate(sid, to, _, 0, ack) => (sid, ack, to)
      }
    }
    def expectSubscribe(to: AggregateKey, from: Long) = {
      t.expectMsgPF() {
        case SubscribeToAggregate(sid, `to`, _, `from`, ack) => (sid, ack)
      }
    }
    def expectUnsubscribe(sid: String, from: AggregateKey) = {
      t.expectMsgPF() {
        case UnsubscribeFromAggregate(`sid`, `from`, ack) => ack
      }
    }
    def expectCommand[A](pf: PartialFunction[OrderProcess.Command, A]) = {
      t.expectMsgPF() {
        case Execute(cmd, ok, fail) if pf.isDefinedAt(cmd) => (ok, fail, pf(cmd))
      }
    }
  }

  "processManager instance" must {
    "handle an initiation event" in {
      val o = new TestOrder
      val commandProbe = TestProbe()
      val pmi = startPmi(o.id, commandProbe.ref)
      pmi ! orderPmi.InitiateProcess(o.placed, "ack1")
      expectMsg("ack1")
    }

    "subscribe to the source of the initiation event" in {
      val o = new TestOrder
      val commandProbe = TestProbe()
      val pmi = startPmi(o.id, commandProbe.ref)
      pmi ! orderPmi.InitiateProcess(o.placed, "ack1")
      expectMsg("ack1")
      val (_, ack) = commandProbe.expectSubscribe(o.key, o.placed.sequence)
      commandProbe.reply(ack)
    }

    "execute the actions returned by the process manager" in {
      val o = new TestOrder
      val commandProbe = TestProbe()
      val pmi = startPmi(o.id, commandProbe.ref)
      pmi ! orderPmi.InitiateProcess(o.placed, "ack1")
      expectMsg("ack1")
      val (orderSubscription, a1) = commandProbe.expectSubscribe(o.key, o.placed.sequence)
      commandProbe.reply(a1)

      pmi ! AggregateEvent(orderSubscription, o.placed, "ack2")
      expectMsg("ack2")

      //Subscribe to payment (earlier than Request, but no problem)
      val (_, a2, paymentKey) = commandProbe.expectSubscribe()
      commandProbe.reply(a2)
      //Unsubscribe from order
      val a3 = commandProbe.expectUnsubscribe(orderSubscription, o.key)
      commandProbe.reply(a3)
      //Request payment
      val (a4, _, paymentKey2) = commandProbe.expectCommand {
        case Payment.RequestPayment(100, "test-bill", id) => id
      }
      commandProbe.reply(a4)
      assert(paymentKey.aggregateType == Payment)
      assert(paymentKey.id === paymentKey2)
    }

    "unsubscribe from all when done" in {
      val o = new TestOrder
      val commandProbe = TestProbe()
      val pmi = startPmi(o.id, commandProbe.ref)
      pmi ! orderPmi.InitiateProcess(o.placed, "ack1")
      expectMsg("ack1")
      val (orderSubscription, a1) = commandProbe.expectSubscribe(o.key, o.placed.sequence)
      pmi ! a1

      pmi ! AggregateEvent(orderSubscription, o.placed, "ack2")
      expectMsg("ack2")

      //Subscribe to payment (earlier than Request, but no problem)
      val (paymentSubscription, a2, paymentKey) = commandProbe.expectSubscribe()
      commandProbe.reply(a2)
      //Unsubscribe from order
      val a3 = commandProbe.expectUnsubscribe(orderSubscription, o.key)
      commandProbe.reply(a3)
      //Request payment
      val (a4, _, paymentKey2) = commandProbe.expectCommand {
        case Payment.RequestPayment(100, "test-bill", id) => id
      }
      commandProbe.reply(a4)

      pmi ! AggregateEvent(paymentSubscription, Payment.EventData(paymentKey2, 1, PaymentConfirmed), "ack3")
      expectMsg("ack3")

      //Unsubscribe from payment
      val a5 = commandProbe.expectUnsubscribe(paymentSubscription, paymentKey)
      commandProbe.reply(a5)
      //Complete order
      val (a6, _, orderId2) = commandProbe.expectCommand {
        case Order.CompleteOrder(id) => id
      }
      commandProbe.reply(a6)
      assert(orderId2 === o.id)

      commandProbe.expectNoMsg()
    }

    "resends unconfirmed commands" in {
      val o = new TestOrder
      val commandProbe = TestProbe()
      val pmi = startPmi(o.id, commandProbe.ref)
      pmi ! orderPmi.InitiateProcess(o.placed, "ack1")
      expectMsg("ack1")
      val (orderSubscription, a1) = commandProbe.expectSubscribe(o.key, o.placed.sequence)
      commandProbe.reply(a1)

      pmi ! AggregateEvent(orderSubscription, o.placed, "ack2")
      expectMsg("ack2")

      //Subscribe to payment (earlier than Request, but no problem)
      val (_, a2, paymentKey) = commandProbe.expectSubscribe()
      commandProbe.reply(a2)
      //Unsubscribe from order
      val a3 = commandProbe.expectUnsubscribe(orderSubscription, o.key)
      commandProbe.reply(a3)
      //Request payment
      val (a4, _, paymentKey2) = commandProbe.expectCommand {
        case Payment.RequestPayment(100, "test-bill", id) => id
      }

      Thread.sleep(1000)
      val (a5, _, _) = commandProbe.expectCommand {
        case Payment.RequestPayment(100, "test-bill", `paymentKey2`) => ()
      }
      commandProbe.reply(a5)
      commandProbe.expectNoMsg()
    }

    "resends unconfirmed commands after restart" in {
      val o = new TestOrder
      val commandProbe = TestProbe()
      val pmi = startPmi(o.id, commandProbe.ref)
      pmi ! orderPmi.InitiateProcess(o.placed, "ack1")
      expectMsg("ack1")
      val (orderSubscription, a1) = commandProbe.expectSubscribe(o.key, o.placed.sequence)
      commandProbe.reply(a1)

      pmi ! AggregateEvent(orderSubscription, o.placed, "ack2")
      expectMsg("ack2")

      //Subscribe to payment (earlier than Request, but no problem)
      val (_, a2, paymentKey) = commandProbe.expectSubscribe()
      commandProbe.reply(a2)
      //Unsubscribe from order
      val a3 = commandProbe.expectUnsubscribe(orderSubscription, o.key)
      commandProbe.reply(a3)
      //Request payment
      val (a4, _, paymentKey2) = commandProbe.expectCommand {
        case Payment.RequestPayment(100, "test-bill", id) => id
      }
      pmi ! PoisonPill

      val commandProbe2 = TestProbe()
      val pmi2 = startPmi(o.id, commandProbe2.ref)
      val (a5, _, _) = commandProbe.expectCommand {
        case Payment.RequestPayment(100, "test-bill", `paymentKey`) => ()
      }
      commandProbe2.reply(a5)
      commandProbe2.expectNoMsg()
    }

    "does not resend confirmed commands after restart" in {
      val o = new TestOrder
      val commandProbe = TestProbe()
      val pmi = startPmi(o.id, commandProbe.ref)
      pmi ! orderPmi.InitiateProcess(o.placed, "ack1")
      expectMsg("ack1")
      val (orderSubscription, a1) = commandProbe.expectSubscribe(o.key, o.placed.sequence)
      commandProbe.reply(a1)

      pmi ! AggregateEvent(orderSubscription, o.placed, "ack2")
      expectMsg("ack2")

      //Subscribe to payment (earlier than Request, but no problem)
      val (_, a2, paymentKey) = commandProbe.expectSubscribe()
      commandProbe.reply(a2)
      //Unsubscribe from order
      val a3 = commandProbe.expectUnsubscribe(orderSubscription, o.key)
      commandProbe.reply(a3)
      //Request payment
      val (a4, _, paymentKey2) = commandProbe.expectCommand {
        case Payment.RequestPayment(100, "test-bill", id) => id
      }
      commandProbe.reply(a4)
      pmi ! PoisonPill

      val commandProbe2 = TestProbe()
      val pmi2 = startPmi(o.id, commandProbe2.ref)
      commandProbe2.expectNoMsg()
    }
  }

}
