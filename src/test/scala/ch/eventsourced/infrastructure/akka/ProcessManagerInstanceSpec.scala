package ch.eventsourced.infrastructure.akka

import akka.actor.{PoisonPill, ActorRef}
import akka.testkit.TestProbe
import ch.eventsourced.api.AggregateKey
import ch.eventsourced.infrastructure.akka.AggregateActor._
import ch.eventsourced.infrastructure.akka.Payment.PaymentConfirmed

class ProcessManagerInstanceSpec extends AbstractSpec {

  def startPmi(orderId: Order.Id, cmdDist: ActorRef) = {
    val orderPmi = new ProcessManagerActor("test", OrderProcess, cmdDist)
    val actor = system actorOf LocalSharder.props(orderPmi)
    (orderPmi, actor)
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
      t.expectMsgPF(hint = s"subscribe starting at 0") {
        case SubscribeToAggregate(sid, to, _, 0, ack) => (sid, ack, to)
      }
    }
    def expectSubscribe(to: AggregateKey, from: Long) = {
      t.expectMsgPF(hint = s"subscribe to $to starting at $from") {
        case SubscribeToAggregate(sid, `to`, _, `from`, ack) => (sid, ack)
      }
    }
    def expectUnsubscribe(sid: SubscriptionId, from: AggregateKey) = {
      t.expectMsgPF(hint = s"unsubscribe $sid from $from") {
        case UnsubscribeFromAggregate(`sid`, `from`, ack) => ack
      }
    }
    def expectCommand[A](pf: PartialFunction[OrderProcess.Command, A]) = {
      t.expectMsgPF(hint = s"command") {
        case Execute(cmd, ok, fail) if pf.isDefinedAt(cmd) => (ok, fail, pf(cmd))
      }
    }
  }

  "processManager instance" must {
    "handle an initiation event" in {
      val o = new TestOrder
      val commandProbe = TestProbe()
      val (orderPmi, pmi) = startPmi(o.id, commandProbe.ref)
      pmi ! orderPmi.InitiateProcess(o.placed, "ack1")
      expectMsg("ack1")
    }

    "subscribe to the source of the initiation event" in {
      val o = new TestOrder
      val commandProbe = TestProbe()
      val (orderPmi, pmi) = startPmi(o.id, commandProbe.ref)
      pmi ! orderPmi.InitiateProcess(o.placed, "ack1")
      expectMsg("ack1")
      val (_, ack) = commandProbe.expectSubscribe(o.key, o.placed.sequence)
      commandProbe.reply(ack)
    }

    "execute the actions returned by the process manager" in {
      val o = new TestOrder
      val commandProbe = TestProbe()
      val (orderPmi, pmi) = startPmi(o.id, commandProbe.ref)
      pmi ! orderPmi.InitiateProcess(o.placed, "ack1")
      expectMsg("ack1")
      val (orderSubscription, ackSubO) = commandProbe.expectSubscribe(o.key, o.placed.sequence)
      commandProbe.reply(ackSubO)

      pmi ! AggregateEvent(orderSubscription, o.placed, "ack2")
      expectMsg("ack2")

      //Request payment
      val (ackRequest, _, paymentKey2) = commandProbe.expectCommand {
        case Payment.RequestPayment(100, "test-bill", id) => id
      }
      commandProbe.reply(ackRequest)
      //Subscribe to payment
      val (_, ackSubP, paymentKey) = commandProbe.expectSubscribe()
      commandProbe.reply(ackSubP)
      //Unsubscribe from order
      val ackUnsO = commandProbe.expectUnsubscribe(orderSubscription, o.key)
      commandProbe.reply(ackUnsO)
      assert(paymentKey.aggregateType == Payment)
      assert(paymentKey.id === paymentKey2)
    }

    "unsubscribe from all when done" in {
      val o = new TestOrder
      val commandProbe = TestProbe()
      val (orderPmi, pmi) = startPmi(o.id, commandProbe.ref)
      pmi ! orderPmi.InitiateProcess(o.placed, "ack1")
      expectMsg("ack1")
      val (orderSubscription, ackSubO) = commandProbe.expectSubscribe(o.key, o.placed.sequence)
      commandProbe.reply(ackSubO)

      pmi ! AggregateEvent(orderSubscription, o.placed, "ack2")
      expectMsg("ack2")

      //Request payment
      val (ackReq, _, paymentKey2) = commandProbe.expectCommand {
        case Payment.RequestPayment(100, "test-bill", id) => id
      }
      commandProbe.reply(ackReq)
      //Subscribe to payment
      val (paymentSubscription, ackSubP, paymentKey) = commandProbe.expectSubscribe()
      commandProbe.reply(ackSubP)
      //Unsubscribe from order
      val ackUnsO = commandProbe.expectUnsubscribe(orderSubscription, o.key)
      commandProbe.reply(ackUnsO)

      pmi ! AggregateEvent(paymentSubscription, Payment.EventData(paymentKey2, 1, PaymentConfirmed), "ack3")
      expectMsg("ack3")

      //Complete order
      val (ackCompO, _, orderId2) = commandProbe.expectCommand {
        case Order.CompleteOrder(id) => id
      }
      commandProbe.reply(ackCompO)
      //Unsubscribe from payment
      val ackUnsP = commandProbe.expectUnsubscribe(paymentSubscription, paymentKey)
      commandProbe.reply(ackUnsP)
      assert(orderId2 === o.id)

      commandProbe.expectNoMsg()
    }

    "resend unconfirmed commands" in {
      val o = new TestOrder
      val commandProbe = TestProbe()
      val (orderPmi, pmi) = startPmi(o.id, commandProbe.ref)
      pmi ! orderPmi.InitiateProcess(o.placed, "ack1")
      expectMsg("ack1")
      val (orderSubscription, ackSubO) = commandProbe.expectSubscribe(o.key, o.placed.sequence)
      commandProbe.reply(ackSubO)

      pmi ! AggregateEvent(orderSubscription, o.placed, "ack2")
      expectMsg("ack2")

      //Request payment
      val (_, _, paymentKey2) = commandProbe.expectCommand {
        case Payment.RequestPayment(100, "test-bill", id) => id
      }

      Thread.sleep(1000)
      val (ackReq, _, _) = commandProbe.expectCommand {
        case Payment.RequestPayment(100, "test-bill", `paymentKey2`) => ()
      }
      commandProbe.reply(ackReq)
      //Subscribe to payment
      val (_, ackSubP, paymentKey) = commandProbe.expectSubscribe()
      commandProbe.reply(ackSubP)
      //Unsubscribe from order
      val ackUnsO = commandProbe.expectUnsubscribe(orderSubscription, o.key)
      commandProbe.reply(ackUnsO)
    }

    "resend unconfirmed commands after restart" in {
      val o = new TestOrder
      val commandProbe = TestProbe()
      val (orderPmi, pmi) = startPmi(o.id, commandProbe.ref)
      pmi ! orderPmi.InitiateProcess(o.placed, "ack1")
      expectMsg("ack1")
      val (orderSubscription, ackSubO) = commandProbe.expectSubscribe(o.key, o.placed.sequence)
      commandProbe.reply(ackSubO)

      pmi ! AggregateEvent(orderSubscription, o.placed, "ack2")
      expectMsg("ack2")

      //Request payment
      val (_, _, paymentKey2) = commandProbe.expectCommand {
        case Payment.RequestPayment(100, "test-bill", id) => id
      }

      Thread.sleep(500)
      pmi ! PoisonPill

      val commandProbe2 = TestProbe()
      val pmi2 = startPmi(o.id, commandProbe2.ref)
      val (ackReq, _, _) = commandProbe2.expectCommand {
        case Payment.RequestPayment(100, "test-bill", `paymentKey2`) => ()
      }
      commandProbe2.reply(ackReq)
      //Subscribe to payment
      val (_, ackSubP, paymentKey) = commandProbe2.expectSubscribe()
      commandProbe2.reply(ackSubP)
      //Unsubscribe from order
      val ackUnsO = commandProbe2.expectUnsubscribe(orderSubscription, o.key)
      commandProbe2.reply(ackUnsO)
    }

    "resend unconfirmed subscribes after restart" in {
      val o = new TestOrder
      val commandProbe = TestProbe()
      val (orderPmi, pmi) = startPmi(o.id, commandProbe.ref)
      pmi ! orderPmi.InitiateProcess(o.placed, "ack1")
      expectMsg("ack1")
      val (orderSubscription, ackSubO) = commandProbe.expectSubscribe(o.key, o.placed.sequence)
      commandProbe.reply(ackSubO)

      pmi ! AggregateEvent(orderSubscription, o.placed, "ack2")
      expectMsg("ack2")

      //Request payment
      //Request payment
      val (ackReq, _, paymentKey2) = commandProbe.expectCommand {
        case Payment.RequestPayment(100, "test-bill", id) => id
      }
      commandProbe.reply(ackReq)
      //Subscribe to payment
      val (_, _, _) = commandProbe.expectSubscribe()

      Thread.sleep(500)
      pmi ! PoisonPill

      val commandProbe2 = TestProbe()
      val pmi2 = startPmi(o.id, commandProbe2.ref)
      //Subscribe to payment
      val (_, ackSubP, paymentKey) = commandProbe2.expectSubscribe()
      assert(paymentKey.id == paymentKey2)
      commandProbe2.reply(ackSubP)
      //Unsubscribe from order
      val ackUnsO = commandProbe2.expectUnsubscribe(orderSubscription, o.key)
      commandProbe2.reply(ackUnsO)
    }

    "resend unconfirmed unsubscribes after restart" in {
      val o = new TestOrder
      val commandProbe = TestProbe()
      val (orderPmi, pmi) = startPmi(o.id, commandProbe.ref)
      pmi ! orderPmi.InitiateProcess(o.placed, "ack1")
      expectMsg("ack1")
      val (orderSubscription, ackSubO) = commandProbe.expectSubscribe(o.key, o.placed.sequence)
      commandProbe.reply(ackSubO)

      pmi ! AggregateEvent(orderSubscription, o.placed, "ack2")
      expectMsg("ack2")

      //Request payment
      val (ackReq, _, paymentKey2) = commandProbe.expectCommand {
        case Payment.RequestPayment(100, "test-bill", id) => id
      }
      commandProbe.reply(ackReq)
      //Subscribe to payment
      val (_, ackSubP, paymentKey) = commandProbe.expectSubscribe()
      commandProbe.reply(ackSubP)
      //Unsubscribe from order
      commandProbe.expectUnsubscribe(orderSubscription, o.key)

      Thread.sleep(500)
      pmi ! PoisonPill

      val commandProbe2 = TestProbe()
      val pmi2 = startPmi(o.id, commandProbe2.ref)
      //Unsubscribe from order
      val ackUnsO = commandProbe2.expectUnsubscribe(orderSubscription, o.key)
      commandProbe2.reply(ackUnsO)
    }

    "not resend confirmed commands/subs after restart" in {
      val o = new TestOrder
      val commandProbe = TestProbe()
      val (orderPmi, pmi) = startPmi(o.id, commandProbe.ref)
      pmi ! orderPmi.InitiateProcess(o.placed, "ack1")
      expectMsg("ack1")
      val (orderSubscription, ackSubO) = commandProbe.expectSubscribe(o.key, o.placed.sequence)
      commandProbe.reply(ackSubO)

      pmi ! AggregateEvent(orderSubscription, o.placed, "ack2")
      expectMsg("ack2")

      //Request payment
      val (ackReq, _, paymentKey2) = commandProbe.expectCommand {
        case Payment.RequestPayment(100, "test-bill", id) => id
      }
      commandProbe.reply(ackReq)
      //Subscribe to payment
      val (_, ackSubP, paymentKey) = commandProbe.expectSubscribe()
      commandProbe.reply(ackSubP)
      //Unsubscribe from order
      val ackUnsP = commandProbe.expectUnsubscribe(orderSubscription, o.key)
      commandProbe.reply(ackUnsP)

      Thread.sleep(500)
      pmi ! PoisonPill

      val commandProbe2 = TestProbe()
      val pmi2 = startPmi(o.id, commandProbe2.ref)
      commandProbe2.expectNoMsg()
    }

    "unapply the subscriptionId" in {
      val o = new TestOrder
      val (orderPmi, _) = startPmi(o.id, TestProbe().ref)
      val id = OrderProcess.Id(o.id.guid)
      val s = orderPmi.SubscriptionId(id, o.key)
      assert(orderPmi.SubscriptionId.unapply(s) === Some(id))
    }
  }
}
