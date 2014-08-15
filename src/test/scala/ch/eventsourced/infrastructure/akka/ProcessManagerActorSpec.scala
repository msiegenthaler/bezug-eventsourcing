package ch.eventsourced.infrastructure.akka

import akka.actor.{Actor, Props, PoisonPill, ActorRef}
import akka.testkit.TestProbe
import ch.eventsourced.api.AggregateKey
import ch.eventsourced.infrastructure.akka.AggregateActor._
import ch.eventsourced.infrastructure.akka.Payment.PaymentConfirmed
import ch.eventsourced.support.CompositeIdentifier

class ProcessManagerInstanceSpec extends AbstractSpec {

  def startPmi(orderId: Order.Id, cmdDist: ActorRef) = {
    val orderPmi = new ProcessManagerActor("test", OrderProcess, cmdDist)
    val runner = Props(new Actor {
      val pid = OrderProcess.initiate(Order.EventData(orderId, 0, Order.OrderPlaced(Nil, 0, "")))
      val pm = context actorOf orderPmi.props(context.self, pid, CompositeIdentifier(OrderProcess.serializeId(pid)))
      def receive = {
        case msg => pm forward msg
      }
    })
    val actor = system actorOf runner
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
        case SubscribeToAggregate(sid, `to`, subscriber, `from`, ack) => (sid, ack, subscriber)
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
      val (_, ack, _) = commandProbe.expectSubscribe(o.key, o.placed.sequence)
      commandProbe.reply(ack)
    }

    "execute the actions returned by the process manager" in {
      val o = new TestOrder
      val commandProbe = TestProbe()
      val (orderPmi, pmi) = startPmi(o.id, commandProbe.ref)
      pmi ! orderPmi.InitiateProcess(o.placed, "ack1")
      expectMsg("ack1")
      val (orderSubscription, ackSubO, _) = commandProbe.expectSubscribe(o.key, o.placed.sequence)
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
      val (orderSubscription, ackSubO, _) = commandProbe.expectSubscribe(o.key, o.placed.sequence)
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
      val (orderSubscription, ackSubO, _) = commandProbe.expectSubscribe(o.key, o.placed.sequence)
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
      val (orderSubscription, ackSubO, _) = commandProbe.expectSubscribe(o.key, o.placed.sequence)
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
      val (orderSubscription, ackSubO, _) = commandProbe.expectSubscribe(o.key, o.placed.sequence)
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
      val (orderSubscription, ackSubO, _) = commandProbe.expectSubscribe(o.key, o.placed.sequence)
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
      val (orderSubscription, ackSubO, _) = commandProbe.expectSubscribe(o.key, o.placed.sequence)
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

    "subscribe with a absolute actor path (/user/)" in {
      val o = new TestOrder
      val testProbe = TestProbe()
      val (orderPmi, pmi) = startPmi(o.id, testProbe.ref)
      val id = orderPmi.processManagerType.initiate(o.placed)

      pmi ! orderPmi.InitiateProcess(id, o.placed, "ack1")
      expectMsg("ack1")

      val (orderSubscription, ackSubO, subscriber) = testProbe.expectSubscribe(o.key, o.placed.sequence)
      testProbe.reply(ackSubO)
      assert(subscriber.elements.head === "user")
    }

    "subscribe to path of the sharding root" in {
      val o = new TestOrder
      val testProbe = TestProbe()
      val (orderPmi, pmi) = startPmi(o.id, testProbe.ref)
      val id = orderPmi.processManagerType.initiate(o.placed)

      pmi ! orderPmi.InitiateProcess(id, o.placed, "ack1")
      expectMsg("ack1")

      val (orderSubscription, ackSubO, subscriber) = testProbe.expectSubscribe(o.key, o.placed.sequence)
      testProbe.reply(ackSubO)
      assert(subscriber === pmi.path)
    }

    "allow passivation if no pending ack" in {
      val o = new TestOrder
      val commandProbe = TestProbe()
      val (orderPmi, pmi) = startPmi(o.id, commandProbe.ref)
      pmi ! orderPmi.InitiateProcess(o.placed, "ack1")
      expectMsg("ack1")
      val (orderSubscription, ackSubO, _) = commandProbe.expectSubscribe(o.key, o.placed.sequence)
      commandProbe.reply(ackSubO)

      pmi ! orderPmi.RequestPassivation("yes", "no")
      expectMsg("yes")

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

      Thread.sleep(200)
      pmi ! orderPmi.RequestPassivation("yes", "no")
      expectMsg("yes")
    }

    "not allow passivation if pending command ack" in {
      val o = new TestOrder
      val commandProbe = TestProbe()
      val (orderPmi, pmi) = startPmi(o.id, commandProbe.ref)
      pmi ! orderPmi.InitiateProcess(o.placed, "ack1")
      expectMsg("ack1")
      val (orderSubscription, ackSubO, _) = commandProbe.expectSubscribe(o.key, o.placed.sequence)
      commandProbe.reply(ackSubO)

      pmi ! AggregateEvent(orderSubscription, o.placed, "ack2")
      expectMsg("ack2")

      pmi ! orderPmi.RequestPassivation("yes", "no")
      expectMsg("no")

      //Request payment
      val (ackRequest, _, paymentKey2) = commandProbe.expectCommand {
        case Payment.RequestPayment(100, "test-bill", id) => id
      }

      pmi ! orderPmi.RequestPassivation("yes", "no")
      expectMsg("no")

      commandProbe.reply(ackRequest)
      //Subscribe to payment
      val (_, ackSubP, paymentKey) = commandProbe.expectSubscribe()
      commandProbe.reply(ackSubP)
      //Unsubscribe from order
      val ackUnsO = commandProbe.expectUnsubscribe(orderSubscription, o.key)
      commandProbe.reply(ackUnsO)

      Thread.sleep(200)
      pmi ! orderPmi.RequestPassivation("yes", "no")
      expectMsg("yes")
    }

    "not allow passivation if pending subscription ack" in {
      val o = new TestOrder
      val commandProbe = TestProbe()
      val (orderPmi, pmi) = startPmi(o.id, commandProbe.ref)
      pmi ! orderPmi.InitiateProcess(o.placed, "ack1")
      expectMsg("ack1")
      val (orderSubscription, ackSubO, _) = commandProbe.expectSubscribe(o.key, o.placed.sequence)
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

      pmi ! orderPmi.RequestPassivation("yes", "no")
      expectMsg("no")

      commandProbe.reply(ackSubP)

      pmi ! orderPmi.RequestPassivation("yes", "no")
      expectMsg("no")

      //Unsubscribe from order
      val ackUnsO = commandProbe.expectUnsubscribe(orderSubscription, o.key)

      pmi ! orderPmi.RequestPassivation("yes", "no")
      expectMsg("no")

      commandProbe.reply(ackUnsO)

      Thread.sleep(200)
      pmi ! orderPmi.RequestPassivation("yes", "no")
      expectMsg("yes")
    }
  }
}
