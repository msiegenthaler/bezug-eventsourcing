package ch.eventsourced.infrastructure.akka

import akka.testkit.TestProbe
import ch.eventsourced.api.AggregateKey
import ch.eventsourced.infrastructure.akka.AggregateActor._
import ch.eventsourced.infrastructure.akka.Order.{StartOrder, OrderPlaced}

class ProcessManagerActorSpec extends AbstractSpec {

  val testProbe = TestProbe()
  val pma = new ProcessManagerActor("test", OrderProcess, testProbe.ref)(system, 10)

  implicit class RichCommandProbe(t: TestProbe) {
    def expectSubscribe() = {
      t.expectMsgPF(hint = s"subscribe starting at 0") {
        case SubscribeToAggregate(sid, to, _, 0, ack) => (sid, ack, to)
      }
    }
    def expectSubscribe(to: AggregateKey, from: Long) = {
      t.expectMsgPF(hint = s"subscribe to $to starting at $from") {
        case SubscribeToAggregate(sid, `to`, subscriber, `from`, ack) => (sid, subscriber, ack)
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

  "processManager actor" must {
    "start a process manager instance on initiation message" in {
      val orderId = StartOrder().order
      val op = OrderPlaced(Nil, 100, "test-ref")
      val ope = Order.EventData(orderId, 1, op)
      val id = pma.processManagerType.initiate(ope)

      pma.ref ! pma.ProcessInitationMessage(id, ope, "ack1")
      expectMsg("ack1")

      val (orderSubscription, subscriber, ackSubO) = testProbe.expectSubscribe(ope.aggregateKey, ope.sequence)
      testProbe.reply(ackSubO)
      system.actorSelection(subscriber) ! AggregateEvent(orderSubscription, ope, "ack2")
      expectMsg("ack2")


      //Request payment
      val (ackRequest, _, paymentKey2) = testProbe.expectCommand {
        case Payment.RequestPayment(100, "test-ref", id) => id
      }
      testProbe.reply(ackRequest)
      //Subscribe to payment
      val (_, ackSubP, paymentKey) = testProbe.expectSubscribe()
      testProbe.reply(ackSubP)
      //Unsubscribe from order
      val ackUnsO = testProbe.expectUnsubscribe(orderSubscription, ope.aggregateKey)
      testProbe.reply(ackUnsO)
      assert(paymentKey.aggregateType == Payment)
      assert(paymentKey.id === paymentKey2)
    }

    "subscribe with a absolute actor path (/user/)" in {
      val orderId = StartOrder().order
      val op = OrderPlaced(Nil, 100, "test-ref")
      val ope = Order.EventData(orderId, 1, op)
      val id = pma.processManagerType.initiate(ope)

      pma.ref ! pma.ProcessInitationMessage(id, ope, "ack1")
      expectMsg("ack1")

      val (orderSubscription, subscriber, ackSubO) = testProbe.expectSubscribe(ope.aggregateKey, ope.sequence)
      testProbe.reply(ackSubO)
      assert(subscriber.elements.head === "user")
    }

    "subscribe to path of the sharding root" in {
      val orderId = StartOrder().order
      val op = OrderPlaced(Nil, 100, "test-ref")
      val ope = Order.EventData(orderId, 1, op)
      val id = pma.processManagerType.initiate(ope)

      pma.ref ! pma.ProcessInitationMessage(id, ope, "ack1")
      expectMsg("ack1")

      val (orderSubscription, subscriber, ackSubO) = testProbe.expectSubscribe(ope.aggregateKey, ope.sequence)
      testProbe.reply(ackSubO)
      assert(subscriber === pma.ref.path)
    }
  }
}
