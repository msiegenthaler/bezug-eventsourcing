package ch.eventsourced.infrastructure.akka

import akka.actor.ActorRef
import akka.testkit.TestProbe
import ch.eventsourced.api.{AggregateKey, EventData}
import ch.eventsourced.infrastructure.akka.AggregateManager.{UnsubscribeFromAggregate, Execute, AggregateEvent, SubscribeToAggregate}
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
      pmi ! ack
    }

    "execute the actions returned by the process manager" in {
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
      val (_, a2, paymentKey) = commandProbe.expectSubscribe()
      pmi ! a2
      //Unsubscribe from order
      val a3 = commandProbe.expectUnsubscribe(orderSubscription, o.key)
      pmi ! a3
      //Request payment
      val (a4, _, paymentKey2) = commandProbe.expectCommand {
        case Payment.RequestPayment(100, "test-bill", id) => id
      }
      pmi ! a4
      assert(paymentKey.aggregateType == Payment)
      assert(paymentKey.id === paymentKey2)
    }

  }

}
