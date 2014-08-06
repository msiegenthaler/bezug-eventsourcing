package es.infrastructure.akka

import scala.concurrent.duration._
import akka.actor.{PoisonPill, Actor, Props}
import akka.testkit.TestProbe
import es.api.EventData
import es.infrastructure.akka.AggregateActor.OnEvent
import es.infrastructure.akka.AggregateSubscription.{Close, Start}
import es.infrastructure.akka.EventBus.AggregateEvent
import es.infrastructure.akka.counter.{Incremented, Initialize}


class AggregateSubscriptionSpec extends AbstractSpec {

  def journalReplay(events: List[EventData], expectFrom: Long, expectUntil: Long)(from: Long, until: Long) = Props {
    assert(expectFrom === from)
    assert(expectUntil === until)
    new Actor {
      events.foreach(context.parent ! _)
      def receive = PartialFunction.empty
    }
  }
  def noJournalAccessExpected(from: Long, until: Long) = Props {
    fail("Unexpected access to journal")
    val a: Actor = ???
    a
  }

  implicit class EventProbe(probe: TestProbe) {
    def expectEvent(event: EventData, doAck: Boolean = true)(implicit id: String) = probe.expectMsgPF() {
      case AggregateEvent(id, `event`, ack) =>
        if (doAck) probe.reply(ack)
        ack
    }
  }


  val event1 = counter.EventData(Initialize().counter, 0, Incremented(0))
  val event2 = counter.EventData(Initialize().counter, 1, Incremented(1))
  val event3 = counter.EventData(Initialize().counter, 2, Incremented(2))

  "AggregateSubscription" must {
    "forward first message when at beginning and live stream from beginning" in {
      val probe = TestProbe()
      implicit val id = "test1"
      val sub = system actorOf AggregateSubscription.props(id, probe.ref, noJournalAccessExpected)
      probe.expectNoMsg()
      sub ! Start(0)

      sub ! OnEvent(event1, "ack1")
      expectMsg("ack1")
      probe.expectEvent(event1)
    }

    "forward messages when at beginning and live stream from beginning" in {
      val probe = TestProbe()
      implicit val id = "test2"
      val sub = system actorOf AggregateSubscription.props(id, probe.ref, noJournalAccessExpected)
      probe.expectNoMsg()
      sub ! Start(0)

      sub ! OnEvent(event1, "ack1")
      expectMsg("ack1")
      probe.expectEvent(event1)

      sub ! OnEvent(event2, "ack2")
      expectMsg("ack2")
      probe.expectEvent(event2)
    }

    "forward messages without journal access when start is the next live" in {
      val probe = TestProbe()
      implicit val id = "test3"
      val sub = system actorOf AggregateSubscription.props(id, probe.ref, noJournalAccessExpected, 1)
      probe.expectNoMsg()
      sub ! Start(1)

      sub ! OnEvent(event2, "ack2")
      expectMsg("ack2")
      probe.expectEvent(event2)
    }

    "must not replay ack'ed messages" in {
      val probe = TestProbe()
      implicit val id = "test4"
      val sub = system actorOf AggregateSubscription.props(id, probe.ref, noJournalAccessExpected)
      probe.expectNoMsg()
      sub ! Start(0)
      sub ! OnEvent(event1, "ack1")
      expectMsg("ack1")
      probe.expectEvent(event1)
      sub ! PoisonPill

      val probe2 = TestProbe()
      val sub2 = system actorOf AggregateSubscription.props(id, probe2.ref, noJournalAccessExpected)
      sub2 ! Start(1)
      sub2 ! OnEvent(event2, "ack2")
      expectMsg("ack2")
      probe2.expectEvent(event2)
      probe.expectNoMsg(10.millis)
    }

    "must replay non ack'ed messages" in {
      val probe = TestProbe()
      implicit val id = "test5"
      val sub = system actorOf AggregateSubscription.props(id, probe.ref, noJournalAccessExpected)
      probe.expectNoMsg()
      sub ! Start(0)
      sub ! OnEvent(event1, "ack1")
      expectMsg("ack1")
      probe.expectEvent(event1, doAck = false)
      sub ! PoisonPill

      val probe2 = TestProbe()
      val sub2 = system actorOf AggregateSubscription.props(id, probe2.ref, journalReplay(event1 :: Nil, 0, 0))
      sub2 ! Start(1)
      probe2.expectEvent(event1)

      sub2 ! OnEvent(event2, "ack2")
      expectMsg("ack2")
      probe2.expectEvent(event2)
      probe.expectNoMsg(10.millis)
    }

    "must only deliver next message after ack is received" in {
      val probe = TestProbe()
      implicit val id = "test6"
      val sub = system actorOf AggregateSubscription.props(id, probe.ref, noJournalAccessExpected)
      probe.expectNoMsg()
      sub ! Start(0)

      sub ! OnEvent(event1, "ack1")
      expectMsg("ack1")
      val ack1 = probe.expectEvent(event1, doAck = false)

      sub ! OnEvent(event2, "ack2")
      sub ! OnEvent(event3, "ack3")

      probe.expectNoMsg()
      sub ! ack1
      probe.expectEvent(event2)
      probe.expectEvent(event3)
    }
  }

}
