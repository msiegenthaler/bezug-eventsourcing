package ch.eventsourced.infrastructure.akka

import scala.concurrent.duration._
import akka.actor.{PoisonPill, Actor, Props}
import akka.testkit.TestProbe
import ch.eventsourced.api.EventData
import ch.eventsourced.infrastructure.akka.AggregateActor.{AggregateEvent, SubscriptionId}
import ch.eventsourced.infrastructure.akka.AggregateSubscription.{OnEvent, Start}
import ch.eventsourced.infrastructure.akka.counter.{Incremented, Initialize}


class AggregateSubscriptionSpec extends AbstractSpec {

  def journalReplay(events: Seq[EventData], expectFrom: Long, expectUntil: Long)(from: Long, until: Long) = Props {
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

  implicit val timeout = 2.seconds

  implicit class EventProbe(probe: TestProbe) {
    def expectEvent(event: EventData, doAck: Boolean = true)(implicit id: SubscriptionId) = probe.expectMsgPF(timeout, event.toString) {
      case AggregateEvent(id, `event`, ack) =>
        if (doAck) probe.reply(ack)
        val s = probe.sender()
        () => s ! ack
    }
  }

  val event1 = counter.EventData(Initialize().counter, 0, Incremented(0))
  val event2 = counter.EventData(Initialize().counter, 1, Incremented(1))
  val event3 = counter.EventData(Initialize().counter, 2, Incremented(2))
  val events = {
    val c = Initialize().counter
    (0 to 100).map(i => counter.EventData(c, i, Incremented(i)))
  }

  "AggregateSubscription" must {
    "forward first message when at beginning and live stream from beginning" in {
      val probe = TestProbe()
      implicit val id = CompositeName("test1")
      val sub = system actorOf AggregateSubscription.props(id, "main", probe.ref, noJournalAccessExpected)
      probe.expectNoMsg()
      sub ! Start(0)

      sub ! OnEvent(event1, "ack1")
      expectMsg("ack1")
      probe.expectEvent(event1)
    }

    "forward messages when at beginning and live stream from beginning" in {
      val probe = TestProbe()
      implicit val id = CompositeName("test2")
      val sub = system actorOf AggregateSubscription.props(id, "main", probe.ref, noJournalAccessExpected)
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
      implicit val id = CompositeName("test3")
      val sub = system actorOf AggregateSubscription.props(id, "main", probe.ref, noJournalAccessExpected, 1)
      probe.expectNoMsg()
      sub ! Start(1)

      sub ! OnEvent(event2, "ack2")
      expectMsg("ack2")
      probe.expectEvent(event2)
    }

    "not replay ack'ed messages" in {
      val probe = TestProbe()
      implicit val id = CompositeName("test4")
      val sub = system actorOf AggregateSubscription.props(id, "main", probe.ref, noJournalAccessExpected)
      probe.expectNoMsg()
      sub ! Start(0)
      sub ! OnEvent(event1, "ack1")
      expectMsg("ack1")
      probe.expectEvent(event1)
      sub ! PoisonPill

      val probe2 = TestProbe()
      val sub2 = system actorOf AggregateSubscription.props(id, "main", probe2.ref, noJournalAccessExpected)
      sub2 ! Start(1)
      sub2 ! OnEvent(event2, "ack2")
      expectMsg("ack2")
      probe2.expectEvent(event2)
      probe.expectNoMsg(10.millis)
    }

    "replay non ack'ed messages" in {
      val probe = TestProbe()
      implicit val id = CompositeName("test5")
      val sub = system actorOf AggregateSubscription.props(id, "main", probe.ref, noJournalAccessExpected)
      probe.expectNoMsg()
      sub ! Start(0)
      sub ! OnEvent(event1, "ack1")
      expectMsg("ack1")
      probe.expectEvent(event1, doAck = false)
      sub ! PoisonPill

      val probe2 = TestProbe()
      val sub2 = system actorOf AggregateSubscription.props(id, "main", probe2.ref, journalReplay(event1 :: Nil, 0, 0))
      sub2 ! Start(1)
      probe2.expectEvent(event1)

      sub2 ! OnEvent(event2, "ack2")
      expectMsg("ack2")
      probe2.expectEvent(event2)
      probe.expectNoMsg(10.millis)
    }

    "only deliver next message after ack is received" in {
      val probe = TestProbe()
      implicit val id = CompositeName("test6")
      val sub = system actorOf AggregateSubscription.props(id, "main", probe.ref, noJournalAccessExpected)
      probe.expectNoMsg()
      sub ! Start(0)

      sub ! OnEvent(event1, "ack1")
      expectMsg("ack1")
      val ack1 = probe.expectEvent(event1, doAck = false)

      sub ! OnEvent(event2, "ack2")
      sub ! OnEvent(event3, "ack3")
      expectMsg("ack2")
      expectMsg("ack3")

      probe.expectNoMsg(500.millis)

      ack1()
      probe.expectEvent(event2)
      probe.expectEvent(event3)
    }

    "deliver in order if it receives new events while replaying the journal" in {
      val probe = TestProbe()
      implicit val id = CompositeName("test7")
      val sub = system actorOf AggregateSubscription.props(id, "main", probe.ref, noJournalAccessExpected)
      probe.expectNoMsg()
      sub ! Start(0)

      val count = 10
      val acks = events.take(count).map { event =>
        val ack = "ack" + event.sequence
        sub ! OnEvent(event, ack)
        expectMsg(ack)
      }
      probe.expectEvent(events(0))
      probe.expectEvent(events(1), doAck = false)
      sub ! PoisonPill

      val probe2 = TestProbe()
      val sub2 = system actorOf AggregateSubscription.props(id, "main", probe2.ref,
        journalReplay(events.take(count).drop(1), 1, count - 1))
      sub2 ! Start(count)

      sub2 ! OnEvent(events(count), "ae")
      expectMsg("ae")
      sub2 ! OnEvent(events(count + 1), "ae2")
      expectMsg("ae2")

      probe2.expectEvent(events(1))
      events.take(count + 2).drop(2).foreach(probe2.expectEvent(_))
      probe2.expectNoMsg(10.millis)
    }

    "retry event delivery" in {
      val probe = TestProbe()
      implicit val id = CompositeName("test8")
      val sub = system actorOf AggregateSubscription.props(id, "main", probe.ref, noJournalAccessExpected)
      probe.expectNoMsg()
      sub ! Start(0)

      sub ! OnEvent(event1, "ack1")
      expectMsg("ack1")
      probe.expectEvent(event1, doAck = false)
      probe.expectNoMsg(500.millis)
      probe.expectEvent(event1, doAck = false)
      probe.expectNoMsg(500.millis)
      probe.expectEvent(event1)
      probe.expectNoMsg()
    }

    "be efficient for large numbers of messages" in {
      val probe = TestProbe()
      implicit val id = CompositeName("test9")
      val sub = system actorOf AggregateSubscription.props(id, "main", probe.ref, noJournalAccessExpected)
      sub ! Start(0)
      val counterId = Initialize().counter

      //send and ack a lot of messages
      (0 to 10001).foreach { i =>
        val event = counter.EventData(counterId, i, Incremented(i))
        sub ! OnEvent(event, s"ack-$i")
        expectMsg(s"ack-$i")
        probe.expectEvent(event)
      }
      Thread.sleep(500)
      sub ! PoisonPill

      //should load from snapshot
      val sub2 = system actorOf AggregateSubscription.props(id, "main", probe.ref, noJournalAccessExpected)
      sub2 ! Start(10002)
      probe.expectNoMsg()
      (10002 to 10010).foreach { i =>
        val event = counter.EventData(counterId, i, Incremented(i))
        sub2 ! OnEvent(event, s"ack-$i")
        expectMsg(s"ack-$i")
        probe.expectEvent(event)
      }
      sub2 ! PoisonPill
    }
  }

}
