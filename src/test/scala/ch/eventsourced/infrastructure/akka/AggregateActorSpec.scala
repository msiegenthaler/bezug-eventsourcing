package ch.eventsourced.infrastructure.akka

import akka.testkit.TestProbe
import akka.util.Timeout
import ch.eventsourced.api.EventData
import ch.eventsourced.infrastructure.akka.AggregateActor._
import ch.eventsourced.infrastructure.akka.counter.Kill
import ch.eventsourced.support.CompositeIdentifier
import scala.concurrent.duration._

class AggregateActorSpec extends AbstractSpec {
  val eventHandler = TestProbe()
  val manager = {
    val subs = Map(CompositeIdentifier("testProbe") -> eventHandler.ref)
    val aa = new AggregateActor("AggregateActorSpec", counter, subs)(system, 1, 3.seconds)
    system actorOf LocalSharder.props(aa)
  }

  implicit val timeout = Timeout(1.seconds)

  def executeSuccess(cmd: counter.Command) = {
    manager ! Execute(cmd, "ok", (e: counter.Error) => s"fail: $e")
    expectMsg(5.seconds, "ok")
  }

  def expectNoEvent() = eventHandler.expectNoMsg()
  def expectNoEvents() = eventHandler.expectNoMsg(0.seconds)
  def expectEvent(event: EventData, doAck: Boolean = true) = eventHandler.expectMsgPF(hint = event.toString) {
    case AggregateEvent(_, `event`, ack) =>
      if (doAck) eventHandler.reply(ack)
  }

  def killManager(id: counter.Id) = manager ! Execute(Kill(id), (), (_: counter.Error) => ())

  "aggregate manager" must {
    import ch.eventsourced.infrastructure.akka.counter._

    "create a new aggregate root for never used id" in {
      executeSuccess(Initialize())

      expectNoEvent()
    }

    "preserve state between invocations" in {
      val init = Initialize()
      executeSuccess(init)

      executeSuccess(Increment(init.counter))
      expectEvent(counter.Event.Data(init.counter, 0, Incremented(1)))
      expectNoEvents()

      executeSuccess(Increment(init.counter))
      expectEvent(counter.Event.Data(init.counter, 1, Incremented(2)))
      expectNoEvents()
    }

    "preserve state if killed" in {
      val init = Initialize()
      executeSuccess(init)

      executeSuccess(Increment(init.counter))
      expectEvent(counter.Event.Data(init.counter, 0, Incremented(1)))
      expectNoEvents()

      manager ! Kill(init.counter)

      executeSuccess(Increment(init.counter))
      expectEvent(counter.Event.Data(init.counter, 1, Incremented(2)))
      expectNoEvents()
    }

    "preserve state if killed after more messages" in {
      val init = Initialize()
      executeSuccess(init)

      executeSuccess(Increment(init.counter))
      executeSuccess(Increment(init.counter))
      expectEvent(counter.Event.Data(init.counter, 0, Incremented(1)))
      expectEvent(counter.Event.Data(init.counter, 1, Incremented(2)))
      expectNoEvents()

      killManager(init.counter)

      executeSuccess(Increment(init.counter))
      expectEvent(counter.Event.Data(init.counter, 2, Incremented(3)))
      expectNoEvents()
    }

    "preserve state if passivated" in {
      val init = Initialize()
      executeSuccess(init)

      executeSuccess(Increment(init.counter))
      expectEvent(counter.Event.Data(init.counter, 0, Incremented(1)))
      expectNoEvents()

      Thread.sleep(5.seconds.toMillis)

      executeSuccess(Increment(init.counter))
      expectEvent(counter.Event.Data(init.counter, 1, Incremented(2)))
      expectNoEvents()
    }

    "retry to redeliver not acked event to eventHandler" in {
      val init = Initialize()
      executeSuccess(init)
      executeSuccess(Increment(init.counter))
      expectEvent(counter.Event.Data(init.counter, 0, Incremented(1)), doAck = false)
      expectNoEvents()

      Thread.sleep(2.seconds.toMillis)

      expectEvent(counter.Event.Data(init.counter, 0, Incremented(1)))
      expectNoEvents()
    }

    "redeliver not acked event to eventHandler on restart" in {
      val init = Initialize()
      executeSuccess(init)
      executeSuccess(Increment(init.counter))
      expectEvent(counter.Event.Data(init.counter, 0, Incremented(1)), doAck = false)
      expectNoEvents()

      killManager(init.counter)

      expectEvent(counter.Event.Data(init.counter, 0, Incremented(1)))
      expectNoEvents()
    }

    "preserve order in redelivers" in {
      val init = Initialize()
      executeSuccess(init)
      executeSuccess(Increment(init.counter))
      expectEvent(counter.Event.Data(init.counter, 0, Incremented(1)), doAck = false)
      expectNoEvents()

      executeSuccess(Increment(init.counter))
      expectNoEvents()

      expectEvent(counter.Event.Data(init.counter, 0, Incremented(1)))
      expectEvent(counter.Event.Data(init.counter, 1, Incremented(2)))
      expectNoEvents()
    }

    "preserve order in redelivers after restart" in {
      val init = Initialize()
      executeSuccess(init)
      executeSuccess(Increment(init.counter))
      expectEvent(counter.Event.Data(init.counter, 0, Incremented(1)), doAck = false)
      expectNoEvents()

      executeSuccess(Increment(init.counter))
      expectNoEvents()

      killManager(init.counter)

      expectEvent(counter.Event.Data(init.counter, 0, Incremented(1)))
      expectEvent(counter.Event.Data(init.counter, 1, Incremented(2)))
      expectNoEvents()
    }

    "preserve order in redelivers after restart with new commands" in {
      val init = Initialize()
      executeSuccess(init)
      executeSuccess(Increment(init.counter))
      expectEvent(counter.Event.Data(init.counter, 0, Incremented(1)), doAck = false)
      expectNoEvents()

      executeSuccess(Increment(init.counter))
      expectNoEvents()

      killManager(init.counter)
      executeSuccess(Increment(init.counter))

      expectEvent(counter.Event.Data(init.counter, 0, Incremented(1)))
      expectEvent(counter.Event.Data(init.counter, 1, Incremented(2)))
      expectEvent(counter.Event.Data(init.counter, 2, Incremented(3)))
      expectNoEvents()
    }

    "do not redeliver acked events on restart" in {
      val init = Initialize()
      executeSuccess(init)
      executeSuccess(Increment(init.counter))
      expectEvent(counter.Event.Data(init.counter, 0, Incremented(1)))
      executeSuccess(Increment(init.counter))
      expectEvent(counter.Event.Data(init.counter, 1, Incremented(2)), doAck = false)
      executeSuccess(Increment(init.counter))
      expectNoEvents()

      killManager(init.counter)

      expectEvent(counter.Event.Data(init.counter, 1, Incremented(2)))
      expectEvent(counter.Event.Data(init.counter, 2, Incremented(3)))
      expectNoEvents()
    }

    "do not redeliver later acked events after next restart" in {
      val init = Initialize()
      executeSuccess(init)
      executeSuccess(Increment(init.counter))
      expectEvent(counter.Event.Data(init.counter, 0, Incremented(1)), doAck = false)
      expectNoEvents()

      killManager(init.counter)
      expectEvent(counter.Event.Data(init.counter, 0, Incremented(1)))
      executeSuccess(Increment(init.counter))
      expectEvent(counter.Event.Data(init.counter, 1, Incremented(2)), doAck = false)
      expectNoEvents()

      killManager(init.counter)
      expectEvent(counter.Event.Data(init.counter, 1, Incremented(2)))
      expectNoEvents()

      executeSuccess(Increment(init.counter))
      expectEvent(counter.Event.Data(init.counter, 2, Incremented(3)))
    }
  }
}