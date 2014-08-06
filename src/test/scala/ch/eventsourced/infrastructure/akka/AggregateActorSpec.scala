package ch.eventsourced.infrastructure.akka

import scala.concurrent.duration._
import akka.util.Timeout
import akka.testkit.TestProbe
import ch.eventsourced.api.EventData
import ch.eventsourced.infrastructure.akka.counter.Kill
import ch.eventsourced.infrastructure.akka.AggregateManager._

class AggregateActorSpec() extends AbstractSpec() {
  val eventHandler = TestProbe()
  val manager = {
    val subs = Map("testProbe" -> eventHandler.ref)
    new AggregateManager("AggregateActorSpec", counter, subs)(system, 1, 3.seconds)
  }

  implicit val timeout = Timeout(5.seconds)

  def executeSuccess(cmd: counter.Command) = {
    manager.ref ! Execute(cmd, "ok", (e: counter.Error) => s"fail: $e")
    expectMsg(5.seconds, "ok")
  }

  def expectNoEvent() = eventHandler.expectNoMsg()
  def expectNoEvents() = eventHandler.expectNoMsg(0.seconds)
  def expectEvent(event: EventData, doAck: Boolean = true) = eventHandler.expectMsgPF() {
    case AggregateEvent("testProbe", `event`, ack) =>
      if (doAck) eventHandler.reply(ack)
  }

  def killManager(id: counter.Id) = manager.ref ! Execute(Kill(id), (), (_: counter.Error) => ())

  "aggregate actor" must {
    import counter._

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

      manager.ref ! Kill(init.counter)

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