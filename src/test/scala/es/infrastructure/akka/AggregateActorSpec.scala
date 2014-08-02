package es.infrastructure.akka

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import akka.util.Timeout
import es.api.EventData
import pubsub._

class AggregateActorSpec() extends AbstractSpec() {
  val manager = {
    new AggregateActorManager(counter)(system, pubSub.ref, eventBusConfig, 1, 3.seconds)
  }

  implicit val timeout = Timeout(5.seconds)

  def executeSuccess(cmd: counter.Command) = {
    val res = manager.execute(cmd)
    awaitCond(res.isCompleted, timeout.duration)
    assert(res.value.get.isSuccess)
  }
  def expectNoEvent() = pubSub.expectNoMsg()
  def expectNoEvents() = pubSub.expectNoMsg(0.seconds)
  def expectEvent(event: EventData, doAck: Boolean = true) = {
    val aggregateTopic = eventBusConfig.topicFor(event.aggregateKey)
    val topics = Set(eventBusConfig.topicFor(event.aggregateType), aggregateTopic)
    pubSub.expectMsgPF() {
      case Producer.Publish(`topics`, `event`, ack) =>
        if (doAck) pubSub reply ack
    }
  }

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

      manager.execute(Kill(init.counter))

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

      manager.execute(Kill(init.counter))

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

    "try to redeliver not acked event to pubSub" in {
      val init = Initialize()
      executeSuccess(init)
      executeSuccess(Increment(init.counter))
      expectEvent(counter.Event.Data(init.counter, 0, Incremented(1)), doAck = false)
      expectNoEvents()

      Thread.sleep(2.seconds.toMillis)

      expectEvent(counter.Event.Data(init.counter, 0, Incremented(1)))
      expectNoEvents()
    }

    "redeliver not acked event to pubSub on restart" in {
      val init = Initialize()
      executeSuccess(init)
      executeSuccess(Increment(init.counter))
      expectEvent(counter.Event.Data(init.counter, 0, Incremented(1)), doAck = false)
      expectNoEvents()

      //force restart
      manager.execute(Kill(init.counter))

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

      Thread.sleep(2.seconds.toMillis)

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

      //force restart
      manager.execute(Kill(init.counter))

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

      //force restart
      manager.execute(Kill(init.counter))
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

      //force restart
      manager.execute(Kill(init.counter))

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

      //force restart
      manager.execute(Kill(init.counter))
      expectEvent(counter.Event.Data(init.counter, 0, Incremented(1)))
      executeSuccess(Increment(init.counter))
      expectEvent(counter.Event.Data(init.counter, 1, Incremented(2)), doAck = false)
      expectNoEvents()

      //force restart
      manager.execute(Kill(init.counter))
      expectEvent(counter.Event.Data(init.counter, 1, Incremented(2)))
      expectNoEvents()

      executeSuccess(Increment(init.counter))
      expectEvent(counter.Event.Data(init.counter, 2, Incremented(3)))
    }
  }
}