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
  def expectEvent(event: EventData) = {
    val aggregateTopic = eventBusConfig.topicFor(event.aggregateKey)
    val topics = Set(eventBusConfig.topicFor(event.aggregateType), aggregateTopic)
    pubSub.expectMsgPF() {
      case Producer.Publish(`topics`, `event`, ack) =>
        pubSub reply ack
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

      executeSuccess(Increment(init.counter))
      expectEvent(counter.Event.Data(init.counter, 1, Incremented(2)))
    }

    "preserve state if killed" in {
      val init = Initialize()
      executeSuccess(init)

      executeSuccess(Increment(init.counter))
      expectEvent(counter.Event.Data(init.counter, 0, Incremented(1)))

      manager.execute(Kill(init.counter))

      executeSuccess(Increment(init.counter))
      expectEvent(counter.Event.Data(init.counter, 1, Incremented(2)))
    }

    "preserve state if passivated" in {
      val init = Initialize()
      executeSuccess(init)

      executeSuccess(Increment(init.counter))
      expectEvent(counter.Event.Data(init.counter, 0, Incremented(1)))

      Thread.sleep(5.seconds.toMillis)

      executeSuccess(Increment(init.counter))
      expectEvent(counter.Event.Data(init.counter, 1, Incremented(2)))
    }
  }
}