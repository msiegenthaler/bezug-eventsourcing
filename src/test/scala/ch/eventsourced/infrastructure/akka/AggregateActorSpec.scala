package ch.eventsourced.infrastructure.akka

import akka.actor.{ActorRef, Actor, Props}
import akka.testkit.TestProbe
import akka.util.Timeout
import ch.eventsourced.api.EventData
import ch.eventsourced.infrastructure.akka.AggregateActor._
import ch.eventsourced.infrastructure.akka.counter.Kill
import ch.eventsourced.support.CompositeIdentifier
import scala.concurrent.duration._

class AggregateActorSpec extends AbstractSpec {
  val eventHandler = TestProbe()
  val subs = Map(CompositeIdentifier("testProbe") -> eventHandler.ref)
  val aggregateActor = new AggregateActor("AggregateActorSpec", counter, subs)
  def startAggregate(id: counter.Id) = {
    val runner = Props(new Actor {
      val a = context actorOf aggregateActor.props(context.self, id, CompositeIdentifier(counter.serializeId(id)))
      def receive = {
        case msg => a forward msg
      }
    })
    system actorOf runner
  }

  implicit val timeout = Timeout(1.seconds)

  implicit class RichAggregate(a: ActorRef) {
    def executeSuccess(cmd: counter.Command) = {
      a ! Execute(cmd, "ok", (e: counter.Error) => s"fail: $e")
      expectMsg(5.seconds, "ok")
    }
    def kill(id: counter.Id) = {
      a ! Execute(Kill(id), (), (_: counter.Error) => ())
    }
  }

  def expectNoEvent() = eventHandler.expectNoMsg()
  def expectNoEvents() = eventHandler.expectNoMsg(0.seconds)
  def expectEvent(event: EventData, doAck: Boolean = true) = eventHandler.expectMsgPF(hint = event.toString) {
    case AggregateEvent(_, `event`, ack) =>
      if (doAck) eventHandler.reply(ack)
      ack
  }


  "aggregate manager" must {
    import ch.eventsourced.infrastructure.akka.counter._

    "create a new aggregate root for never used id" in {
      val init = Initialize()
      val aggregate = startAggregate(init.counter)
      aggregate.executeSuccess(init)

      expectNoEvent()
    }

    "preserve state between invocations" in {
      val init = Initialize()
      val aggregate = startAggregate(init.counter)
      aggregate.executeSuccess(init)

      aggregate.executeSuccess(Increment(init.counter))
      expectEvent(counter.Event.Data(init.counter, 0, Incremented(1)))
      expectNoEvents()

      aggregate.executeSuccess(Increment(init.counter))
      expectEvent(counter.Event.Data(init.counter, 1, Incremented(2)))
      expectNoEvents()
    }

    "preserve state if killed" in {
      val init = Initialize()
      val aggregate = startAggregate(init.counter)
      aggregate.executeSuccess(init)

      aggregate.executeSuccess(Increment(init.counter))
      expectEvent(counter.Event.Data(init.counter, 0, Incremented(1)))
      expectNoEvents()

      aggregate.kill(init.counter)

      aggregate.executeSuccess(Increment(init.counter))
      expectEvent(counter.Event.Data(init.counter, 1, Incremented(2)))
      expectNoEvents()
    }

    "preserve state if killed after more messages" in {
      val init = Initialize()
      val aggregate = startAggregate(init.counter)
      aggregate.executeSuccess(init)

      aggregate.executeSuccess(Increment(init.counter))
      aggregate.executeSuccess(Increment(init.counter))
      expectEvent(counter.Event.Data(init.counter, 0, Incremented(1)))
      expectEvent(counter.Event.Data(init.counter, 1, Incremented(2)))
      expectNoEvents()

      aggregate.kill(init.counter)

      aggregate.executeSuccess(Increment(init.counter))
      expectEvent(counter.Event.Data(init.counter, 2, Incremented(3)))
      expectNoEvents()
    }

    "retry to redeliver not acked event to eventHandler" in {
      val init = Initialize()
      val aggregate = startAggregate(init.counter)
      aggregate.executeSuccess(init)
      aggregate.executeSuccess(Increment(init.counter))
      expectEvent(counter.Event.Data(init.counter, 0, Incremented(1)), doAck = false)
      expectNoEvents()

      Thread.sleep(2.seconds.toMillis)

      expectEvent(counter.Event.Data(init.counter, 0, Incremented(1)))
      expectNoEvents()
    }

    "redeliver not acked event to eventHandler on restart" in {
      val init = Initialize()
      val aggregate = startAggregate(init.counter)
      aggregate.executeSuccess(init)
      aggregate.executeSuccess(Increment(init.counter))
      expectEvent(counter.Event.Data(init.counter, 0, Incremented(1)), doAck = false)
      expectNoEvents()

      aggregate.kill(init.counter)

      expectEvent(counter.Event.Data(init.counter, 0, Incremented(1)))
      expectNoEvents()
    }

    "preserve order in redelivers" in {
      val init = Initialize()
      val aggregate = startAggregate(init.counter)
      aggregate.executeSuccess(init)
      aggregate.executeSuccess(Increment(init.counter))
      expectEvent(counter.Event.Data(init.counter, 0, Incremented(1)), doAck = false)
      expectNoEvents()

      aggregate.executeSuccess(Increment(init.counter))
      expectNoEvents()

      expectEvent(counter.Event.Data(init.counter, 0, Incremented(1)))
      expectEvent(counter.Event.Data(init.counter, 1, Incremented(2)))
      expectNoEvents()
    }

    "preserve order in redelivers after restart" in {
      val init = Initialize()
      val aggregate = startAggregate(init.counter)
      aggregate.executeSuccess(init)
      aggregate.executeSuccess(Increment(init.counter))
      expectEvent(counter.Event.Data(init.counter, 0, Incremented(1)), doAck = false)
      expectNoEvents()

      aggregate.executeSuccess(Increment(init.counter))
      expectNoEvents()

      aggregate.kill(init.counter)

      expectEvent(counter.Event.Data(init.counter, 0, Incremented(1)))
      expectEvent(counter.Event.Data(init.counter, 1, Incremented(2)))
      expectNoEvents()
    }

    "preserve order in redelivers after restart with new commands" in {
      val init = Initialize()
      val aggregate = startAggregate(init.counter)
      aggregate.executeSuccess(init)
      aggregate.executeSuccess(Increment(init.counter))
      expectEvent(counter.Event.Data(init.counter, 0, Incremented(1)), doAck = false)
      expectNoEvents()

      aggregate.executeSuccess(Increment(init.counter))
      expectNoEvents()

      aggregate.kill(init.counter)
      aggregate.executeSuccess(Increment(init.counter))

      expectEvent(counter.Event.Data(init.counter, 0, Incremented(1)))
      expectEvent(counter.Event.Data(init.counter, 1, Incremented(2)))
      expectEvent(counter.Event.Data(init.counter, 2, Incremented(3)))
      expectNoEvents()
    }

    "do not redeliver acked events on restart" in {
      val init = Initialize()
      val aggregate = startAggregate(init.counter)
      aggregate.executeSuccess(init)
      aggregate.executeSuccess(Increment(init.counter))
      expectEvent(counter.Event.Data(init.counter, 0, Incremented(1)))
      aggregate.executeSuccess(Increment(init.counter))
      expectEvent(counter.Event.Data(init.counter, 1, Incremented(2)), doAck = false)
      aggregate.executeSuccess(Increment(init.counter))
      expectNoEvents()

      aggregate.kill(init.counter)

      expectEvent(counter.Event.Data(init.counter, 1, Incremented(2)))
      expectEvent(counter.Event.Data(init.counter, 2, Incremented(3)))
      expectNoEvents()
    }

    "do not redeliver later acked events after next restart" in {
      val init = Initialize()
      val aggregate = startAggregate(init.counter)
      aggregate.executeSuccess(init)
      aggregate.executeSuccess(Increment(init.counter))
      expectEvent(counter.Event.Data(init.counter, 0, Incremented(1)), doAck = false)
      expectNoEvents()

      aggregate.kill(init.counter)
      expectEvent(counter.Event.Data(init.counter, 0, Incremented(1)))
      aggregate.executeSuccess(Increment(init.counter))
      expectEvent(counter.Event.Data(init.counter, 1, Incremented(2)), doAck = false)
      expectNoEvents()

      aggregate.kill(init.counter)
      expectEvent(counter.Event.Data(init.counter, 1, Incremented(2)))
      expectNoEvents()

      aggregate.executeSuccess(Increment(init.counter))
      expectEvent(counter.Event.Data(init.counter, 2, Incremented(3)))
    }

    "allow passivation if no outstanding event ack" in {
      val init = Initialize()
      val aggregate = startAggregate(init.counter)
      aggregate.executeSuccess(init)

      Thread.sleep(200)
      aggregate ! aggregateActor.RequestPassivation("yes", "no")
      expectMsg("yes")

      aggregate.executeSuccess(Increment(init.counter))
      expectEvent(counter.Event.Data(init.counter, 0, Incremented(1)))
      expectNoEvents()

      Thread.sleep(200)
      aggregate ! aggregateActor.RequestPassivation("yes", "no")
      expectMsg("yes")

      aggregate.executeSuccess(Increment(init.counter))
      expectEvent(counter.Event.Data(init.counter, 1, Incremented(2)))
      expectNoEvents()

      Thread.sleep(200)
      aggregate ! aggregateActor.RequestPassivation("yes", "no")
      expectMsg("yes")
    }

    "not allow passivation if there is an outstanding event ack" in {
      val init = Initialize()
      val aggregate = startAggregate(init.counter)
      aggregate.executeSuccess(init)

      aggregate.executeSuccess(Increment(init.counter))

      aggregate ! aggregateActor.RequestPassivation("yes", "no")
      expectMsg("no")

      expectEvent(counter.Event.Data(init.counter, 0, Incremented(1)))
      expectNoEvents()

      Thread.sleep(200)
      aggregate ! aggregateActor.RequestPassivation("yes", "no")
      expectMsg("yes")

      expectNoEvents()

      aggregate.executeSuccess(Increment(init.counter))

      aggregate ! aggregateActor.RequestPassivation("yes", "no")
      expectMsg("no")

      expectEvent(counter.Event.Data(init.counter, 1, Incremented(2)))
      expectNoEvents()

      Thread.sleep(200)
      aggregate ! aggregateActor.RequestPassivation("yes", "no")
      expectMsg("yes")
    }


    "preserve state if passivated" in {
      val init = Initialize()
      val aggregate = startAggregate(init.counter)
      aggregate.executeSuccess(init)

      aggregate.executeSuccess(Increment(init.counter))
      expectEvent(counter.Event.Data(init.counter, 0, Incremented(1)))
      expectNoEvents()

      aggregate ! aggregateActor.Passivate
      Thread.sleep(500)
      expectNoEvents()

      val aggregate2 = startAggregate(init.counter)
      aggregate2.executeSuccess(Increment(init.counter))
      expectEvent(counter.Event.Data(init.counter, 1, Incremented(2)))
      expectNoEvents()
    }
  }
}