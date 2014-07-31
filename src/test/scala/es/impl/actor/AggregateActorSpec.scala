package es.impl.actor

import akka.actor.ActorSystem
import akka.cluster.Cluster
import akka.testkit.{TestProbe, ImplicitSender, TestKit}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import es.api.EventData
import org.scalatest.{WordSpecLike, BeforeAndAfterAll, Matchers}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

class AggregateActorSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with Matchers with BeforeAndAfterAll {
  def this() = this {
    val config = ConfigFactory.parseString(
      """
        |akka.actor.provider = "akka.cluster.ClusterActorRefProvider"
        |akka.loglevel = "WARNING"
        |akka.actor.debug.receive = "on"
        |akka.actor.debug.autoreceive = "off"
        |akka.actor.debug.lifecycle = "off"
        |akka.remote.netty.tcp.hostname = "127.0.0.1"
        |akka.remote.netty.tcp.port = 0
        |akka.persistence.journal.plugin = "in-memory-journal"
      """.stripMargin)
    ActorSystem("AggregateActorSpec", config)
  }

  override def beforeAll {
    Cluster.get(system).join(Cluster.get(system).selfAddress)
  }

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  val manager = {
    new AggregateActorManager(CounterActorBinding)(system, system.eventStream, 1, 3.seconds)
  }

  def eventProbe() = {
    val eventProbe = TestProbe()
    system.eventStream.subscribe(eventProbe.ref, classOf[EventData])
    eventProbe
  }


  implicit val timeout = Timeout(5.seconds)

  def executeSuccess(cmd: counter.Command) = {
    val res = manager.execute(cmd)
    awaitCond(res.isCompleted, timeout.duration)
    assert(res.value.get.isSuccess)
  }

  "aggregate actor" must {
    import counter._

    "create a new aggregate root for never used id" in {
      val ep = eventProbe()
      executeSuccess(Initialize())

      ep.expectNoMsg()
    }
    "preserve state between invocations" in {
      val ep = eventProbe()
      val init = Initialize()
      executeSuccess(init)

      executeSuccess(Increment(init.counter))
      ep.expectMsg(counter.Event.Data(init.counter, 0, Incremented(1)))

      executeSuccess(Increment(init.counter))
      ep.expectMsg(counter.Event.Data(init.counter, 1, Incremented(2)))
    }

    "preserve state if killed" in {
      val ep = eventProbe()
      val init = Initialize()
      executeSuccess(init)

      executeSuccess(Increment(init.counter))
      ep.expectMsg(counter.Event.Data(init.counter, 0, Incremented(1)))

      manager.execute(Kill(init.counter))

      executeSuccess(Increment(init.counter))
      ep.expectMsg(counter.Event.Data(init.counter, 1, Incremented(2)))
    }

    "preserve state if passivated" in {
      val ep = eventProbe()
      val init = Initialize()
      executeSuccess(init)

      executeSuccess(Increment(init.counter))
      ep.expectMsg(counter.Event.Data(init.counter, 0, Incremented(1)))

      Thread.sleep(5.seconds.toMillis)

      executeSuccess(Increment(init.counter))
      ep.expectMsg(counter.Event.Data(init.counter, 1, Incremented(2)))
    }
  }
}