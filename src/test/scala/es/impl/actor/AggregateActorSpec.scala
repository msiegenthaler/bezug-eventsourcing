package es.impl.actor

import akka.actor.ActorSystem
import akka.cluster.Cluster
import akka.testkit.{TestProbe, ImplicitSender, TestKit}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import es.api.{AggregateType, EventData}
import es.impl.actor.PubSub.Topic
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

  val pubSub = TestProbe()
  val eventBusConfig = EventBusConfig(Topic.root)

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
    val topic = eventBusConfig.topicFor(event.aggregateKey)
    pubSub.expectMsgPF() {
      case PubSub.Producer.Publish(`topic`, `event`, ack) =>
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