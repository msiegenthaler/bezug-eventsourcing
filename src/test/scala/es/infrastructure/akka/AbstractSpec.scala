package es.infrastructure.akka

import akka.actor.ActorSystem
import akka.cluster.Cluster
import akka.testkit.{TestProbe, ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import pubsub.Topic

abstract class AbstractSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with Matchers with BeforeAndAfterAll {
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
}
