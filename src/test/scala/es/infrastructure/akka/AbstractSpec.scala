package es.infrastructure.akka

import akka.actor.ActorSystem
import akka.cluster.Cluster
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

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
        |akka.persistence.at-least-once-delivery.redeliver-interval = "1s"
      """.stripMargin)
    ActorSystem(getClass.getName.filter(_.isLetterOrDigit), config)
  }

  override def beforeAll {
    Cluster.get(system).join(Cluster.get(system).selfAddress)
  }

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }
}