package ch.eventsourced.infrastructure.akka

import java.io.File
import akka.actor.ActorSystem
import akka.cluster.Cluster
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterEach, BeforeAndAfterAll, Matchers, WordSpecLike}

abstract class AbstractSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {
  def this() = this {
    val config = ConfigFactory.parseString(
      """
        |akka.loglevel = "WARNING"
        |akka.actor.debug.receive = "on"
        |akka.actor.debug.autoreceive = "off"
        |akka.actor.debug.lifecycle = "off"
        |akka.actor.warn-about-java-serializer-usage = "off"
        |akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
        |akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.local"
        |akka.persistence.at-least-once-delivery.redeliver-interval = "1s"
        |ch.eventsourced.aggregate-subscription.retry-interval = 1s
      """.stripMargin)
    ActorSystem(getClass.getName.filter(_.isLetterOrDigit), config)
  }

  override def beforeEach = deleteSnapshots()
  override def afterEach = deleteSnapshots()
  private def deleteSnapshots() {
    val snapshotDir = new File("snapshots")
    if (snapshotDir.isDirectory) {
      for (list <- Option(snapshotDir.listFiles()); child <- list) {
        child.delete()
      }
    }
  }


  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }
}