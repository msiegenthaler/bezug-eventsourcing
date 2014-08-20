package ch.eventsourced.infrastructure.akka

import scala.language.existentials
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import akka.actor.ActorSystem
import akka.testkit.{TestProbe, TestKit}
import com.typesafe.config.ConfigFactory
import org.scalatest.{WordSpecLike, BeforeAndAfterAll, BeforeAndAfterEach}
import ch.eventsourced.api.BoundedContextBackendType

/** TestKit for BoundedContextBackendType's. */
abstract class ContextBackendTestKit(_system: ActorSystem) extends TestKit(_system) with WordSpecLike with BeforeAndAfterEach with BeforeAndAfterAll {
  def this() = this {
    val config = ConfigFactory.parseString(
      """
        |akka.loglevel = "WARNING"
        |akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
        |akka.persistence.snapshot-store.plugin = "in-memory-snapshot-store"
        |akka.log-dead-letters = "false"
        |akka.log-dead-letters-during-shutdown = "false"
      """.stripMargin)
    ActorSystem(getClass.getName.filter(_.isLetterOrDigit), config)
  }

  val context: BoundedContextBackendType
  type Context = context.type

  val infrastructure = new AkkaInfrastructure(system)
  def backend = _backend.getOrElse(throw new IllegalStateException("Backend not initialized"))
  private var _backend = Option.empty[Context#Backend]

  val pubSub = TestProbe()

  def await[A](f: Future[A])(implicit timeout: Duration) = Await.result(f, timeout)

  override def beforeEach = {
    _backend = Some(infrastructure.startContext(context, pubSub.ref))
  }
  override def afterEach = {
    implicit val timeout = 5.seconds
    await(backend.shutdown())
    Thread.sleep(200)
  }

  override def afterAll = {
    system.shutdown()
  }
}