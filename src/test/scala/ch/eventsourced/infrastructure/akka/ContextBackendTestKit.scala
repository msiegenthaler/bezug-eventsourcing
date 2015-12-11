package ch.eventsourced.infrastructure.akka

import scala.annotation.tailrec
import scala.language.existentials
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import akka.actor.ActorSystem
import akka.testkit.{TestProbe, TestKit}
import com.typesafe.config.ConfigFactory
import org.scalatest.{WordSpecLike, BeforeAndAfterAll, BeforeAndAfterEach}
import pubsub.Producer.Publish
import ch.eventsourced.api.{EventData, BoundedContextBackendType}

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

  object pubSub {
    private val probe = TestProbe()
    def ref = probe.ref

    private var buffer = Seq.empty[EventData]

    def expect[A](hint: String = "", max: FiniteDuration = 5.seconds)(pf: PartialFunction[EventData, A]): A = {
      val event = buffer.find(pf.isDefinedAt).getOrElse {
        within(max) {
          fish(hint, pf.isDefinedAt)
        }
      }
      pf(event)
    }
    def expectEvent[A](hint: String = "", max: FiniteDuration = 5.seconds)(pf: PartialFunction[Context#Event, A]): A = {
      expect(hint, max) {
        case EventData(_, _, _, event) if pf.isDefinedAt(event.asInstanceOf[Context#Event]) =>
          pf(event.asInstanceOf[Context#Event])
      }
    }

    def expectNoEvents = {
      assert(buffer.isEmpty)
      probe.expectNoMsg(500.millis)
    }

    def allEvents(timeout: Duration = 1.second) = {
      val msgs = probe.receiveWhile(max = timeout)(publishMessage)
      msgs.foreach(addToBuffer)
      val res = buffer
      buffer = Seq.empty
      res
    }
    def printAllEvents() = allEvents().foreach(e => println(s"- $e"))

    def reset() = {
      probe.receiveWhile(max = 500.millis)(publishMessage)
      buffer = Seq.empty
    }

    private def addToBuffer(msg: EventData) = {
      if (!buffer.exists(e => e.aggregateKey == msg.aggregateKey && e.sequence == msg.sequence))
        buffer = buffer :+ msg
    }

    @tailrec
    private def fish(hint: String, filter: EventData => Boolean): EventData = {
      val event = probe.expectMsgPF(hint = hint)(publishMessage)
      if (filter(event)) event
      else {
        addToBuffer(event)
        fish(hint, filter)
      }
    }

    private val publishMessage: PartialFunction[Any, EventData] = {
      case Publish(_, _, event: EventData, ack) =>
        probe.reply(ack)
        event
    }
  }

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
    system.terminate()
  }


  def execute(cmd: Context#Command)(implicit timeout: FiniteDuration): Unit = {
    val res = await(backend.execute(cmd))
    assert(res.isSuccess)
  }

  def commandFails(cmd: Context#Command)(implicit timeout: FiniteDuration): Context#Error = {
    val res = await(backend.execute(cmd))
    assert(res.isFailure)
    res.fold(f => f, _ => ???)
  }
  def expectCommandFailure(cmd: Context#Command, expect: Context#Error)(implicit timeout: FiniteDuration): Unit = {
    val err = commandFails(cmd)
    assert(err === expect)
  }
}