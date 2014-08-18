package bezug

import bezug.Monat.April
import bezug.debitor.Buchung
import bezug.fakturierung.Faktura.{FakturaPosition, FakturaBeauftragen}
import ch.eventsourced.api.BoundedContextBackend
import pubsub.Producer.Publish
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import akka.actor.ActorSystem
import akka.testkit.{TestProbe, ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterEach, BeforeAndAfterAll, Matchers, WordSpecLike}
import com.typesafe.config.ConfigFactory
import scalaz._
import Scalaz._
import ch.eventsourced.infrastructure.akka.AkkaInfrastructure
import bezug.fakturierung.Faktura

class BezugSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {
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

  val pubSub = TestProbe()

  implicit val timeout = 5.second
  val infrastructure = new AkkaInfrastructure(system)
  def await[A](f: Future[A]) = Await.result(f, 5.seconds)

  var backend: BoundedContextBackend[BezugContext.Command, BezugContext.Event, BezugContext.Error] = null
  override def beforeEach = {
    backend = infrastructure.startContext(BezugContext, pubSub.ref)
  }
  override def afterEach = {
    await(backend.shutdown())
    Thread.sleep(200)
  }

  "Bezug" must {
    "be startable" in {
      pubSub.expectNoMsg(500.millis)
    }

    "process a FakturaBeauftragen" in {
      val person1 = Person(Person.Id("1"))
      val institution1 = Institution("BE")
      val institution2 = Institution("CH")
      val kat1 = KatId("A")

      val valuta = Datum(1, April, Jahr(2014))
      val grundlagen = Faktura.Grundlagen("ref-1", DatumBereich(valuta, valuta), "Rechnung")
      val positionen = FakturaPosition(institution1, kat1, Betrag(100)) ::
        FakturaPosition(institution2, kat1, Betrag(6)) ::
        Nil
      val cmd = Faktura.FakturaBeauftragen(person1, Register.NP, Jahr(2014), valuta, grundlagen, positionen)

      assert(await(backend.execute(cmd)).isSuccess)

      Thread.sleep(3000)

      println("#####################")
      (1 to 10) foreach { _ =>
        val msg = pubSub.expectMsgClass(2.seconds, classOf[Publish])
        pubSub.reply(msg.onPublished)
        println(s"@@@@ Received: $msg")
      }
      println("#####################")
      //TODO expect events
    }
  }
}