package bezug

import scala.concurrent.duration._
import akka.testkit.ImplicitSender
import org.scalatest.Matchers
import ch.eventsourced.infrastructure.akka.ContextBackendTestKit
import pubsub.Producer.Publish
import bezug.Monat.April
import bezug.debitor.{BelegartUrbeleg, Buchung}
import bezug.debitor.Buchung.Urbeleg
import bezug.fakturierung.Faktura
import bezug.fakturierung.Faktura.FakturaPosition

class BezugSpec extends ContextBackendTestKit with ImplicitSender with Matchers {
  val context = BezugContext

  implicit def timeout = 5.second

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

      pubSub.fishForMessage(hint = "Gebucht") {
        case Publish(_, _, Buchung.Event(Buchung.Gebucht(_, `valuta`, Urbeleg(BelegartUrbeleg.Faktura), Nil)), ack) =>
          pubSub reply ack
          true
        case Publish(_, _, event, ack) =>
          //println(s"- $event")
          pubSub reply ack
          false
      }
    }
  }
}