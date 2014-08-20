package bezug

import scala.concurrent.duration._
import akka.testkit.ImplicitSender
import org.scalatest.Matchers
import ch.eventsourced.infrastructure.akka.ContextBackendTestKit
import bezug.Monat.April
import bezug.debitor.{Debitor, BelegartUrbeleg, Buchung}
import bezug.debitor.Buchung.Urbeleg
import bezug.fakturierung.{Schuldner, Faktura}
import bezug.fakturierung.Faktura.FakturaPosition

class BezugSpec extends ContextBackendTestKit with ImplicitSender with Matchers {
  val context = BezugContext

  implicit def timeout = 10.seconds

  "Bezug" must {
    "be startable" in {
      pubSub.expectNoEvents
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

      execute(Faktura.FakturaBeauftragen(person1, Register.NP, Jahr(2014), valuta, grundlagen, positionen))


      val (k, ps) = pubSub.expectEvent() { case Faktura.FakturaVervollständigt(kopf, positionen) => (kopf, positionen)}
      assert(k === Faktura.FakturaKopf(person1, Register.NP, Jahr(2014), valuta, grundlagen))
      assert(ps === positionen)

      val (inkassoFall, fakturaFall) = pubSub.expectEvent() { case Debitor.InkassoFallEröffnet(ikf, ff) => (ikf, ff)}

      pubSub.expectEvent() { case Schuldner.InkassoFallZugeordnet(`fakturaFall`, `inkassoFall`) => ()}

      pubSub.expectEvent() { case Buchung.Gebucht(_, `valuta`, Urbeleg(BelegartUrbeleg.Faktura), Nil) => ()}
    }
  }
}