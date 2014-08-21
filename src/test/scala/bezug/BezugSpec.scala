package bezug

import scala.concurrent.duration._
import akka.testkit.ImplicitSender
import org.scalatest.Matchers
import ch.eventsourced.infrastructure.akka.ContextBackendTestKit
import bezug.Monat.April
import bezug.debitor._
import bezug.debitor.Buchung.Urbeleg
import bezug.fakturierung.{Schuldner, Faktura}
import bezug.fakturierung.Faktura.Position

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
      val positionen = Position(institution1, kat1, Betrag(100)) ::
        Position(institution2, kat1, Betrag(6)) ::
        Nil

      execute(Faktura.FakturaBeauftragen(person1, Register.NP, Jahr(2014), valuta, grundlagen, positionen))


      val (k, ps) = pubSub.expectEvent() { case Faktura.FakturaVervollständigt(kopf, positionen) => (kopf, positionen)}
      assert(k === Faktura.Kopf(person1, Register.NP, Jahr(2014), valuta, grundlagen))
      assert(ps === positionen)

      val (debitor, inkassoFall, fakturaFall) = pubSub.expect() {
        case Debitor.EventData(id, _, Debitor.InkassoFallEröffnet(ikf, ff)) => (id, ikf, ff)
      }

      pubSub.expectEvent() { case Schuldner.InkassoFallZugeordnet(`fakturaFall`, `inkassoFall`) => ()}

      pubSub.expectEvent() {
        case Buchung.Gebucht(_, `valuta`, Urbeleg(BelegartUrbeleg.Faktura), Debitorkonto(deb), haben, positionen) =>
          assert(haben === Ertragkonto)
          assert(deb == debitor)
          assert(positionen.size === 2)
          assert(positionen(0).betrag === Betrag(100))
          assert(positionen(0).inkassofall === inkassoFall)
          assert(positionen(0).betragskategorie === kat1)
          assert(positionen(0).institution === institution1)
          assert(positionen(1).betrag === Betrag(6))
      }

      pubSub.expectEvent("SaldoAktualisiert") {
        case InkassoFall.SaldoAktualisiert(_, saldo, alt) =>
          assert(saldo === Betrag(106))
          assert(alt === Betrag(0))
      }
    }
  }
}