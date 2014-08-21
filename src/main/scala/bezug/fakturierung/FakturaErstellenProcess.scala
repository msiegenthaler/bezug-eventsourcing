package bezug
package fakturierung

import bezug.debitor.Debitor.InkassoFallEröffnet
import bezug.debitor._
import bezug.debitor.Buchung.Urbeleg
import bezug.fakturierung.Faktura.{FakturaVervollständigt}
import bezug.fakturierung.Schuldner.{InkassoFallZugeordnet, FakturaFall, FakturaFallErstellt}
import ch.eventsourced.api.ProcessManager.{Unsubscribe, Subscribe}
import ch.eventsourced.api.ProcessManagerType
import ch.eventsourced.support.DerivedId

object FakturaErstellenProcess extends ProcessManagerType with DerivedId[Faktura.Id] {
  def name = "FakturaErstellen"

  def seed(id: Id) = FakturaHinzufügenZuSchuldner(id)

  def triggeredBy = Set(Faktura)
  def initiate = {
    case Faktura.EventData(id, _, FakturaVervollständigt(kopf, _)) => generateId(id)
  }

  type Command = Bezug.Command
  type Error = Nothing
  sealed trait Transition
  case class ToFakturaFallErmitteln(schuldner: Schuldner.Id, fakutura: Faktura.Id, kopf: Faktura.Kopf, positionen: Seq[Faktura.Position]) extends Transition
  case class ToInkassoFallErmitteln(fakturaFall: FakturaFall.Id) extends Transition
  case class ToBuchungErstellen(inkassoFall: InkassoFall.Id) extends Transition

  sealed trait Manager extends BaseManager

  case class FakturaHinzufügenZuSchuldner(id: Id) extends Manager {
    def handle = {
      case Faktura.EventData(faktura, _, FakturaVervollständigt(kopf, positionen)) =>
        val cmd = Schuldner.FakturaHinzufügen(faktura, kopf.person, kopf.register, kopf.steuerjahr)
        Continue(ToFakturaFallErmitteln(cmd.schuldner, faktura, kopf, positionen)) + cmd

    }
    def applyTransition = {
      case ToFakturaFallErmitteln(schuldner, faktura, kopf, ps) =>
        FakturaFallErmitteln(id, schuldner, faktura, kopf, ps) + Subscribe(Schuldner.AggregateKey(schuldner))
    }
  }

  case class FakturaFallErmitteln(id: Id, schuldner: Schuldner.Id, faktura: Faktura.Id, kopf: Faktura.Kopf, positionen: Seq[Faktura.Position]) extends Manager {
    def handle = {
      case Schuldner.EventData(`schuldner`, _, FakturaFallErstellt(fall, person, register, steuerjahr, `faktura`)) =>
        Continue(ToInkassoFallErmitteln(fall))
    }
    def applyTransition = {
      case ToInkassoFallErmitteln(fakturaFall) =>
        InkassoFallErmitteln(id, schuldner, faktura, fakturaFall, kopf, positionen) +
          //Alle Events vom Schuldner nochmals empfangen
          Unsubscribe(Schuldner.AggregateKey(schuldner)) +
          Subscribe(Schuldner.AggregateKey(schuldner))
    }
  }

  case class InkassoFallErmitteln(id: Id, schuldner: Schuldner.Id, faktura: Faktura.Id, fakturaFall: FakturaFall.Id, kopf: Faktura.Kopf, positionen: Seq[Faktura.Position]) extends Manager {
    def handle = {
      case Schuldner.Event(InkassoFallZugeordnet(`fakturaFall`, inkassoFall)) =>
        Continue(ToBuchungErstellen(inkassoFall))
    }
    def applyTransition = {
      case ToBuchungErstellen(inkassoFall) =>
        BuchungErstellen(id, faktura, kopf, fakturaFall, positionen, inkassoFall) +
          Subscribe(InkassoFall.AggregateKey(inkassoFall))
    }
  }

  case class BuchungErstellen(id: Id, faktura: Faktura.Id, kopf: Faktura.Kopf, fakturaFall: FakturaFall.Id, positionen: Seq[Faktura.Position], inkassoFall: InkassoFall.Id) extends Manager {
    def handle = {
      case InkassoFall.Event(InkassoFall.Eröffnet(debitor, _, _)) =>
        Completed() + Buchung.Buchen(valuta = kopf.valuta,
          urbeleg = Urbeleg(BelegartUrbeleg.Faktura),
          soll = Debitorkonto(debitor), haben = Ertragkonto,
          positionen = positionen.map(p => Buchung.Position(inkassoFall, p.betragskategorie, p.institution, p.betrag)))
    }
    def applyTransition = PartialFunction.empty
  }

  protected def types = typeInfo
}
