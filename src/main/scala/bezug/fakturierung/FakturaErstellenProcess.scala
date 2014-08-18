package bezug
package fakturierung

import bezug.debitor.Buchung
import bezug.fakturierung.Faktura.{FakturaKopf, FakturaKopfErstellt}
import bezug.fakturierung.Schuldner.{InkassoFallZugeordnet, FakturaFall, FakturaFallErstellt}
import ch.eventsourced.api.ProcessManager.{Unsubscribe, Subscribe}
import ch.eventsourced.api.ProcessManagerType
import ch.eventsourced.support.DerivedId

object FakturaErstellenProcess extends ProcessManagerType with DerivedId[Faktura.Id] {
  def name = "FakturaErstellen"

  def seed(id: Id) = FakturaHinzufügenZuSchuldner(id)

  def triggeredBy = Set(Faktura)
  def initiate = {
    case Faktura.EventData(id, _, FakturaKopfErstellt(kopf)) => generateId(id)
  }

  type Command = Bezug.Command
  type Error = Nothing
  sealed trait Transition
  case class ToFakturaFallErmitteln(schuldner: Schuldner.Id, fakutura: Faktura.Id, kopf: FakturaKopf) extends Transition
  case class ToBuchungErstellen(fakturaFall: FakturaFall.Id) extends Transition

  sealed trait Manager extends BaseManager

  case class FakturaHinzufügenZuSchuldner(id: Id) extends Manager {
    def handle = {
      case Faktura.EventData(faktura, _, FakturaKopfErstellt(kopf)) =>
        val cmd = Schuldner.FakturaHinzufügen(faktura, kopf.person, kopf.register, kopf.steuerjahr)
        Continue(ToFakturaFallErmitteln(cmd.schuldner, faktura, kopf)) + cmd

    }
    def applyTransition = {
      case ToFakturaFallErmitteln(schuldner, faktura, kopf) =>
        FakturaFallErmitteln(id, schuldner, faktura, kopf) + Subscribe(Schuldner.AggregateKey(schuldner))
    }
  }

  case class FakturaFallErmitteln(id: Id, schuldner: Schuldner.Id, faktura: Faktura.Id, kopf: FakturaKopf) extends Manager {
    def handle = {
      case Schuldner.EventData(`schuldner`, _, FakturaFallErstellt(fall, person, register, steuerjahr, `faktura`)) =>
        Continue(ToBuchungErstellen(fall))
    }
    def applyTransition = {
      case ToBuchungErstellen(fakturaFall) =>
        BuchungErstellen(id, faktura, kopf, fakturaFall) +
          //Alle Events vom Schuldner nochmals empfangen
          Unsubscribe(Schuldner.AggregateKey(schuldner)) +
          Subscribe(Schuldner.AggregateKey(schuldner))

    }
  }

  case class BuchungErstellen(id: Id, faktura: Faktura.Id, kopf: FakturaKopf, fakturaFall: FakturaFall.Id) extends Manager {
    def handle = {
      case Schuldner.Event(InkassoFallZugeordnet(`fakturaFall`, inkassoFall)) =>
        val positionen = ???
        val cmd = Buchung.Buchen(kopf.valuta, ???, positionen)
        Completed() + cmd
    }
    def applyTransition = PartialFunction.empty
  }

  protected def types = typeInfo
}
