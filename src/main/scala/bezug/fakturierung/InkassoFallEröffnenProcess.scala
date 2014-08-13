package bezug.fakturierung

import bezug.debitor.Debitor
import bezug.debitor.Debitor.InkassoFallEröffnet
import bezug.fakturierung.Schuldner.{FakturaFall, FakturaFallErstellt}
import ch.eventsourced.api.ProcessManager.Subscribe
import ch.eventsourced.support.GuidProcessManagerType

/** Wenn ein neuer FakturaFall erstellt wird, dann für diesen einen InkassoFall erstellen und den InkassoFall dem
  * FakturaFall zuordnen
  */
object InkassoFallEröffnenProcess extends GuidProcessManagerType {
  def name = "InkassoFallEröffnen(Fakturierung)"

  def triggeredBy = Set(Faktura)
  def initiate = {
    case Schuldner.EventData(_, _, e: FakturaFallErstellt) =>
      Id(e.fall.guid)
  }

  type Command = Any
  type Error = Nothing
  sealed trait Transition
  case class ToInkassoFallZuordnen(schuldner: Schuldner.Id, faktura: Faktura.Id, fakturaFall: FakturaFall.Id) extends Transition

  sealed trait Manager extends BaseManager
  case class ZuDebitorHinzufügen(id: Id) extends Manager {
    def handle = {
      case Schuldner.EventData(schuldner, _, event: FakturaFallErstellt) =>
        val cmd = Debitor.InkassoFallEröffnen(event.person, event.register, event.steuerjahr, event.fall)
        Continue(ToInkassoFallZuordnen(schuldner, event.aufgrund, event.fall)) +
          cmd +
          Subscribe(Debitor.AggregateKey(cmd.debitor))
    }
    def applyTransition = {
      case ToInkassoFallZuordnen(schuldner, faktura, fall) => InkassoFallZuordnen(id, schuldner, faktura, fall)
    }
  }
  case class InkassoFallZuordnen(id: Id, schuldner: Schuldner.Id, faktura: Faktura.Id, fakturaFall: FakturaFall.Id) extends Manager {
    def handle = {
      case Debitor.Event(InkassoFallEröffnet(inkassoFall, `fakturaFall`)) =>
        Completed() +
          Schuldner.InkassoFallZuordnen(schuldner, fakturaFall, inkassoFall)
    }
    def applyTransition = PartialFunction.empty

  }
  def seed(id: Id) = ZuDebitorHinzufügen(id)
}
