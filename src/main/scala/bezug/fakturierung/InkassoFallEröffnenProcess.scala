package bezug.fakturierung

import bezug.debitor.{Buchung, Debitor, InkassoFall}
import bezug.fakturierung.Faktura.{FakturaKopf, FakturaKopfErstellt}
import bezug.fakturierung.Schuldner.FakturaFallErstellt
import ch.eventsourced.api.ProcessManager.Subscribe
import ch.eventsourced.support.GuidProcessManagerType

object InkassoFallEröffnenProcess extends GuidProcessManagerType {
  def name = "InkassoFallEröffnen(Fakturierung)"

  def triggeredBy = Set(Faktura)
  def initiate = {
    case Faktura.EventData(id, _, FakturaKopfErstellt(kopf)) =>
      Id(id.guid)
  }

  type Command = Any
  type Error = Nothing
  sealed trait Transition
  case class Step1To2(fakutura: Faktura.Id, kopf: FakturaKopf) extends Transition

  sealed trait Manager extends BaseManager
  /** Neue Faktura zu Schuldner hinzufügen. */
  case class Step1(id: Id) extends Manager {
    def handle = {
      case Faktura.EventData(id, _, FakturaKopfErstellt(kopf)) =>
        val cmd = Schuldner.FakturaHinzufügen(id, kopf.person, kopf.register, kopf.steuerjahr)
        Continue(Step1To2(id, kopf)) +
          cmd +
          Subscribe(Schuldner.AggregateKey(cmd.schuldner))
    }
    def applyTransition = {
      case Step1To2(faktura, kopf) => Step2(id, faktura, kopf)
    }
  }
  /** - InkassoFall für FakturaGruppe erstellen, falls nötig.
    * -  */
  case class Step2(id: Id, faktura: Faktura.Id, kopf: FakturaKopf) extends Manager {
    def handle = {
      case Schuldner.Event(FakturaFallErstellt(id, person, register, steuerjahr)) =>
        Completed() +
          Debitor.InkassoFallEröffnen(person, register, steuerjahr)
      case Schuldner.Event(FakturaFallErstellt(id, person, register, steuerjahr)) =>
        Completed() +
          Debitor.InkassoFallEröffnen(person, register, steuerjahr)
    }
    def applyTransition = {
      case _ => ???
    }
  }

  /*
  case class Manager22(id: Id) extends BaseManager {
    def handle = {
      case Schuldner.Event(FakturaGruppeErstellt(id, person, register, steuerjahr)) =>
        Debitor.InkassoFallEröffnen(person, register, steuerjahr)

      case Debitor.InkassoFallEröffnet(inkassofall) =>
        Buchung.Buchen()
      //TODO faktura buchen
    }
    def applyTransition = ???
  }
  def seed(id: Id) = Manager(id)
  */
}
