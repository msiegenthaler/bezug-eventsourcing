package bezug.fakturierung

import bezug.fakturierung.Faktura.{FakturaVervollständigt, FakturaKopfErstellt}
import ch.eventsourced.support.GuidProcessManagerType

object FakturaBuchenProcess extends GuidProcessManagerType {
  def name = "FakturaBuchen"

  type Command = Nothing
  type Error = Nothing

  def triggeredBy = Set(Faktura)
  def initiate = {
    case Faktura.EventData(id, _, FakturaKopfErstellt(kopf)) =>
      Id(id.guid)
  }
  def seed(id: Id) = Manager(id)

  case class Manager(id: Id) extends BaseManager {
    def handle = {
      case Faktura.EventData(id, _, FakturaVervollständigt(kopf)) =>
        Completed() + Schuldner.FakturaHinzufügen(id, kopf.person, kopf.register, kopf.steuerjahr)
    }
    def applyTransition = PartialFunction.empty
  }
}
