package bezug
package fakturierung

import bezug.fakturierung.Faktura.FakturaKopfErstellt
import ch.eventsourced.support.GuidProcessManagerType

object FakturaErstellenProcess extends GuidProcessManagerType {
  def name = "FakturaErstellen"

  type Command = Schuldner.Command
  type Error = Nothing

  def triggeredBy = Set(Faktura)
  def initiate = {
    case Faktura.EventData(id, _, FakturaKopfErstellt(kopf)) =>
      Id(id.guid)
  }
  def seed(id: Id) = Manager(id)

  case class Manager(id: Id) extends BaseManager {
    def handle = {
      case Faktura.EventData(id, _, FakturaKopfErstellt(kopf)) =>
        Completed() + Schuldner.FakturaHinzufügen(id, kopf.person, kopf.register, kopf.steuerjahr)
    }
    def applyTransition = PartialFunction.empty
  }
}
