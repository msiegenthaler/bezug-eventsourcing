package bezug.debitor

import bezug.Bezug
import bezug.debitor.Buchung.{Buchungskonto, KontoMitVerwendung, Gebucht}
import bezug.debitor.InkassoFall.BuchungRegistrieren
import ch.eventsourced.api.ProcessManagerType
import ch.eventsourced.support.DerivedId

object SaldoAktualisierenProcess extends ProcessManagerType with DerivedId[Buchung.Id] {
  def name = "SaldoAktualisieren"
  protected def types = typeInfo

  def triggeredBy = Set(Buchung)
  def initiate = {
    case Buchung.EventData(id, _, _: Gebucht) => generateId(id)
  }

  type Transition = this.type
  type Command = Bezug.Command
  type Error = Bezug.Error

  case class Manager(id: Id) extends BaseManager {
    def handle = {
      case Buchung.EventData(id, _, gebucht: Gebucht) =>
        val cmds = (gebucht.soll.inkassofälle ++ gebucht.haben.inkassofälle).toSeq map { inkassofall =>
          BuchungRegistrieren(inkassofall, id, gebucht)
        }
        Completed() ++ cmds
    }
    def applyTransition = PartialFunction.empty
  }
  def seed(id: SaldoAktualisierenProcess.Id) = Manager(id)
}
