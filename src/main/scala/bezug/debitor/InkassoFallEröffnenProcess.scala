package bezug
package debitor

import bezug.debitor.Debitor.InkassoFallErstellenVorbereitet
import bezug.debitor.InkassoFall.Eröffnet
import ch.eventsourced.api.ProcessManager.{Unsubscribe, Subscribe}
import ch.eventsourced.api.ProcessManagerType
import ch.eventsourced.support.TypedGuid

object InkassoFallEröffnenProcess extends ProcessManagerType with TypedGuid {
  def name = "InkassoFallEröffnen(Debitor)"
  def triggeredBy = Set(Debitor)

  def initiate = {
    case Debitor.Event(_: InkassoFallErstellenVorbereitet) => generateId
  }
  type Command = Any
  type Error = Nothing
  sealed trait Transition
  case class MitInkassoFall(debitor: Debitor.Id, inkassoFall: InkassoFall.Id, referenz: Any) extends Transition

  sealed trait Manager extends BaseManager
  case class Step1(id: Id) extends Manager {
    def handle = {
      case Debitor.EventData(debitorId, _, InkassoFallErstellenVorbereitet(register, steuerjahr, referenz)) =>
        val eröffnen = InkassoFall.Eröffnen(debitorId, register, steuerjahr)
        Continue(MitInkassoFall(debitorId, eröffnen.inkassoFall, referenz)) + eröffnen
    }
    def applyTransition = {
      case MitInkassoFall(debitor, fall, ref) =>
        Step2(id, fall, ref) +
          Unsubscribe(Debitor.AggregateKey(debitor)) +
          Subscribe(InkassoFall.AggregateKey(fall))

    }
  }
  case class Step2(id: Id, inkassoFall: InkassoFall.Id, referenz: Any) extends Manager {
    def handle = {
      case InkassoFall.Event(Eröffnet(debitor, _, _)) =>
        Completed() + Debitor.InkassoFallHinzufügen(debitor, inkassoFall, referenz)
    }
    def applyTransition = PartialFunction.empty
  }
  def seed(id: Id) = Step1(id)

  protected def types = typeInfo
}
