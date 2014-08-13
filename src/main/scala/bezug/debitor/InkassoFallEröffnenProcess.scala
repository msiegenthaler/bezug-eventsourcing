package bezug.debitor

import bezug.debitor.Debitor.InkassoFallErstellenVorbereitet
import bezug.debitor.InkassoFall.Eröffnet
import ch.eventsourced.api.ProcessManager.{Unsubscribe, Subscribe}
import ch.eventsourced.support.GuidProcessManagerType

class InkassoFallEröffnenProcess extends GuidProcessManagerType {
  def name = "InkassoFallEröffnen(Debitor)"
  def triggeredBy = Set(Debitor)

  def initiate = {
    case Debitor.Event(InkassoFallErstellenVorbereitet(_, _)) => generateId
  }
  type Command = Any
  type Error = Nothing
  sealed trait Transition
  case class MitInkassoFall(inkassoFall: InkassoFall.Id) extends Transition

  sealed trait Manager extends BaseManager
  case class Step1(id: Id) extends Manager {
    def handle = {
      case Debitor.EventData(debitorId, _, InkassoFallErstellenVorbereitet(register, steuerjahr)) =>
        val eröffnen = InkassoFall.Eröffnen(debitorId, register, steuerjahr)
        Continue(MitInkassoFall(eröffnen.inkassoFall)) +
          eröffnen +
          Unsubscribe(Debitor.AggregateKey(debitorId)) +
          Subscribe(InkassoFall.AggregateKey(eröffnen.inkassoFall))
    }
    def applyTransition = {
      case MitInkassoFall(i) => Step2(id, i)
    }
  }
  case class Step2(id: Id, inkassoFall: InkassoFall.Id) extends Manager {
    def handle = {
      case InkassoFall.Event(Eröffnet(debitor, _, _)) =>
        Completed() + Debitor.InkassoFallHinzufügen(debitor, inkassoFall)
    }
    def applyTransition = PartialFunction.empty
  }
  def seed(id: Id) = Step1(id)
}
