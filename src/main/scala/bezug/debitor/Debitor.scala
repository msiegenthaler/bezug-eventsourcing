package bezug
package debitor

import ch.eventsourced.api.{AggregateType, Entity}
import ch.eventsourced.support.{GuidAggregateType, Guid}

object Debitor extends AggregateType {
  def name = "Debitor"
  case class Id(person: Person.Id)
  def serializeId(id: Id) = id.person.id
  def parseId(serialized: String) = Some(Id(Person.Id(serialized)))


  sealed trait Command {
    def debitor: Id
  }
  case class InkassoFallEröffnen(person: Person.Id, register: Register, steuerjahr: Jahr, referenz: Any) extends Command {
    def debitor = Id(person)
  }
  case class InkassoFallHinzufügen(debitor: Id, inkassofall: InkassoFall.Id, referenz: Any) extends Command
  def aggregateIdForCommand(command: Command) = Some(command.debitor)

  sealed trait Event
  case class InkassoFallErstellenVorbereitet(register: Register, steuerjahr: Jahr, referenz: Any) extends Event
  case class InkassoFallEröffnet(inkassoFall: InkassoFall.Id, referenz: Any) extends Event

  type Error = this.type

  type Root = Debitor
  case class Debitor(id: Id, person: Person.Id, inkassoFälle: Seq[InkassoFall.Id]) extends RootBase {
    def execute(c: Command) = c match {
      case InkassoFallEröffnen(_, register, steuerjahr, referenz) => InkassoFallErstellenVorbereitet(register, steuerjahr, referenz)
      case InkassoFallHinzufügen(_, inkassoFall, referenz: Any) => InkassoFallEröffnet(inkassoFall, referenz)
    }
    def applyEvent = {
      case _: InkassoFallErstellenVorbereitet => this
      case InkassoFallEröffnet(inkassoFall, _) => copy(inkassoFälle = inkassoFälle :+ inkassoFall)
    }
  }
  def seed(id: Id) = Debitor(id, id.person, Nil)
  protected def types = typeInfo
}