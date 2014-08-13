package bezug
package debitor

import ch.eventsourced.support.GuidAggregateType

object InkassoFall extends GuidAggregateType {
  def name = "Inkassofall"

  //  case class InkassoKey(debitor: Debitor.Id, register: Register, steuerjahr: Jahr, laufnummer: Int)

  sealed trait Command {
    def inkassoFall: Id
  }
  case class Eröffnen(debitor: Debitor.Id, register: Register, steuerjahr: Jahr, inkassoFall: Id = generateId) extends Command
  def aggregateIdForCommand(command: Command) = Some(command.inkassoFall)

  sealed trait Event
  case class Eröffnet(debitor: Debitor.Id, register: Register, steuerjahr: Jahr) extends Event

  sealed trait Error

  //TODO Attribute
  //Personentyp (NP,JP,Virtu)
  //Inkassostand (Mahnung, Betreibung etc.)


  sealed trait Root extends RootBase
  def seed(id: Id) = EmptyInkassoFall(id)
  case class EmptyInkassoFall(id: Id) extends Root {
    def execute(c: Command) = ???
    def applyEvent = ???
  }
  case class InkassoFall(id: Id, buchungen: Seq[Buchung.Id]) extends Root {
    def execute(c: Command) = ???
    def applyEvent = ???
  }

  protected def types = typeInfo

}