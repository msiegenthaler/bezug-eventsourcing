package bezug
package fakturierung

import ch.eventsourced.api.Entity
import ch.eventsourced.support.{Guid, GuidAggregateType}
import bezug.debitor.InkassoKey

object Schuldner extends GuidAggregateType {
  def name = "Schuldner"

  sealed trait Command {
    def schuldner: Id
  }
  case class FakturaHinzufügen(faktura: Faktura.Id, person: Person, register: Register, steuerjahr: Jahr) extends Command {
    def schuldner = Id(???) //person.id
  }
  def aggregateIdForCommand(command: Command) = Some(command.schuldner)

  sealed trait Event
  //TODO FakturaGruppe..
  case class FakturaHinzugefügt(zu: InkassoKey, faktura: Faktura.Id) extends Event

  sealed trait Error

  type Root = Schuldner
  case class Schuldner(id: Id, fakturen: Map[FakturaGruppe, Seq[Faktura.Id]]) extends RootBase {
    def execute(c: Command) = c match {
      case c: FakturaHinzufügen =>
        val key = InkassoKey(c.person, c.register, c.steuerjahr, fakturen.size + 1)
        //TODO logik wann neuer key und wann added
        FakturaHinzugefügt(key, c.faktura)
    }
    def applyEvent = {
      case FakturaHinzugefügt(key, faktura) =>
        val fs = fakturen.getOrElse(key, Nil) :+ faktura
        copy(fakturen = fakturen + (key -> fs))
    }
  }

  case class FakturaGruppeId(id: Guid)
  case class FakturaGruppe(id: FakturaGruppeId, person: Person, register: Register, steuerjahr: Jahr)
    extends Entity

  protected def types = typeInfo
  def seed(id: Id) = Schuldner(id, Map.empty)
}