package bezug
package fakturierung

import bezug.debitor.InkassoFall
import ch.eventsourced.api.{AggregateType, Entity}
import ch.eventsourced.support.Guid

object Schuldner extends AggregateType {
  def name = "Schuldner"

  case class Id(value: Person.Id)
  def serializeId(id: Id) = id.value.id
  def parseId(serialized: String) = Some(Id(Person.Id(serialized)))

  sealed trait Command {
    def schuldner: Id
  }
  case class FakturaHinzufügen(faktura: Faktura.Id, person: Person, register: Register, steuerjahr: Jahr) extends Command {
    def schuldner = Id(person.id)
  }
  case class InkassoFallZuordnen(schuldner: Id, zu: FakturaFall.Id, inkassoFall: InkassoFall.Id) extends Command
  def aggregateIdForCommand(command: Command) = Some(command.schuldner)

  sealed trait Event
  case class FakturaFallErstellt(id: FakturaFall.Id, person: Person.Id, register: Register, steuerjahr: Jahr) extends Event
  case class FakturaHinzugefügt(zu: FakturaFall.Id, faktura: Faktura.Id) extends Event

  sealed trait Error

  type Root = Schuldner
  case class Schuldner(id: Id, fakturaGruppen: Map[FakturaFall.Id, FakturaFall]) extends RootBase {
    def execute(c: Command) = c match {
      case c: FakturaHinzufügen =>
        def neueGruppe = {
          val gruppeId = FakturaFall.Id(Guid.generate)
          FakturaFallErstellt(gruppeId, c.person.id, c.register, c.steuerjahr) ::
            FakturaHinzugefügt(gruppeId, c.faktura) ::
            Nil
        }

        if (c.register.periodisch) {
          fakturaGruppen.values.find(g => g.register == c.register && g.steuerjahr == c.steuerjahr) match {
            case Some(gruppe) =>
              FakturaHinzugefügt(gruppe.id, c.faktura) :: Nil
            case None =>
              neueGruppe
          }
        } else neueGruppe
    }

    def applyEvent = {
      case FakturaFallErstellt(gruppeId, _, register, steuerjahr) =>
        copy(fakturaGruppen = fakturaGruppen + (gruppeId -> FakturaFall(gruppeId, register, steuerjahr, None, Seq
          .empty)))
      case FakturaHinzugefügt(zu, faktura) =>
        val gruppe = fakturaGruppen.getOrElse(zu,
          throw new IllegalStateException(s"FakturaGruppe $zu existiert nicht"))
        copy(fakturaGruppen = fakturaGruppen + (zu -> gruppe.add(faktura)))
    }
  }

  case class FakturaFall(id: FakturaFall.Id, register: Register, steuerjahr: Jahr,
    inkassoFall: Option[InkassoFall.Id],
    fakturen: Seq[Faktura.Id])
    extends Entity[FakturaFall.Id] {
    def add(faktura: Faktura.Id) = copy(fakturen = fakturen :+ faktura)
  }
  object FakturaFall {
    case class Id(guid: Guid)
  }

  protected def types = typeInfo
  def seed(id: Id) = Schuldner(id, Map.empty)
}