package bezug
package fakturierung

import bezug.debitor.InkassoFall
import ch.eventsourced.api.{AggregateType, Entity}
import ch.eventsourced.support.{DerivedId, TypedGuid}
import Person.Id.stringSerialize

object Schuldner extends AggregateType with DerivedId[Person.Id] {
  def name = "Schuldner"

  sealed trait Command extends Bezug.Command {
    def schuldner: Id
  }
  case class FakturaHinzufügen(faktura: Faktura.Id, person: Person, register: Register, steuerjahr: Jahr) extends Command {
    def schuldner = generateId(person.id)
  }
  case class InkassoFallZuordnen(schuldner: Id, zu: FakturaFall.Id, inkassoFall: InkassoFall.Id) extends Command
  def aggregateIdForCommand(command: Command) = Some(command.schuldner)

  sealed trait Event extends Bezug.Event
  case class FakturaFallErstellt(fall: FakturaFall.Id, person: Person.Id, register: Register, steuerjahr: Jahr, aufgrund: Faktura.Id) extends Event
  case class FakturaHinzugefügt(zu: FakturaFall.Id, faktura: Faktura.Id) extends Event
  case class InkassoFallZugeordnet(zu: FakturaFall.Id, inkassoFall: InkassoFall.Id) extends Event

  sealed trait Error

  type Root = Schuldner
  case class Schuldner(id: Id, fälle: Map[FakturaFall.Id, FakturaFall]) extends RootBase {
    def execute(c: Command) = c match {
      case c: FakturaHinzufügen =>
        def neueGruppe = {
          val gruppeId = FakturaFall.create
          FakturaFallErstellt(gruppeId, c.person.id, c.register, c.steuerjahr, c.faktura) ::
            FakturaHinzugefügt(gruppeId, c.faktura) ::
            Nil
        }

        if (c.register.periodisch) {
          fälle.values.find(g => g.register == c.register && g.steuerjahr == c.steuerjahr) match {
            case Some(gruppe) =>
              FakturaHinzugefügt(gruppe.id, c.faktura) :: Nil
            case None =>
              neueGruppe
          }
        } else neueGruppe

      case InkassoFallZuordnen(_, fallId, inkassoFall) =>
        InkassoFallZugeordnet(fallId, inkassoFall)
    }

    def applyEvent = {
      case FakturaFallErstellt(gruppeId, _, register, steuerjahr, _) =>
        copy(fälle = fälle + (gruppeId -> FakturaFall(gruppeId, register, steuerjahr, None, Seq
          .empty)))
      case FakturaHinzugefügt(zu, faktura) =>
        val gruppe = fälle.getOrElse(zu,
          throw new IllegalStateException(s"Fall $zu existiert nicht"))
        copy(fälle = fälle + (zu -> gruppe.add(faktura)))
      case InkassoFallZugeordnet(fallId, inkassoFall) =>
        fälle.get(fallId).map { fall =>
          val fs = fälle + (fallId -> fall.inkassoFallZuordnen(inkassoFall))
          copy(fälle = fs)
        }.getOrElse(throw new IllegalStateException(s"Fall $fallId existiert nicht"))
    }
  }

  case class FakturaFall(id: FakturaFall.Id, register: Register, steuerjahr: Jahr,
    inkassoFall: Option[InkassoFall.Id],
    fakturen: Seq[Faktura.Id])
    extends Entity[FakturaFall.Id] {
    def add(faktura: Faktura.Id) = copy(fakturen = fakturen :+ faktura)
    def inkassoFallZuordnen(fall: InkassoFall.Id) = {
      require(inkassoFall.isEmpty, "schon zugeordnet")
      copy(inkassoFall = Some(fall))
    }
  }
  object FakturaFall extends TypedGuid {
    def create = generateId
  }

  protected def types = typeInfo
  def seed(id: Id) = Schuldner(id, Map.empty)
}