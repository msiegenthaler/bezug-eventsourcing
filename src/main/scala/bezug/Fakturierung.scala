package bezug

import bezug.Faktura.FakturaKopfErstellt
import ch.eventsourced.api.Entity
import ch.eventsourced.api.ProcessManagerType.BaseManager
import ch.eventsourced.api.ProcessManagerType.Completed
import ch.eventsourced.support.GuidProcessManagerType.Id
import ch.eventsourced.support.{GuidProcessManagerType, Guid, GuidAggregateType}


object Faktura extends GuidAggregateType {
  def name = "Faktura"

  sealed trait Command {
    def faktura: Id
  }
  case class FakturaBeauftragen(schuldner: Person, register: Register, steuerjahr: Jahr,
    valuta: Datum, fremdreferenz: String, positionen: Traversable[FakturaBeauftragen.Position],
    faktura: Id = generateId)
    extends Command
  object FakturaBeauftragen {
    case class Position(institution: Institution, kategorie: Kategorie, betrag: Betrag)
  }

  sealed trait Event
  case class FakturaKopfErstellt(kopf: FakturaKopf) extends Event
  case class FakturaPositionHinzugefügt(position: FakturaPosition) extends Event

  case class FakturaKopf(person: Person, register: Register, steuerjahr: Jahr,
    valuta: Datum, fremdreferenz: String)
  case class FakturaPosition(institution: Institution, kategorie: Kategorie, betrag: Betrag)

  sealed trait Error
  protected def types = typeInfo

  type Root = FakturaLike
  sealed trait FakturaLike extends RootBase
  case class EmptyFaktura(id: Id) extends FakturaLike {
    def execute(c: Command) = c match {
      case f: FakturaBeauftragen =>
        val kopf = FakturaKopf(f.schuldner, f.register, f.steuerjahr, f.valuta, f.fremdreferenz)
        val positionen = f.positionen.map { p =>
          val pos = FakturaPosition(p.institution, p.kategorie, p.betrag)
          FakturaPositionHinzugefügt(pos)
        }.toSeq
        FakturaKopfErstellt(kopf) +: positionen
    }
    def applyEvent = {
      case FakturaKopfErstellt(kopf) => Faktura(id, kopf, Nil)
    }
  }

  case class Faktura(id: Id, kopf: FakturaKopf, positionen: Seq[FakturaPosition]) extends FakturaLike {
    def execute(c: Command) = ???
    def applyEvent = {
      case FakturaPositionHinzugefügt(pos) => copy(positionen = positionen :+ pos)
    }
  }

  def aggregateIdForCommand(command: Command) = Some(command.faktura)
  def seed(id: Id) = EmptyFaktura(id)
}


object Schuldner extends GuidAggregateType {
  def name = "Schuldner"

  sealed trait Command {
    schuldner: Id
  }
  case class FakturaHinzufügen(faktura: Faktura.Id, person: Person, register: Register, steuerjahr: Jahr) extends Command {
    def schuldner = Id(person.id)
  }
  def aggregateIdForCommand(command: Command) = command.schuldner

  sealed trait Event
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


object Xxx extends GuidProcessManagerType {
  def name = ???

  type Command = Schuldner.Command


  def triggeredBy = Set(Faktura)
  def initiate = {
    case Faktura.EventData(id, _, FakturaKopfErstellt(kopf)) =>
      Id(id.guid)
  }

  def seed(id: Id) = ???

  case class Manager(id: Id) extends BaseManager {
    def handle = {
      case Faktura.EventData(id, _, FakturaKopfErstellt(kopf)) =>
        Completed() + Schuldner.FakturaHinzufügen(id, kopf.person, kopf.register, kopf.steuerjahr)
    }
  }

  type Error = this.type
}


case class Betrag(value: BigDecimal)
case class Kategorie(name: String)
class Institution
class Person
class Jahr
class Datum
sealed trait Register
object Register {
  object NP extends Register
}