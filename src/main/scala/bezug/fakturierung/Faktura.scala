package bezug
package fakturierung

import ch.eventsourced.support.GuidAggregateType

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