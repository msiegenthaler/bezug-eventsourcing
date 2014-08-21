package bezug
package fakturierung

import ch.eventsourced.api.AggregateType
import ch.eventsourced.support.TypedGuid

object Faktura extends AggregateType with TypedGuid {
  def name = "Faktura"

  case class Grundlagen(fremdreferenz: String, versandInnerhalb: DatumBereich, art: String)

  sealed trait Command extends Bezug.Command {
    def faktura: Id
  }
  case class FakturaBeauftragen(schuldner: Person, register: Register, steuerjahr: Jahr,
    valuta: Datum, grundlagen: Grundlagen, positionen: Traversable[Position],
    faktura: Id = generateId)
    extends Command

  sealed trait Event extends Bezug.Event
  case class FakturaKopfErstellt(kopf: Kopf) extends Event
  case class FakturaPositionHinzugefügt(position: Position) extends Event
  case class FakturaVervollständigt(kopf: Kopf, positionen: Seq[Position]) extends Event

  case class Kopf(person: Person, register: Register, steuerjahr: Jahr,
    valuta: Datum, grundlagen: Grundlagen)
  case class Position(institution: Institution, betragskategorie: KatId, betrag: Betrag)

  sealed trait Error
  protected def types = typeInfo

  type Root = FakturaLike
  sealed trait FakturaLike extends RootBase
  case class EmptyFaktura(id: Id) extends FakturaLike {
    def execute(c: Command) = c match {
      case f: FakturaBeauftragen =>
        val kopf = Kopf(f.schuldner, f.register, f.steuerjahr, f.valuta, f.grundlagen)
        val positionen = f.positionen.map { p => Position(p.institution, p.betragskategorie, p.betrag)}.toSeq
        FakturaKopfErstellt(kopf) +:
          positionen.map(FakturaPositionHinzugefügt(_)) :+
          FakturaVervollständigt(kopf, positionen)
    }
    def applyEvent = {
      case FakturaKopfErstellt(kopf) => Faktura(id, kopf, Nil)
    }
  }

  case class Faktura(id: Id, kopf: Kopf, positionen: Seq[Position]) extends FakturaLike {
    def execute(c: Command) = ???
    def applyEvent = {
      case FakturaPositionHinzugefügt(pos) => copy(positionen = positionen :+ pos)
      case _: FakturaVervollständigt => this
    }
  }

  def aggregateIdForCommand(command: Command) = Some(command.faktura)
  def seed(id: Id) = EmptyFaktura(id)
}