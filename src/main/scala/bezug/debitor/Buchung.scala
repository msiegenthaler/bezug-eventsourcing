package bezug.debitor

import bezug._
import ch.eventsourced.support.GuidAggregateType

object Buchung extends GuidAggregateType {
  def name = "Buchung"

  case class Urbeleg(art: BelegartUrbeleg)

  case class Position(soll: Konto, haben: Konto, betrag: Betrag)

  case class DetailPosition(soll: DetailKonto, haben: DetailKonto, betrag: Betrag)
  object DetailPosition {
    def summarize(pos: Traversable[DetailPosition]) = {
      pos.map { dp => Position(dp.soll.konto, dp.haben.konto, dp.betrag)}.
        groupBy(p => (p.soll, p.haben)).
        map {
        case ((soll, haben), positionen) =>
          Position(soll, haben, positionen.map(_.betrag).reduce(_ + _))
      }
    }
  }


  sealed trait Command {
    def buchung: Id
  }
  case class Buchen(valuta: Datum, urbeleg: Urbeleg, positionen: Seq[DetailPosition], buchung: Id = generateId)
    extends Command
  def aggregateIdForCommand(command: Command) = Some(command.buchung)


  sealed trait Event
  case class Gebucht(zeitpunkt: Zeitpunkt, valuta: Datum, urbeleg: Urbeleg, positionen: Seq[DetailPosition])
    extends Event
  sealed trait Error


  sealed trait Root extends RootBase
  def seed(id: Buchung.Id) = EmptyBuchung(id)
  case class EmptyBuchung(id: Id) extends Root {
    def execute(c: Command) = c match {
      case Buchen(valuta, urbeleg, positionen, _) =>
        Gebucht(Zeitpunkt.now, valuta, urbeleg, positionen)
    }
    def applyEvent = {
      case e: Gebucht => Kopf(id, e.zeitpunkt, e.valuta, e.urbeleg, e.positionen)
    }
  }
  case class Kopf(id: Id, zeitpunkt: Zeitpunkt, valuta: Datum, urbeleg: Urbeleg, detailPositionen: Seq[DetailPosition]) extends Root {
    def positionen = DetailPosition.summarize(detailPositionen)

    def execute(c: Command) = ???
    def applyEvent = ???
  }

  protected def types = typeInfo
}
