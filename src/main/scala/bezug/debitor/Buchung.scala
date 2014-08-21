package bezug.debitor

import bezug._
import ch.eventsourced.api.AggregateType
import ch.eventsourced.support.TypedGuid

object Buchung extends AggregateType with TypedGuid {
  def name = "Buchung"

  case class Urbeleg(art: BelegartUrbeleg)
  case class Position(inkassofall: InkassoFall.Id, betragskategorie: KatId, institution: Institution, betrag: Betrag)

  sealed trait Command extends Bezug.Command {
    def buchung: Id
  }
  case class Buchen(valuta: Datum, urbeleg: Urbeleg,
    soll: Konto, haben: Konto,
    positionen: Seq[Position],
    buchung: Id = generateId)
    extends Command {
    require(Debitorkonto.is(soll) || Debitorkonto.is(haben), "Soll oder Haben muss Debitorkonto sein")
    require(positionen.nonEmpty, "Mind. eine Position")
  }
  def aggregateIdForCommand(command: Command) = Some(command.buchung)


  sealed trait Event extends Bezug.Event
  case class Gebucht(zeitpunkt: Zeitpunkt, valuta: Datum, urbeleg: Urbeleg, soll: Konto, haben: Konto, positionen: Seq[Position])
    extends Event

  sealed trait Error extends Bezug.Error
  case object IstAbgeschlossen extends Error


  sealed trait Root extends RootBase
  def seed(id: Buchung.Id) = EmptyBuchung(id)
  case class EmptyBuchung(id: Id) extends Root {
    def execute(c: Command) = c match {
      case Buchen(valuta, urbeleg, soll, haben, positionen, _) =>
        Gebucht(Zeitpunkt.now, valuta, urbeleg, soll, haben, positionen)
    }
    def applyEvent = {
      case e: Gebucht => Kopf(id, e.zeitpunkt, e.valuta, e.urbeleg, e.soll, e.haben, e.positionen)
    }
  }
  case class Kopf(id: Id, zeitpunkt: Zeitpunkt, valuta: Datum, urbeleg: Urbeleg,
    soll: Konto, haben: Konto, positionen: Seq[Position]) extends Root {
    def execute(c: Command) = IstAbgeschlossen
    def applyEvent = PartialFunction.empty
  }

  protected def types = typeInfo
}
