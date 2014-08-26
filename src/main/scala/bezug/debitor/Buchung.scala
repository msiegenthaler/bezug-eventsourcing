package bezug.debitor

import bezug._
import ch.eventsourced.api.AggregateType
import ch.eventsourced.support.TypedGuid

object Buchung extends AggregateType with TypedGuid {
  def name = "Buchung"

  case class Urbeleg(art: BelegartUrbeleg)
  case class Position(inkassofall: InkassoFall.Id, betragskategorie: KatId, institution: Institution, betrag: Betrag)

  sealed trait Buchungskonto {
    def konto: Konto
    def inkassofälle: Set[InkassoFall.Id]
    def betragFür(inkassoFall: InkassoFall.Id): Betrag
  }
  case class KontoMitVerwendung(konto: Debitorkonto, verwendungspositionen: Seq[Position]) extends Buchungskonto {
    require(verwendungspositionen.nonEmpty, "Debitorenkonto muss Verwendungspositionen haben")
    def summe = verwendungspositionen.map(_.betrag).reduce(_ + _)
    def inkassofälle = verwendungspositionen.map(_.inkassofall).toSet
    def betragFür(inkassoFall: InkassoFall.Id) =
      verwendungspositionen.filter(_.inkassofall == inkassoFall).map(_.betrag).foldLeft(Betrag(0))(_ + _)
  }
  case class KontoOhneVerwendung(konto: Konto) extends Buchungskonto {
    require(!Debitorkonto.is(konto), "Debitorenkonto muss Verwendungspositionen haben")
    def inkassofälle = Set.empty
    def betragFür(inkassoFall: InkassoFall.Id) = Betrag(0)
  }

  sealed trait Command extends Bezug.Command {
    def buchung: Id
  }
  case class Buchen(valuta: Datum, urbeleg: Urbeleg,
    soll: Buchungskonto, haben: Buchungskonto,
    buchung: Id = generateId)
    extends Command {
    require(Debitorkonto.is(soll.konto) || Debitorkonto.is(haben.konto), "Soll oder Haben muss Debitorkonto sein")
    (soll, haben) match {
      case (soll: KontoMitVerwendung, haben: KontoMitVerwendung) =>
        require(soll.summe == haben.summe, "Umbuchung muss Soll=Haben sein")
      case _ => ()
    }
  }
  case class Stornieren(buchung: Id = generateId) extends Command
  def aggregateIdForCommand(command: Command) = Some(command.buchung)


  sealed trait Event extends Bezug.Event
  case class Gebucht(zeitpunkt: Zeitpunkt, valuta: Datum, urbeleg: Urbeleg, soll: Buchungskonto, haben: Buchungskonto)
    extends Event
  case class Storniert(zeitpunkt: Zeitpunkt, mit: Buchung.Id) extends Event

  sealed trait Error extends Bezug.Error
  case object IstAbgeschlossen extends Error


  sealed trait Root extends RootBase
  def seed(id: Buchung.Id) = EmptyBuchung(id)
  case class EmptyBuchung(id: Id) extends Root {
    def execute(c: Command) = c match {
      case Buchen(valuta, urbeleg, soll, haben, _) =>
        Gebucht(Zeitpunkt.now, valuta, urbeleg, soll, haben)
    }
    def applyEvent = {
      case e: Gebucht => Kopf(id, e.zeitpunkt, e.valuta, e.urbeleg, e.soll, e.haben, None)
    }
  }
  case class Kopf(id: Id, zeitpunkt: Zeitpunkt, valuta: Datum, urbeleg: Urbeleg,
    soll: Buchungskonto, haben: Buchungskonto, storniertMit: Option[Buchung.Id]) extends Root {
    def execute(c: Command) = c match {
      case _: Buchen =>
        IstAbgeschlossen
      case Stornieren(`id`) if storniertMit.isEmpty =>
        Storniert(Zeitpunkt.now, generateId)
      case _: Storniert => ()
    }
    def applyEvent = {
      case Storniert(_, mit) => copy(storniertMit = Some(mit))
    }
  }

  protected def types = typeInfo
}
