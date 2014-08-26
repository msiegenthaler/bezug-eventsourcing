package bezug
package debitor

import bezug.debitor.Buchung.{Gebucht, KontoMitVerwendung, Buchungskonto}
import ch.eventsourced.api.AggregateType
import ch.eventsourced.support.TypedGuid

object InkassoFall extends AggregateType with TypedGuid {
  def name = "Inkassofall"

  case class Betragsposition(institution: Institution, betragskategorie: KatId, betrag: Betrag)

  sealed trait Command extends Bezug.Command {
    def inkassoFall: Id
  }
  case class Eröffnen(debitor: Debitor.Id, register: Register, steuerjahr: Jahr, inkassoFall: Id = generateId) extends Command
  case class GeschuldeterBetragSetzen(inkassoFall: Id, positionen: Seq[Betragsposition]) extends Command {
    require(positionen.nonEmpty, "Positionen sind leer")
    def betrag = positionen.map(_.betrag).reduce(_ + _)
  }
  //TODO
  //  case class ZusätzlichGeschuldeterBetragHinzufügen(inkassoFall: Id, betrag: Betrag) extends Command
  //  case class Minderung

  case class BuchungRegistrieren(inkassoFall: Id, buchung: Buchung.Id, gebucht: Gebucht) extends Command
  def aggregateIdForCommand(command: Command) = Some(command.inkassoFall)

  sealed trait Event extends Bezug.Event
  case class Eröffnet(debitor: Debitor.Id, register: Register, steuerjahr: Jahr) extends Event
  case class SaldoAktualisiert(buchung: Buchung.Id, aufgrundEvent: Gebucht, saldo: Betrag, saldoVorher: Betrag) extends Event


  sealed trait Error extends Bezug.Error
  case object FalscheBuchung extends Error

  //TODO Attribute
  //Personentyp (NP,JP,Virtu)
  //Inkassostand (Mahnung, Betreibung etc.)


  sealed trait Root extends RootBase
  def seed(id: Id) = EmptyInkassoFall(id)
  case class EmptyInkassoFall(id: Id) extends Root {
    def execute(c: Command) = c match {
      case Eröffnen(debitor, register, steuerjahr, fall) =>
        Eröffnet(debitor, register, steuerjahr)
    }
    def applyEvent = {
      case Eröffnet(debitor, _, _) => InkassoFall(id, debitor, Map.empty, Betrag(0))
    }
  }
  case class InkassoFall(id: Id, debitor: Debitor.Id, buchungen: Map[Buchung.Id, Gebucht], saldo: Betrag) extends Root {
    def execute(c: Command) = c match {
      case GeschuldeterBetragSetzen(`id`, positionen) =>
        //        buchungen.values.filter(_.urbeleg.art.isAbschreibung).map(gebucht =>
        //        )
        // TODO alle Abschreibungen killen
        // TODO alle Erlasse killen
        // TODO Delta des geschuldeten Betrags berechnen und buchen
        // TODO Zinsen berechnen (VGZ und VZZ)
        // TODO Restanz buchen (wenn a la < 20 CHF)

        // TODO Ereignis in dem steht: neuer Saldo (exkl. Marchzinse), Marchzinse
        // TODO a la Updated() und dann ein PM der die Abschreibungen und Erlasse storniert und die Restanz bucht
        ???

      case BuchungRegistrieren(`id`, buchung, gebucht) =>
        if (gebucht.soll.inkassofälle.contains(id) || gebucht.haben.inkassofälle.contains(id)) {
          if (!buchungen.contains(buchung)) {
            val sollBetrag = gebucht.soll.betragFür(id)
            val habenBetrag = gebucht.haben.betragFür(id)
            SaldoAktualisiert(buchung, gebucht, saldo + sollBetrag + habenBetrag, saldo)
          } else Seq.empty
        } else FalscheBuchung
    }
    def applyEvent = {
      case SaldoAktualisiert(buchungId, gebucht, saldo, _) => copy(saldo = saldo, buchungen = buchungen + (buchungId -> gebucht))
    }
  }

  protected def types = typeInfo

}
