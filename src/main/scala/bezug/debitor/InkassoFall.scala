package bezug
package debitor

import bezug.debitor.Buchung.{KontoMitVerwendung, Buchungskonto}
import ch.eventsourced.api.AggregateType
import ch.eventsourced.support.TypedGuid

object InkassoFall extends AggregateType with TypedGuid {
  def name = "Inkassofall"

  sealed trait Command extends Bezug.Command {
    def inkassoFall: Id
  }
  case class Eröffnen(debitor: Debitor.Id, register: Register, steuerjahr: Jahr, inkassoFall: Id = generateId) extends Command
  case class BuchungRegistrieren(inkassoFall: Id, buchung: Buchung.Id, valuta: Datum, soll: Buchungskonto, haben: Buchungskonto) extends Command
  def aggregateIdForCommand(command: Command) = Some(command.inkassoFall)

  sealed trait Event extends Bezug.Event
  case class Eröffnet(debitor: Debitor.Id, register: Register, steuerjahr: Jahr) extends Event
  case class SaldoAktualisiert(aufgrund: Buchung.Id, saldo: Betrag, saldoVorher: Betrag) extends Event


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
      case Eröffnet(debitor, _, _) => InkassoFall(id, debitor, Nil, Betrag(0))
    }
  }
  case class InkassoFall(id: Id, debitor: Debitor.Id, buchungen: Seq[Buchung.Id], saldo: Betrag) extends Root {
    def execute(c: Command) = c match {
      case BuchungRegistrieren(`id`, buchung, _, soll, haben) =>
        if (soll.inkassofälle.contains(id) || haben.inkassofälle.contains(id)) {
          val sollBetrag = soll.betragFür(id)
          val habenBetrag = haben.betragFür(id)
          SaldoAktualisiert(buchung, saldo + sollBetrag + habenBetrag, saldo)
        } else FalscheBuchung
    }
    def applyEvent = {
      case SaldoAktualisiert(buchung, saldo, _) => copy(saldo = saldo, buchungen = buchungen :+ buchung)
    }
  }

  protected def types = typeInfo

}
