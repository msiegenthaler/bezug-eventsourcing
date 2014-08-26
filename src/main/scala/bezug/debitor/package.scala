package bezug

package object debitor {
  case class InkassoKey(person: Person, register: Register, steuerjahr: Jahr, laufnummer: Int)

  sealed trait BelegartUrbeleg {
    def isAbschreibung: Boolean
    def isErlass: Boolean
    def isSchuld: Boolean
  }
  object BelegartUrbeleg {
    case object Faktura extends BelegartUrbeleg {
      def isAbschreibung = false
      def isErlass = false
      def isSchuld = true
    }
  }

  sealed trait Konto
  case class Debitorkonto(debitor: Debitor.Id) extends Konto
  object Debitorkonto {
    def is(konto: Konto) = konto match {
      case _: Debitorkonto => true
      case _ => false
    }
  }
  case class FinanzinstitutKonto(name: String) extends Konto
  case object Ertragkonto extends Konto
  case object Aufwandkonto extends Konto
}
