package bezug

import bezug.fakturierung.Register

package object debitor {
  case class InkassoKey(person: Person, register: Register, steuerjahr: Jahr, laufnummer: Int)
  trait BelegartUrbeleg

  sealed trait SollHaben
  case class Soll(betrag: Betrag) extends SollHaben
  case class Haben(betrag: Betrag) extends SollHaben

  case class Verwendung(kategorie: KatId, institution: Institution)


  sealed trait Konto
  sealed trait DetailKonto {
    def konto: Konto
  }
  case class Debitorenkonto(debitor: Debitor.Id) extends Konto
  //TODO debitor hier ist irgendwie doof (Inkonsistenzen)
  case class Inkassofallkonto(debitor: Debitor.Id, inkassoFall: InkassoFall.Id) extends DetailKonto {
    def konto = Debitorenkonto(debitor)
  }
  case class Verwendungskonto(verwendung: Verwendung) extends DetailKonto with Konto {
    def konto = this
  }
}
