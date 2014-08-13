package object bezug {

  case class Betrag(value: BigDecimal) {
    def +(other: Betrag) = Betrag(value + other.value)
  }
  class Datum
  case class DatumBereich(von: Datum, bis: Datum)
  class Jahr
  class Zeitpunkt
  object Zeitpunkt {
    def now: Zeitpunkt = ???
  }

  case class KatId(name: String)

  case class Person(id: Person.Id)
  object Person {
    case class Id(id: String)
  }
  case class Institution(id: String)

  sealed trait Register {
    def periodisch: Boolean
  }
  object Register {
    object NP extends Register {
      def periodisch = true
    }
  }

}
