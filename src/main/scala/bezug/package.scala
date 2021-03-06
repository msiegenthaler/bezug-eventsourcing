import ch.eventsourced.api.StringSerialize
package object bezug {
  case class Betrag(value: BigDecimal) {
    def +(other: Betrag) = Betrag(value + other.value)
    def *(faktor: BigDecimal) = Betrag(value * faktor)
  }

  case class KatId(name: String)

  case class Person(id: Person.Id)
  object Person {
    case class Id(id: String) {
      override def toString = s"Person-$id"
    }
    object Id {
      implicit def stringSerialize: StringSerialize[Id] = new StringSerialize[Id] {
        def serialize(value: Id) = value.id
        def parse(serialized: String) = Some(Id(serialized))
      }
    }
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
