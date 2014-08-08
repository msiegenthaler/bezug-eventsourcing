package object bezug {

  case class Betrag(value: BigDecimal)
  class Datum
  class Jahr

  case class Kategorie(name: String)

  case class Person(id: String)
  case class Institution(id: String)

}
