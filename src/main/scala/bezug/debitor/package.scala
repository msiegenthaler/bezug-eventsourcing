package bezug

import bezug.fakturierung.Register

package object debitor {
  case class InkassoKey(person: Person, register: Register, steuerjahr: Jahr, laufnummer: Int)
}
