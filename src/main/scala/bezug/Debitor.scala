package bezug

import bezug.Faktura.FakturaKopfErstellt
import ch.eventsourced.api.Entity
import ch.eventsourced.support.{Guid, GuidProcessManagerType, GuidAggregateType}

case class InkassoKey(person: Person, register: Register, steuerjahr: Jahr, laufnummer: Int)

