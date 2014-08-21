package bezug

import bezug.debitor.{InkassoFall, Debitor, Buchung}
import bezug.fakturierung.{InkassoFallEröffnenProcess, FakturaErstellenProcess, Schuldner, Faktura}
import ch.eventsourced.api.BoundedContextBackendType

object BezugContext extends BoundedContextBackendType {
  def name = "bezug"

  type Command = Bezug.Command
  type Event = Bezug.Event
  type Error = Any
  def unknownCommand = "don't know"

  def aggregates = {
    Aggregates(Faktura, Schuldner) ++
      Aggregates(Buchung, Debitor, InkassoFall)
  }

  def processManagers = {
    ProcessManagers(FakturaErstellenProcess, fakturierung.InkassoFallEröffnenProcess) ++
      ProcessManagers(debitor.InkassoFallEröffnenProcess, debitor.SaldoAktualisierenProcess)
  }

  def readModels = Set()
}

object Bezug {
  trait Command
  trait Event
  trait Error
}