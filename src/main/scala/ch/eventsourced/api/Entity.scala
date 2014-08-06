package ch.eventsourced.api

trait Entity[Id] {
  def id: Id
}
