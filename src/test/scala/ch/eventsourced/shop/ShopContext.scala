package ch.eventsourced.shop

import ch.eventsourced.api.BoundedContextBackendType

object ShopContext extends BoundedContextBackendType {
  def name = "shop"

  type Command = OrderPayment.Command
  def unknownCommand = UnknownCommand
  case object UnknownCommand extends OrderPayment.Error

  type Event = OrderPayment.Event

  type Error = OrderPayment.Error


  def aggregates = Aggregates(Order, Payment)
  def processManagers = ProcessManagers(OrderProcess)
  def readModels = Nil
}

object OrderPayment {
  trait Command
  trait Event
  trait Error
}