package ch.eventsourced.infrastructure.akka

import ch.eventsourced.api.AggregateType
import ch.eventsourced.support.TypedGuid

/** An purchase order (used for tests). */
object Order extends AggregateType with TypedGuid {
  def name = "Order"

  sealed trait Event
  case class ItemAdded(item: String, amount: Money) extends Event
  case class OrderPlaced(items: List[(String, Money)], total: Money, billingRef: String) extends Event
  case object OrderCompleted extends Event
  case object OrderCanceled extends Event

  sealed trait Command {
    def order: Id
  }
  case class StartOrder(order: Id = generateId) extends Command
  case class AddItem(order: Id, item: String, cost: Money) extends Command
  case class PlaceOrder(order: Id) extends Command
  case class CancelOrder(order: Id) extends Command
  case class CompleteOrder(order: Id) extends Command
  def aggregateIdForCommand(command: Command) = Some(command.order)

  sealed trait Error
  case object OrderIsOpen extends Error
  case object OrderAlreadyComplete extends Error
  case object OrderAlreadyPlaced extends Error
  case object OrderWasCancelled extends Error

  type Money = Int

  type Root = Order
  sealed trait Order extends RootBase
  case class OpenOrder(id: Id, items: List[(String, Money)]) extends Order {
    def execute(c: Command) = c match {
      case AddItem(`id`, item, amount) => ItemAdded(item, amount)
      case PlaceOrder(`id`) => OrderPlaced(items, items.map(_._2).sum, s"Your order of ${items.size} items")
      case CancelOrder(`id`) => OrderCanceled
      case _ => OrderIsOpen
    }
    def applyEvent = {
      case ItemAdded(item, amount) => copy(items = items :+ (item , amount))
      case OrderPlaced(items, amount, ref) => PlacedOrder(id, items.map(_._1), amount, ref)
      case OrderCanceled => FinishedOrder(id, OrderWasCancelled)
    }
  }
  case class PlacedOrder(id: Id, items: List[String], total: Money, billingRef: String) extends Order {
    def execute(c: Command) = c match {
      case CompleteOrder(`id`) => OrderCompleted
      case CancelOrder(`id`) => OrderCanceled
      case _ => OrderAlreadyPlaced
    }
    def applyEvent = {
      case OrderCompleted => FinishedOrder(id, OrderAlreadyComplete)
      case OrderCanceled => FinishedOrder(id, OrderWasCancelled)
    }
  }
  case class FinishedOrder(id: Id, rejectReason: Error) extends Order {
    def execute(c: Command) = rejectReason
    def applyEvent = PartialFunction.empty
  }
  def seed(id: Id) = OpenOrder(id, Nil)

  protected def types = typeInfo
}
