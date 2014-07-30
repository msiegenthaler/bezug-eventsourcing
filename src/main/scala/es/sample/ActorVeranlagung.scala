package es.sample

import java.util.UUID
import es.impl.actor.AggregateBinding

object VeranlagungActorBinding extends AggregateBinding[veranlagung.type] {
  val aggregateType = veranlagung
  def name = "Veranlagung"
  def commandToId(cmd: veranlagung.Command) = cmd.veranlagung.toString
  def seed(idString: String) = {
    val id = parseId(idString)
    veranlagung.seed(id)
  }
  private def parseId(id: String) = UUID.fromString(id)
}