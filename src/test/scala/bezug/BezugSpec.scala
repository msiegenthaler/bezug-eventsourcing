package bezug

import scala.concurrent.duration._
import akka.actor.ActorSystem
import akka.testkit.{TestProbe, ImplicitSender, TestKit}
import bezug.fakturierung.Faktura
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import ch.eventsourced.infrastructure.akka.AkkaInfrastructure

class BezugSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with Matchers with BeforeAndAfterAll {
  def this() = this(ActorSystem(getClass.getName.filter(_.isLetterOrDigit)))

  implicit val timeout = 1.second

  "Bezug" must {
    "be startable" in {
      val pubSub = TestProbe()
      val infrastructure = new AkkaInfrastructure(system)
      val backend = infrastructure.startContext(BezugContext, pubSub.ref)
      backend.shutdown()
    }
  }
}