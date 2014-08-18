package ch.eventsourced.infrastructure.akka

import java.net.URLEncoder
import akka.util.Timeout
import ch.eventsourced.infrastructure.akka.AggregateActor.Execute
import scala.concurrent.duration._
import akka.actor.{ActorSystem, ActorRef}
import akka.pattern.ask
import ch.eventsourced.api._
import scalaz._
import Scalaz._
import ContextActor._

/** BoundedContext infrastructure using clustered akka actors to execute the commands and react to the events. */
class AkkaInfrastructure(system: ActorSystem) extends Infrastructure {
  def startContext(definition: BoundedContextBackendType, pubSub: ActorRef): BoundedContextBackend[definition.Command, definition.Event, definition.Error] = {
    val name = URLEncoder.encode("context-" + definition.name, "UTF-8")
    val actor = system.actorOf(ContextActor.props(definition, pubSub, DefaultConfig), name)
    new AkkaBoundedContext[definition.Command, definition.Event, definition.Error](actor)
  }

  private class AkkaBoundedContext[Command, Event, Error](actor: ActorRef) extends BoundedContextBackend[Command, Event, Error] {
    implicit def exec = system.dispatcher

    def execute(c: Command)(implicit t: FiniteDuration) = {
      implicit def timeout = Timeout(t)
      val f = actor ? Execute(c, (), (e: Error) => e)
      f.map {
        case () => ().success
        case error => error.asInstanceOf[Error].fail
      }
    }

    def shutdown()(implicit t: FiniteDuration) = {
      implicit def timeout = Timeout(t)
      (actor ? Shutdown("ok")).map(_ => ())
    }
  }
}

