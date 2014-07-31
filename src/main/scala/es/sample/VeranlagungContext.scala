package es.sample

import es.api.{Infrastructure, BoundedContextBackendType}

final class VeranlagungContext private(backend: VeranlagungContext.Backend) {
  //TODO use it
}

object VeranlagungContext {
  def start(infrastructure: Infrastructure): VeranlagungContext = {
    val backend = infrastructure.startContext(BackendType)
    new VeranlagungContext(backend)
  }

  private type Backend = BackendType.Backend
  private object BackendType extends BoundedContextBackendType {
    type Command = Any
    type Event = Any

    def aggregates = veranlagung :: Nil
    def readModels = Nil
    def processManagers = Nil
  }
}
