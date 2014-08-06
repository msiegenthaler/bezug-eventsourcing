package ch.eventsourced.infrastructure.akka

class ProcessManagerActorSpec extends AbstractSpec {

  "processManager actor" must {
    "start the process on initiation event" in ???

    "send the start event to the process manager" in ???

    "auto-setup a subscription to the source of the initial event" in ???

    "support subscribing to another aggregates" in ???

    "support unsubscribing from an aggregate" in ???

    "newly set up subscription must be retroactive" in ???

    "initial subscription must not be retroactive" in ???

    "next event from the initial subscription must be received" in ???
  }

}
