package es.infrastructure.akka

/**
 * Responsible to handle events emitted by aggregates.
 * - Forward to pubSub infrastructure
 * - Start process manager if it is an initiation event
 * - Forward to active processes
 */
class EventPublisher {

}
