/**
 * Failover mode: this client is connected to a peer site because its own
 * site's NATS is unreachable.
 *
 * While it is on, two things change:
 *
 *  - Every room subscription uses the GLOBAL subject root regardless of the
 *    room's `crossSite` flag. The local root (`chat.local.room.…`) is filtered
 *    from gateway interest advertisement, so a client sitting on a peer cluster
 *    would never receive a same-site room's events — silently, and for the
 *    rooms that make up most of its traffic.
 *  - Message sends carry a `failover` token, routing them to the standby
 *    ingress stream on the buddy cluster. The live stream lives on the cluster
 *    that is down, so a send published there would go nowhere.
 *
 * The server half flips on the same condition: during an outage it publishes
 * those events to the global root and consumes the standby ingress. Both sides
 * must agree, and neither is useful alone.
 *
 * Module-level rather than a parameter threaded through every caller: a single
 * missed call site is an invisible bug, and there are dozens of them.
 */
let failoverMode = false

export function setFailoverMode(on: boolean): void {
  failoverMode = on
}

export function isFailoverMode(): boolean {
  return failoverMode
}
