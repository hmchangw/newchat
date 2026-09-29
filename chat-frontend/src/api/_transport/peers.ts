export type Peer = { siteId: string; natsUrl: string }

/**
 * Order the failover candidates: every site except home, uniformly shuffled.
 *
 * Uniform rather than nearest-first on purpose. A site's users are
 * geographically together — that is why they share a home site — so ranking by
 * latency would make them all pick the same peer and flatten it, which is the
 * hotspot spreading exists to prevent.
 *
 * Home is excluded because home is the site that is down. The caller retries it
 * separately on its own backoff.
 *
 * Fisher-Yates over a copy; the input is never mutated.
 */
export function shufflePeers(sites: Peer[], homeSiteId: string): Peer[] {
  const out = sites.filter((s) => s.siteId !== homeSiteId)
  for (let i = out.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1))
    ;[out[i], out[j]] = [out[j], out[i]]
  }
  return out
}
