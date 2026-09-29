# NATS Failover — Client and Portal Peer List Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A user whose home site's NATS is down reconnects automatically to a randomly chosen surviving peer, keeps chatting, and returns home on its own when home recovers.

**Architecture:** `portal-service` exposes the peer list it already holds. The client shuffles it, walks it, sticks to the first peer that accepts, and flips a transport-level failover flag that redirects message publishes to the `.failover.` subject and forces every room subscription onto the global root.

**Tech Stack:** Go 1.25 + Gin (portal), React 19 + `nats.ws` + TypeScript (client), Vitest.

**Design spec:** `docs/superpowers/specs/2026-08-15-nats-site-failover-design.md` §F, and §E for the subscription half.

**Depends on Plans 1-3.** Plan 3 in particular: the server forces global routing on the failover lane, and this plan is the matching client half. **Neither works alone** — server publishing global while the client subscribes local is exactly as broken as the reverse.

## Global Constraints

- Go 1.25 for portal; no new third-party Go dependencies.
- Frontend: React 19, TypeScript for `src/api/**`, Vitest for tests. No new npm dependencies.
- All Go commands via `make` targets.
- TDD is mandatory: failing test first, confirm it fails, then implement.
- Go: `log/slog` structured logging, `errhttp.Write` for Gin error responses.
- Any change to a client-facing contract updates `docs/client-api.md` **and** its derived views (`docs/client-api/request-reply.md`, `docs/client-api/events.md`) in the same PR.

## Out of scope

- **Region tags / capacity weights** in peer selection. The spec rejects
  latency ranking outright and defers weighting until real distribution data
  exists. Uniform shuffle only.
- **Bot and admin sessions.** `portalLoginResponse` serves those; failover for
  automated clients is a separate question — they are not displaced users
  sitting at a keyboard. They keep today's behaviour.

---

### Task 1: Portal exposes the peer list

**Files:**
- Modify: `portal-service/handler.go:49-56` (`settingsResponse`), and its handler
- Test: `portal-service/handler_test.go`

**Interfaces:**
- Consumes: the existing `h.sites map[string]siteURL` parsed from `PORTAL_SITE_URLS`.
- Produces: a `sites` array on `GET /api/settings`, each entry `{siteId, natsUrl}`.

Portal holds the full registry today but every response exposes only the
caller's own site (`handler.go:189-220`). A displaced client therefore has no way
to learn any peer's URL. `/api/settings` is the right home: it is already the
deployment config, it is identical for every caller, and the client already
fetches it — so no new endpoint and no extra round trip.

**`baseUrl` is deliberately omitted.** A displaced client relocates only its
NATS connection; its HTTP calls still go to its home gateway, which is up.
Publishing every site's `baseUrl` would widen the disclosure for no use.

- [x] **Step 1: Write the failing test**

Add to `portal-service/handler_test.go`:

```go
func TestHandleSettings_IncludesPeerList(t *testing.T) {
	t.Setenv("PORTAL_SITE_URLS",
		`{"site-a":{"baseUrl":"https://a.com","natsUrl":"wss://nats.a.com"},`+
			`"site-b":{"baseUrl":"https://b.com","natsUrl":"wss://nats.b.com"}}`)

	rec := doSettingsRequest(t)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		APIVersion string `json:"apiVersion"`
		Sites      []struct {
			SiteID  string `json:"siteId"`
			NATSURL string `json:"natsUrl"`
		} `json:"sites"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	require.Len(t, resp.Sites, 2)
	byID := map[string]string{}
	for _, s := range resp.Sites {
		byID[s.SiteID] = s.NATSURL
	}
	assert.Equal(t, "wss://nats.a.com", byID["site-a"])
	assert.Equal(t, "wss://nats.b.com", byID["site-b"])
}

// Deterministic ordering keeps the response cacheable and diffable; the client
// shuffles it anyway, so server-side order carries no meaning.
func TestHandleSettings_PeerListIsSortedBySiteID(t *testing.T) {
	t.Setenv("PORTAL_SITE_URLS",
		`{"site-c":{"baseUrl":"https://c.com","natsUrl":"wss://nats.c.com"},`+
			`"site-a":{"baseUrl":"https://a.com","natsUrl":"wss://nats.a.com"}}`)

	rec := doSettingsRequest(t)
	var resp struct {
		Sites []struct {
			SiteID string `json:"siteId"`
		} `json:"sites"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Sites, 2)
	assert.Equal(t, "site-a", resp.Sites[0].SiteID)
	assert.Equal(t, "site-c", resp.Sites[1].SiteID)
}

// baseUrl must not leak: a displaced client relocates NATS only, and its HTTP
// calls still go to its own home gateway.
func TestHandleSettings_PeerListOmitsBaseURL(t *testing.T) {
	t.Setenv("PORTAL_SITE_URLS", `{"site-a":{"baseUrl":"https://a.com","natsUrl":"wss://nats.a.com"}}`)
	rec := doSettingsRequest(t)
	assert.NotContains(t, rec.Body.String(), "https://a.com")
}
```

Write `doSettingsRequest` using whatever handler-construction helper the file
already uses for the other endpoints.

- [x] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=portal-service`

Expected: FAIL — the response has no `sites` field.

- [x] **Step 3: Write minimal implementation**

In `portal-service/handler.go`, extend the response type:

```go
// sitePeer is one entry of the failover peer list. natsUrl only: a displaced
// client relocates its NATS connection, not its HTTP calls, which still go to
// its own home gateway.
type sitePeer struct {
	SiteID  string `json:"siteId"`
	NATSURL string `json:"natsUrl"`
}

// settingsResponse is the deployment config served to the frontend: the
// backend API generation, the OTEL base URL (client appends /trace, /log),
// whether bot-role accounts may log in through this client, and the federation
// peer list a client shuffles when its home site's NATS is unreachable.
type settingsResponse struct {
	APIVersion      string     `json:"apiVersion"`
	OTELBaseURL     string     `json:"otelBaseUrl"`
	BotLoginEnabled bool       `json:"botLoginEnabled"`
	Sites           []sitePeer `json:"sites"`
}
```

Build the slice once at startup, where `settingsResponse` is already assembled,
sorted by site ID so the payload is stable:

```go
// buildPeerList flattens the site registry into the client-facing peer list,
// sorted for a deterministic, cacheable response. The client shuffles it, so
// this order carries no selection meaning.
func buildPeerList(sites map[string]siteURL) []sitePeer {
	peers := make([]sitePeer, 0, len(sites))
	for id, s := range sites {
		peers = append(peers, sitePeer{SiteID: id, NATSURL: s.NATSURL})
	}
	sort.Slice(peers, func(i, j int) bool { return peers[i].SiteID < peers[j].SiteID })
	return peers
}
```

- [x] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=portal-service && make build SERVICE=portal-service`

Expected: PASS, builds clean.

- [x] **Step 5: Commit**

```bash
git add portal-service/handler.go portal-service/handler_test.go
git commit -m "feat(portal-service): expose the federation peer list on /api/settings"
```

---

### Task 2: Transport-level failover state and subject selection

**Files:**
- Create: `chat-frontend/src/api/_transport/failover.ts`
- Modify: `chat-frontend/src/api/_transport/subjects.ts:33-34`
- Test: `chat-frontend/src/api/_transport/failover.test.ts`, `subjects.test.ts`

**Interfaces:**
- Consumes: nothing.
- Produces: `setFailoverMode(on: boolean): void`, `isFailoverMode(): boolean`; `roomEvent(roomId, crossSite)` and the message-send subject builder become failover-aware.

A module-level flag consulted by the subject builders is the smallest change
that covers every call site at once. Threading a parameter through every caller
would touch dozens of components and invite one of them to be missed — and a
missed one is a silently silent room.

- [x] **Step 1: Write the failing test**

Create `chat-frontend/src/api/_transport/failover.test.ts`:

```ts
import { beforeEach, describe, expect, it } from 'vitest'
import { isFailoverMode, setFailoverMode } from './failover'
import { roomEvent } from './subjects'

describe('failover mode', () => {
  beforeEach(() => setFailoverMode(false))

  it('defaults to off', () => {
    expect(isFailoverMode()).toBe(false)
  })

  it('routes same-site rooms to the local subject when off', () => {
    expect(roomEvent('r1', false)).toBe('chat.local.room.r1.event')
  })

  it('forces the global subject for same-site rooms when on', () => {
    setFailoverMode(true)
    expect(roomEvent('r1', false)).toBe('chat.room.r1.event')
  })

  it('leaves cross-site rooms on the global subject either way', () => {
    expect(roomEvent('r1', true)).toBe('chat.room.r1.event')
    setFailoverMode(true)
    expect(roomEvent('r1', true)).toBe('chat.room.r1.event')
  })
})
```

- [x] **Step 2: Run test to verify it fails**

Run: `cd chat-frontend && npm test -- failover`

Expected: FAIL — cannot resolve `./failover`.

- [x] **Step 3: Write minimal implementation**

Create `chat-frontend/src/api/_transport/failover.ts`:

```ts
/**
 * Failover mode: the client is connected to a peer site because its own site's
 * NATS is unreachable.
 *
 * While it is on, every room subscription must use the GLOBAL subject root
 * regardless of the room's `crossSite` flag. The local root
 * (`chat.local.room.…`) is filtered from gateway interest advertisement, so a
 * client sitting on a peer cluster would never receive a same-site room's
 * events — silently, and only for the rooms that make up most of its traffic.
 *
 * The server half of this flips on the same condition: during an outage it
 * publishes those events to the global root. Both sides must agree, and neither
 * is useful alone.
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
```

In `subjects.ts`, make `roomEvent` consult it:

```ts
import { isFailoverMode } from './failover'

/**
 * Room event subject. `crossSite === false` selects the site-local root, which
 * does not cross a gateway — so in failover mode, when this client is on a peer
 * cluster, it is ignored and every room uses the global root.
 */
export function roomEvent(roomId: string, crossSite: boolean): string {
  if (isFailoverMode()) return `chat.room.${roomId}.event`
  return crossSite === false ? `chat.local.room.${roomId}.event` : `chat.room.${roomId}.event`
}
```

Apply the identical treatment to every other builder in `subjects.ts` that
branches on `crossSite` (the message-stream and metadata-update subjects), and
to the message-send builder, which in failover mode inserts the `failover` token:
`chat.user.{account}.room.{roomId}.{siteId}.failover.msg.send`.

- [x] **Step 4: Run tests**

Run: `cd chat-frontend && npm test -- subjects failover`

Expected: PASS.

- [x] **Step 5: Commit**

```bash
git add chat-frontend/src/api/_transport/failover.ts chat-frontend/src/api/_transport/failover.test.ts chat-frontend/src/api/_transport/subjects.ts
git commit -m "feat(chat-frontend): add failover-mode subject selection"
```

---

### Task 3: Peer selection — shuffle, walk, stick

**Files:**
- Create: `chat-frontend/src/api/_transport/peers.ts`
- Test: `chat-frontend/src/api/_transport/peers.test.ts`

**Interfaces:**
- Consumes: the `sites` array from `GET /api/settings` (Task 1).
- Produces: `shufflePeers(sites: Peer[], homeSiteId: string): Peer[]`, `type Peer = { siteId: string; natsUrl: string }`.

Selection is pure and separately testable; the connect loop that consumes it is
Task 4.

- [x] **Step 1: Write the failing test**

Create `chat-frontend/src/api/_transport/peers.test.ts`:

```ts
import { describe, expect, it } from 'vitest'
import { shufflePeers, type Peer } from './peers'

const sites: Peer[] = [
  { siteId: 'site-a', natsUrl: 'wss://a' },
  { siteId: 'site-b', natsUrl: 'wss://b' },
  { siteId: 'site-c', natsUrl: 'wss://c' },
]

describe('shufflePeers', () => {
  it('excludes the home site — it is the one that is down', () => {
    const out = shufflePeers(sites, 'site-a')
    expect(out.map((p) => p.siteId)).not.toContain('site-a')
    expect(out).toHaveLength(2)
  })

  it('returns every remaining peer exactly once', () => {
    const ids = shufflePeers(sites, 'site-a').map((p) => p.siteId).sort()
    expect(ids).toEqual(['site-b', 'site-c'])
  })

  it('returns empty when there are no peers', () => {
    expect(shufflePeers([{ siteId: 'site-a', natsUrl: 'wss://a' }], 'site-a')).toEqual([])
  })

  it('does not always produce the same order', () => {
    const many = Array.from({ length: 40 }, () => shufflePeers(sites, 'site-a')[0].siteId)
    expect(new Set(many).size).toBeGreaterThan(1)
  })

  it('does not mutate its input', () => {
    const before = sites.map((p) => p.siteId)
    shufflePeers(sites, 'site-a')
    expect(sites.map((p) => p.siteId)).toEqual(before)
  })
})
```

- [x] **Step 2: Run test to verify it fails**

Run: `cd chat-frontend && npm test -- peers`

Expected: FAIL — cannot resolve `./peers`.

- [x] **Step 3: Write minimal implementation**

Create `chat-frontend/src/api/_transport/peers.ts`:

```ts
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
```

- [x] **Step 4: Run tests**

Run: `cd chat-frontend && npm test -- peers`

Expected: PASS.

- [x] **Step 5: Commit**

```bash
git add chat-frontend/src/api/_transport/peers.ts chat-frontend/src/api/_transport/peers.test.ts
git commit -m "feat(chat-frontend): add uniform peer shuffling for failover"
```

---

### Task 4: NatsContext failover and revert

**Files:**
- Modify: `chat-frontend/src/context/NatsContext/NatsContext.jsx` (connect at `:196-199`, close handler at `:216-225`)
- Test: `chat-frontend/src/context/NatsContext/NatsContext.test.jsx`

**Interfaces:**
- Consumes: `shufflePeers` (Task 3), `setFailoverMode` (Task 2), the `sites` array from `/api/settings` (Task 1).
- Produces: no new exports; `NatsContext` gains failover behaviour internally.

**The coupling to record in code:** `HOME_PROBE_MAX_MS` below must stay under
the server's `FAILOVER_REVERT_GRACE` (Plan 3, default 30m). Raising this without
raising that reopens the silent recovery gap dual-publishing exists to close.

- [x] **Step 1: Write the failing test**

Add to `NatsContext.test.jsx`:

```jsx
it('falls back to a peer when the home site will not connect', async () => {
  const attempts = []
  mockNatsConnect.mockImplementation(({ servers }) => {
    attempts.push(servers)
    if (servers === 'wss://home') return Promise.reject(new Error('ECONNREFUSED'))
    return Promise.resolve(makeFakeConn())
  })
  mockSettings({ sites: [
    { siteId: 'site-a', natsUrl: 'wss://home' },
    { siteId: 'site-b', natsUrl: 'wss://peer' },
  ] })

  renderWithProvider()
  await waitFor(() => expect(screen.getByTestId('connected')).toHaveTextContent('true'))

  expect(attempts[0]).toBe('wss://home')
  expect(attempts).toContain('wss://peer')
  expect(isFailoverMode()).toBe(true)
})

it('does not enter failover mode when home connects', async () => {
  mockNatsConnect.mockResolvedValue(makeFakeConn())
  mockSettings({ sites: [{ siteId: 'site-a', natsUrl: 'wss://home' }] })

  renderWithProvider()
  await waitFor(() => expect(screen.getByTestId('connected')).toHaveTextContent('true'))

  expect(isFailoverMode()).toBe(false)
})

it('reports failure when no peer accepts', async () => {
  mockNatsConnect.mockRejectedValue(new Error('ECONNREFUSED'))
  mockSettings({ sites: [
    { siteId: 'site-a', natsUrl: 'wss://home' },
    { siteId: 'site-b', natsUrl: 'wss://peer' },
  ] })

  renderWithProvider()
  await waitFor(() => expect(screen.getByTestId('error')).not.toBeEmptyDOMElement())
  expect(isFailoverMode()).toBe(false)
})

it('leaves failover mode after reconnecting home', async () => {
  // home fails once, then succeeds on the probe
  let homeUp = false
  mockNatsConnect.mockImplementation(({ servers }) => {
    if (servers === 'wss://home' && !homeUp) return Promise.reject(new Error('down'))
    return Promise.resolve(makeFakeConn())
  })
  mockSettings({ sites: [
    { siteId: 'site-a', natsUrl: 'wss://home' },
    { siteId: 'site-b', natsUrl: 'wss://peer' },
  ] })

  renderWithProvider()
  await waitFor(() => expect(isFailoverMode()).toBe(true))

  homeUp = true
  await vi.advanceTimersByTimeAsync(HOME_PROBE_BASE_MS)
  await waitFor(() => expect(isFailoverMode()).toBe(false))
})
```

Match the file's existing mocking helpers (`mockNatsConnect`, `renderWithProvider`)
rather than assuming these names; add `mockSettings` if the suite does not
already stub `/api/settings`. Use fake timers for the probe test.

- [x] **Step 2: Run test to verify it fails**

Run: `cd chat-frontend && npm test -- NatsContext`

Expected: FAIL — a home connect rejection propagates instead of falling back.

- [x] **Step 3: Write minimal implementation**

Add the constants with the coupling documented:

```js
// Home re-probe backoff while in failover mode.
//
// HOME_PROBE_MAX_MS is COUPLED to the server's FAILOVER_REVERT_GRACE (default
// 30m): while a client may still be on a peer, publishers keep emitting to both
// subject roots. Raising this cap without raising that window reopens the
// silent recovery gap where servers have reverted to local routing and
// stragglers hear nothing.
const HOME_PROBE_BASE_MS = 5_000
const HOME_PROBE_MAX_MS = 300_000
```

Replace the single dial at `:196-199` with a walk. Home first — the common case
is that home is fine and nothing else runs:

```js
      const candidates = [
        { siteId: portal.siteId, natsUrl: portal.natsUrl },
        ...shufflePeers(settings.sites ?? [], portal.siteId),
      ]

      let nc = null
      let landedOn = null
      for (const candidate of candidates) {
        try {
          nc = await natsConnect({ servers: candidate.natsUrl, authenticator })
          landedOn = candidate
          break
        } catch {
          // Try the next candidate. A peer that is also down is just the next
          // failed attempt — no liveness tracking anywhere.
        }
      }
      if (!nc) throw new Error('no reachable NATS site')

      // Failover mode is on whenever we are not on our home site. It makes every
      // room subscription use the global subject root, matching the server,
      // which publishes globally while this site's own NATS is down.
      const onFailover = landedOn.siteId !== portal.siteId
      setFailoverMode(onFailover)
      if (onFailover) {
        startHomeProbe(portal, myGen)
      }
```

Add `startHomeProbe`, which retries home on exponential backoff and, on success,
reconnects and clears failover mode. Guard every callback with the existing
`connectGenRef` generation check so a stale probe cannot clobber a newer session,
and clear its timer in the same `stop()` path that disarms the JWT refresh loop —
a probe that outlives its session is a leak.

Sticky behaviour falls out of this shape: the walk runs once per connect
attempt, so a client stays on its chosen peer until that connection closes,
at which point `nc.closed()` triggers a fresh walk with a fresh shuffle.

- [x] **Step 4: Run tests**

Run: `cd chat-frontend && npm test -- NatsContext`

Expected: PASS.

- [ ] **Step 5: Verify in the browser** — **not run**: no Docker in this
environment, so the local stack cannot be started. Covered by unit tests
(peer walk, revert probe, probe teardown) but not yet exercised end to end.

- [x] **Step 6: Commit**

```bash
git add chat-frontend/src/context/NatsContext/NatsContext.jsx chat-frontend/src/context/NatsContext/NatsContext.test.jsx
git commit -m "feat(chat-frontend): fail over to a peer site and revert home automatically"
```

---

### Task 5: Client API documentation

**Files:**
- Modify: `docs/client-api.md`
- Modify: `docs/client-api/request-reply.md`, `docs/client-api/events.md`

- [x] **Step 1: Document the `sites` field**

In the portal section covering `GET /api/settings`, add `sites` to the response
field table as `SitePeer[]`, with a named `SitePeer` table (`siteId: string`,
`natsUrl: string`) and a JSON example. State that it is the failover candidate
list, that the client shuffles it, and that `baseUrl` is deliberately absent
because a displaced client relocates only its NATS connection.

- [x] **Step 2: Document the failover send subject**

Add `chat.user.{account}.room.{roomId}.{siteId}.failover.msg.send` beside the
live send subject: same payload, same response, used only while the client is
connected to a peer site. Note that it stays inside the account's JWT scope, so
no additional permission is involved.

- [x] **Step 3: Document the failover subscription rule**

Where `crossSite` is described (§ around the tri-state note), add that a client
in failover mode MUST ignore `crossSite` and subscribe to
`chat.room.{roomId}.>` for every room, because the local root does not cross a
gateway and the server publishes globally for the duration.

- [x] **Step 4: Update the derived views**

Mirror the send-subject and subscription-rule changes into
`docs/client-api/request-reply.md` and `docs/client-api/events.md`. They must
never drift from the canonical document.

- [x] **Step 5: Commit**

```bash
git add docs/client-api.md docs/client-api/
git commit -m "docs(client-api): document the peer list, failover send subject, and subscription rule"
```

---

## Final Verification

- [x] `make test SERVICE=portal-service` and `make lint` — clean.
- [x] `cd chat-frontend && npm test` — full suite green (974 tests).
- [x] `cd chat-frontend && npm run build` — type-checks and builds.
- [ ] **Manual end-to-end** — **not run**: no Docker in this environment, so
      the local stack cannot be started. The peer walk, the revert probe and
      probe teardown are covered by unit tests, but the browser path against a
      real NATS is unverified.
- [x] **Confirm the steady-state path is untouched:** with every site healthy,
      the client connects home on the first attempt and never enters failover
      mode. `shufflePeers` should not even be reached.

## The coupled constants

Recorded here because they live in two repositories' worth of distance from each
other and a future change to either alone reintroduces a silent bug:

| Constant | Where | Default |
|---|---|---|
| `HOME_PROBE_MAX_MS` | `chat-frontend` NatsContext | 5 min |
| `FAILOVER_REVERT_GRACE` | `broadcast-worker`, `room-service` | 30 min |

The server window must always exceed the client cap. Both sites carry a comment
pointing at the other.
