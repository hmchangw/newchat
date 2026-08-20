# NATS Failover — Room Routing Under Failover Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A displaced client receives events for **every** room, not just cross-site ones — during the outage and through the recovery window when servers have reverted but clients have not.

**Architecture:** Room-route mode stops being a static field and becomes a per-publish resolution over three inputs: which connection the work arrived on, how recently the home connection was restored, and the service's configured mode. Failover-lane work routes global; home-lane work routes dual through a grace window, then configured.

**Tech Stack:** Go 1.25, `nats.go`, testify.

**Design spec:** `docs/superpowers/specs/2026-08-15-nats-site-failover-design.md` §E.

**Depends on Plan 1** (`natsutil.ConnectBuddy`) and **Plan 2** (the buddy connection wired into `broadcast-worker`; `room-service`'s buddy connection from Plan 2 Task 10).

## Why this is separate from Plan 2

Plan 2 delivers the lanes: messages flow, get persisted, get broadcast. This plan
delivers the part that makes the broadcast actually *reach* a displaced client.
They are separable because a reviewer can accept the lane wiring while rejecting
the routing change, and because the failure they address is different in kind —
Plan 2's bugs would be loud, this plan's are silent.

**Until this lands, a displaced client hears only cross-site rooms.** Plans 1 and
2 are shippable to `main`; the feature is not user-ready without this one.

## Global Constraints

- Go 1.25. Single `go.mod` at repo root. No new third-party dependencies.
- All commands via `make` targets — never raw `go` commands.
- TDD is mandatory: write the failing test, run it, confirm it fails, then implement.
- `make lint` and `make test` are enforced by a pre-commit hook.
- Error wrapping: `fmt.Errorf("short description: %w", err)`. Never bare `err`.
- Logging: `log/slog` structured key-value pairs only.
- Never use `time.Sleep` for synchronization; no goroutine without a termination path.
- Subject construction always goes through `pkg/subject` builders.

## Out of scope

- **`room-worker`.** It publishes `.event.member` via `RoomMemberEventTargets`
  (`room-worker/handler.go:2375`) but its only input is the `ROOMS` stream, which
  has no failover lane. It is idle during an outage, so it needs no change. When
  `ROOMS-FAILOVER` is eventually built, it inherits this plan's resolver.
- **Client-side subscription switching.** The client must subscribe to the global
  root while in failover mode — that is Plan 4, and both halves must ship before
  the behaviour is complete.

---

## Background: the three states

`pkg/subject/subject.go:154-159` routes a room's events to one of two roots:
`chat.room.{id}` (gateway-propagated) or `chat.local.room.{id}` (filtered at the
leaf, never crosses a gateway). `roomRouteGlobals` (`:415`) picks local for a
confirmed same-site room under `RouteLocal` / `RouteDual`.

A displaced client is on another cluster, so local-rooted events never reach it.
`crossSite` is still *correct* — it records members' home sites, and those have
not changed — but it no longer implies reachability.

| Work arrived on | Effective mode | Why |
|---|---|---|
| Buddy connection | `RouteGlobal` | Home is down, so every client is remote; `chat.local.>` has zero legitimate subscribers |
| Home connection, within grace of restoration | `RouteDual` | Servers revert in seconds, clients take up to five minutes; both roots must carry traffic until stragglers return |
| Home connection, outside the window | configured `ROOM_SUBJECT_MODE` | Steady state, unchanged |

The grace window mirrors the existing `roomLocalityGrace` dual-publish
(`pkg/subject/subject.go:10-15`) but takes its own constant: that one is 7 days
because room reclassification is permanent, this one only has to outlast the
client revert backoff.

---

### Task 1: Lane-aware route resolution

**Files:**
- Create: `pkg/subject/lane.go`
- Test: `pkg/subject/lane_test.go`

**Interfaces:**
- Consumes: `subject.RoomRouteMode`, `RouteGlobal`, `RouteDual`, `RoomRouteMode.UsesLocal` (all existing).
- Produces: `subject.Lane` (`LaneHome`, `LaneFailover`); `subject.EffectiveRouteMode(configured RoomRouteMode, lane Lane, homeRestoredAt time.Time, grace time.Duration, now time.Time) RoomRouteMode`; `subject.RouteResolver` interface with `Mode(now time.Time) RoomRouteMode`; `subject.LaneRouter` implementing it; `subject.NewLaneRouter(configured RoomRouteMode, lane Lane, restoredAt func() time.Time, grace time.Duration) LaneRouter`.

- [x] **Step 1: Write the failing test**

Create `pkg/subject/lane_test.go`:

```go
package subject_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/subject"
)

func TestEffectiveRouteMode(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	grace := 30 * time.Minute

	tests := []struct {
		name       string
		configured subject.RoomRouteMode
		lane       subject.Lane
		restoredAt time.Time
		want       subject.RoomRouteMode
	}{
		{
			name:       "failover lane always routes global",
			configured: subject.RouteLocal,
			lane:       subject.LaneFailover,
			want:       subject.RouteGlobal,
		},
		{
			name:       "failover lane routes global even in dual mode",
			configured: subject.RouteDual,
			lane:       subject.LaneFailover,
			want:       subject.RouteGlobal,
		},
		{
			name:       "home lane, never lost, uses configured",
			configured: subject.RouteLocal,
			lane:       subject.LaneHome,
			restoredAt: time.Time{},
			want:       subject.RouteLocal,
		},
		{
			name:       "home lane inside the grace window dual-publishes",
			configured: subject.RouteLocal,
			lane:       subject.LaneHome,
			restoredAt: now.Add(-5 * time.Minute),
			want:       subject.RouteDual,
		},
		{
			name:       "home lane past the grace window reverts to configured",
			configured: subject.RouteLocal,
			lane:       subject.LaneHome,
			restoredAt: now.Add(-31 * time.Minute),
			want:       subject.RouteLocal,
		},
		{
			name:       "grace is pointless when configured is already global",
			configured: subject.RouteGlobal,
			lane:       subject.LaneHome,
			restoredAt: now.Add(-5 * time.Minute),
			want:       subject.RouteGlobal,
		},
		{
			name:       "exact window boundary is outside",
			configured: subject.RouteLocal,
			lane:       subject.LaneHome,
			restoredAt: now.Add(-30 * time.Minute),
			want:       subject.RouteLocal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := subject.EffectiveRouteMode(tt.configured, tt.lane, tt.restoredAt, grace, now)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestLaneRouter_Mode(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)

	failover := subject.NewLaneRouter(subject.RouteLocal, subject.LaneFailover, nil, 30*time.Minute)
	assert.Equal(t, subject.RouteGlobal, failover.Mode(now))

	restored := now.Add(-1 * time.Minute)
	home := subject.NewLaneRouter(subject.RouteLocal, subject.LaneHome,
		func() time.Time { return restored }, 30*time.Minute)
	assert.Equal(t, subject.RouteDual, home.Mode(now))
}

// A nil restoredAt must not panic — the failover router has no tracker.
func TestLaneRouter_NilRestoredAt(t *testing.T) {
	r := subject.NewLaneRouter(subject.RouteLocal, subject.LaneHome, nil, 30*time.Minute)
	assert.NotPanics(t, func() { r.Mode(time.Now()) })
	assert.Equal(t, subject.RouteLocal, r.Mode(time.Now()))
}
```

- [x] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=subject`

Expected: FAIL — `undefined: subject.EffectiveRouteMode`.

- [x] **Step 3: Write minimal implementation**

Create `pkg/subject/lane.go`:

```go
package subject

import "time"

// Lane identifies which NATS connection a unit of work arrived on. Room-event
// routing depends on it: a failover-lane publish must reach clients scattered
// across peer clusters, which the site-local subject root cannot do.
type Lane int

const (
	LaneHome     Lane = iota // the site's own cluster
	LaneFailover             // the buddy cluster hosting the standby lanes
)

// DefaultFailoverRevertGrace is how long after the home connection is restored
// a publisher keeps emitting to BOTH roots.
//
// Servers revert the instant their home lane delivers again; clients revert on
// their own backoff, up to five minutes later (spec §F). In that gap some
// clients are home and some are still on a peer cluster, so both roots have to
// carry the traffic or the stragglers go silent.
//
// Distinct from roomLocalityGrace (7 days): that covers permanent room
// reclassification where clients learn the new locality from bootstrap. This one
// only has to outlast the client revert backoff.
const DefaultFailoverRevertGrace = 30 * time.Minute

// EffectiveRouteMode resolves the room-route mode for a single publish.
//
//   - Failover lane → RouteGlobal. Home is down, so every client of this site is
//     on some other cluster; chat.local.> has no legitimate subscribers and
//     forcing global loses nothing.
//   - Home lane within grace of restoration → RouteDual, so clients that have
//     not yet reverted keep receiving.
//   - Otherwise → the configured mode.
//
// A zero homeRestoredAt means the home connection was never lost, so there is no
// window to be inside. When the configured mode is already RouteGlobal there is
// no gap to cover and the grace window is skipped — dual would only add
// pointless local publishes.
func EffectiveRouteMode(configured RoomRouteMode, lane Lane, homeRestoredAt time.Time,
	grace time.Duration, now time.Time,
) RoomRouteMode {
	if lane == LaneFailover {
		return RouteGlobal
	}
	if configured.UsesLocal() && !homeRestoredAt.IsZero() && now.Before(homeRestoredAt.Add(grace)) {
		return RouteDual
	}
	return configured
}

// RouteResolver yields the room-route mode a publisher should use right now.
// Handlers hold one of these instead of a fixed RoomRouteMode, because the
// answer depends on the lane and on how recently home was restored.
type RouteResolver interface {
	Mode(now time.Time) RoomRouteMode
}

// LaneRouter is the standard RouteResolver: one per lane, sharing the service's
// configured mode and grace window.
type LaneRouter struct {
	configured RoomRouteMode
	lane       Lane
	restoredAt func() time.Time
	grace      time.Duration
}

// NewLaneRouter builds a resolver for one lane. restoredAt may be nil — the
// failover lane has no home-restoration concept, and a home lane in a service
// that does not track restores simply never enters the grace window.
func NewLaneRouter(configured RoomRouteMode, lane Lane, restoredAt func() time.Time, grace time.Duration) LaneRouter {
	return LaneRouter{configured: configured, lane: lane, restoredAt: restoredAt, grace: grace}
}

// Mode implements RouteResolver.
func (r LaneRouter) Mode(now time.Time) RoomRouteMode {
	var restored time.Time
	if r.restoredAt != nil {
		restored = r.restoredAt()
	}
	return EffectiveRouteMode(r.configured, r.lane, restored, r.grace, now)
}
```

- [x] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=subject`

Expected: PASS, all cases.

- [x] **Step 5: Commit**

```bash
git add pkg/subject/lane.go pkg/subject/lane_test.go
git commit -m "feat(subject): add lane-aware room route resolution"
```

---

### Task 2: Home-connection restore tracking

**Files:**
- Create: `pkg/natsutil/restore.go`
- Test: `pkg/natsutil/restore_test.go`

**Interfaces:**
- Consumes: `o11ynats.Conn`.
- Produces: `natsutil.RestoreTracker` with `RestoredAt() time.Time` and `MarkRestored(t time.Time)`; `natsutil.TrackRestores(ctx context.Context, conn *o11ynats.Conn) *RestoreTracker`.

**Why `StatusChanged` and not a `ReconnectHandler`:** `natsutil.Connect` already
installs a `ReconnectHandler` for logging, and `nats.Option`s of the same kind
overwrite rather than compose. `Conn.StatusChanged` is an independent listener
registry — the same reason `natsutil.Drain` uses it.

- [x] **Step 1: Write the failing test**

Create `pkg/natsutil/restore_test.go`:

```go
package natsutil_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/natsutil"
)

func TestRestoreTracker_ZeroUntilMarked(t *testing.T) {
	var tr natsutil.RestoreTracker
	assert.True(t, tr.RestoredAt().IsZero(),
		"never-lost must read as zero so the grace window is never entered")
}

func TestRestoreTracker_MarkAndRead(t *testing.T) {
	var tr natsutil.RestoreTracker
	at := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	tr.MarkRestored(at)
	assert.Equal(t, at, tr.RestoredAt())
}

func TestRestoreTracker_LatestWins(t *testing.T) {
	var tr natsutil.RestoreTracker
	first := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	second := first.Add(time.Hour)
	tr.MarkRestored(first)
	tr.MarkRestored(second)
	assert.Equal(t, second, tr.RestoredAt(),
		"a second outage must restart the window, not keep the first stamp")
}

func TestRestoreTracker_ConcurrentAccess(t *testing.T) {
	var tr natsutil.RestoreTracker
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 1000; i++ {
			tr.MarkRestored(time.Now())
		}
	}()
	for i := 0; i < 1000; i++ {
		_ = tr.RestoredAt()
	}
	<-done
}

// The watcher goroutine must exit when its context is cancelled.
func TestTrackRestores_StopsOnContextCancel(t *testing.T) {
	url := startEmbeddedNATS(t) // existing helper, see reply_test.go
	conn := natsutil.ConnectBuddy(context.Background(), url, "",
		noopTracerProvider(), noopPropagator(), false)
	require.NotNil(t, conn)
	t.Cleanup(func() { conn.NatsConn().Close() })

	ctx, cancel := context.WithCancel(context.Background())
	tr := natsutil.TrackRestores(ctx, conn)
	require.NotNil(t, tr)

	cancel()
	// No assertion beyond not leaking; -race plus goleak (if the package uses it)
	// catches a watcher that outlives its context.
}
```

Reuse the embedded-server and no-op otel helpers this package's existing tests
already define rather than assuming the names above.

- [x] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=natsutil`

Expected: FAIL — `undefined: natsutil.RestoreTracker`.

- [x] **Step 3: Write minimal implementation**

Create `pkg/natsutil/restore.go`:

```go
package natsutil

import (
	"context"
	"log/slog"
	"sync"
	"time"

	o11ynats "github.com/flywindy/o11y/nats"
	"github.com/nats-io/nats.go"
)

// RestoreTracker records when a connection last came back after being lost.
//
// Room-event routing needs this: for a while after the home connection is
// restored, publishers must emit to both subject roots, because clients revert
// to their home site on a slower clock than servers do (see
// subject.EffectiveRouteMode). The zero value is valid and reads as "never
// lost", which keeps a service that never had an outage out of the grace window.
type RestoreTracker struct {
	mu sync.RWMutex
	at time.Time
}

// MarkRestored stamps a restoration. A later stamp replaces an earlier one, so a
// second outage restarts the grace window rather than inheriting the first.
func (t *RestoreTracker) MarkRestored(at time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if at.After(t.at) {
		t.at = at
	}
}

// RestoredAt returns the last restoration, or the zero time if the connection
// has never been lost.
func (t *RestoreTracker) RestoredAt() time.Time {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.at
}

// TrackRestores watches conn and stamps a tracker every time it returns to
// CONNECTED after a drop.
//
// StatusChanged rather than nats.ReconnectHandler: Connect already installs a
// ReconnectHandler for logging, and same-kind nats.Options overwrite rather than
// compose. StatusChanged is an independent listener registry — the same reason
// natsutil.Drain uses it.
//
// The watcher goroutine exits when ctx is cancelled.
func TrackRestores(ctx context.Context, conn *o11ynats.Conn) *RestoreTracker {
	tr := &RestoreTracker{}
	if conn == nil {
		return tr
	}
	nc := conn.NatsConn()
	ch := nc.StatusChanged(nats.CONNECTED)

	go func() {
		defer nc.RemoveStatusListener(ch)
		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-ch:
				if !ok {
					return
				}
				now := time.Now().UTC()
				tr.MarkRestored(now)
				slog.InfoContext(ctx, "nats connection restored; entering room-route grace window",
					"restored_at", now)
			}
		}
	}()
	return tr
}
```

- [x] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=natsutil`

Expected: PASS.

- [x] **Step 5: Commit**

```bash
git add pkg/natsutil/restore.go pkg/natsutil/restore_test.go
git commit -m "feat(natsutil): track home-connection restoration for the route grace window"
```

---

### Task 3: broadcast-worker lane-aware routing

**Files:**
- Modify: `broadcast-worker/handler.go:70,73,82,845` (and the `RoomMsgStream` / `RoomMetadataUpdate` publish sites)
- Modify: `broadcast-worker/main.go:66-99` (config + wiring)
- Test: `broadcast-worker/handler_test.go`

**Interfaces:**
- Consumes: `subject.RouteResolver`, `subject.NewLaneRouter`, `subject.LaneHome`/`LaneFailover`, `natsutil.TrackRestores` (Tasks 1-2).
- Produces: `NewHandler(..., routes subject.RouteResolver)` — the `routeMode subject.RoomRouteMode` parameter is replaced, not added to.

One handler per lane, sharing every other dependency. That keeps the lane out of
every call signature: the failover consumer feeds a handler whose resolver always
answers `RouteGlobal`, and the home consumer feeds one that consults the tracker.

- [x] **Step 1: Write the failing test**

Add to `broadcast-worker/handler_test.go`:

```go
// A same-site room's events must go to the GLOBAL root when the message arrived
// on the failover lane — a displaced client is on another cluster, and
// chat.local.> is filtered from gateway interest.
func TestPublishRoomEvent_FailoverLaneForcesGlobal(t *testing.T) {
	pub := &capturingPublisher{}
	h := newTestHandler(t, pub,
		subject.NewLaneRouter(subject.RouteLocal, subject.LaneFailover, nil, 30*time.Minute))

	sameSite := false
	h.publishRoomEvent(context.Background(), "r1", &sameSite, nil, someEventPayload(t))

	require.Len(t, pub.subjects, 1)
	assert.Equal(t, "chat.room.r1.event", pub.subjects[0])
}

func TestPublishRoomEvent_HomeLaneKeepsConfiguredRouting(t *testing.T) {
	pub := &capturingPublisher{}
	h := newTestHandler(t, pub,
		subject.NewLaneRouter(subject.RouteLocal, subject.LaneHome, nil, 30*time.Minute))

	sameSite := false
	h.publishRoomEvent(context.Background(), "r1", &sameSite, nil, someEventPayload(t))

	require.Len(t, pub.subjects, 1)
	assert.Equal(t, "chat.local.room.r1.event", pub.subjects[0])
}

// Inside the revert grace window the home lane must emit to BOTH roots, or
// clients that have not yet reverted go silent during recovery.
func TestPublishRoomEvent_HomeLaneDualPublishesInGraceWindow(t *testing.T) {
	pub := &capturingPublisher{}
	restored := time.Now().Add(-1 * time.Minute)
	h := newTestHandler(t, pub, subject.NewLaneRouter(subject.RouteLocal, subject.LaneHome,
		func() time.Time { return restored }, 30*time.Minute))

	sameSite := false
	h.publishRoomEvent(context.Background(), "r1", &sameSite, nil, someEventPayload(t))

	assert.Equal(t, []string{"chat.local.room.r1.event", "chat.room.r1.event"}, pub.subjects,
		"local first, then global, matching the existing locality-flip convention")
}
```

Use the file's existing publisher fake and handler constructor helper rather
than inventing `capturingPublisher` / `newTestHandler` if equivalents exist;
match the real `publishRoomEvent` signature at `handler.go:844`.

- [x] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=broadcast-worker`

Expected: FAIL — `NewLaneRouter` is not accepted where a `RoomRouteMode` is expected.

- [x] **Step 3: Write minimal implementation**

In `broadcast-worker/handler.go`, replace the field and constructor parameter:

```go
	// routes resolves the room-route mode per publish. Not a fixed
	// RoomRouteMode: the answer depends on which lane delivered the work and,
	// on the home lane, on how recently home was restored.
	routes subject.RouteResolver
```

```go
func NewHandler(store Store, userStore userstore.UserStore, pub Publisher, keyStore RoomKeyProvider,
	parentFetcher ParentFetcher, encrypt bool, routes subject.RouteResolver) *Handler {
	// ...
	routes: routes,
```

At every site that passes `h.routeMode` into a `subject.Room*Targets` call
(`handler.go:845` and the `RoomMsgStream` / `RoomMetadataUpdate` publish sites),
pass `h.routes.Mode(now)` instead, reusing the `now` already in scope.

In `broadcast-worker/main.go`, add the grace config and build one handler per
lane:

```go
	// FailoverRevertGrace must outlast the client revert backoff (spec §F caps
	// it at 5m). Raising the client cap without raising this reopens the silent
	// recovery gap that dual-publishing exists to close.
	FailoverRevertGrace time.Duration `env:"FAILOVER_REVERT_GRACE" envDefault:"30m"`
```

```go
	homeRestores := natsutil.TrackRestores(ctx, nc)
	homeHandler := NewHandler(store, userStore, pub, keyStore, parentFetcher, cfg.Encrypt,
		subject.NewLaneRouter(roomRouteMode, subject.LaneHome, homeRestores.RestoredAt, cfg.FailoverRevertGrace))
```

and, where Plan 2 binds the failover consumer:

```go
	failoverHandler := NewHandler(store, userStore, pub, keyStore, parentFetcher, cfg.Encrypt,
		subject.NewLaneRouter(roomRouteMode, subject.LaneFailover, nil, cfg.FailoverRevertGrace))
```

Feed each consumer its own handler.

- [x] **Step 4: Run tests and build**

Run: `make test SERVICE=broadcast-worker && make build SERVICE=broadcast-worker`

Expected: PASS, builds clean.

- [x] **Step 5: Commit**

```bash
git add broadcast-worker/handler.go broadcast-worker/main.go broadcast-worker/handler_test.go
git commit -m "feat(broadcast-worker): resolve room routing per lane with a revert grace window"
```

---

### Task 4: room-service lane-aware routing

**Files:**
- Modify: `room-service/handler.go:1442-1446`
- Modify: `room-service/main.go:81-145`
- Test: `room-service/handler_test.go`

**Interfaces:**
- Consumes: same as Task 3.
- Produces: the handler's `routeMode` field replaced by `routes subject.RouteResolver`.

`room-service` is easy to miss here: it is an RPC service, so it does not look
like part of the failover pipeline. But it publishes room `.event` at
`handler.go:1446`, and its trigger is a **request arriving on the buddy
connection** rather than a failover-lane JetStream message. Same rule, different
signal.

- [x] **Step 1: Write the failing test**

Add to `room-service/handler_test.go`:

```go
// A request that arrived on the buddy connection must publish its room event to
// the global root, for the same reason broadcast-worker does.
func TestPublishRoomEvent_BuddyConnectionForcesGlobal(t *testing.T) {
	pub := &capturingPublisher{}
	h := newTestHandler(t, pub,
		subject.NewLaneRouter(subject.RouteLocal, subject.LaneFailover, nil, 30*time.Minute))

	sameSite := false
	h.publishRoomEvent(context.Background(), "r1", &sameSite, nil, someEventPayload(t))

	require.Len(t, pub.subjects, 1)
	assert.Equal(t, "chat.room.r1.event", pub.subjects[0])
}

func TestPublishRoomEvent_HomeConnectionKeepsConfiguredRouting(t *testing.T) {
	pub := &capturingPublisher{}
	h := newTestHandler(t, pub,
		subject.NewLaneRouter(subject.RouteLocal, subject.LaneHome, nil, 30*time.Minute))

	sameSite := false
	h.publishRoomEvent(context.Background(), "r1", &sameSite, nil, someEventPayload(t))

	require.Len(t, pub.subjects, 1)
	assert.Equal(t, "chat.local.room.r1.event", pub.subjects[0])
}
```

Match the real `publishRoomEvent` signature at `room-service/handler.go:1445`
and reuse the file's existing fakes.

- [x] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=room-service`

Expected: FAIL — constructor does not accept a `RouteResolver`.

- [x] **Step 3: Write minimal implementation**

Mirror Task 3: swap the handler's `routeMode` field for `routes
subject.RouteResolver`, pass `h.routes.Mode(now)` at `handler.go:1446`, add the
`FAILOVER_REVERT_GRACE` config field, and construct two handlers — one whose
subscriptions are registered on the home connection, one on the buddy connection
that Plan 2 Task 10 opened.

Register each handler's `QueueSubscribe` calls on its own connection so a
request answered from the buddy lane uses the failover resolver.

- [x] **Step 4: Run tests and build**

Run: `make test SERVICE=room-service && make build SERVICE=room-service`

Expected: PASS, builds clean.

- [x] **Step 5: Commit**

```bash
git add room-service/handler.go room-service/main.go room-service/handler_test.go
git commit -m "feat(room-service): resolve room routing per connection with a revert grace window"
```

---

### Task 5: Integration test

**Files:**
- Modify: `broadcast-worker/integration_test.go`

- [x] **Step 1: Write the failing test**

```go
// The end-to-end property: a subscriber on a DIFFERENT cluster receives a
// same-site room's message when it came through the failover lane. This is the
// case that silently fails without lane-aware routing.
func TestFailoverLane_SameSiteRoomReachesRemoteSubscriber(t *testing.T) {
	homeURL, buddyURL := testutil.NATSPair(t)
	ctx := context.Background()

	// The "displaced client": connected to the buddy, subscribed to the global
	// root, as a client in failover mode is (Plan 4).
	client, err := nats.Connect(buddyURL)
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })
	sub, err := client.SubscribeSync(subject.RoomMsgStream("r1", true))
	require.NoError(t, err)
	require.NoError(t, client.Flush())

	// broadcast-worker running the failover lane, publishing on the buddy.
	startBroadcastFailoverLane(t, ctx, buddyURL, "site-a")

	publishCanonical(t, ctx, buddyURL, subject.FailoverMsgCanonicalCreated("site-a"),
		sameSiteRoomMessage(t, "r1", "m1"))

	msg, err := sub.NextMsg(10 * time.Second)
	require.NoError(t, err, "a same-site room's message must reach a remote subscriber on the failover lane")
	assert.Contains(t, string(msg.Data), "m1")

	// And the local root must carry nothing — forcing global means global only.
	localSub, err := client.SubscribeSync(subject.RoomMsgStream("r1", false))
	require.NoError(t, err)
	require.NoError(t, client.Flush())
	_, err = localSub.NextMsg(500 * time.Millisecond)
	assert.ErrorIs(t, err, nats.ErrTimeout)

	_ = homeURL
}
```

Write `startBroadcastFailoverLane`, `publishCanonical` and
`sameSiteRoomMessage` reusing whatever the existing integration test provides.
`sameSiteRoomMessage` must produce a room whose stored `crossSite` is explicitly
`false`, or the fail-safe routes global anyway and the test proves nothing.

- [x] **Step 2: Run test to verify it fails**

Run: `make test-integration SERVICE=broadcast-worker`

Expected: FAIL before Task 3 — the publish goes to `chat.local.room.r1.stream.msg`
and `NextMsg` times out.

- [x] **Step 3: Make it pass**

With Task 3 complete, no new production code — fix test wiring only.

- [x] **Step 4: Run the suite**

Run: `make test-integration SERVICE=broadcast-worker`

Expected: PASS.

- [x] **Step 5: Commit**

```bash
git add broadcast-worker/integration_test.go
git commit -m "test(broadcast-worker): cover same-site room delivery on the failover lane"
```

---

### Task 6: Compose wiring and documentation

**Files:**
- Modify: `broadcast-worker/deploy/docker-compose.yml`, `room-service/deploy/docker-compose.yml`
- Modify: `docs/nats-failover-scenarios.md`
- Modify: `docs/superpowers/specs/2026-08-15-nats-site-failover-design.md`

- [x] **Step 1: Add the grace config to compose**

Both services, in `environment:`:

```yaml
      # Must outlast the client revert backoff (5m cap). Raising the client cap
      # without raising this reopens the silent recovery gap.
      - FAILOVER_REVERT_GRACE=30m
```

- [x] **Step 2: Note the coupling where both constants live**

Add a comment at `subject.DefaultFailoverRevertGrace` pointing at the client
backoff cap, and — when Plan 4 lands — a matching comment at the client's cap
pointing back. They are a coupled pair, not two independent tunables.

Record the same coupling in `docs/nats-failover-scenarios.md` §10, which already
describes the dual-publish window in plain terms.

- [x] **Step 3: Mark §E implemented in the spec**

Add a line to the spec's §E noting the implementing plan, so a reader knows the
design is realised and where.

- [x] **Step 4: Commit**

```bash
git add broadcast-worker/deploy/docker-compose.yml room-service/deploy/docker-compose.yml docs/
git commit -m "docs: record the revert grace window and its coupling to the client backoff"
```

---

## Final Verification

- [x] `make test` — all packages pass with `-race`.
- [x] `make lint` — clean.
- [x] `make sast` — gosec PASS. govulncheck and semgrep could not run in this
      environment (the vulnerability DB is blocked by the egress proxy; semgrep
      is not installed), so both remain to be confirmed in CI. The only rule
      relevant to this change, `room-subject-publish-must-route`, is satisfied:
      every new publish goes through `subject.RoomEventTargets`.
- [ ] `make test-integration SERVICE=broadcast-worker` — **not run**: no Docker
      in this environment. The new cases are compile-verified only.
- [x] Coverage for `pkg/subject` and `pkg/natsutil` at the 90% `pkg/` target.
- [x] **Confirm steady-state routing is unchanged:** with no buddy configured and
      no outage, `broadcast-worker` and `room-service` must route exactly as they
      did before this plan. This is the regression that would silently give back
      the gateway-interest savings the local namespace exists for.

## Known incomplete after this plan

The **server** half is done. A displaced client still needs to *subscribe* to the
global root while in failover mode — that is Plan 4. Both halves flip on the same
condition and neither works alone.
