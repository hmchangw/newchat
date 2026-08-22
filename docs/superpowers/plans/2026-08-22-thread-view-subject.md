# Thread-view Subject Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver live thread replies to a user who has the thread panel open but does not follow the thread.

**Architecture:** `broadcast-worker` mirrors every channel thread event onto a room-derived, thread-scoped NATS subject. The client subscribes to that subject for the lifetime of the open panel, so NATS's own subscription table is the viewer registry. The existing per-follower fan-out is untouched.

**Tech Stack:** Go 1.25, NATS core, `go.uber.org/mock`, `testify`, testcontainers; React + vitest for `chat-frontend`.

**Spec:** `docs/superpowers/specs/2026-08-22-thread-view-subject-design.md`

## Global Constraints

- All commands go through `make` targets — never raw `go` commands.
- TDD: write the failing test, watch it fail, implement, watch it pass, commit.
- Minimum 80% coverage; cover error paths and edge cases.
- Subject strings are built only via `pkg/subject` builders — never `fmt.Sprintf` at a call site.
- Thread-subject publish is **best-effort**: log, count the failure, never fail the handler.
- Kill switch `THREAD_VIEW_SUBJECT_ENABLED`, `envDefault:"true"`.
- Channel rooms only. DM/botDM thread paths are untouched.
- Comments: no WHAT-comments, at most 2 lines, explain WHY.
- Do not mention the history-access-window interaction in code comments or docs.
- `docs/client-api.md` and `docs/client-api/events.md` must be updated in the same PR.

---

### Task 1: Thread-scoped subject builders

**Files:**
- Modify: `pkg/subject/subject.go` (after `RoomMemberEventTargets`, ~line 460)
- Test: `pkg/subject/subject_test.go`

**Interfaces:**
- Consumes: unexported `roomBase(roomID string, global bool) string`, `roomRouteGlobals(crossSite *bool, crossSiteAt *time.Time, mode RoomRouteMode, now time.Time) []bool`
- Produces:
  - `subject.RoomThreadEvent(roomID, parentMessageID string, global bool) string`
  - `subject.RoomThreadEventTargets(roomID, parentMessageID string, crossSite *bool, crossSiteAt *time.Time, mode RoomRouteMode, now time.Time) []string`

- [ ] **Step 1: Write the failing test**

Append to `pkg/subject/subject_test.go`:

```go
func TestRoomThreadEvent(t *testing.T) {
	assert.Equal(t, "chat.room.r1.thread.p1.event", subject.RoomThreadEvent("r1", "p1", true))
	assert.Equal(t, "chat.local.room.r1.thread.p1.event", subject.RoomThreadEvent("r1", "p1", false))
}

// The thread subject must not be caught by the 4-token room-event wildcard that
// existing server-side subscribers use.
func TestRoomThreadEvent_DoesNotMatchRoomEventWildcard(t *testing.T) {
	assert.Equal(t, 6, len(strings.Split(subject.RoomThreadEvent("r1", "p1", true), ".")))
	assert.Equal(t, 4, len(strings.Split(subject.RoomEvent("r1", true), ".")))
}

func TestRoomThreadEventTargets(t *testing.T) {
	g := "chat.room.r1.thread.p1.event"
	l := "chat.local.room.r1.thread.p1.event"
	trueP, falseP := true, false
	now := time.Unix(1_700_000_000, 0).UTC()

	assert.Equal(t, []string{g}, subject.RoomThreadEventTargets("r1", "p1", &trueP, nil, subject.RouteGlobal, now))
	assert.Equal(t, []string{g}, subject.RoomThreadEventTargets("r1", "p1", &trueP, nil, subject.RouteLocal, now))
	assert.Equal(t, []string{g}, subject.RoomThreadEventTargets("r1", "p1", &falseP, nil, subject.RouteGlobal, now))
	assert.Equal(t, []string{l, g}, subject.RoomThreadEventTargets("r1", "p1", &falseP, nil, subject.RouteDual, now))
	assert.Equal(t, []string{l}, subject.RoomThreadEventTargets("r1", "p1", &falseP, nil, subject.RouteLocal, now))
	// nil locality is the global fail-safe in every mode.
	assert.Equal(t, []string{g}, subject.RoomThreadEventTargets("r1", "p1", nil, nil, subject.RouteLocal, now))
}

// A room that flipped local->global dual-publishes for the grace window, so a
// viewer still on the local lane keeps receiving thread events.
func TestRoomThreadEventTargets_TransitionGrace(t *testing.T) {
	g := "chat.room.r1.thread.p1.event"
	l := "chat.local.room.r1.thread.p1.event"
	trueP := true
	flip := time.Unix(1_700_000_000, 0).UTC()

	within := flip.Add(subject.DefaultRoomLocalityGrace - time.Minute)
	assert.Equal(t, []string{l, g}, subject.RoomThreadEventTargets("r1", "p1", &trueP, &flip, subject.RouteLocal, within))

	after := flip.Add(subject.DefaultRoomLocalityGrace + time.Minute)
	assert.Equal(t, []string{g}, subject.RoomThreadEventTargets("r1", "p1", &trueP, &flip, subject.RouteLocal, after))
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=pkg/subject`
Expected: FAIL — `undefined: subject.RoomThreadEvent`

- [ ] **Step 3: Write minimal implementation**

Append to `pkg/subject/subject.go` after `RoomMemberEventTargets`:

```go
// RoomThreadEvent returns the thread-scoped event subject a client subscribes to
// while a thread panel is open. Routes on the same namespaces as RoomEvent.
func RoomThreadEvent(roomID, parentMessageID string, global bool) string {
	return roomBase(roomID, global) + ".thread." + parentMessageID + ".event"
}

// RoomThreadEventTargets returns the thread-scoped subject(s) to publish to.
// Shares roomRouteGlobals with RoomEventTargets so a same-site room's thread
// events stay local and a flipped room dual-publishes for the grace window.
func RoomThreadEventTargets(roomID, parentMessageID string, crossSite *bool, crossSiteAt *time.Time, mode RoomRouteMode, now time.Time) []string {
	globals := roomRouteGlobals(crossSite, crossSiteAt, mode, now)
	out := make([]string, len(globals))
	for i, g := range globals {
		out[i] = RoomThreadEvent(roomID, parentMessageID, g)
	}
	return out
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=pkg/subject` — Expected: PASS
Run: `make lint` — Expected: clean

- [ ] **Step 5: Commit**

```bash
git add pkg/subject/subject.go pkg/subject/subject_test.go
git commit -m "feat(subject): thread-scoped room event subject builders"
```

---

### Task 2: Thread-view publish failure counter

**Files:**
- Modify: `broadcast-worker/nats_metrics.go`
- Test: `broadcast-worker/nats_metrics_test.go`

**Interfaces:**
- Consumes: `broadcastMetrics`, `natsmetrics.EventType`, `allBroadcastEvents`
- Produces: `(*broadcastMetrics).ThreadViewPublishFailed(ctx context.Context, event natsmetrics.EventType)`

- [ ] **Step 1: Write the failing test**

Append to `broadcast-worker/nats_metrics_test.go`:

```go
func TestBroadcastMetrics_ThreadViewPublishFailed(t *testing.T) {
	m := newBroadcastMetrics(noop.NewMeterProvider().Meter("test"))
	require.NotNil(t, m.threadViewFailures)
	// Every classified event plus an unknown one must resolve to a prebuilt
	// attribute set rather than allocating per call.
	for _, evt := range allBroadcastEvents {
		assert.NotNil(t, m.threadViewOpts[evt], "missing prebuilt opts for %s", evt)
	}
	assert.NotPanics(t, func() {
		m.ThreadViewPublishFailed(context.Background(), natsmetrics.EventCreated)
		m.ThreadViewPublishFailed(context.Background(), natsmetrics.EventType("bogus"))
	})
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=broadcast-worker`
Expected: FAIL — `m.threadViewFailures undefined`

- [ ] **Step 3: Write minimal implementation**

Add the fields to `broadcastMetrics`:

```go
	threadViewFailures metric.Int64Counter
	threadViewOpts     map[natsmetrics.EventType]metric.MeasurementOption
```

In `newBroadcastMetrics`, after the `deliveries` instrument:

```go
	threadViewFailures, err := meter.Int64Counter("broadcast_worker_thread_view_publish_failures_total",
		metric.WithDescription("Thread-scoped view subject publishes that failed."))
	if err != nil {
		threadViewFailures, _ = noopMeter.Int64Counter("broadcast_worker_thread_view_publish_failures_total")
	}
```

Add to the struct literal: `threadViewFailures: threadViewFailures,` and
`threadViewOpts: make(map[natsmetrics.EventType]metric.MeasurementOption, len(allBroadcastEvents)),`

After the existing attribute-prebuild loops:

```go
	for _, event := range allBroadcastEvents {
		m.threadViewOpts[event] = metric.WithAttributes(attribute.String("event_type", string(event)))
	}
```

Add the method:

```go
// ThreadViewPublishFailed counts a failed publish to the thread-scoped view
// subject. Failures only — the lane is best-effort and viewers refetch on open.
func (m *broadcastMetrics) ThreadViewPublishFailed(ctx context.Context, event natsmetrics.EventType) {
	m.threadViewFailures.Add(ctx, 1, m.threadViewOpts[normalizeBroadcastEvent(event)])
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=broadcast-worker` — Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add broadcast-worker/nats_metrics.go broadcast-worker/nats_metrics_test.go
git commit -m "feat(broadcast-worker): thread-view publish failure counter"
```

---

### Task 3: Thread-view publish helper and handler wiring

**Files:**
- Modify: `broadcast-worker/handler.go` (`Handler` struct ~line 64, `handlerOptions` ~line 76, `handleThreadCreated` ~line 241, `handleThreadUpdated` ~line 356, `handleThreadDeleted` ~line 405, new helper next to `publishRoomEvent` ~line 884)
- Test: `broadcast-worker/handler_test.go`

**Interfaces:**
- Consumes: `subject.RoomThreadEventTargets` (Task 1), `(*broadcastMetrics).ThreadViewPublishFailed` (Task 2)
- Produces:
  - `withThreadViewSubject(enabled bool) handlerOption`
  - `(*Handler).publishThreadViewEvent(ctx context.Context, roomID, parentMsgID string, crossSite *bool, crossSiteAt *time.Time, payload []byte)`

- [ ] **Step 1: Write the failing test**

Append to `broadcast-worker/handler_test.go`. `capturePublisher` records every `(subject, payload)`; if the existing fake publisher in this file has a different name, reuse it rather than adding one.

```go
// A viewer who follows nothing still gets the reply, because the thread-scoped
// subject is published independently of the follower set.
func TestHandler_ThreadCreated_PublishesThreadViewSubject(t *testing.T) {
	tests := []struct {
		name      string
		crossSite *bool
		want      []string
	}{
		{"cross-site room routes global", ptrBool(true), []string{"chat.room.room1.thread.parent1.event"}},
		{"same-site room routes global in global mode", ptrBool(false), []string{"chat.room.room1.thread.parent1.event"}},
		{"unclassified room routes global", nil, []string{"chat.room.room1.thread.parent1.event"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, pub := newThreadViewHandler(t, true, tt.crossSite)
			require.NoError(t, h.HandleMessage(context.Background(), threadReplyEvent(t)))
			assert.Equal(t, tt.want, pub.subjectsWithPrefix("chat.room.room1.thread."))
		})
	}
}

func TestHandler_ThreadCreated_ThreadViewSubjectDisabled(t *testing.T) {
	h, pub := newThreadViewHandler(t, false, ptrBool(true))
	require.NoError(t, h.HandleMessage(context.Background(), threadReplyEvent(t)))
	assert.Empty(t, pub.subjectsWithPrefix("chat.room.room1.thread."))
}

// The follower set is empty only when sender and parent author are both bots —
// but that is precisely a lone viewer's case, so the view lane must still fire.
func TestHandler_ThreadCreated_EmptyFollowerSetStillPublishesViewSubject(t *testing.T) {
	h, pub := newThreadViewHandlerNoFollowers(t)
	require.NoError(t, h.HandleMessage(context.Background(), threadReplyEvent(t)))
	assert.Len(t, pub.subjectsWithPrefix("chat.room.room1.thread."), 1)
}

func TestHandler_ThreadUpdated_PublishesThreadViewSubject(t *testing.T) {
	h, pub := newThreadViewHandler(t, true, ptrBool(true))
	require.NoError(t, h.HandleMessage(context.Background(), threadEditEvent(t)))
	assert.Equal(t, []string{"chat.room.room1.thread.parent1.event"},
		pub.subjectsWithPrefix("chat.room.room1.thread."))
}

func TestHandler_ThreadDeleted_PublishesThreadViewSubject(t *testing.T) {
	h, pub := newThreadViewHandler(t, true, ptrBool(true))
	require.NoError(t, h.HandleMessage(context.Background(), threadDeleteEvent(t)))
	assert.Equal(t, []string{"chat.room.room1.thread.parent1.event"},
		pub.subjectsWithPrefix("chat.room.room1.thread."))
}

// Best-effort: a view-subject failure must not NAK the message, because the
// per-follower fan-out has already been attempted on the same delivery.
func TestHandler_ThreadView_PublishFailureDoesNotFailHandler(t *testing.T) {
	h, pub := newThreadViewHandler(t, true, ptrBool(true))
	pub.failOnPrefix = "chat.room.room1.thread."
	assert.NoError(t, h.HandleMessage(context.Background(), threadReplyEvent(t)))
}

func TestHandler_ThreadView_EmptyParentIDSkipped(t *testing.T) {
	h, pub := newThreadViewHandler(t, true, ptrBool(true))
	h.publishThreadViewEvent(context.Background(), "room1", "", ptrBool(true), nil, []byte(`{}`))
	assert.Empty(t, pub.subjectsWithPrefix("chat.room.room1.thread."))
}

func TestHandler_ThreadView_DMRoomPublishesNothing(t *testing.T) {
	h, pub := newThreadViewDMHandler(t)
	require.NoError(t, h.HandleMessage(context.Background(), threadReplyEvent(t)))
	assert.Empty(t, pub.subjectsWithPrefix("chat.room.room1.thread."))
}
```

Add the fixtures this file needs (reuse the existing mock-store setup already used by the thread tests around `handler_test.go:185`; only the option and the assertions are new):

```go
func ptrBool(b bool) *bool { return &b }

// subjectsWithPrefix returns the captured subjects starting with prefix, in
// publish order — lets a test assert the view lane without matching the
// per-follower lane.
func (p *capturePublisher) subjectsWithPrefix(prefix string) []string {
	var out []string
	for _, rec := range p.records() {
		if strings.HasPrefix(rec.subject, prefix) {
			out = append(out, rec.subject)
		}
	}
	return out
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=broadcast-worker`
Expected: FAIL — `undefined: withThreadViewSubject`, `h.publishThreadViewEvent undefined`

- [ ] **Step 3: Write minimal implementation**

Add to `Handler`: `threadViewSubject bool`. Add to `handlerOptions`: `threadViewSubject bool`. Add:

```go
func withThreadViewSubject(enabled bool) handlerOption {
	return func(opts *handlerOptions) { opts.threadViewSubject = enabled }
}
```

Set `threadViewSubject: opts.threadViewSubject` in the `NewHandler` struct literal.

Add next to `publishRoomEvent`:

```go
// publishThreadViewEvent mirrors an already-built thread event onto the
// thread-scoped subject open thread panels subscribe to, so a viewer who
// follows nothing still sees the reply.
//
// Best-effort by contract: a failure is counted and logged, never returned. The
// caller is mid-delivery on the per-follower lane, and returning an error would
// NAK and re-run that fan-out; viewers reconcile when the panel reopens.
func (h *Handler) publishThreadViewEvent(ctx context.Context, roomID, parentMsgID string, crossSite *bool, crossSiteAt *time.Time, payload []byte) {
	if !h.threadViewSubject || parentMsgID == "" || len(payload) == 0 {
		return
	}
	eventType := broadcastLabels(ctx).eventType
	now := time.Now().UTC()
	for _, subj := range subject.RoomThreadEventTargets(roomID, parentMsgID, crossSite, crossSiteAt, h.routeMode, now) {
		if err := h.pub.Publish(ctx, subj, payload); err != nil {
			h.metrics.ThreadViewPublishFailed(ctx, eventType)
			slog.ErrorContext(ctx, "publish thread view event failed",
				"error", err,
				"subject", subj,
				"parentMessageID", parentMsgID,
				"room_id", roomID,
				"request_id", natsutil.RequestIDFromContext(ctx))
		}
	}
}
```

In `handleThreadCreated`, delete the empty-fan-out early return:

```go
		if len(fanOut) == 0 {
			slog.DebugContext(ctx, "no thread subscribers to notify for thread reply",
				"parentMessageID", parentMsgID,
				"request_id", natsutil.RequestIDFromContext(ctx))
			return nil
		}
```

and in its `case model.RoomTypeChannel:` branch, insert the view publish between the marshal and the fan-out:

```go
		h.publishThreadViewEvent(ctx, meta.ID, parentMsgID, meta.CrossSite, meta.CrossSiteAt, payload)
		return h.publishToThreadAccounts(ctx, fanOut, payload, parentMsgID)
```

In `handleThreadUpdated`, replace the empty-fan-out early return with a fall-through and insert the same call before `publishToThreadAccounts`:

```go
		payload, err := sonic.Marshal(&edit)
		if err != nil {
			return fmt.Errorf("marshal thread edit event for parent %s: %w", parentMsgID, err)
		}
		h.publishThreadViewEvent(ctx, room.ID, parentMsgID, room.CrossSite, room.CrossSiteAt, payload)
		return h.publishToThreadAccounts(ctx, fanOut, payload, parentMsgID)
```

In `handleThreadDeleted`, replace the `if len(fanOut) > 0 { … }` guard so marshal and the view publish always run:

```go
		payload, err := sonic.Marshal(&del)
		if err != nil {
			return fmt.Errorf("marshal thread delete event for parent %s: %w", parentMsgID, err)
		}
		h.publishThreadViewEvent(ctx, room.ID, parentMsgID, room.CrossSite, room.CrossSiteAt, payload)
		if err := h.publishToThreadAccounts(ctx, fanOut, payload, parentMsgID); err != nil {
			return fmt.Errorf("publish thread delete event for parent %s: %w", parentMsgID, err)
		}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=broadcast-worker` — Expected: PASS
Run: `make lint` — Expected: clean

- [ ] **Step 5: Commit**

```bash
git add broadcast-worker/handler.go broadcast-worker/handler_test.go
git commit -m "feat(broadcast-worker): mirror channel thread events onto a thread-scoped subject"
```

---

### Task 4: Config kill switch

**Files:**
- Modify: `broadcast-worker/main.go` (`config` struct ~line 40, `NewHandler` call site)
- Modify: `broadcast-worker/deploy/docker-compose.yml`
- Test: `broadcast-worker/config_test.go`

**Interfaces:**
- Consumes: `withThreadViewSubject` (Task 3)
- Produces: `config.ThreadViewSubjectEnabled bool`

- [ ] **Step 1: Write the failing test**

Append to `broadcast-worker/config_test.go`:

```go
func TestConfig_ThreadViewSubjectEnabled(t *testing.T) {
	t.Setenv("MODE", "user")
	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	assert.True(t, cfg.ThreadViewSubjectEnabled, "the view lane ships on by default; the env var is a kill switch")

	t.Setenv("THREAD_VIEW_SUBJECT_ENABLED", "false")
	cfg, err = env.ParseAs[config]()
	require.NoError(t, err)
	assert.False(t, cfg.ThreadViewSubjectEnabled)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=broadcast-worker`
Expected: FAIL — `cfg.ThreadViewSubjectEnabled undefined`

- [ ] **Step 3: Write minimal implementation**

Add to `config`, next to `RoomLocalityGrace`:

```go
	// ThreadViewSubjectEnabled: kill switch for the thread-scoped view lane.
	ThreadViewSubjectEnabled bool `env:"THREAD_VIEW_SUBJECT_ENABLED" envDefault:"true"`
```

Pass it at the `NewHandler` call site in `main.go`:

```go
	withThreadViewSubject(cfg.ThreadViewSubjectEnabled),
```

Add `THREAD_VIEW_SUBJECT_ENABLED: "true"` to the broadcast-worker service environment in `broadcast-worker/deploy/docker-compose.yml`.

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=broadcast-worker` — Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add broadcast-worker/main.go broadcast-worker/config_test.go broadcast-worker/deploy/docker-compose.yml
git commit -m "feat(broadcast-worker): THREAD_VIEW_SUBJECT_ENABLED kill switch"
```

---

### Task 5: Integration test over real NATS

**Files:**
- Modify: `broadcast-worker/integration_test.go`

**Interfaces:**
- Consumes: everything from Tasks 1-4; `testutil.NATS(t)`

- [ ] **Step 1: Write the failing test**

Append to `broadcast-worker/integration_test.go` (build tag `integration` is already on the file). Follow the fixture style of the thread tests already in the file — the point is that the subscriber is a **non-follower** with no thread subscription.

```go
// End-to-end proof of the reported bug: a room member who never replied — so is
// absent from thread_rooms.replyAccounts — receives the reply on the thread
// subject alone.
func TestIntegration_ThreadViewSubject_NonFollowerReceivesReply(t *testing.T) {
	ctx := context.Background()
	url := testutil.NATS(t)
	nc, err := nats.Connect(url)
	require.NoError(t, err)
	t.Cleanup(func() { _ = nc.Drain() })

	h, roomID, parentID := newIntegrationThreadHandler(t, nc)

	sub, err := nc.SubscribeSync(subject.RoomThreadEvent(roomID, parentID, true))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	require.NoError(t, nc.Flush())

	require.NoError(t, h.HandleMessage(ctx, threadReplyEventFor(t, roomID, parentID)))

	msg, err := sub.NextMsg(5 * time.Second)
	require.NoError(t, err, "non-follower viewer must receive the thread reply")

	var evt model.RoomEvent
	require.NoError(t, json.Unmarshal(msg.Data, &evt))
	assert.Equal(t, model.RoomEventNewThreadMessage, evt.Type)
	assert.Equal(t, roomID, evt.RoomID)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test-integration SERVICE=broadcast-worker`
Expected: FAIL — `NextMsg` times out before Task 3's publish exists, or the helper is undefined.

- [ ] **Step 3: Write minimal implementation**

No production code. Add only the `newIntegrationThreadHandler` / `threadReplyEventFor` fixtures, modelled on the existing thread integration fixtures in this file: a channel room with `crossSite: true`, a `thread_rooms` document whose `replyAccounts` does **not** contain the subscriber, and a handler built with `withThreadViewSubject(true)`.

- [ ] **Step 4: Run test to verify it passes**

Run: `make test-integration SERVICE=broadcast-worker` — Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add broadcast-worker/integration_test.go
git commit -m "test(broadcast-worker): non-follower receives thread replies over real NATS"
```

---

### Task 6: Frontend subject builder and subscription module

**Files:**
- Modify: `chat-frontend/src/api/_transport/subjects.ts`
- Create: `chat-frontend/src/api/subscribeToThreadEvents/index.ts`
- Modify: `chat-frontend/src/api/index.ts`
- Test: `chat-frontend/src/api/_transport/subjects.test.js`

**Interfaces:**
- Consumes: `Nats`, `NatsSubscription`, `SubscriptionCallback` from `../types`
- Produces:
  - `roomThreadEvent(roomId: string, parentMessageId: string, crossSite: boolean): string`
  - `subscribeToThreadEvents({ subscribe }, { roomId, parentMessageId, crossSite }, callback): NatsSubscription`

- [ ] **Step 1: Write the failing test**

Append to `chat-frontend/src/api/_transport/subjects.test.js`:

```js
describe('roomThreadEvent', () => {
  it('routes cross-site threads to the global namespace', () => {
    expect(roomThreadEvent('r1', 'p1', true)).toBe('chat.room.r1.thread.p1.event')
  })

  it('routes same-site threads to the local namespace', () => {
    expect(roomThreadEvent('r1', 'p1', false)).toBe('chat.local.room.r1.thread.p1.event')
  })

  // Fail-safe parity with roomEvent: only an explicit false routes local.
  it.each([undefined, null])('treats %s crossSite as global', (value) => {
    expect(roomThreadEvent('r1', 'p1', value)).toBe('chat.room.r1.thread.p1.event')
  })
})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd chat-frontend && npx vitest run src/api/_transport/subjects.test.js`
Expected: FAIL — `roomThreadEvent is not a function`

- [ ] **Step 3: Write minimal implementation**

Add to `subjects.ts` next to `roomEvent`:

```ts
// Thread-scoped lane a client subscribes to while a thread panel is open.
// Same fail-safe as roomEvent: only an explicit `false` routes site-local.
export function roomThreadEvent(roomId: string, parentMessageId: string, crossSite: boolean): string {
  const base = crossSite === false ? `chat.local.room.${roomId}` : `chat.room.${roomId}`
  return `${base}.thread.${parentMessageId}.event`
}
```

Create `chat-frontend/src/api/subscribeToThreadEvents/index.ts`:

```ts
import { roomThreadEvent } from '../_transport/subjects'
import type { Nats, NatsSubscription, SubscriptionCallback } from '../types'

/** Subscribe to one thread's event stream for as long as its panel is open.
 *  Delivers replies to viewers who do not follow the thread. */
export function subscribeToThreadEvents(
  { subscribe }: Pick<Nats, 'subscribe'>,
  { roomId, parentMessageId, crossSite }: { roomId: string; parentMessageId: string; crossSite: boolean },
  callback: SubscriptionCallback,
): NatsSubscription {
  return subscribe(roomThreadEvent(roomId, parentMessageId, crossSite), callback)
}
```

Re-export from `chat-frontend/src/api/index.ts` alongside `subscribeToRoomEvents`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd chat-frontend && npx vitest run src/api/_transport/subjects.test.js` — Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add chat-frontend/src/api
git commit -m "feat(frontend): thread-scoped event subject and subscription module"
```

---

### Task 7: Subscribe for the lifetime of the open thread panel

**Files:**
- Modify: `chat-frontend/src/context/ThreadEventsContext/ThreadEventsContext.jsx` (`openThread` ~line 58, `closeThread` ~line 87)
- Modify: `chat-frontend/src/components/MainApp/ChatPage/ChatPage.jsx:84`
- Test: `chat-frontend/src/context/ThreadEventsContext/ThreadEventsContext.test.jsx`

**Interfaces:**
- Consumes: `subscribeToThreadEvents` (Task 6)
- Produces: no new exports — `openThread(parent)` gains an optional `parent.crossSite`

- [ ] **Step 1: Write the failing test**

Append to `ThreadEventsContext.test.jsx`, matching the existing harness in that file:

```jsx
it('subscribes to the thread subject while the panel is open', async () => {
  const { subscribe, unsubscribe } = renderThreadHarness()
  await openThreadPanel({ roomId: 'r1', parentId: 'p1', crossSite: true })
  expect(subscribe).toHaveBeenCalledWith('chat.room.r1.thread.p1.event', expect.any(Function))

  await closeThreadPanel()
  expect(unsubscribe).toHaveBeenCalled()
})

// Subscribe before fetching: a reply landing between the two would otherwise be
// lost, which is the bug this feature exists to fix.
it('subscribes before fetching history', async () => {
  const { calls } = renderThreadHarness()
  await openThreadPanel({ roomId: 'r1', parentId: 'p1', crossSite: true })
  expect(calls.indexOf('subscribe')).toBeLessThan(calls.indexOf('fetchThreadMessages'))
})

it('applies a reply arriving on the thread subject', async () => {
  const { emitThreadEvent, screen } = renderThreadHarness()
  await openThreadPanel({ roomId: 'r1', parentId: 'p1', crossSite: true })
  emitThreadEvent({ type: 'new_thread_message', roomId: 'r1', message: { id: 'm9', content: 'hi' } })
  expect(await screen.findByText('hi')).toBeInTheDocument()
})

// The follower lane and the view lane both deliver to a follower who has the
// panel open; the reducer's id guard must keep that to one rendered reply.
it('renders a reply delivered on both lanes once', async () => {
  const { emitThreadEvent, emitUserRoomEvent, screen } = renderThreadHarness()
  await openThreadPanel({ roomId: 'r1', parentId: 'p1', crossSite: true })
  const evt = { type: 'new_thread_message', roomId: 'r1', message: { id: 'm9', content: 'hi' } }
  emitUserRoomEvent(evt)
  emitThreadEvent(evt)
  expect(await screen.findAllByText('hi')).toHaveLength(1)
})

it('resubscribes to the new thread when the panel switches parents', async () => {
  const { subscribe, unsubscribe } = renderThreadHarness()
  await openThreadPanel({ roomId: 'r1', parentId: 'p1', crossSite: true })
  await openThreadPanel({ roomId: 'r1', parentId: 'p2', crossSite: true })
  expect(unsubscribe).toHaveBeenCalledTimes(1)
  expect(subscribe).toHaveBeenLastCalledWith('chat.room.r1.thread.p2.event', expect.any(Function))
})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd chat-frontend && npx vitest run src/context/ThreadEventsContext`
Expected: FAIL — `subscribe` never called with a thread subject.

- [ ] **Step 3: Write minimal implementation**

In `ThreadEventsContext.jsx` add a subscription ref and a teardown helper:

```jsx
  const threadSubRef = useRef(null)

  const closeThreadSub = useCallback(() => {
    threadSubRef.current?.unsubscribe()
    threadSubRef.current = null
  }, [])
```

In `openThread`, after `dispatch({ type: 'OPEN_THREAD', parent })` and before `fetchThreadMessages`:

```jsx
      closeThreadSub()
      if (!user) return
      // Subscribe before fetching: a reply landing in between would be lost.
      threadSubRef.current = subscribeToThreadEvents(
        nats,
        { roomId: parent.roomId, parentMessageId: parent.messageId, crossSite: parent.crossSite },
        (evt) => {
          if (evt?.type === 'new_thread_message') {
            dispatch({ type: 'THREAD_REPLY_RECEIVED', parentId: parent.messageId, message: evt.message })
          } else if (evt?.type === 'message_edited') {
            dispatch({
              type: 'REPLY_EDITED',
              messageId: evt.messageId,
              content: evt.newContent ?? '',
              editedAt: evt.editedAt,
            })
          } else if (evt?.type === 'message_deleted') {
            dispatch({ type: 'REPLY_DELETED', messageId: evt.messageId })
          }
        },
      )
```

Note the existing `if (!user) return` guard moves above the subscribe so an unauthenticated open still short-circuits.

In `closeThread`, call `closeThreadSub()` before the dispatch. Add an unmount/logout teardown:

```jsx
  useEffect(() => closeThreadSub, [closeThreadSub])
```

In `ChatPage.jsx:84`, pass the room's locality through:

```jsx
    openThread({
      roomId: selectedRoom.id,
      siteId: selectedRoom.siteId,
      crossSite: selectedRoom.crossSite,
      messageId: msg.id,
      createdAtMs: new Date(msg.createdAt).getTime(),
    })
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd chat-frontend && npx vitest run src/context/ThreadEventsContext` — Expected: PASS
Run: `cd chat-frontend && npx vitest run` — Expected: full suite PASS

- [ ] **Step 5: Commit**

```bash
git add chat-frontend/src
git commit -m "feat(frontend): subscribe to the thread subject while a thread panel is open"
```

---

### Task 8: Client API documentation

**Files:**
- Modify: `docs/client-api.md` (§4.1 neighbourhood ~line 6507; Send Message triggered events ~line 6469; Edit ~line 3410; Delete ~line 3488)
- Modify: `docs/client-api/events.md`

- [ ] **Step 1: Add the thread-scoped subject section**

After §4.1, add §4.2 documenting:

- Subjects: `chat.room.{roomID}.thread.{parentMessageID}.event` and the `chat.local.room.…` form, selected by the room's `crossSite` exactly as `chat.room.{roomID}.event` is.
- Channel rooms only.
- Events carried: `new_thread_message`, `message_edited`, `message_deleted` — identical payloads to the per-follower `chat.user.{account}.event.room` delivery, including the `encryptedMessage` envelope.
- Client contract: subscribe on panel open **before** fetching, unsubscribe on close, dedupe by message id because a follower with the panel open receives both copies.

- [ ] **Step 2: Update the three triggered-event lists**

In Send Message, Edit Message, and Delete Message, extend the thread bullets to state that the event is additionally published to the thread-scoped subject. Keep the existing per-follower wording intact.

- [ ] **Step 3: Mirror into the derived events view**

Apply the same additions to `docs/client-api/events.md` so the derived view does not drift.

- [ ] **Step 4: Verify**

Run: `grep -n "thread.{parentMessageID}.event" docs/client-api.md docs/client-api/events.md`
Expected: both files match.

- [ ] **Step 5: Commit**

```bash
git add docs/client-api.md docs/client-api/events.md
git commit -m "docs(client-api): thread-scoped event subject for open thread panels"
```

---

### Task 9: Full verification

- [ ] **Step 1:** `make fmt`
- [ ] **Step 2:** `make lint` — Expected: clean
- [ ] **Step 3:** `make test` — Expected: all pass
- [ ] **Step 4:** `make test-integration SERVICE=broadcast-worker` — Expected: pass
- [ ] **Step 5:** `cd chat-frontend && npx vitest run` — Expected: pass
- [ ] **Step 6:** `make sast` — Expected: no medium+ findings
- [ ] **Step 7:** Commit any formatting fallout
