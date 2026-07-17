# message-worker Thread-Subscription OUTBOX Durability — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Route message-worker's cross-site `thread_subscription_upserted` event through the local OUTBOX stream so a destination-site outage delays-not-drops it within retention, matching the #410 membership-federation durability guarantee.

**Architecture:** Add `thread_subscription_upserted` to the OUTBOX concurrent partition (`pkg/outbox.ConcurrentEventTypes`), then change message-worker's `publishThreadSubInboxIfRemote` from a direct `InboxExternal` publish to the shared `outbox.Publish(...)` onto the local OUTBOX. `outbox-worker`'s per-destination concurrent consumer forwards it to the destination INBOX with `MaxDeliver=-1` — no code change in `outbox-worker` or `inbox-worker`, since both are event-type-generic.

**Tech Stack:** Go 1.25, NATS JetStream (`nats.go/jetstream`), `pkg/outbox`, `pkg/subject`, `pkg/model`, `go.uber.org/mock`, `stretchr/testify`, `testcontainers` (integration).

## Global Constraints

- Go 1.25; monorepo, single `go.mod` at root; services are flat `package main` at repo root.
- Always use `make` targets, never raw `go`: `make test SERVICE=<name>`, `make test-integration SERVICE=<name>`, `make lint`, `make sast`.
- TDD is mandatory: Red → Green → Refactor → Commit. Never write implementation before its failing test.
- Minimum 80% coverage per package (target 90% for handlers).
- All NATS payloads are typed structs from `pkg/model`, never `map[string]interface{}`.
- Never log tokens/bodies; structured `slog` key-value fields only.
- Error wrapping: `fmt.Errorf("short description: %w", err)`; never bare `err`.
- Commit message trailer (every commit):
  ```
  Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01FNMS2LV1evxmEZTKXxvFKX
  ```
- Work on branch `claude/gatekeeper-attachments-type-90o2hy`. Do not open a PR unless explicitly asked.
- No store-interface changes in this plan → `make generate` (mocks) is NOT required.
- No client-facing handler change → `docs/client-api.md` is NOT touched.

---

### Task 1: Add `thread_subscription_upserted` to the OUTBOX concurrent partition

**Files:**
- Modify: `pkg/outbox/outbox.go` (`ConcurrentEventTypes`, package + var docs)
- Test: `pkg/outbox/outbox_test.go` (retarget the rejection test; add a membership assertion)

**Interfaces:**
- Consumes: `model.InboxThreadSubscriptionUpserted` (existing constant = `"thread_subscription_upserted"`), `model.InboxUserStatusUpdated` (existing constant = `"user_status_updated"`, in neither partition set — used as the new "unrouted" example).
- Produces: `outbox.ConcurrentEventTypes` now contains `model.InboxThreadSubscriptionUpserted`, so `outbox.Publish(..., model.InboxThreadSubscriptionUpserted, ...)` is accepted rather than rejected. Task 2 relies on this.

- [ ] **Step 1: Update the rejection test to use a still-unrouted type, and assert the new type is now accepted+concurrent**

In `pkg/outbox/outbox_test.go`, replace `TestPublish_RejectsEventTypeOutsideThePartition` (currently uses `model.InboxThreadSubscriptionUpserted` as the rejected example — that stops being rejected after this task) and add a membership test:

```go
func TestPublish_RejectsEventTypeOutsideThePartition(t *testing.T) {
	// user_status_updated is in neither ConcurrentEventTypes nor OrderedEventTypes.
	err := Publish(context.Background(), func(context.Context, string, []byte, string) error { return nil },
		"site-a", "r1", "site-b", model.InboxUserStatusUpdated, []byte(`{}`), "d", 1)
	require.Error(t, err,
		"an event type with no outbox-worker filter would sit in the stream unconsumed — must fail fast at the publish site")
	assert.Contains(t, err.Error(), "filter set")
}

func TestThreadSubscriptionUpsertedIsConcurrent(t *testing.T) {
	assert.Contains(t, ConcurrentEventTypes, model.InboxThreadSubscriptionUpserted,
		"message-worker federates thread_subscription_upserted; it is order-insensitive at the destination "+
			"(inbox-worker upsert is $setOnInsert + $max hasMention) so it rides the concurrent lane")
	assert.NotContains(t, OrderedEventTypes, model.InboxThreadSubscriptionUpserted)
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make test SERVICE=outbox` — no; `pkg/outbox` is a package, run:
```bash
go test ./pkg/outbox/ -run 'TestPublish_RejectsEventTypeOutsideThePartition|TestThreadSubscriptionUpsertedIsConcurrent' -v
```
Expected: `TestThreadSubscriptionUpsertedIsConcurrent` FAILs (`ConcurrentEventTypes` does not yet contain the type). `TestPublish_RejectsEventTypeOutsideThePartition` PASSes already (user_status_updated is unrouted) — that's fine.

- [ ] **Step 3: Add the type to `ConcurrentEventTypes` and update docs**

In `pkg/outbox/outbox.go`, add the entry to the slice and refresh the two doc comments:

```go
// Package outbox is the cross-site federation relay contract shared by the
// producers (room-service, room-worker, message-worker) and the consumer
// (outbox-worker): which event types ride which OUTBOX consumer lane, and the
// one way to publish a relay event onto the stream.
package outbox
```

```go
// ConcurrentEventTypes are the OUTBOX event types forwarded by outbox-worker's
// shared concurrent consumer. They are order-insensitive at the destination
// (inbox-worker applies them under high-water-mark / idempotent-upsert guards),
// so parallel forwarding is safe.
var ConcurrentEventTypes = []model.InboxEventType{
	model.InboxRoleUpdated,
	model.InboxSubscriptionRead,
	model.InboxThreadRead,
	model.InboxSubscriptionMuteToggled,
	model.InboxSubscriptionFavoriteToggled,
	model.InboxRoomRestricted,
	// message-worker: thread-subscription federation. Order-insensitive —
	// inbox-worker's UpsertThreadSubscription is $setOnInsert (immutable identity)
	// + $max hasMention (monotonic), so out-of-order/duplicate applies converge.
	model.InboxThreadSubscriptionUpserted,
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run:
```bash
go test ./pkg/outbox/ -v
```
Expected: PASS (including `TestEventTypeSetsAreDisjoint`, `TestThreadSubscriptionUpsertedIsConcurrent`, and the existing `Publish` tests).

- [ ] **Step 5: Commit**

```bash
git add pkg/outbox/outbox.go pkg/outbox/outbox_test.go
git commit -m "feat(outbox): add thread_subscription_upserted to concurrent partition

message-worker will federate thread subscriptions through the OUTBOX relay;
route the event on the order-insensitive concurrent lane.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01FNMS2LV1evxmEZTKXxvFKX"
```

---

### Task 2: Route message-worker thread-subscription federation through OUTBOX

**Files:**
- Modify: `message-worker/handler.go` (`publishThreadSubInboxIfRemote`, imports)
- Test: `message-worker/handler_test.go` (add `unwrapOutbox` helper; retarget 5 decode sites + subject assertion)

**Interfaces:**
- Consumes: `outbox.Publish(ctx, publish func(ctx,string,[]byte,string) error, originSiteID, roomID, destSiteID string, eventType model.InboxEventType, payload []byte, dedupID string, ts int64) error` (from Task 1's accepted type); `subject.Outbox(origin, dest, eventType) string`; `model.OutboxEvent{RoomID string, Envelope json.RawMessage, DedupID string, Timestamp int64}`.
- Produces: `publishThreadSubInboxIfRemote` now publishes an `OutboxEvent` (wrapping the `InboxEvent`) to `chat.outbox.{origin}.{dest}.thread_subscription_upserted` with the dedup ID as `Nats-Msg-Id`. Signature unchanged: `func (h *Handler) publishThreadSubInboxIfRemote(ctx context.Context, sub *model.ThreadSubscription, ownerSiteID, msgID string) error`.

- [ ] **Step 1: Add the `unwrapOutbox` test helper**

Append to `message-worker/handler_test.go` (near the other test helpers). `json` and `model` are already imported by this file.

```go
// unwrapOutbox decodes an OutboxEvent published on the OUTBOX relay and the
// inner InboxEvent envelope it carries. Thread-subscription federation rides
// the durable OUTBOX (chat.outbox.…), not a direct INBOX publish.
func unwrapOutbox(t *testing.T, data []byte) (model.OutboxEvent, model.InboxEvent) {
	t.Helper()
	var relay model.OutboxEvent
	require.NoError(t, json.Unmarshal(data, &relay))
	var env model.InboxEvent
	require.NoError(t, json.Unmarshal(relay.Envelope, &env))
	return relay, env
}
```

- [ ] **Step 2: Retarget the unit tests to expect the OUTBOX subject + envelope**

In `message-worker/handler_test.go`, `TestHandler_PublishThreadSubInboxIfRemote`, "remote owner …" subtest — replace the subject assertion and the decode block (currently lines ~1364 and ~1389-1399):

Replace:
```go
		assert.Equal(t, "chat.inbox.site-b.external.thread_subscription_upserted", captured.subj)
```
with:
```go
		assert.Equal(t, subject.Outbox("site-a", "site-b", model.InboxThreadSubscriptionUpserted), captured.subj)
		// = "chat.outbox.site-a.site-b.thread_subscription_upserted"
```

Replace the `var outer model.InboxEvent … json.Unmarshal(captured.data, &outer) …` block through the inner `ThreadSubscription` assertion with:
```go
		// The captured data is an OutboxEvent wrapping the InboxEvent envelope.
		relay, outer := unwrapOutbox(t, captured.data)
		assert.Equal(t, "r1", relay.RoomID, "OutboxEvent.RoomID is the channel room ID")
		assert.Equal(t, captured.msgID, relay.DedupID, "OutboxEvent.DedupID equals the publish Nats-Msg-Id")
		assert.Equal(t, model.InboxThreadSubscriptionUpserted, outer.Type)
		assert.Equal(t, "site-a", outer.SiteID)
		assert.Equal(t, "site-b", outer.DestSiteID)
		assert.Greater(t, outer.Timestamp, int64(0))

		var inner model.ThreadSubscription
		require.NoError(t, json.Unmarshal(outer.Payload, &inner))
		assert.Equal(t, *baseSub, inner)
		assert.Equal(t, "site-a", inner.SiteID, "inner SiteID stays as the room's site")
```

Now retarget the four remaining `var outer model.InboxEvent` decode sites of thread-sub publishes:

**Site A — `TestHandler_FirstReply_InboxPublishes` loop** (currently ~1501-1504), replace:
```go
			for _, c := range calls {
				var outer model.InboxEvent
				require.NoError(t, json.Unmarshal(c.data, &outer))
				assert.Equal(t, model.InboxThreadSubscriptionUpserted, outer.Type)
				gotByDest[outer.DestSiteID]++
			}
```
with:
```go
			for _, c := range calls {
				_, outer := unwrapOutbox(t, c.data)
				assert.Equal(t, model.InboxThreadSubscriptionUpserted, outer.Type)
				gotByDest[outer.DestSiteID]++
			}
```

**Sites B & C — inside publish closures** (currently ~1635-1641 and ~1771-1777). Both closures return `error`, so decode the two layers inline (identical code at both sites). Replace each:
```go
				var outer model.InboxEvent
				if err := json.Unmarshal(data, &outer); err != nil {
					return err
				}
				publishedDests = append(publishedDests, outer.DestSiteID)
				return nil
```
with:
```go
				var relay model.OutboxEvent
				if err := json.Unmarshal(data, &relay); err != nil {
					return err
				}
				var outer model.InboxEvent
				if err := json.Unmarshal(relay.Envelope, &outer); err != nil {
					return err
				}
				publishedDests = append(publishedDests, outer.DestSiteID)
				return nil
```

**Site D — mention-carrying single decode** (currently ~1845-1848), replace:
```go
	var outer model.InboxEvent
	require.NoError(t, json.Unmarshal(captured, &outer))
	var sub model.ThreadSubscription
	require.NoError(t, json.Unmarshal(outer.Payload, &sub))
```
with:
```go
	_, outer := unwrapOutbox(t, captured)
	var sub model.ThreadSubscription
	require.NoError(t, json.Unmarshal(outer.Payload, &sub))
```

- [ ] **Step 3: Run the message-worker tests to verify they fail**

Run:
```bash
make test SERVICE=message-worker
```
Expected: FAIL — the handler still publishes to `chat.inbox.…` with a bare `InboxEvent`, so `unwrapOutbox` (expecting an `OutboxEvent` with a non-empty `Envelope`) and the new subject assertion fail.

- [ ] **Step 4: Rewrite `publishThreadSubInboxIfRemote` to publish via OUTBOX**

In `message-worker/handler.go`, add the import (keep the block goimports-sorted):
```go
	"github.com/hmchangw/chat/pkg/outbox"
```

Replace the whole `publishThreadSubInboxIfRemote` function (doc comment + body) with:

```go
// publishThreadSubInboxIfRemote federates a thread_subscription_upserted event
// to ownerSiteID when that site differs from the local site, via the durable
// OUTBOX relay (outbox-worker forwards it to the destination INBOX with
// retry-forever, so a destination outage delays — never drops — the event
// within retention). Same-site is a no-op; empty ownerSiteID is a no-op that
// logs a warning (caller bug). ownerSiteID is the subscription owner's home
// site — NOT sub.SiteID, which is the room's home site.
func (h *Handler) publishThreadSubInboxIfRemote(ctx context.Context, sub *model.ThreadSubscription, ownerSiteID, msgID string) error {
	if ownerSiteID == "" {
		slog.WarnContext(ctx, "owner siteID empty, skipping outbox publish",
			"threadRoomID", sub.ThreadRoomID, "user_id", sub.UserID, "msgID", msgID,
			"request_id", natsutil.RequestIDFromContext(ctx))
		return nil
	}
	// outbox.Publish also no-ops a local destination, but short-circuit here so the
	// marshal below is skipped on the common same-site path.
	if ownerSiteID == h.siteID {
		return nil
	}

	payload, err := sonic.Marshal(sub)
	if err != nil {
		return fmt.Errorf("marshal thread subscription: %w", err)
	}
	// Dedup-ID seed (threadRoomID + userID + msg.ID + hasMention + destSiteID):
	// msg.ID is stable across MESSAGES_CANONICAL redeliveries so the same publish
	// yields the same ID; different users on the same destination differ via userID;
	// hasMention is in the seed so a HasMention=false upsert and a later
	// HasMention=true update get distinct dedup IDs (else stream-level dedup would
	// swallow the mention update). It rides the OUTBOX publish as its Nats-Msg-Id
	// AND the forward's Nats-Msg-Id at the destination.
	dedupID := fmt.Sprintf("thread-sub-inbox:%s:%s:%s:%t:%s", sub.ThreadRoomID, sub.UserID, msgID, sub.HasMention, ownerSiteID)
	if err := outbox.Publish(ctx, h.publish, h.siteID, sub.RoomID, ownerSiteID,
		model.InboxThreadSubscriptionUpserted, payload, dedupID, time.Now().UTC().UnixMilli()); err != nil {
		return fmt.Errorf("publish thread subscription outbox to %s: %w", ownerSiteID, err)
	}
	return nil
}
```

Note: `subject` is still imported/used by `publishThreadReplyEvent` (`subject.ServerBroadcastThreadTCount`); `sonic`, `time`, `fmt`, `slog`, `natsutil` all remain used. `subject.InboxExternal` is no longer referenced from this function — confirm no other `handler.go` use remains (Step 5 grep).

- [ ] **Step 5: Verify no dangling `InboxExternal` reference and imports are clean**

Run:
```bash
grep -n "InboxExternal" message-worker/handler.go || echo "no InboxExternal in handler.go — good"
```
Expected: `no InboxExternal in handler.go — good`.

- [ ] **Step 6: Run the message-worker tests to verify they pass**

Run:
```bash
make test SERVICE=message-worker
```
Expected: PASS (all subtests, including same-site no-publish, empty-owner warn, dedup determinism, HasMention flip, first/subsequent-reply dest counts, and the NAK-on-publish-error path — the error still propagates because `outbox.Publish` returns the publish closure's error).

- [ ] **Step 7: Commit**

```bash
git add message-worker/handler.go message-worker/handler_test.go
git commit -m "feat(message-worker): federate thread subscriptions via OUTBOX

Replace the direct InboxExternal publish in publishThreadSubInboxIfRemote
with the durable outbox.Publish relay, so a destination-site outage delays
(MaxDeliver=-1) rather than drops the cross-site thread subscription —
matching #410 membership federation durability. Envelope bytes and dedup
ID are unchanged; the event rides the order-insensitive concurrent lane.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01FNMS2LV1evxmEZTKXxvFKX"
```

---

### Task 3: Integration test — thread_subscription_upserted forwarded via the concurrent lane

**Files:**
- Test: `outbox-worker/integration_test.go` (add one round-trip test with the new type)

**Interfaces:**
- Consumes: existing test helpers in the package — `startEmbeddedJetStreamNATS(t)`, `createStream(t, js, cfg)`, `buildConcurrentConsumerConfig(settings, siteID, destSiteID)`, `NewHandler(jsPublish(js))`, `stream.Outbox(siteID)`, `stream.Inbox(destSiteID)`, `subject.Outbox(...)`, `subject.InboxExternal(...)`.
- Produces: end-to-end proof that adding `thread_subscription_upserted` to `ConcurrentEventTypes` makes `buildConcurrentConsumerConfig`'s FilterSubjects include it, so outbox-worker consumes and forwards it.

- [ ] **Step 1: Write the failing integration test**

Append to `outbox-worker/integration_test.go`, modeled on `TestIntegration_OutboxRoundTrip`:

```go
// TestIntegration_ThreadSubscriptionUpsertedForwardedViaConcurrentLane proves the
// message-worker durability fix end-to-end: a thread_subscription_upserted relay
// event on the OUTBOX is consumed by the per-destination concurrent consumer
// (whose FilterSubjects derive from outbox.ConcurrentEventTypes) and forwarded to
// the destination INBOX verbatim.
func TestIntegration_ThreadSubscriptionUpsertedForwardedViaConcurrentLane(t *testing.T) {
	ctx := context.Background()

	const (
		siteID     = "site-ts-origin"
		destSiteID = "site-ts-dest"
		roomID     = "room-ts-roundtrip"
	)

	nc := startEmbeddedJetStreamNATS(t)
	js, err := jetstream.New(nc)
	require.NoError(t, err)

	outboxCfg := stream.Outbox(siteID)
	createStream(t, js, outboxCfg)
	createStream(t, js, stream.Inbox(destSiteID))

	destSubject := subject.InboxExternal(destSiteID, model.InboxThreadSubscriptionUpserted)
	type received struct {
		subject string
		data    []byte
	}
	var mu sync.Mutex
	var forwarded []received
	sub, err := nc.Subscribe(destSubject, func(msg *nats.Msg) {
		mu.Lock()
		forwarded = append(forwarded, received{subject: msg.Subject, data: append([]byte(nil), msg.Data...)})
		mu.Unlock()
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	require.NoError(t, nc.Flush())

	h := NewHandler(jsPublish(js))

	innerPayload := []byte(`{"id":"sub-1","threadRoomId":"tr-1","userId":"u-bob","userAccount":"bob","roomId":"room-ts-roundtrip","siteId":"site-ts-origin","hasMention":false}`)
	envelope, err := json.Marshal(model.InboxEvent{
		Type:       model.InboxThreadSubscriptionUpserted,
		SiteID:     siteID,
		DestSiteID: destSiteID,
		Payload:    innerPayload,
		Timestamp:  time.Now().UTC().UnixMilli(),
	})
	require.NoError(t, err)

	const dedupID = "thread-sub-inbox:tr-1:u-bob:msg-1:false:site-ts-dest"
	relayEvt, err := json.Marshal(model.OutboxEvent{
		RoomID:    roomID,
		Envelope:  envelope,
		DedupID:   dedupID,
		Timestamp: time.Now().UTC().UnixMilli(),
	})
	require.NoError(t, err)

	cons, err := js.CreateOrUpdateConsumer(ctx, outboxCfg.Name, buildConcurrentConsumerConfig(stream.ConsumerSettings{
		AckWait: 30 * time.Second, MaxDeliver: 5, MaxWaiting: 512, MaxAckPending: 1000,
	}, siteID, destSiteID))
	require.NoError(t, err)

	cc, err := cons.Consume(func(msg jetstream.Msg) {
		if err := h.HandleEvent(ctx, msg.Subject(), msg.Data()); err != nil {
			t.Errorf("HandleEvent: %v", err)
		}
		_ = msg.Ack()
	})
	require.NoError(t, err)
	t.Cleanup(cc.Stop)

	_, err = js.Publish(ctx, subject.Outbox(siteID, destSiteID, model.InboxThreadSubscriptionUpserted), relayEvt)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(forwarded) >= 1
	}, 5*time.Second, 20*time.Millisecond, "expected one forwarded INBOX publish on the destination subject")

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, forwarded, 1, "exactly one forward per target")
	assert.Equal(t, destSubject, forwarded[0].subject)
	assert.Equal(t, envelope, forwarded[0].data,
		"forwarded bytes must equal the target's pre-marshaled Envelope verbatim")
}
```

- [ ] **Step 2: Run the integration test to verify it passes (Docker required)**

Run:
```bash
make test-integration SERVICE=outbox-worker
```
Expected: PASS, including the new `TestIntegration_ThreadSubscriptionUpsertedForwardedViaConcurrentLane`. (If run before Task 1's set change, the concurrent consumer would have no filter for the type and the forward would never arrive → `Eventually` times out; with Task 1 applied it passes.)

- [ ] **Step 3: Commit**

```bash
git add outbox-worker/integration_test.go
git commit -m "test(outbox-worker): forward thread_subscription_upserted via concurrent lane

End-to-end coverage that the new concurrent-partition type is consumed by the
per-destination concurrent consumer and forwarded to the destination INBOX.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01FNMS2LV1evxmEZTKXxvFKX"
```

---

### Task 4: Full verification

**Files:** none (verification only; commit any fixups the tools require).

- [ ] **Step 1: Lint**

Run:
```bash
make lint
```
Expected: no findings. If goimports reorders the new `pkg/outbox` import in `message-worker/handler.go`, run `make fmt` and re-lint.

- [ ] **Step 2: Unit tests (changed packages + full suite)**

Run:
```bash
go test ./pkg/outbox/ -race
make test SERVICE=message-worker
make test
```
Expected: all PASS.

- [ ] **Step 3: Coverage floor on changed packages**

Run:
```bash
go test ./pkg/outbox/ -coverprofile=/tmp/cov-outbox.out && go tool cover -func=/tmp/cov-outbox.out | tail -1
go test ./message-worker/ -coverprofile=/tmp/cov-mw.out && go tool cover -func=/tmp/cov-mw.out | tail -1
```
Expected: both ≥ 80% (message-worker handler path should stay ≥ 90%). If `publishThreadSubInboxIfRemote` branches are under-covered, the Task 2 subtests already cover same-site / empty-owner / remote / error — confirm none were dropped.

- [ ] **Step 4: Integration (outbox-worker)**

Run:
```bash
make test-integration SERVICE=outbox-worker
```
Expected: PASS.

- [ ] **Step 5: SAST**

Run:
```bash
make sast
```
Expected: no medium+ findings introduced (this change adds no new unsafe conversions, TLS, or exec).

- [ ] **Step 6: Push the branch**

```bash
git push -u origin claude/gatekeeper-attachments-type-90o2hy
```
(Do NOT open a PR unless explicitly asked.)

---

## Self-Review

**Spec coverage:**
- Spec §3.1 (concurrent lane) → Task 1 (`TestThreadSubscriptionUpsertedIsConcurrent`, doc rationale).
- Spec §3.2 (producer reuses `outbox.Publish`, preserved payload/dedup/guards) → Task 2 (rewrite + retargeted unit tests).
- Spec §3.3 (outbox-worker no change) → Task 3 verifies forwarding with zero outbox-worker prod change.
- Spec §3.4 (inbox-worker no change) → no task needed; Task 3 exercises the destination path unchanged.
- Spec §3.5 (contract bookkeeping) → Task 1 Step 3 (set + package/var docs).
- Spec §4/§5 (durability + failure-path improvement) → behavioral, covered by Task 2's error-path test (`NAK`) + Task 3's forward test.
- Spec §6 (testing) → Tasks 1–3 unit + integration; Task 4 coverage floor.
- Spec §7 (out-of-scope: reconciliation, client-api docs) → none; asserted in Global Constraints.
- Spec §8 (files touched) → matches Tasks 1–3 exactly; `main.go`/`bootstrap.go`/`outbox-worker`/`inbox-worker` prod code untouched.

**Placeholder scan:** none — every code and command step is concrete.

**Type consistency:** `outbox.Publish` arg order matches `pkg/outbox/outbox.go` (`ctx, publish, originSiteID, roomID, destSiteID, eventType, payload, dedupID, ts`); `model.OutboxEvent` fields (`RoomID`, `Envelope`, `DedupID`, `Timestamp`) and `model.InboxEvent` fields (`Type`, `SiteID`, `DestSiteID`, `Payload`, `Timestamp`) match `pkg/model/event.go`; `unwrapOutbox` returns `(model.OutboxEvent, model.InboxEvent)` and is used consistently; dedup seed string is byte-identical to the pre-change formula.
