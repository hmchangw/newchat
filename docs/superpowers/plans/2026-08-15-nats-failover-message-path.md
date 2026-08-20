# NATS Failover — Message Path Standby Lanes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** When a site's NATS cluster is down, its users can still send and receive messages — validated, persisted, broadcast, notified, and indexed by that site's own services, against that site's own databases, through standby streams on its buddy cluster.

**Architecture:** Four more standby streams beside Plan 1's `INBOX-FAILOVER`, all on the buddy: ingress, canonical, push, and outbox. Every pipeline service opens the same non-fatal buddy connection Plan 1 introduced and binds a second consumer there. The two lanes drain concurrently with no mode flag, so recovery needs no intervention.

**Tech Stack:** Go 1.25, `nats.go` + `jetstream`, Cassandra (`gocql`), MongoDB (`mongo-driver/v2`), Valkey, Elasticsearch, testcontainers, testify.

**Design spec:** `docs/superpowers/specs/2026-08-15-nats-site-failover-design.md` (§B subjects, §C streams + placement, §D dual connections, §H federation).

**Depends on Plan 1** (`2026-08-15-nats-failover-inbound-redirect.md`) being merged. It provides `natsutil.ConnectBuddy`, `stream.CheckPlacement`, `testutil.NATSPair`, and the `chat.failover.>` subject root.

## Global Constraints

- Go 1.25. Single `go.mod` at repo root. No new third-party dependencies.
- All commands via `make` targets — never raw `go` commands.
- TDD is mandatory: write the failing test, run it, confirm it fails, then implement.
- `make lint` and `make test` are enforced by a pre-commit hook.
- Error wrapping: `fmt.Errorf("short description: %w", err)`. Never bare `err`.
- Logging: `log/slog` structured key-value pairs only. Never log tokens or full message bodies.
- Integration tests use `//go:build integration`, live in the same package, containers from `pkg/testutil`.
- Stream creation is gated by `BOOTSTRAP_STREAMS`; production verifies rather than creates.
- Subject construction always goes through `pkg/subject` builders, never `fmt.Sprintf` at a call site.
- Hot-path services (`message-gatekeeper`, `message-worker`, `broadcast-worker`, `notification-worker`) marshal via `github.com/bytedance/sonic`, not `encoding/json`. Do not change a service's codec.

## Out of scope for this plan

- **Forced global room routing and the revert grace window** (spec §E). That is
  the subtle correctness half and gets its own plan, because it is reviewable
  independently and touches `broadcast-worker` and `room-service` for a
  different reason than the lane wiring here. Until it lands, a displaced client
  receives events only for cross-site rooms — the lanes work, the delivery is
  incomplete. **This plan is therefore not shippable to users on its own**; it is
  shippable to `main` as inert infrastructure.
- **Client failover and the portal peer list.** Separate plan.
- **The bot pipeline.** `stream.Resolve` has a `PipelineBot` branch with its own
  `BOT-MESSAGES-CANONICAL` / `BOT-PUSH-NOTIFICATION` streams. Failover covers the
  user pipeline only; the bot branch returns empty failover configs and services
  skip the lane when they are empty. Deliberate: bots are not displaced users.
- **`ROOMS-FAILOVER`.** Deferred in the spec.
- **Direct publishers** (`message-worker`'s and `user-service`'s
  `InboxExternal` publishes). Task 10 covers `room-service` → OUTBOX only;
  the direct publishers' three-argument publish signature still needs the audit
  Plan 1 deferred.

---

## File Structure

| File | Responsibility |
|------|----------------|
| `pkg/subject/subject.go` | Failover builders for msg-send, canonical, push, outbox |
| `pkg/stream/stream.go` | `MessagesFailover`, `MessagesCanonicalFailover`, `PushNotificationFailover`, `OutboxFailover` |
| `pkg/stream/pipeline.go` | Failover fields on `Wiring`, populated by `Resolve` |
| `pkg/stream/failover.go` | New — `EnsureFailoverStream`: bootstrap-or-verify plus placement assertion |
| `message-gatekeeper/main.go` | Consume ingress failover, publish canonical failover |
| `message-worker/main.go` | Consume canonical failover |
| `broadcast-worker/main.go` | Consume canonical failover |
| `notification-worker/main.go` | Consume canonical failover, publish push failover |
| `search-sync-worker/main.go` | Consume canonical failover |
| `push-notification-service/main.go` | Consume push failover |
| `room-service/*` | Publish OUTBOX events to the failover lane |
| `outbox-worker/main.go` | Consume `OUTBOX-FAILOVER` with the same two-consumer partition |
| `*/deploy/docker-compose.yml` | `BUDDY_SITE_ID` / `BUDDY_NATS_URL` per service |
| `docs/nats-subject-naming.md` | Document the four new failover subjects |

---

### Task 1: Message-path failover subject builders

**Files:**
- Modify: `pkg/subject/subject.go`
- Test: `pkg/subject/subject_test.go`

**Interfaces:**
- Consumes: nothing (Plan 1 established the `chat.failover.>` root).
- Produces: `FailoverMsgSend(account, roomID, siteID) string`, `FailoverMsgSendWildcard(siteID) string`, `FailoverMsgCanonicalCreated(siteID) string`, `FailoverMsgCanonicalWildcard(siteID) string`, `FailoverPushNotification(siteID) string`, `FailoverPushNotificationFilter(siteID) string`, `FailoverOutbox(originSiteID, destSiteID, eventType) string`, `FailoverOutboxWildcard(originSiteID) string`.

The msg-send subject is the only **client-facing** one, so it must stay inside
`chat.user.{account}.>` or the JWT `auth-service` mints will not permit
publishing it. The others are service-to-service and live under
`chat.failover.>`.

- [ ] **Step 1: Write the failing test**

Add to `pkg/subject/subject_test.go`:

```go
func TestFailoverMessagePathSubjects(t *testing.T) {
	assert.Equal(t, "chat.user.alice.room.r1.site-a.failover.msg.send",
		subject.FailoverMsgSend("alice", "r1", "site-a"))
	assert.Equal(t, "chat.user.*.room.*.site-a.failover.msg.>",
		subject.FailoverMsgSendWildcard("site-a"))
	assert.Equal(t, "chat.failover.msg.canonical.site-a.created",
		subject.FailoverMsgCanonicalCreated("site-a"))
	assert.Equal(t, "chat.failover.msg.canonical.site-a.>",
		subject.FailoverMsgCanonicalWildcard("site-a"))
	assert.Equal(t, "chat.failover.push.site-a.send",
		subject.FailoverPushNotification("site-a"))
	assert.Equal(t, "chat.failover.push.site-a.>",
		subject.FailoverPushNotificationFilter("site-a"))
	assert.Equal(t, "chat.failover.outbox.site-a.site-b.member_added",
		subject.FailoverOutbox("site-a", "site-b", "member_added"))
	assert.Equal(t, "chat.failover.outbox.site-a.>",
		subject.FailoverOutboxWildcard("site-a"))
}

// The client-facing failover send subject must stay inside the account's JWT
// scope, or auth-service would have to widen permissions.
func TestFailoverMsgSend_StaysInAccountScope(t *testing.T) {
	assert.True(t, strings.HasPrefix(subject.FailoverMsgSend("alice", "r1", "site-a"), "chat.user.alice."))
}

// Every failover filter must be disjoint from every live filter: two streams in
// one account may not claim overlapping subjects, supercluster-wide.
func TestFailoverMessagePath_DisjointFromLiveFilters(t *testing.T) {
	pairs := []struct{ failover, live string }{
		{subject.FailoverMsgSendWildcard("site-a"), subject.MsgSendWildcard("site-a")},
		{subject.FailoverMsgCanonicalWildcard("site-a"), subject.MsgCanonicalWildcard("site-a")},
		{subject.FailoverPushNotificationFilter("site-a"), subject.PushNotificationFilter("site-a")},
		{subject.FailoverOutboxWildcard("site-a"), subject.OutboxWildcard("site-a")},
	}
	for _, p := range pairs {
		assert.False(t, subjectsOverlap(p.failover, p.live),
			"failover %q must not overlap live %q", p.failover, p.live)
	}
}

// None may sit under chat.local.>, which is filtered from gateway interest.
func TestFailoverMessagePath_NotUnderLocalRoot(t *testing.T) {
	for _, s := range []string{
		subject.FailoverMsgSendWildcard("site-a"),
		subject.FailoverMsgCanonicalWildcard("site-a"),
		subject.FailoverPushNotificationFilter("site-a"),
		subject.FailoverOutboxWildcard("site-a"),
	} {
		assert.False(t, strings.HasPrefix(s, "chat.local."), "%q must not be under chat.local.", s)
	}
}
```

`subjectsOverlap` is the helper added by Plan 1 Task 1; reuse it.

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=subject`

Expected: FAIL — `undefined: subject.FailoverMsgSend`.

- [ ] **Step 3: Write minimal implementation**

Add to `pkg/subject/subject.go`, beside the existing failover inbox builders:

```go
// FailoverMsgSend is the subject a displaced client publishes a message on while
// its home site's NATS is down: the live MsgSend subject with a `failover` token
// inserted before `msg`. Captured by MESSAGES-FAILOVER-{siteID} on the site's
// buddy cluster.
//
// Deliberately still inside chat.user.{account}.> so the JWT auth-service mints
// already permits it — no permission change. The live filter is
// chat.user.*.room.*.{siteID}.msg.>, which does not match `.failover.msg.` at
// that position, so the two stream filters are disjoint.
func FailoverMsgSend(account, roomID, siteID string) string {
	return fmt.Sprintf("chat.user.%s.room.%s.%s.failover.msg.send", EncodeAccount(account), roomID, siteID)
}

// FailoverMsgSendWildcard is the MESSAGES-FAILOVER-{siteID} stream's subject.
func FailoverMsgSendWildcard(siteID string) string {
	return fmt.Sprintf("chat.user.*.room.*.%s.failover.msg.>", siteID)
}

// FailoverMsgCanonicalCreated is where message-gatekeeper publishes a validated
// message on the failover lane.
func FailoverMsgCanonicalCreated(siteID string) string {
	return fmt.Sprintf("chat.failover.msg.canonical.%s.created", siteID)
}

// FailoverMsgCanonicalWildcard is the MESSAGES-CANONICAL-FAILOVER-{siteID}
// stream's subject and its consumers' filter.
func FailoverMsgCanonicalWildcard(siteID string) string {
	return fmt.Sprintf("chat.failover.msg.canonical.%s.>", siteID)
}

// FailoverPushNotification is where notification-worker publishes a push request
// on the failover lane.
func FailoverPushNotification(siteID string) string {
	return fmt.Sprintf("chat.failover.push.%s.send", siteID)
}

// FailoverPushNotificationFilter is the PUSH-NOTIFICATION-FAILOVER-{siteID}
// stream's subject and its consumer's filter.
func FailoverPushNotificationFilter(siteID string) string {
	return fmt.Sprintf("chat.failover.push.%s.>", siteID)
}

// FailoverOutbox mirrors Outbox on the failover lane: destination and event type
// ride the subject so outbox-worker's per-destination consumers can filter on
// one peer exactly as they do on the live stream.
func FailoverOutbox(originSiteID, destSiteID, eventType string) string {
	return fmt.Sprintf("chat.failover.outbox.%s.%s.%s", originSiteID, destSiteID, eventType)
}

// FailoverOutboxWildcard is the OUTBOX-FAILOVER-{originSiteID} stream's subject.
func FailoverOutboxWildcard(originSiteID string) string {
	return fmt.Sprintf("chat.failover.outbox.%s.>", originSiteID)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=subject`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/subject/subject.go pkg/subject/subject_test.go
git commit -m "feat(subject): add message-path failover subject builders"
```

---

### Task 2: Message-path failover stream configs and Wiring

**Files:**
- Modify: `pkg/stream/stream.go`
- Modify: `pkg/stream/pipeline.go:48-67`
- Test: `pkg/stream/stream_test.go`, `pkg/stream/pipeline_test.go`

**Interfaces:**
- Consumes: Task 1's builders.
- Produces: `stream.MessagesFailover(siteID) Config`, `stream.MessagesCanonicalFailover(siteID) Config`, `stream.PushNotificationFailover(siteID) Config`, `stream.OutboxFailover(siteID) Config`; `Wiring.CanonicalFailoverStream Config`, `Wiring.CanonicalFailoverCreated string`, `Wiring.CanonicalFailoverWildcard string`, `Wiring.PushFailoverStream Config`, `Wiring.PushFailoverSendSubject string`, `Wiring.PushFailoverInputWildcard string`, and `Wiring.HasFailover() bool`.

Three services read their stream wiring from `Resolve`, so putting the failover
variants there means each of them changes in one place instead of three.
`PipelineBot` leaves the failover fields zero and `HasFailover` reports false, so
a bot-mode service skips the lane without a special case at every call site.

- [ ] **Step 1: Write the failing test**

Add to `pkg/stream/stream_test.go`:

```go
func TestMessagePathFailoverStreams(t *testing.T) {
	tests := []struct {
		name     string
		got      stream.Config
		wantName string
		wantSubj string
	}{
		{"messages", stream.MessagesFailover("site-a"), "MESSAGES-FAILOVER-site-a",
			"chat.user.*.room.*.site-a.failover.msg.>"},
		{"canonical", stream.MessagesCanonicalFailover("site-a"), "MESSAGES-CANONICAL-FAILOVER-site-a",
			"chat.failover.msg.canonical.site-a.>"},
		{"push", stream.PushNotificationFailover("site-a"), "PUSH-NOTIFICATION-FAILOVER-site-a",
			"chat.failover.push.site-a.>"},
		{"outbox", stream.OutboxFailover("site-a"), "OUTBOX-FAILOVER-site-a",
			"chat.failover.outbox.site-a.>"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wantName, tt.got.Name)
			assert.Equal(t, []string{tt.wantSubj}, tt.got.Subjects)
		})
	}
}
```

Add to `pkg/stream/pipeline_test.go`:

```go
func TestResolve_UserPipelineHasFailover(t *testing.T) {
	w := stream.Resolve(stream.PipelineUser, "site-a")

	require.True(t, w.HasFailover())
	assert.Equal(t, "MESSAGES-CANONICAL-FAILOVER-site-a", w.CanonicalFailoverStream.Name)
	assert.Equal(t, "chat.failover.msg.canonical.site-a.created", w.CanonicalFailoverCreated)
	assert.Equal(t, "chat.failover.msg.canonical.site-a.>", w.CanonicalFailoverWildcard)
	assert.Equal(t, "PUSH-NOTIFICATION-FAILOVER-site-a", w.PushFailoverStream.Name)
	assert.Equal(t, "chat.failover.push.site-a.send", w.PushFailoverSendSubject)
	assert.Equal(t, "chat.failover.push.site-a.>", w.PushFailoverInputWildcard)
}

// Bots are not displaced users; the bot pipeline has no failover lane, and
// HasFailover is how a service skips it without a mode check at each call site.
func TestResolve_BotPipelineHasNoFailover(t *testing.T) {
	w := stream.Resolve(stream.PipelineBot, "site-a")

	assert.False(t, w.HasFailover())
	assert.Empty(t, w.CanonicalFailoverStream.Name)
	assert.Empty(t, w.PushFailoverStream.Name)
}
```

Use the actual `PipelineUser` constant name from `pkg/stream/pipeline.go` if it
differs.

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=stream`

Expected: FAIL — `undefined: stream.MessagesFailover`, `w.HasFailover undefined`.

- [ ] **Step 3: Write minimal implementation**

Add to `pkg/stream/stream.go`:

```go
// MessagesFailover returns MESSAGES-FAILOVER-{siteID}: the standby ingress lane,
// hosted on the site's buddy cluster. Displaced clients publish here while the
// site's own NATS is down; message-gatekeeper consumes it over its buddy
// connection. Placement is ops-owned and must name the buddy's cluster.
func MessagesFailover(siteID string) Config {
	return Config{
		Name:     fmt.Sprintf("MESSAGES-FAILOVER-%s", siteID),
		Subjects: []string{subject.FailoverMsgSendWildcard(siteID)},
	}
}

// MessagesCanonicalFailover returns MESSAGES-CANONICAL-FAILOVER-{siteID}: the
// standby validated-message lane. Fan-in for message-worker, broadcast-worker,
// notification-worker and search-sync-worker on the failover path.
func MessagesCanonicalFailover(siteID string) Config {
	return Config{
		Name:     fmt.Sprintf("MESSAGES-CANONICAL-FAILOVER-%s", siteID),
		Subjects: []string{subject.FailoverMsgCanonicalWildcard(siteID)},
	}
}

// PushNotificationFailover returns PUSH-NOTIFICATION-FAILOVER-{siteID}: the
// standby push-request lane between notification-worker and
// push-notification-service.
func PushNotificationFailover(siteID string) Config {
	return Config{
		Name:     fmt.Sprintf("PUSH-NOTIFICATION-FAILOVER-%s", siteID),
		Subjects: []string{subject.FailoverPushNotificationFilter(siteID)},
	}
}

// OutboxFailover returns OUTBOX-FAILOVER-{siteID}: the standby origin-side
// federation buffer, so a site keeps federating OUT while its own NATS is down.
// Consumed with the same ConcurrentEventTypes / OrderedEventTypes partition as
// the live OUTBOX.
func OutboxFailover(siteID string) Config {
	return Config{
		Name:     fmt.Sprintf("OUTBOX-FAILOVER-%s", siteID),
		Subjects: []string{subject.FailoverOutboxWildcard(siteID)},
	}
}
```

In `pkg/stream/pipeline.go`, add the fields to `Wiring`:

```go
	// Failover lane, populated for the user pipeline only. Zero for
	// PipelineBot — bots are not displaced users. Guard reads with HasFailover.
	CanonicalFailoverStream   Config
	CanonicalFailoverCreated  string
	CanonicalFailoverWildcard string
	PushFailoverStream        Config
	PushFailoverSendSubject   string
	PushFailoverInputWildcard string
```

Add the method:

```go
// HasFailover reports whether this pipeline has a standby failover lane, so a
// service can skip binding one without testing the pipeline mode itself.
func (w Wiring) HasFailover() bool { return w.CanonicalFailoverStream.Name != "" }
```

Populate them in `Resolve`'s user branch (leave the bot branch untouched):

```go
	return Wiring{
		CanonicalStream:   MessagesCanonical(siteID),
		CanonicalCreated:  subject.MsgCanonicalCreated(siteID),
		CanonicalWildcard: subject.MsgCanonicalWildcard(siteID),
		PushStream:        PushNotification(siteID),
		PushSendSubject:   subject.PushNotification(siteID),
		PushInputWildcard: subject.PushNotificationFilter(siteID),

		CanonicalFailoverStream:   MessagesCanonicalFailover(siteID),
		CanonicalFailoverCreated:  subject.FailoverMsgCanonicalCreated(siteID),
		CanonicalFailoverWildcard: subject.FailoverMsgCanonicalWildcard(siteID),
		PushFailoverStream:        PushNotificationFailover(siteID),
		PushFailoverSendSubject:   subject.FailoverPushNotification(siteID),
		PushFailoverInputWildcard: subject.FailoverPushNotificationFilter(siteID),
	}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=stream`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/stream/stream.go pkg/stream/pipeline.go pkg/stream/stream_test.go pkg/stream/pipeline_test.go
git commit -m "feat(stream): add message-path failover streams and pipeline wiring"
```

---

### Task 3: Shared failover-stream bootstrap helper

Seven services need the identical bootstrap-or-verify-plus-assert-placement
sequence. One helper keeps a future service from shipping a variant that skips
the placement check.

**Files:**
- Create: `pkg/stream/failover.go`
- Test: `pkg/stream/failover_test.go`

**Interfaces:**
- Consumes: `stream.CheckPlacement` (Plan 1 Task 3).
- Produces: `stream.EnsureFailoverStream(ctx context.Context, js FailoverStreamManager, cfg Config, bootstrapEnabled bool, expectedCluster string) error` and `type stream.FailoverStreamManager interface`.

- [ ] **Step 1: Write the failing test**

Create `pkg/stream/failover_test.go`:

```go
package stream_test

import (
	"context"
	"errors"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/stream"
)

type fakeJS struct {
	created  []string
	looked   []string
	info     *jetstream.StreamInfo
	createErr error
	lookupErr error
	infoErr   error
}

func (f *fakeJS) CreateOrUpdateStream(_ context.Context, cfg jetstream.StreamConfig) error {
	f.created = append(f.created, cfg.Name)
	return f.createErr
}

func (f *fakeJS) StreamInfo(_ context.Context, name string) (*jetstream.StreamInfo, error) {
	f.looked = append(f.looked, name)
	if f.lookupErr != nil {
		return nil, f.lookupErr
	}
	if f.infoErr != nil {
		return nil, f.infoErr
	}
	return f.info, nil
}

func TestEnsureFailoverStream_BootstrapCreatesWithoutAsserting(t *testing.T) {
	js := &fakeJS{}
	cfg := stream.MessagesCanonicalFailover("site-a")

	err := stream.EnsureFailoverStream(context.Background(), js, cfg, true, "site-b")

	require.NoError(t, err)
	assert.Equal(t, []string{cfg.Name}, js.created)
	assert.Empty(t, js.looked, "dev bootstrap must not assert placement — a single-server NATS has no cluster")
}

func TestEnsureFailoverStream_ProductionVerifiesAndAsserts(t *testing.T) {
	cfg := stream.MessagesCanonicalFailover("site-a")
	js := &fakeJS{info: &jetstream.StreamInfo{
		Config:  jetstream.StreamConfig{Name: cfg.Name},
		Cluster: &jetstream.ClusterInfo{Name: "site-b"},
	}}

	err := stream.EnsureFailoverStream(context.Background(), js, cfg, false, "site-b")

	require.NoError(t, err)
	assert.Empty(t, js.created, "production must never create a stream")
	assert.Equal(t, []string{cfg.Name}, js.looked)
}

func TestEnsureFailoverStream_WrongClusterIsAnError(t *testing.T) {
	cfg := stream.MessagesCanonicalFailover("site-a")
	js := &fakeJS{info: &jetstream.StreamInfo{
		Config:  jetstream.StreamConfig{Name: cfg.Name},
		Cluster: &jetstream.ClusterInfo{Name: "site-a"},
	}}

	err := stream.EnsureFailoverStream(context.Background(), js, cfg, false, "site-b")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "site-a", "the error must name the cluster actually hosting it")
}

func TestEnsureFailoverStream_MissingStreamIsAnError(t *testing.T) {
	cfg := stream.MessagesCanonicalFailover("site-a")
	js := &fakeJS{lookupErr: errors.New("stream not found")}

	err := stream.EnsureFailoverStream(context.Background(), js, cfg, false, "site-b")

	require.Error(t, err)
	assert.Contains(t, err.Error(), cfg.Name)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=stream`

Expected: FAIL — `undefined: stream.EnsureFailoverStream`.

- [ ] **Step 3: Write minimal implementation**

Create `pkg/stream/failover.go`:

```go
package stream

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
)

// FailoverStreamManager is the JetStream subset EnsureFailoverStream needs,
// narrow enough to fake in a unit test.
type FailoverStreamManager interface {
	CreateOrUpdateStream(ctx context.Context, cfg jetstream.StreamConfig) error
	StreamInfo(ctx context.Context, name string) (*jetstream.StreamInfo, error)
}

// EnsureFailoverStream readies a standby stream on a buddy connection.
//
// In dev (bootstrapEnabled) it creates the stream and does NOT assert placement:
// a single-server NATS reports no cluster at all, and there is no buddy to be
// wrong about.
//
// In production it verifies the stream exists AND is hosted by expectedCluster.
// The second half is the one that matters: names are unique supercluster-wide,
// so a lookup succeeds no matter which cluster hosts the asset — a standby
// stream sitting on the very cluster it exists to outlive would pass an
// existence check and fail only during the outage it was built for.
func EnsureFailoverStream(ctx context.Context, js FailoverStreamManager, cfg Config,
	bootstrapEnabled bool, expectedCluster string,
) error {
	if bootstrapEnabled {
		if err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name:     cfg.Name,
			Subjects: cfg.Subjects,
		}); err != nil {
			return fmt.Errorf("create failover stream %s: %w", cfg.Name, err)
		}
		return nil
	}

	info, err := js.StreamInfo(ctx, cfg.Name)
	if err != nil {
		return fmt.Errorf("verify failover stream %s: %w", cfg.Name, err)
	}
	if err := CheckPlacement(info, expectedCluster); err != nil {
		return fmt.Errorf("failover stream %s placement: %w", cfg.Name, err)
	}
	return nil
}
```

Note the interface uses `StreamInfo(ctx, name)` rather than
`Stream(ctx, name)` + `Info(ctx)`, so a fake needs one method instead of a
nested interface. Adapt the real `jetstream.JetStream` at the call site with a
small wrapper in each service, or add one shared adapter in this file if the
compiler shows the signatures do not line up.

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=stream`

Expected: PASS, all four cases.

- [ ] **Step 5: Commit**

```bash
git add pkg/stream/failover.go pkg/stream/failover_test.go
git commit -m "feat(stream): add EnsureFailoverStream bootstrap-or-verify helper"
```

---

### Task 4: message-gatekeeper failover ingress

**Files:**
- Modify: `message-gatekeeper/main.go` (config ~line 28-52; wiring ~line 139-165)
- Test: `message-gatekeeper/main_test.go`

**Interfaces:**
- Consumes: `natsutil.ConnectBuddy` (Plan 1 Task 6b), `stream.EnsureFailoverStream` (Task 3), `stream.MessagesFailover` + `stream.MessagesCanonicalFailover` (Task 2).
- Produces: `buildFailoverConsumerConfig(s stream.ConsumerSettings) jetstream.ConsumerConfig`; config fields `BuddySiteID`, `BuddyNatsURL`.

`message-gatekeeper` is the hinge: it consumes the failover ingress stream and
must publish its output to the failover **canonical** subject, not the live one —
the live canonical stream is on the down cluster.

- [ ] **Step 1: Write the failing test**

Add to `message-gatekeeper/main_test.go`:

```go
func TestBuildFailoverConsumerConfig(t *testing.T) {
	cc := buildFailoverConsumerConfig(stream.ConsumerSettings{})
	assert.Equal(t, "message-gatekeeper-failover", cc.Durable,
		"a distinct durable so the two lanes keep independent cursors")
}

// A message arriving on the failover lane must be published to the failover
// canonical subject. Publishing to the live one would target a stream on the
// cluster that is down.
func TestCanonicalSubjectForLane(t *testing.T) {
	w := stream.Resolve(stream.PipelineUser, "site-a")

	assert.Equal(t, "chat.msg.canonical.site-a.created", canonicalSubjectForLane(w, false))
	assert.Equal(t, "chat.failover.msg.canonical.site-a.created", canonicalSubjectForLane(w, true))
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=message-gatekeeper`

Expected: FAIL — `undefined: buildFailoverConsumerConfig`, `undefined: canonicalSubjectForLane`.

- [ ] **Step 3: Write minimal implementation**

Add the two config fields to the `config` struct:

```go
	// Buddy cluster hosting this site's standby failover streams. BUDDY_SITE_ID
	// doubles as the expected StreamInfo.Cluster.Name — NATS cluster names match
	// site IDs. Empty disables the failover lane.
	BuddySiteID  string `env:"BUDDY_SITE_ID"  envDefault:""`
	BuddyNatsURL string `env:"BUDDY_NATS_URL" envDefault:""`
```

Add beside the existing `buildConsumerConfig`:

```go
// buildFailoverConsumerConfig is the durable consumer on the buddy-hosted
// MESSAGES-FAILOVER lane. Distinct durable name from the home lane so the two
// keep independent cursors.
func buildFailoverConsumerConfig(s stream.ConsumerSettings) jetstream.ConsumerConfig {
	cc := stream.DurableConsumerDefaults(s)
	cc.Durable = "message-gatekeeper-failover"
	return cc
}

// canonicalSubjectForLane picks the canonical subject matching the lane a
// message arrived on. A failover-lane message must go to the failover canonical
// stream — the live one lives on the cluster that is down.
func canonicalSubjectForLane(w stream.Wiring, failover bool) string {
	if failover {
		return w.CanonicalFailoverCreated
	}
	return w.CanonicalCreated
}
```

In `main`, after the existing home-lane consumer setup, add the buddy lane. The
handler is the same; only the consumer source and the output subject differ:

```go
	if bnc := natsutil.ConnectBuddy(ctx, cfg.BuddyNatsURL, cfg.NatsCredsFile,
		sdk.TracerProvider(), sdk.Propagator, sdk.Toggles.Trace); bnc != nil && cfg.BuddySiteID != "" {
		if err := startFailoverIngress(ctx, bnc, cfg, wiring, handler); err != nil {
			slog.Warn("failover ingress unavailable", "buddy_site_id", cfg.BuddySiteID, "error", err)
		}
	}
```

```go
// startFailoverIngress binds the buddy-hosted MESSAGES-FAILOVER consumer and
// points its output at the failover canonical stream.
func startFailoverIngress(ctx context.Context, bnc *o11ynats.Conn, cfg config,
	wiring stream.Wiring, handler *Handler,
) error {
	if !wiring.HasFailover() {
		return nil // bot pipeline: no failover lane
	}
	bjs, err := bnc.JetStream()
	if err != nil {
		return fmt.Errorf("buddy jetstream init: %w", err)
	}
	for _, c := range []stream.Config{
		stream.MessagesFailover(cfg.SiteID),
		wiring.CanonicalFailoverStream,
	} {
		if err := stream.EnsureFailoverStream(ctx, bjs, c, cfg.Bootstrap.Enabled, cfg.BuddySiteID); err != nil {
			return err
		}
	}
	cons, err := bjs.CreateOrUpdateConsumer(ctx, stream.MessagesFailover(cfg.SiteID).Name,
		buildFailoverConsumerConfig(cfg.Consumer))
	if err != nil {
		return fmt.Errorf("create failover ingress consumer: %w", err)
	}
	slog.Info("failover ingress bound", "buddy_site_id", cfg.BuddySiteID)
	return startIngestLoop(ctx, bjs, cons, handler, canonicalSubjectForLane(wiring, true))
}
```

Extract the existing home-lane pull loop into
`startIngestLoop(ctx, js, cons, handler, canonicalSubject string) error` and call
it for both lanes, passing `canonicalSubjectForLane(wiring, false)` for home.
Register `natsutil.Drain(ctx, bnc)` in the same `shutdown.Wait` hook list as the
home connection.

- [ ] **Step 4: Run tests and build**

Run: `make test SERVICE=message-gatekeeper && make build SERVICE=message-gatekeeper`

Expected: PASS, builds clean.

- [ ] **Step 5: Commit**

```bash
git add message-gatekeeper/main.go message-gatekeeper/main_test.go
git commit -m "feat(message-gatekeeper): consume the failover ingress lane"
```

---

### Task 5: message-worker failover canonical consumer

**Files:**
- Modify: `message-worker/main.go` (config ~line 55; wiring ~line 173-190)
- Test: `message-worker/main_test.go`

**Interfaces:**
- Consumes: `natsutil.ConnectBuddy`, `stream.EnsureFailoverStream`, `stream.MessagesCanonicalFailover`.
- Produces: `buildFailoverConsumerConfig(s stream.ConsumerSettings, mode, siteID string) jetstream.ConsumerConfig`.

`message-worker` writes to Cassandra, which is up — the whole point. Its handler
is unchanged; only where it reads from is new.

- [ ] **Step 1: Write the failing test**

Add to `message-worker/main_test.go`:

```go
func TestBuildFailoverConsumerConfig(t *testing.T) {
	cc := buildFailoverConsumerConfig(stream.ConsumerSettings{}, "", "site-a")
	assert.Equal(t, "message-worker-failover", cc.Durable)
	assert.Equal(t, []string{"chat.failover.msg.canonical.site-a.>"}, cc.FilterSubjects)
}

func TestFailoverConsumerDurable_DiffersFromPrimary(t *testing.T) {
	primary := buildConsumerConfig(stream.ConsumerSettings{}, "", "site-a")
	failover := buildFailoverConsumerConfig(stream.ConsumerSettings{}, "", "site-a")
	assert.NotEqual(t, primary.Durable, failover.Durable)
}
```

Match `buildConsumerConfig`'s real parameter list at `message-worker/main.go:290`.

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=message-worker`

Expected: FAIL — `undefined: buildFailoverConsumerConfig`.

- [ ] **Step 3: Write minimal implementation**

Add the same two `BUDDY_*` config fields as Task 4. Add:

```go
// buildFailoverConsumerConfig is the durable consumer on the buddy-hosted
// MESSAGES-CANONICAL-FAILOVER lane. Distinct durable from the home lane so the
// two keep independent cursors; the handler and the Cassandra writes are
// identical, because a failover-lane message is still this site's message.
func buildFailoverConsumerConfig(s stream.ConsumerSettings, mode, siteID string) jetstream.ConsumerConfig {
	cc := stream.DurableConsumerDefaults(s)
	cc.Durable = "message-worker-failover"
	cc.FilterSubjects = []string{subject.FailoverMsgCanonicalWildcard(siteID)}
	return cc
}
```

In `main`, after the home consumer setup:

```go
	if bnc := natsutil.ConnectBuddy(ctx, cfg.BuddyNatsURL, cfg.NatsCredsFile,
		sdk.TracerProvider(), sdk.Propagator, sdk.Toggles.Trace); bnc != nil && cfg.BuddySiteID != "" {
		bjs, err := bnc.JetStream()
		if err != nil {
			slog.Warn("buddy jetstream init failed", "error", err)
		} else {
			failoverCfg := stream.MessagesCanonicalFailover(cfg.SiteID)
			if err := stream.EnsureFailoverStream(ctx, bjs, failoverCfg, cfg.Bootstrap.Enabled, cfg.BuddySiteID); err != nil {
				slog.Warn("failover lane unavailable", "error", err)
			} else if fcons, err := bjs.CreateOrUpdateConsumer(ctx, failoverCfg.Name,
				buildFailoverConsumerConfig(cfg.Consumer, cfg.Mode, cfg.SiteID)); err != nil {
				slog.Warn("create failover consumer failed", "error", err)
			} else {
				slog.Info("failover lane bound", "stream", failoverCfg.Name)
				startCanonicalLoop(ctx, fcons, handler)
			}
		}
	}
```

Extract the existing pull loop into `startCanonicalLoop(ctx, cons, handler)` and
call it for both lanes. Register `natsutil.Drain(ctx, bnc)` in the shutdown hooks.

- [ ] **Step 4: Run tests and build**

Run: `make test SERVICE=message-worker && make build SERVICE=message-worker`

Expected: PASS, builds clean.

- [ ] **Step 5: Commit**

```bash
git add message-worker/main.go message-worker/main_test.go
git commit -m "feat(message-worker): consume the failover canonical lane"
```

---

### Task 6: broadcast-worker failover canonical consumer

**Files:**
- Modify: `broadcast-worker/main.go` (config ~line 66-70; wiring ~line 178-240)
- Test: `broadcast-worker/main_test.go`

**Interfaces:**
- Consumes: `natsutil.ConnectBuddy`, `stream.EnsureFailoverStream`, `Wiring.CanonicalFailoverStream` / `HasFailover` (Task 2).
- Produces: `buildFailoverConsumerConfig(s stream.ConsumerSettings, siteID string) jetstream.ConsumerConfig`.

**Note:** `broadcast-worker` publishes room events over core NATS. Which *root*
it publishes to under failover — global vs local — is spec §E and belongs to the
next plan. This task only binds the consumer; routing is untouched here.

- [ ] **Step 1: Write the failing test**

Add to `broadcast-worker/main_test.go`:

```go
func TestBuildFailoverConsumerConfig(t *testing.T) {
	cc := buildFailoverConsumerConfig(stream.ConsumerSettings{}, "site-a")
	assert.Equal(t, "broadcast-worker-failover", cc.Durable)
	assert.Equal(t, []string{"chat.failover.msg.canonical.site-a.>"}, cc.FilterSubjects)
}

func TestFailoverConsumerDurable_DiffersFromPrimary(t *testing.T) {
	primary := buildConsumerConfig(stream.ConsumerSettings{}, "broadcast-worker", "chat.msg.canonical.site-a.>")
	failover := buildFailoverConsumerConfig(stream.ConsumerSettings{}, "site-a")
	assert.NotEqual(t, primary.Durable, failover.Durable)
}
```

Match `buildConsumerConfig`'s real signature at `broadcast-worker/main.go:359`.

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=broadcast-worker`

Expected: FAIL — `undefined: buildFailoverConsumerConfig`.

- [ ] **Step 3: Write minimal implementation**

Add the two `BUDDY_*` config fields. Add:

```go
// buildFailoverConsumerConfig is the durable consumer on the buddy-hosted
// MESSAGES-CANONICAL-FAILOVER lane. Distinct durable from the home lane so the
// two keep independent cursors.
func buildFailoverConsumerConfig(s stream.ConsumerSettings, siteID string) jetstream.ConsumerConfig {
	cc := stream.DurableConsumerDefaults(s)
	cc.Durable = "broadcast-worker-failover"
	cc.FilterSubjects = []string{subject.FailoverMsgCanonicalWildcard(siteID)}
	return cc
}
```

In `main`, after the home consumer, add the buddy lane guarded by
`wiring.HasFailover()` so bot mode skips it:

```go
	if wiring.HasFailover() {
		if bnc := natsutil.ConnectBuddy(ctx, cfg.BuddyNatsURL, cfg.NatsCredsFile,
			sdk.TracerProvider(), sdk.Propagator, sdk.Toggles.Trace); bnc != nil && cfg.BuddySiteID != "" {
			bjs, err := bnc.JetStream()
			if err != nil {
				slog.Warn("buddy jetstream init failed", "error", err)
			} else if err := stream.EnsureFailoverStream(ctx, bjs, wiring.CanonicalFailoverStream,
				cfg.Bootstrap.Enabled, cfg.BuddySiteID); err != nil {
				slog.Warn("failover lane unavailable", "error", err)
			} else if fcons, err := bjs.CreateOrUpdateConsumer(ctx, wiring.CanonicalFailoverStream.Name,
				buildFailoverConsumerConfig(cfg.Consumer, cfg.SiteID)); err != nil {
				slog.Warn("create failover consumer failed", "error", err)
			} else {
				slog.Info("failover lane bound", "stream", wiring.CanonicalFailoverStream.Name)
				startFanoutLoop(ctx, fcons, processor)
			}
		}
	}
```

Extract the existing pull loop into `startFanoutLoop(ctx, cons, processor)` and
call it for both lanes. Register `natsutil.Drain(ctx, bnc)` in the shutdown hooks.

- [ ] **Step 4: Run tests and build**

Run: `make test SERVICE=broadcast-worker && make build SERVICE=broadcast-worker`

Expected: PASS, builds clean.

- [ ] **Step 5: Commit**

```bash
git add broadcast-worker/main.go broadcast-worker/main_test.go
git commit -m "feat(broadcast-worker): consume the failover canonical lane"
```

---

### Task 7: notification-worker failover canonical consumer and push output

**Files:**
- Modify: `notification-worker/main.go` (config ~line 62; wiring ~line 202-280)
- Test: `notification-worker/main_test.go`

**Interfaces:**
- Consumes: `natsutil.ConnectBuddy`, `stream.EnsureFailoverStream`, `Wiring` failover fields.
- Produces: `buildFailoverConsumerConfig(s stream.ConsumerSettings, siteID string) jetstream.ConsumerConfig`, `pushSubjectForLane(w stream.Wiring, failover bool) string`.

Like `message-gatekeeper`, this service is a hinge: it both consumes a failover
lane and must publish onward to a failover lane, because the live
`PUSH-NOTIFICATION` stream is on the down cluster.

- [ ] **Step 1: Write the failing test**

Add to `notification-worker/main_test.go`:

```go
func TestBuildFailoverConsumerConfig(t *testing.T) {
	cc := buildFailoverConsumerConfig(stream.ConsumerSettings{}, "site-a")
	assert.Equal(t, "notification-worker-failover", cc.Durable)
	assert.Equal(t, []string{"chat.failover.msg.canonical.site-a.>"}, cc.FilterSubjects)
}

// A notification derived from a failover-lane message must be published to the
// failover push stream; the live one is on the cluster that is down.
func TestPushSubjectForLane(t *testing.T) {
	w := stream.Resolve(stream.PipelineUser, "site-a")
	assert.Equal(t, "chat.server.notification.push.site-a.send", pushSubjectForLane(w, false))
	assert.Equal(t, "chat.failover.push.site-a.send", pushSubjectForLane(w, true))
}
```

Use the real live push subject string that `subject.PushNotification("site-a")`
returns; correct the expected value if it differs.

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=notification-worker`

Expected: FAIL — `undefined: buildFailoverConsumerConfig`, `undefined: pushSubjectForLane`.

- [ ] **Step 3: Write minimal implementation**

Add the two `BUDDY_*` config fields. Add:

```go
// buildFailoverConsumerConfig is the durable consumer on the buddy-hosted
// MESSAGES-CANONICAL-FAILOVER lane.
func buildFailoverConsumerConfig(s stream.ConsumerSettings, siteID string) jetstream.ConsumerConfig {
	cc := stream.DurableConsumerDefaults(s)
	cc.Durable = "notification-worker-failover"
	cc.FilterSubjects = []string{subject.FailoverMsgCanonicalWildcard(siteID)}
	return cc
}

// pushSubjectForLane picks the push-request subject matching the lane the
// triggering message arrived on. A failover-lane notification must go to the
// failover push stream — the live one is on the cluster that is down.
func pushSubjectForLane(w stream.Wiring, failover bool) string {
	if failover {
		return w.PushFailoverSendSubject
	}
	return w.PushSendSubject
}
```

Wire the buddy lane exactly as in Task 6, guarded by `wiring.HasFailover()`, but
ensure both the canonical failover stream **and** the push failover stream are
readied:

```go
			for _, c := range []stream.Config{
				wiring.CanonicalFailoverStream,
				wiring.PushFailoverStream,
			} {
				if err := stream.EnsureFailoverStream(ctx, bjs, c, cfg.Bootstrap.Enabled, cfg.BuddySiteID); err != nil {
					slog.Warn("failover lane unavailable", "stream", c.Name, "error", err)
					return
				}
			}
```

Thread the lane's push subject through to the handler's publish call so a
failover-lane message publishes to `pushSubjectForLane(wiring, true)`.

**Leave the `ROOMS` invalidation consumer on the home connection only** — `ROOMS`
has no failover lane (spec, out of scope), so binding it on the buddy would fail
the stream check every time.

- [ ] **Step 4: Run tests and build**

Run: `make test SERVICE=notification-worker && make build SERVICE=notification-worker`

Expected: PASS, builds clean.

- [ ] **Step 5: Commit**

```bash
git add notification-worker/main.go notification-worker/main_test.go
git commit -m "feat(notification-worker): consume the failover canonical lane and publish failover push"
```

---

### Task 8: search-sync-worker failover canonical consumer

**Files:**
- Modify: `search-sync-worker/main.go`, `search-sync-worker/messages.go:100`
- Test: `search-sync-worker/main_test.go`

**Interfaces:**
- Consumes: `natsutil.ConnectBuddy`, `stream.EnsureFailoverStream`, `stream.MessagesCanonicalFailover`.
- Produces: `buildFailoverMessagesConsumerConfig(s stream.ConsumerSettings, siteID string) jetstream.ConsumerConfig`.

Elasticsearch is up, so indexing continues normally. Only the message source is
new. **The INBOX consumer stays home-only** — Plan 1 handles inbound federation,
and the internal search feed has no failover lane.

- [ ] **Step 1: Write the failing test**

Add to `search-sync-worker/main_test.go`:

```go
func TestBuildFailoverMessagesConsumerConfig(t *testing.T) {
	cc := buildFailoverMessagesConsumerConfig(stream.ConsumerSettings{}, "site-a")
	assert.Equal(t, "search-sync-worker-messages-failover", cc.Durable)
	assert.Equal(t, []string{"chat.failover.msg.canonical.site-a.>"}, cc.FilterSubjects)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=search-sync-worker`

Expected: FAIL — `undefined: buildFailoverMessagesConsumerConfig`.

- [ ] **Step 3: Write minimal implementation**

Add the two `BUDDY_*` config fields. Add beside the existing messages consumer
builder in `messages.go`:

```go
// buildFailoverMessagesConsumerConfig is the durable consumer on the
// buddy-hosted MESSAGES-CANONICAL-FAILOVER lane. Indexing is unchanged —
// Elasticsearch is up — only the source is new.
func buildFailoverMessagesConsumerConfig(s stream.ConsumerSettings, siteID string) jetstream.ConsumerConfig {
	cc := stream.DurableConsumerDefaults(s)
	cc.Durable = "search-sync-worker-messages-failover"
	cc.FilterSubjects = []string{subject.FailoverMsgCanonicalWildcard(siteID)}
	return cc
}
```

Wire the buddy lane in `main` following Task 6's shape, binding only the
canonical failover stream. Leave the INBOX and HR consumers on the home
connection.

- [ ] **Step 4: Run tests and build**

Run: `make test SERVICE=search-sync-worker && make build SERVICE=search-sync-worker`

Expected: PASS, builds clean.

- [ ] **Step 5: Commit**

```bash
git add search-sync-worker/main.go search-sync-worker/messages.go search-sync-worker/main_test.go
git commit -m "feat(search-sync-worker): index messages from the failover canonical lane"
```

---

### Task 9: push-notification-service failover consumer

**Files:**
- Modify: `push-notification-service/main.go` (config ~line 29; wiring ~line 63-80)
- Test: `push-notification-service/main_test.go`

**Interfaces:**
- Consumes: `natsutil.ConnectBuddy`, `stream.EnsureFailoverStream`, `Wiring.PushFailoverStream`.
- Produces: `buildFailoverConsumerConfig(s stream.ConsumerSettings, siteID string) jetstream.ConsumerConfig`.

APNs and FCM are external and unaffected, so pushes keep going out.

- [ ] **Step 1: Write the failing test**

Add to `push-notification-service/main_test.go`:

```go
func TestBuildFailoverConsumerConfig(t *testing.T) {
	cc := buildFailoverConsumerConfig(stream.ConsumerSettings{}, "site-a")
	assert.Equal(t, "push-notification-service-failover", cc.Durable)
	assert.Equal(t, []string{"chat.failover.push.site-a.>"}, cc.FilterSubjects)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=push-notification-service`

Expected: FAIL — `undefined: buildFailoverConsumerConfig`.

- [ ] **Step 3: Write minimal implementation**

Add the two `BUDDY_*` config fields. Add:

```go
// buildFailoverConsumerConfig is the durable consumer on the buddy-hosted
// PUSH-NOTIFICATION-FAILOVER lane. Delivery to APNs/FCM is unchanged — those
// are external and unaffected by a site's NATS outage.
func buildFailoverConsumerConfig(s stream.ConsumerSettings, siteID string) jetstream.ConsumerConfig {
	cc := stream.DurableConsumerDefaults(s)
	cc.Durable = "push-notification-service-failover"
	cc.FilterSubjects = []string{subject.FailoverPushNotificationFilter(siteID)}
	return cc
}
```

Wire the buddy lane in `main` following Task 6's shape, guarded by
`wiring.HasFailover()`, binding `wiring.PushFailoverStream`.

- [ ] **Step 4: Run tests and build**

Run: `make test SERVICE=push-notification-service && make build SERVICE=push-notification-service`

Expected: PASS, builds clean.

- [ ] **Step 5: Commit**

```bash
git add push-notification-service/main.go push-notification-service/main_test.go
git commit -m "feat(push-notification-service): consume the failover push lane"
```

---

### Task 10: OUTBOX-FAILOVER — room-service publish, outbox-worker consume

**Files:**
- Modify: `pkg/outbox/outbox.go` (`Publish`)
- Modify: `room-service/main.go` (buddy connection + publish target)
- Modify: `outbox-worker/main.go` (config ~line 30-43; consumers ~line 117-145)
- Test: `pkg/outbox/outbox_test.go`, `outbox-worker/consumer_config_test.go`

**Interfaces:**
- Consumes: `subject.FailoverOutbox` / `FailoverOutboxWildcard` (Task 1), `stream.OutboxFailover` (Task 2), `stream.EnsureFailoverStream` (Task 3), `natsutil.ConnectBuddy`.
- Produces: `outbox.PublishTo(ctx, publish, originSiteID, roomID, destSiteID string, eventType model.InboxEventType, payload []byte, dedupID string, ts int64, failover bool) error`; `buildFailoverConcurrentConsumerConfig(s, siteID, dest string)`, `buildFailoverOrderedConsumerConfig(s, siteID, dest string)`.

This keeps a site federating **outward** during its own outage. The failover
stream must carry the same two-consumer partition as the live OUTBOX, or an
event type would sit unconsumed.

- [ ] **Step 1: Write the failing test**

Add to `pkg/outbox/outbox_test.go`:

```go
func TestPublishTo_SelectsLaneSubject(t *testing.T) {
	tests := []struct {
		name     string
		failover bool
		want     string
	}{
		{"live lane", false, "chat.outbox.site-a.site-b.member_added"},
		{"failover lane", true, "chat.failover.outbox.site-a.site-b.member_added"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got string
			publish := func(_ context.Context, subj string, _ []byte, _ string) error {
				got = subj
				return nil
			}
			err := outbox.PublishTo(context.Background(), publish, "site-a", "r1", "site-b",
				model.InboxMemberAdded, []byte(`{}`), "d1", 1234, tt.failover)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// The partition guard must hold on both lanes — an unknown type on the failover
// stream would sit unconsumed exactly as it would on the live one.
func TestPublishTo_RejectsUnknownEventTypeOnFailoverLane(t *testing.T) {
	publish := func(_ context.Context, _ string, _ []byte, _ string) error { return nil }
	err := outbox.PublishTo(context.Background(), publish, "site-a", "r1", "site-b",
		model.InboxEventType("not_a_real_type"), []byte(`{}`), "d1", 1234, true)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "filter set")
}
```

Add to `outbox-worker/consumer_config_test.go`:

```go
func TestFailoverConsumerConfigs(t *testing.T) {
	conc := buildFailoverConcurrentConsumerConfig(stream.ConsumerSettings{}, "site-a", "site-b")
	assert.Contains(t, conc.Durable, "failover")
	assert.Equal(t, -1, conc.MaxDeliver, "a down peer must retry forever on the failover lane too")

	ord := buildFailoverOrderedConsumerConfig(stream.ConsumerSettings{}, "site-a", "site-b")
	assert.Contains(t, ord.Durable, "failover")
	assert.Equal(t, 1, ord.MaxAckPending, "ordered events must not overtake each other")
	assert.NotEqual(t, conc.Durable, ord.Durable)

	// The two filter sets must partition the failover stream exactly as they do
	// the live one, or an event type lands with no consumer.
	assert.Len(t, conc.FilterSubjects, len(outbox.ConcurrentEventTypes))
	assert.Len(t, ord.FilterSubjects, len(outbox.OrderedEventTypes))
	for _, s := range append(conc.FilterSubjects, ord.FilterSubjects...) {
		assert.True(t, strings.HasPrefix(s, "chat.failover.outbox.site-a.site-b."), "got %q", s)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=outbox && make test SERVICE=outbox-worker`

Expected: FAIL — `undefined: outbox.PublishTo`, `undefined: buildFailoverConcurrentConsumerConfig`.

- [ ] **Step 3: Write minimal implementation**

In `pkg/outbox/outbox.go`, add `PublishTo` carrying the existing `Publish`
body with a lane-selected subject, and reduce `Publish` to a wrapper so no
caller changes:

```go
// PublishTo is Publish with an explicit lane. failover=true targets the
// buddy-hosted OUTBOX-FAILOVER stream, which is what a site uses to keep
// federating outward while its own NATS is down.
func PublishTo(ctx context.Context, publish func(ctx context.Context, subj string, data []byte, msgID string) error,
	originSiteID, roomID, destSiteID string, eventType model.InboxEventType, payload []byte, dedupID string, ts int64,
	failover bool,
) error {
	// ... existing Publish body, except the subject line:
	//   subj := subject.Outbox(originSiteID, destSiteID, string(eventType))
	// becomes:
	//   subj := subject.Outbox(...); if failover { subj = subject.FailoverOutbox(...) }
}

// Publish targets the live OUTBOX lane. Retained so existing callers are
// unchanged.
func Publish(ctx context.Context, publish func(ctx context.Context, subj string, data []byte, msgID string) error,
	originSiteID, roomID, destSiteID string, eventType model.InboxEventType, payload []byte, dedupID string, ts int64,
) error {
	return PublishTo(ctx, publish, originSiteID, roomID, destSiteID, eventType, payload, dedupID, ts, false)
}
```

In `outbox-worker/main.go`, add the two `BUDDY_*` config fields and add failover
twins of the two consumer builders, mirroring
`buildConcurrentConsumerConfig` / `buildOrderedConsumerConfig` but built from
`subject.FailoverOutbox(...)` and with `-failover` appended to each durable name.
Keep `MaxDeliver = -1` and `MaxAckPending = 1` exactly as the live ones set them.

Then, on the buddy connection, ready `stream.OutboxFailover(cfg.SiteID)` and
create the same per-destination consumer pair for every remote peer in
`cfg.AllSiteIDs`, running them through the same `process` disposition.

In `room-service`, open a buddy connection with `natsutil.ConnectBuddy`, and when
the home publish fails, publish through `outbox.PublishTo(..., failover: true)`
on the buddy connection. Gate on the same `ErrNoResponders` rule Plan 1
established in `pkg/outbox/failover.go` — reuse `isNoResponders` by exporting it
as `outbox.IsNoResponders(err) bool` if it is not already exported.

- [ ] **Step 4: Run tests and build**

Run: `make test SERVICE=outbox && make test SERVICE=outbox-worker && make test SERVICE=room-service && make build SERVICE=outbox-worker`

Expected: PASS, builds clean.

- [ ] **Step 5: Commit**

```bash
git add pkg/outbox/outbox.go pkg/outbox/outbox_test.go outbox-worker/main.go outbox-worker/consumer_config_test.go room-service/
git commit -m "feat(outbox): add the failover outbox lane for outbound federation during an outage"
```

---

### Task 11: Compose wiring for local dev

**Files:**
- Modify: `deploy/docker-compose.yml` for each of the seven services touched in Tasks 4-10, plus `room-service`.

Dev runs one NATS, so pointing `BUDDY_NATS_URL` at the same server exercises the
whole path — stream creation, consumer binding, both lanes draining — with no
second container. Placement is not asserted because `BOOTSTRAP_STREAMS=true`
takes the create branch.

- [ ] **Step 1: Add the env vars to each service**

In each service's `environment:` block, matching that file's existing
`NATS_URL` host and port:

```yaml
      - BUDDY_SITE_ID=site-local
      - BUDDY_NATS_URL=nats://nats:4222
```

Services: `message-gatekeeper`, `message-worker`, `broadcast-worker`,
`notification-worker`, `search-sync-worker`, `push-notification-service`,
`outbox-worker`, `room-service`.

- [ ] **Step 2: Verify each service binds its lane**

For each, bring the compose file up and grep the logs for
`failover lane bound` (or `failover ingress bound` for `message-gatekeeper`).

- [ ] **Step 3: Tear down and commit**

```bash
git add */deploy/docker-compose.yml
git commit -m "chore: wire the failover lanes into local compose"
```

---

### Task 12: End-to-end integration test

**Files:**
- Create: `message-gatekeeper/failover_integration_test.go`

**Interfaces:**
- Consumes: everything above, plus `testutil.NATSPair` (Plan 1 Task 6).

The property that matters: a message sent on the failover lane is persisted to
the **origin site's** Cassandra, not the buddy's.

- [ ] **Step 1: Write the failing test**

```go
//go:build integration

package main

// A message published to the failover ingress lane must traverse the whole
// standby pipeline and land in the ORIGIN site's Cassandra — the site's own
// services doing the site's own work against the site's own store is the entire
// correctness argument for this design.
func TestFailoverIngress_PersistsToOriginSiteStore(t *testing.T) {
	homeURL, buddyURL := testutil.NATSPair(t)
	keyspace, sess, _ := testutil.CassandraKeyspace(t, "gkfailover")
	ctx := context.Background()

	buddyJS := connectJS(t, buddyURL)
	for _, c := range []stream.Config{
		stream.MessagesFailover("site-a"),
		stream.MessagesCanonicalFailover("site-a"),
	} {
		_, err := buddyJS.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name: c.Name, Subjects: c.Subjects,
		})
		require.NoError(t, err)
	}

	startGatekeeperFailoverLane(t, ctx, buddyJS, "site-a")
	startMessageWorkerFailoverLane(t, ctx, buddyJS, "site-a", keyspace, sess)

	msg := validSendPayload(t, "alice", "r1", "m-failover-1")
	_, err := buddyJS.Publish(ctx, subject.FailoverMsgSend("alice", "r1", "site-a"), msg)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return messageExists(t, sess, keyspace, "r1", "m-failover-1")
	}, 15*time.Second, 200*time.Millisecond,
		"a failover-lane message must reach the origin site's Cassandra")

	// The home cluster was never involved — nothing should have been created there.
	homeJS := connectJS(t, homeURL)
	_, err = homeJS.Stream(ctx, stream.MessagesCanonical("site-a").Name)
	assert.Error(t, err, "the failover path must not touch the home cluster")
}
```

Write `startGatekeeperFailoverLane`, `startMessageWorkerFailoverLane`,
`connectJS`, `validSendPayload` and `messageExists` in the same file, reusing
whatever the existing `message-gatekeeper/integration_test.go` already provides.
Add `func TestMain(m *testing.M) { testutil.RunTests(m) }` if the package has
none.

- [ ] **Step 2: Run test to verify it fails**

Run: `make test-integration SERVICE=message-gatekeeper`

Expected: FAIL before the lanes are wired — the `Eventually` times out.

- [ ] **Step 3: Make it pass**

If Tasks 1-10 are complete, no new production code is needed; fix only test
wiring.

- [ ] **Step 4: Run the suite**

Run: `make test-integration SERVICE=message-gatekeeper`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add message-gatekeeper/failover_integration_test.go
git commit -m "test(message-gatekeeper): cover the failover message path end to end"
```

---

### Task 13: Documentation

**Files:**
- Modify: `docs/nats-subject-naming.md`
- Modify: `docs/architecture.md`

- [ ] **Step 1: Document the four new subjects**

Extend the `chat.failover.>` section Plan 1 added with the message-path
subjects: the client-facing `chat.user.{acct}.room.{roomID}.{siteID}.failover.msg.send`
(and why it stays inside the account's JWT scope), plus
`chat.failover.msg.canonical.{siteID}.>`, `chat.failover.push.{siteID}.>`, and
`chat.failover.outbox.{siteID}.{destSiteID}.{eventType}`.

- [ ] **Step 2: Add the standby streams to the topology diagram**

In `docs/architecture.md` §4, add the four standby streams alongside
`INBOX-FAILOVER` from Plan 1, noting they are hosted on the buddy cluster and
carry traffic only during an outage.

- [ ] **Step 3: Commit**

```bash
git add docs/nats-subject-naming.md docs/architecture.md
git commit -m "docs: document the message-path failover subjects and standby streams"
```

---

## Final Verification

- [ ] `make test` — all packages pass with `-race`.
- [ ] `make lint` — clean.
- [ ] `make sast` — no medium+ findings.
- [ ] `make test-integration SERVICE=message-gatekeeper`.
- [ ] Coverage floor for `pkg/stream`, `pkg/subject`, `pkg/outbox`: `go test -coverprofile=coverage.out ./pkg/stream/... && go tool cover -func=coverage.out`.
- [ ] **Confirm every touched service still starts with no `BUDDY_*` set** — an unconfigured buddy must be a silent no-op, not a startup failure.

## Staging Verification

Same three items as Plan 1, now covering five streams: subject-overlap
enforcement across clusters, placement of each standby stream on the buddy, and
no-responders behaviour when a cluster is down.

Additionally: **capacity.** Each cluster must have headroom for its own load plus
one peer's full message pipeline. Measure a peer's canonical throughput and
confirm its buddy can absorb it on top of its own.

## Known incomplete after this plan

Displaced clients receive events only for **cross-site** rooms until spec §E
(forced global room routing) lands in the next plan. The lanes are correct and
complete; delivery is not. Do not announce failover as user-ready on this plan
alone.
