# NATS Failover — Inbound Federation Redirect Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** When a site's NATS cluster is down, peers redirect their federation forwards to a buddy-hosted `INBOX-FAILOVER` stream, where the down site's own `inbox-worker` consumes them and writes to its own MongoDB.

**Architecture:** A new subject root (`chat.failover.inbox.{site}.external.>`) backed by a standby stream on the site's buddy cluster. Peers fall back to it only on an unambiguous no-responders error, so an event lands in exactly one stream. `inbox-worker` opens a second NATS connection to its buddy and binds an identical consumer there, so both lanes drain concurrently with no mode flag.

**Tech Stack:** Go 1.25, `nats.go` + `jetstream`, MongoDB (`mongo-driver/v2`), testcontainers, testify, mockgen.

**Design spec:** `docs/superpowers/specs/2026-08-15-nats-site-failover-design.md` (§B subjects, §C streams + placement, §D dual connections, §H federation).

## Global Constraints

- Go 1.25. Single `go.mod` at repo root. No new third-party dependencies.
- All commands via `make` targets — never raw `go` commands.
- TDD is mandatory: write the failing test, run it, confirm it fails, then implement.
- `make lint` and `make test` are enforced by a pre-commit hook.
- Error wrapping: `fmt.Errorf("short description: %w", err)`. Never bare `err`.
- Logging: `log/slog` structured key-value pairs only. Never log tokens or full message bodies.
- Integration tests use `//go:build integration`, live in the same package, and containers come from `pkg/testutil`.
- Stream creation is gated by `BOOTSTRAP_STREAMS`; production verifies rather than creates.
- Subject construction always goes through `pkg/subject` builders, never `fmt.Sprintf` at a call site.

## Out of scope for this plan

- **The message path** (`MESSAGES-FAILOVER`, `MESSAGES-CANONICAL-FAILOVER`, `PUSH-NOTIFICATION-FAILOVER`, `OUTBOX-FAILOVER`, the pipeline workers). Plan 2 — `2026-08-15-nats-failover-message-path.md`.
- **Forced global room routing and the revert grace window** (spec §E). Plan 3.
- **Client failover and the portal peer list** (shuffle/walk/stick/revert, `portal-service`, `docs/client-api.md`). Plan 4.
- **Direct publishers** (`message-worker`, `user-service`). They publish to a remote INBOX with a *different* publish signature than `outbox-worker`'s four-argument `PublishFunc` — `user-service/service/status.go:117` uses a three-argument `Publish(ctx, subj, data)` with no `msgID`. Adapting them needs a signature audit that belongs with Plan 2's work in those services. Their events continue to park on failure, which is today's behavior.
- **`ROOMS-FAILOVER`.** Deferred in the spec.

---

## File Structure

| File | Responsibility |
|------|----------------|
| `pkg/subject/subject.go` | Add `FailoverInboxExternal` + `FailoverInboxExternalAll` beside the existing `InboxExternal` family |
| `pkg/subject/subject_test.go` | Exact-string and disjointness assertions |
| `pkg/stream/stream.go` | Add `InboxFailover(siteID) Config` |
| `pkg/stream/placement.go` | New — `CheckPlacement`, a pure function over `*jetstream.StreamInfo` |
| `pkg/stream/placement_test.go` | New — table test for `CheckPlacement` |
| `pkg/outbox/failover.go` | New — `ForwardWithFailover`, the no-responders-gated fallback |
| `pkg/outbox/failover_test.go` | New — fallback trigger matrix |
| `pkg/natsutil/buddy.go` | New — `ConnectBuddy`, the non-fatal secondary connection shared by every service |
| `pkg/natsutil/buddy_test.go` | New — unreachable/unconfigured/reachable cases |
| `pkg/testutil/nats_buddy.go` | New — second JetStream server for buddy-connection tests |
| `pkg/testutil/terminate.go` | Wire `TerminateNATSBuddy` into `TerminateAll` |
| `outbox-worker/handler.go` | Route the forward through `ForwardWithFailover` |
| `inbox-worker/main.go` | Buddy config, second connection, failover consumer, placement assertion |
| `inbox-worker/bootstrap.go` | Bootstrap/verify `INBOX-FAILOVER` on the buddy connection |
| `inbox-worker/integration_test.go` | End-to-end: both lanes drain to the same Mongo |
| `docs/nats-subject-naming.md` | Document the `chat.failover.>` root |
| `docs/architecture.md` | Correct the stale FANOUT stream and `broadcast-worker → outbox` edge |

---

### Task 1: Failover inbox subject builders

**Files:**
- Modify: `pkg/subject/subject.go` (add after `InboxExternalAll`, around line 308)
- Test: `pkg/subject/subject_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `subject.FailoverInboxExternal(siteID, eventType string) string`, `subject.FailoverInboxExternalAll(siteID string) string`.

- [ ] **Step 1: Write the failing test**

Add to `pkg/subject/subject_test.go`:

```go
func TestFailoverInboxExternal(t *testing.T) {
	assert.Equal(t, "chat.failover.inbox.site-a.external.member_added",
		subject.FailoverInboxExternal("site-a", "member_added"))
	assert.Equal(t, "chat.failover.inbox.site-a.external.>",
		subject.FailoverInboxExternalAll("site-a"))
}

// The failover root must not overlap any live stream's subject filter, or the
// stream create is rejected supercluster-wide (one account, one JS domain).
func TestFailoverInboxExternal_DisjointFromLiveFilters(t *testing.T) {
	failover := subject.FailoverInboxExternalAll("site-a")
	live := []string{
		subject.InboxExternalAll("site-a"),
		"chat.inbox.site-a.internal.>",
		subject.OutboxWildcard("site-a"),
	}
	for _, l := range live {
		assert.False(t, subjectsOverlap(failover, l),
			"failover filter %q must not overlap live filter %q", failover, l)
	}
}

// The failover root must also sit outside chat.local.>, which the platform
// filters from gateway interest advertisement — a failover subject under it
// would never cross a gateway.
func TestFailoverInboxExternal_NotUnderLocalRoot(t *testing.T) {
	assert.False(t, strings.HasPrefix(subject.FailoverInboxExternalAll("site-a"), "chat.local."))
}

// subjectsOverlap reports whether two NATS subject filters can match a common
// subject. Token-by-token: ">" swallows the rest, "*" matches any single token,
// otherwise tokens must be equal.
func subjectsOverlap(a, b string) bool {
	at, bt := strings.Split(a, "."), strings.Split(b, ".")
	for i := 0; i < len(at) && i < len(bt); i++ {
		if at[i] == ">" || bt[i] == ">" {
			return true
		}
		if at[i] == "*" || bt[i] == "*" {
			continue
		}
		if at[i] != bt[i] {
			return false
		}
	}
	return len(at) == len(bt)
}
```

Ensure `strings` is imported in the test file.

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=subject`

Expected: FAIL — `undefined: subject.FailoverInboxExternal`.

- [ ] **Step 3: Write minimal implementation**

Add to `pkg/subject/subject.go` directly after `InboxExternalAll`:

```go
// FailoverInboxExternal is the subject a peer publishes a cross-site federation
// event on when the destination site's own INBOX is unreachable:
// `chat.failover.inbox.{siteID}.external.{eventType}`. The standby stream that
// captures it is hosted on the destination site's buddy cluster, so the
// destination's own inbox-worker consumes it over its buddy connection and
// applies the event to the destination's DB.
//
// The publisher needs no knowledge of which cluster is whose buddy — the
// subject names the destination site, and supercluster interest routing
// delivers it wherever the stream lives.
//
// The chat.failover.> root is deliberately disjoint from chat.inbox.> (two
// streams in one account may not claim overlapping subjects) and from
// chat.local.> (which the platform filters out of gateway interest).
func FailoverInboxExternal(siteID, eventType string) string {
	return fmt.Sprintf("chat.failover.inbox.%s.external.%s", siteID, eventType)
}

// FailoverInboxExternalAll matches every event on a site's failover INBOX lane:
// `chat.failover.inbox.{siteID}.external.>`. Use as the INBOX-FAILOVER-{siteID}
// stream's subject pattern and as its consumer's FilterSubjects.
func FailoverInboxExternalAll(siteID string) string {
	return fmt.Sprintf("chat.failover.inbox.%s.external.>", siteID)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=subject`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/subject/subject.go pkg/subject/subject_test.go
git commit -m "feat(subject): add failover inbox subject builders"
```

---

### Task 2: INBOX-FAILOVER stream config

**Files:**
- Modify: `pkg/stream/stream.go` (add after `Inbox`, around line 76)
- Test: `pkg/stream/stream_test.go`

**Interfaces:**
- Consumes: `subject.FailoverInboxExternalAll` (Task 1).
- Produces: `stream.InboxFailover(siteID string) Config`.

- [ ] **Step 1: Write the failing test**

Add to `pkg/stream/stream_test.go`:

```go
func TestInboxFailover(t *testing.T) {
	c := stream.InboxFailover("site-a")
	assert.Equal(t, "INBOX-FAILOVER-site-a", c.Name)
	assert.Equal(t, []string{"chat.failover.inbox.site-a.external.>"}, c.Subjects)
}

// The stream is named for the ORIGIN site, never the hosting buddy — names are
// unique supercluster-wide, and naming by host would collide if a cluster ever
// buddied for more than one peer.
func TestInboxFailover_NamedForOriginSite(t *testing.T) {
	assert.NotEqual(t, stream.InboxFailover("site-a").Name, stream.InboxFailover("site-b").Name)
	assert.Contains(t, stream.InboxFailover("site-a").Name, "site-a")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=stream`

Expected: FAIL — `undefined: stream.InboxFailover`.

- [ ] **Step 3: Write minimal implementation**

Add to `pkg/stream/stream.go` directly after `Inbox`:

```go
// InboxFailover returns INBOX-FAILOVER-{siteID}: the standby inbound-federation
// lane for a site, hosted on that site's BUDDY cluster so it survives the site's
// own NATS outage. Peers redirect here when the site's primary INBOX is
// unreachable; the site's own inbox-worker consumes it over its buddy connection.
//
// Named for the origin site, not the host — stream names are unique across the
// supercluster (one account, one JetStream domain).
//
// Carries only the external.> lane: the internal.> lane is a same-site search
// feed published by services that are idle during the outage.
//
// Placement is ops-owned and MUST name the buddy's cluster; the owning service
// asserts it at startup via CheckPlacement rather than setting it here.
func InboxFailover(siteID string) Config {
	return Config{
		Name:     fmt.Sprintf("INBOX-FAILOVER-%s", siteID),
		Subjects: []string{subject.FailoverInboxExternalAll(siteID)},
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=stream`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/stream/stream.go pkg/stream/stream_test.go
git commit -m "feat(stream): add INBOX-FAILOVER standby stream config"
```

---

### Task 3: Placement assertion helper

A standby stream placed on the cluster it is meant to outlive is the one
misconfiguration that is both catastrophic and silent: `js.Stream()` resolves it
fine, because the JetStream API is supercluster-wide. Only the hosting cluster
name reveals the problem.

**Files:**
- Create: `pkg/stream/placement.go`
- Test: `pkg/stream/placement_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `stream.CheckPlacement(info *jetstream.StreamInfo, expectedCluster string) error`.

`CheckPlacement` is a pure function over an already-fetched `StreamInfo` rather
than something that does its own lookup — that keeps it unit-testable with no
NATS server and no mock.

- [ ] **Step 1: Write the failing test**

Create `pkg/stream/placement_test.go`:

```go
package stream_test

import (
	"testing"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/stream"
)

func TestCheckPlacement(t *testing.T) {
	tests := []struct {
		name     string
		info     *jetstream.StreamInfo
		expected string
		wantErr  string
	}{
		{
			name: "hosted by the expected cluster",
			info: &jetstream.StreamInfo{
				Config:  jetstream.StreamConfig{Name: "INBOX-FAILOVER-site-a"},
				Cluster: &jetstream.ClusterInfo{Name: "site-b"},
			},
			expected: "site-b",
		},
		{
			name: "hosted by the wrong cluster",
			info: &jetstream.StreamInfo{
				Config:  jetstream.StreamConfig{Name: "INBOX-FAILOVER-site-a"},
				Cluster: &jetstream.ClusterInfo{Name: "site-a"},
			},
			expected: "site-b",
			wantErr:  `hosted by cluster "site-a", want "site-b"`,
		},
		{
			name:     "no cluster info",
			info:     &jetstream.StreamInfo{Config: jetstream.StreamConfig{Name: "INBOX-FAILOVER-site-a"}},
			expected: "site-b",
			wantErr:  "no cluster info",
		},
		{
			name:     "nil info",
			info:     nil,
			expected: "site-b",
			wantErr:  "no cluster info",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := stream.CheckPlacement(tt.info, tt.expected)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=stream`

Expected: FAIL — `undefined: stream.CheckPlacement`.

- [ ] **Step 3: Write minimal implementation**

Create `pkg/stream/placement.go`:

```go
package stream

import (
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
)

// CheckPlacement verifies a stream is hosted by the expected NATS cluster.
//
// A standby failover stream placed on the very cluster it exists to outlive is
// silently fatal: js.Stream() resolves any stream from any cluster (names are
// unique supercluster-wide), so an existence check passes and the
// misconfiguration only surfaces during the outage it was meant to survive.
// Comparing the hosting cluster is the only check that catches it — and it also
// catches a ring disagreement, where ops provisioned against a different buddy
// than the service is configured with.
//
// Callers pass an already-fetched StreamInfo so this stays a pure function.
// Only the production path should call it: a single-server dev NATS reports no
// cluster at all, which is an error here by design.
func CheckPlacement(info *jetstream.StreamInfo, expectedCluster string) error {
	if info == nil || info.Cluster == nil {
		return fmt.Errorf("stream placement unknown: no cluster info (want cluster %q)", expectedCluster)
	}
	if info.Cluster.Name != expectedCluster {
		return fmt.Errorf("stream %q hosted by cluster %q, want %q",
			info.Config.Name, info.Cluster.Name, expectedCluster)
	}
	return nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=stream`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/stream/placement.go pkg/stream/placement_test.go
git commit -m "feat(stream): add CheckPlacement for standby stream host verification"
```

---

### Task 4: No-responders-gated forward fallback

**Files:**
- Create: `pkg/outbox/failover.go`
- Test: `pkg/outbox/failover_test.go`

**Interfaces:**
- Consumes: `subject.InboxExternal`, `subject.FailoverInboxExternal` (Task 1).
- Produces: `outbox.ForwardWithFailover(ctx context.Context, publish PublishFunc, destSiteID, eventType string, data []byte, msgID string) error` and `type outbox.PublishFunc func(ctx context.Context, subj string, data []byte, msgID string) error`.

**Why only `ErrNoResponders`:** `INBOX-{site}` and `INBOX-FAILOVER-{site}` are
separate streams with independent dedup windows, so a shared `Nats-Msg-Id` does
**not** collapse a duplicate across them. A timeout is ambiguous — the publish
may have landed — so redirecting on one could apply an event twice. No-responders
is unambiguous: no interest existed, nothing was delivered.

**Note on the error value:** the `jetstream` package may surface this as
`jetstream.ErrNoStreamResponse` rather than `nats.ErrNoResponders`. The helper
matches both, and the integration test in Task 9 confirms which one actually
arrives.

- [ ] **Step 1: Write the failing test**

Create `pkg/outbox/failover_test.go`:

```go
package outbox_test

import (
	"context"
	"errors"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/outbox"
)

func TestForwardWithFailover(t *testing.T) {
	tests := []struct {
		name         string
		primaryErr   error
		failoverErr  error
		wantSubjects []string
		wantErr      bool
	}{
		{
			name:         "primary succeeds, no fallback",
			wantSubjects: []string{"chat.inbox.site-a.external.member_added"},
		},
		{
			name:       "no responders falls back",
			primaryErr: nats.ErrNoResponders,
			wantSubjects: []string{
				"chat.inbox.site-a.external.member_added",
				"chat.failover.inbox.site-a.external.member_added",
			},
		},
		{
			name:       "jetstream no stream response falls back",
			primaryErr: jetstream.ErrNoStreamResponse,
			wantSubjects: []string{
				"chat.inbox.site-a.external.member_added",
				"chat.failover.inbox.site-a.external.member_added",
			},
		},
		{
			name:         "timeout does NOT fall back",
			primaryErr:   nats.ErrTimeout,
			wantSubjects: []string{"chat.inbox.site-a.external.member_added"},
			wantErr:      true,
		},
		{
			name:         "unknown error does NOT fall back",
			primaryErr:   errors.New("boom"),
			wantSubjects: []string{"chat.inbox.site-a.external.member_added"},
			wantErr:      true,
		},
		{
			name:        "fallback also fails",
			primaryErr:  nats.ErrNoResponders,
			failoverErr: errors.New("buddy down"),
			wantSubjects: []string{
				"chat.inbox.site-a.external.member_added",
				"chat.failover.inbox.site-a.external.member_added",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got []string
			publish := func(_ context.Context, subj string, _ []byte, msgID string) error {
				got = append(got, subj)
				assert.Equal(t, "dedup-1", msgID, "same dedup id on both lanes")
				if len(got) == 1 {
					return tt.primaryErr
				}
				return tt.failoverErr
			}

			err := outbox.ForwardWithFailover(context.Background(), publish,
				"site-a", "member_added", []byte(`{}`), "dedup-1")

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.wantSubjects, got)
		})
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=outbox`

Expected: FAIL — `undefined: outbox.ForwardWithFailover`.

- [ ] **Step 3: Write minimal implementation**

Create `pkg/outbox/failover.go`:

```go
package outbox

import (
	"context"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/subject"
)

// PublishFunc publishes data on subj; msgID becomes the Nats-Msg-Id for
// stream-level dedup.
type PublishFunc func(ctx context.Context, subj string, data []byte, msgID string) error

// ForwardWithFailover publishes a federation event to the destination site's
// INBOX, redirecting to its buddy-hosted failover lane when — and only when —
// the destination is unambiguously unreachable.
//
// The fallback triggers on no-responders alone, never on a timeout. INBOX and
// INBOX-FAILOVER are separate streams with independent dedup windows, so a
// shared Nats-Msg-Id does NOT collapse a duplicate across them; redirecting an
// ambiguous failure (which may already have landed) would risk applying the
// event twice. No-responders carries no such ambiguity — no interest existed,
// so nothing was delivered.
//
// Any other error returns unchanged, so the caller Naks and parks exactly as
// before this fallback existed.
func ForwardWithFailover(ctx context.Context, publish PublishFunc,
	destSiteID, eventType string, data []byte, msgID string,
) error {
	err := publish(ctx, subject.InboxExternal(destSiteID, eventType), data, msgID)
	if err == nil {
		return nil
	}
	if !isNoResponders(err) {
		return fmt.Errorf("forward to %s inbox: %w", destSiteID, err)
	}
	if ferr := publish(ctx, subject.FailoverInboxExternal(destSiteID, eventType), data, msgID); ferr != nil {
		return fmt.Errorf("forward to %s failover inbox (primary unreachable): %w", destSiteID, ferr)
	}
	return nil
}

// isNoResponders reports whether err means nothing was subscribed to the
// subject, so the publish provably did not land. The jetstream package wraps
// core NATS's ErrNoResponders as ErrNoStreamResponse on the publish path;
// matching both keeps the check correct regardless of which surfaces.
func isNoResponders(err error) bool {
	return errors.Is(err, nats.ErrNoResponders) || errors.Is(err, jetstream.ErrNoStreamResponse)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=outbox`

Expected: PASS, all six subtests.

- [ ] **Step 5: Commit**

```bash
git add pkg/outbox/failover.go pkg/outbox/failover_test.go
git commit -m "feat(outbox): add no-responders-gated inbox forward fallback"
```

---

### Task 5: Route outbox-worker's forward through the fallback

**Files:**
- Modify: `outbox-worker/handler.go:55-62`
- Test: `outbox-worker/handler_test.go`

**Interfaces:**
- Consumes: `outbox.ForwardWithFailover` (Task 4).
- Produces: no new exported surface — `HandleEvent`'s signature is unchanged.

- [ ] **Step 1: Write the failing test**

Add to `outbox-worker/handler_test.go`:

```go
func TestHandleEvent_RedirectsToFailoverOnNoResponders(t *testing.T) {
	var got []string
	h := NewHandler(func(_ context.Context, subj string, _ []byte, _ string) error {
		got = append(got, subj)
		if len(got) == 1 {
			return nats.ErrNoResponders
		}
		return nil
	})

	evt := model.OutboxEvent{Envelope: []byte(`{"event":"member_added"}`), DedupID: "d1", RoomID: "r1"}
	data, err := json.Marshal(evt)
	require.NoError(t, err)

	err = h.HandleEvent(context.Background(), "chat.outbox.site-a.site-b.member_added", data)

	require.NoError(t, err)
	assert.Equal(t, []string{
		"chat.inbox.site-b.external.member_added",
		"chat.failover.inbox.site-b.external.member_added",
	}, got)
}

func TestHandleEvent_TimeoutDoesNotRedirect(t *testing.T) {
	var got []string
	h := NewHandler(func(_ context.Context, subj string, _ []byte, _ string) error {
		got = append(got, subj)
		return nats.ErrTimeout
	})

	evt := model.OutboxEvent{Envelope: []byte(`{"event":"member_added"}`), DedupID: "d1", RoomID: "r1"}
	data, err := json.Marshal(evt)
	require.NoError(t, err)

	err = h.HandleEvent(context.Background(), "chat.outbox.site-a.site-b.member_added", data)

	require.Error(t, err, "a timeout must stay transient so jsretry Naks and parks")
	assert.False(t, errcode.IsPermanent(err), "parking, not poisoning")
	assert.Equal(t, []string{"chat.inbox.site-b.external.member_added"}, got,
		"an ambiguous failure must not double-publish")
}
```

Ensure `github.com/nats-io/nats.go` is imported in the test file.

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=outbox-worker`

Expected: FAIL — `TestHandleEvent_RedirectsToFailoverOnNoResponders` gets only the primary subject, because the handler still publishes once.

- [ ] **Step 3: Write minimal implementation**

In `outbox-worker/handler.go`, replace the publish block (currently lines 55-62):

```go
	// Redelivery is idempotent (DedupID), so a timed-out-but-delivered forward re-forwards safely.
	pubCtx, cancel := context.WithTimeout(ctx, federationForwardTimeout)
	defer cancel()
	if err := outbox.ForwardWithFailover(pubCtx, h.publish, destSiteID, eventType, evt.Envelope, evt.DedupID); err != nil {
		return fmt.Errorf("forward outbox event to %s: %w", destSiteID, err)
	}
	return nil
```

Add `"github.com/hmchangw/chat/pkg/outbox"` to the imports. Change the
`PublishFunc` type declaration at line 17 to an alias so the handler and the
helper cannot drift:

```go
// PublishFunc publishes data; non-empty msgID sets Nats-Msg-Id for JetStream stream-level dedup.
type PublishFunc = outbox.PublishFunc
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=outbox-worker`

Expected: PASS, including the pre-existing handler tests.

- [ ] **Step 5: Commit**

```bash
git add outbox-worker/handler.go outbox-worker/handler_test.go
git commit -m "feat(outbox-worker): redirect forwards to the failover inbox on no-responders"
```

---

### Task 6: Two-server test harness

**Files:**
- Create: `pkg/testutil/nats_buddy.go`
- Modify: `pkg/testutil/terminate.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `testutil.NATSPair(t *testing.T) (home, buddy string)`, `testutil.EnsureNATSBuddy() error`, `testutil.TerminateNATSBuddy()`.

**Fidelity limit to record in the doc comment:** these are two *independent*
JetStream servers, not a supercluster. The harness proves consumer binding and
lane handling across two connections. It cannot prove gateway interest routing,
stream placement, or that a down cluster yields no-responders — those need a real
supercluster in staging.

- [ ] **Step 1: Write the failing test**

Create `pkg/testutil/nats_buddy_test.go`:

```go
//go:build integration

package testutil_test

import (
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/testutil"
)

func TestNATSPair_ReturnsTwoIndependentServers(t *testing.T) {
	home, buddy := testutil.NATSPair(t)
	assert.NotEqual(t, home, buddy, "home and buddy must be distinct servers")

	hc, err := nats.Connect(home)
	require.NoError(t, err)
	t.Cleanup(func() { hc.Close() })

	bc, err := nats.Connect(buddy)
	require.NoError(t, err)
	t.Cleanup(func() { bc.Close() })

	require.NoError(t, hc.Flush())
	require.NoError(t, bc.Flush())

	// Independent, not superclustered: a subject published on one is not
	// delivered on the other. This is the harness's documented fidelity limit.
	sub, err := bc.SubscribeSync("probe.subject")
	require.NoError(t, err)
	require.NoError(t, bc.Flush())

	require.NoError(t, hc.Publish("probe.subject", []byte("x")))
	require.NoError(t, hc.Flush())

	_, err = sub.NextMsg(500 * time.Millisecond)
	assert.ErrorIs(t, err, nats.ErrTimeout, "servers must not be linked")
}
```

Ensure `time` is imported.

- [ ] **Step 2: Run test to verify it fails**

Run: `make test-integration SERVICE=testutil`

Expected: FAIL — `undefined: testutil.NATSPair`.

- [ ] **Step 3: Write minimal implementation**

Create `pkg/testutil/nats_buddy.go`:

```go
//go:build integration

package testutil

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	natsmod "github.com/testcontainers/testcontainers-go/modules/nats"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/hmchangw/chat/pkg/testutil/testimages"
)

var (
	natsBuddyOnce      sync.Once
	natsBuddyContainer testcontainers.Container
	natsBuddyStopProc  func()
	natsBuddyURL       string
	natsBuddyInitErr   error
)

// ensureNATSBuddy starts a SECOND JetStream server, independent of the one
// ensureNATS provides. Same subprocess-then-container strategy as the primary.
func ensureNATSBuddy() (string, error) {
	natsBuddyOnce.Do(func() {
		if u, stop, err := startNATSBinary(); err == nil {
			natsBuddyURL = u
			natsBuddyStopProc = stop
			return
		}
		ctx := context.Background()
		c, err := natsmod.Run(ctx, testimages.NATS,
			testcontainers.WithCmdArgs("--jetstream"),
			testcontainers.WithWaitStrategy(wait.ForLog("Server is ready").WithStartupTimeout(60*time.Second)),
		)
		if err != nil {
			natsBuddyInitErr = fmt.Errorf("start buddy nats: %w", err)
			return
		}
		url, err := c.ConnectionString(ctx)
		if err != nil {
			_ = c.Terminate(ctx)
			natsBuddyInitErr = fmt.Errorf("get buddy nats url: %w", err)
			return
		}
		natsBuddyContainer = c
		natsBuddyURL = url
	})
	return natsBuddyURL, natsBuddyInitErr
}

// NATSPair returns the URLs of two process-shared JetStream servers, for tests
// that exercise a service holding both a home and a buddy connection.
//
// FIDELITY LIMIT: these are two INDEPENDENT servers, not a supercluster. A
// publish on one is not routed to the other. The pair proves consumer binding
// and per-lane handling across two connections; it CANNOT prove gateway
// interest routing, stream placement, or that a down cluster yields
// no-responders. Those require a real supercluster in staging.
func NATSPair(t *testing.T) (home, buddy string) {
	t.Helper()
	h := NATS(t)
	b, err := ensureNATSBuddy()
	if err != nil {
		t.Fatalf("testutil.NATSPair: %v", err)
	}
	return h, b
}

// EnsureNATSBuddy starts the shared buddy server if not already started.
// No-t variant intended for TestMain pre-warming.
func EnsureNATSBuddy() error { _, err := ensureNATSBuddy(); return err }

// TerminateNATSBuddy stops the shared buddy server. Best-effort, idempotent.
func TerminateNATSBuddy() {
	if natsBuddyStopProc != nil {
		natsBuddyStopProc()
		natsBuddyStopProc = nil
		return
	}
	if natsBuddyContainer == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := natsBuddyContainer.Terminate(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "terminate buddy nats: %v\n", err)
	}
	natsBuddyContainer = nil
}
```

In `pkg/testutil/terminate.go`, add `TerminateNATSBuddy()` to `TerminateAll()`
immediately after the existing `TerminateNATS()` call.

- [ ] **Step 4: Run test to verify it passes**

Run: `make test-integration SERVICE=testutil`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/testutil/nats_buddy.go pkg/testutil/nats_buddy_test.go pkg/testutil/terminate.go
git commit -m "test(testutil): add NATSPair two-server harness for buddy-connection tests"
```

---

### Task 6b: Shared buddy-connection helper

Twelve services across this plan and the next open a buddy connection with
identical semantics — unlike the home connection it must never be fatal. Putting
that rule in one place keeps a future service from copying a fail-fast variant by
mistake.

**Files:**
- Create: `pkg/natsutil/buddy.go`
- Test: `pkg/natsutil/buddy_test.go`

**Interfaces:**
- Consumes: `natsutil.Connect`.
- Produces: `natsutil.ConnectBuddy(ctx context.Context, url, credsFile string, tp trace.TracerProvider, prop propagation.TextMapPropagator, tracingEnabled bool) *o11ynats.Conn` — returns `nil` (never an error) when the buddy is unreachable or unconfigured.

- [ ] **Step 1: Write the failing test**

Create `pkg/natsutil/buddy_test.go`:

```go
package natsutil_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/hmchangw/chat/pkg/natsutil"
)

func TestConnectBuddy_UnreachableReturnsNil(t *testing.T) {
	conn := natsutil.ConnectBuddy(context.Background(), "nats://127.0.0.1:1", "",
		noop.NewTracerProvider(), propagation.TraceContext{}, false)
	assert.Nil(t, conn, "an unreachable buddy must degrade to nil, never block startup")
}

func TestConnectBuddy_EmptyURLReturnsNil(t *testing.T) {
	conn := natsutil.ConnectBuddy(context.Background(), "", "",
		noop.NewTracerProvider(), propagation.TraceContext{}, false)
	assert.Nil(t, conn, "an unconfigured buddy is not an error")
}

func TestConnectBuddy_ReachableReturnsConn(t *testing.T) {
	url := startEmbeddedNATS(t) // existing helper used by reply_test.go
	conn := natsutil.ConnectBuddy(context.Background(), url, "",
		noop.NewTracerProvider(), propagation.TraceContext{}, false)
	require.NotNil(t, conn)
	t.Cleanup(func() { conn.NatsConn().Close() })
	assert.True(t, conn.NatsConn().IsConnected())
}
```

Reuse whatever the embedded-server helper in `pkg/natsutil/reply_test.go:40` is
actually named rather than assuming `startEmbeddedNATS`.

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=natsutil`

Expected: FAIL — `undefined: natsutil.ConnectBuddy`.

- [ ] **Step 3: Write minimal implementation**

Create `pkg/natsutil/buddy.go`:

```go
package natsutil

import (
	"context"
	"log/slog"

	o11ynats "github.com/flywindy/o11y/nats"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// ConnectBuddy opens the secondary connection to a site's buddy cluster, where
// its standby failover streams live.
//
// Unlike the home connection — which is fail-fast, because a service with no
// bus cannot work — this NEVER fails startup. A buddy that is already down when
// we start is a double fault, and running home-lane-only is strictly better
// than refusing to boot. A nil return means "no failover lane"; callers skip
// binding it and carry on.
//
// An empty url means the buddy is unconfigured, which is a normal
// single-site deployment, not an error.
func ConnectBuddy(ctx context.Context, url, credsFile string, tp trace.TracerProvider,
	prop propagation.TextMapPropagator, tracingEnabled bool,
) *o11ynats.Conn {
	if url == "" {
		return nil
	}
	conn, err := Connect(ctx, url, credsFile, tp, prop, tracingEnabled)
	if err != nil {
		slog.WarnContext(ctx, "buddy nats connect failed; running without the failover lane",
			"url", url, "error", err)
		return nil
	}
	slog.InfoContext(ctx, "buddy nats connected", "url", url)
	return conn
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=natsutil`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/natsutil/buddy.go pkg/natsutil/buddy_test.go
git commit -m "feat(natsutil): add ConnectBuddy for non-fatal secondary connections"
```

---

### Task 7: inbox-worker buddy connection and failover consumer

**Files:**
- Modify: `inbox-worker/main.go` (config struct ~line 31; wiring ~lines 659-690)
- Modify: `inbox-worker/bootstrap.go`
- Test: `inbox-worker/main_test.go`

**Interfaces:**
- Consumes: `stream.InboxFailover` (Task 2), `stream.CheckPlacement` (Task 3), `subject.FailoverInboxExternalAll` (Task 1), `natsutil.ConnectBuddy` (Task 6b).
- Produces: `buildFailoverConsumerConfig(s stream.ConsumerSettings, siteID string) jetstream.ConsumerConfig`; config fields `BuddySiteID`, `BuddyNatsURL`.

**Connect semantics (spec §D):** home stays fail-fast; a buddy connect failure
logs a warning and the service runs home-lane-only. A buddy that is down at our
startup is a double fault.

- [ ] **Step 1: Write the failing test**

Add to `inbox-worker/main_test.go`:

```go
func TestBuildFailoverConsumerConfig(t *testing.T) {
	cc := buildFailoverConsumerConfig(stream.ConsumerSettings{}, "site-a")

	assert.Equal(t, "inbox-worker-failover", cc.Durable,
		"distinct durable name so the two lanes keep independent cursors")
	assert.Equal(t, []string{"chat.failover.inbox.site-a.external.>"}, cc.FilterSubjects)
}

// The two lanes must never share a durable, or one lane's cursor would clobber
// the other's on the same stream name in a single-cluster dev setup.
func TestFailoverConsumerDurable_DiffersFromPrimary(t *testing.T) {
	primary := buildConsumerConfig(stream.ConsumerSettings{}, "site-a")
	failover := buildFailoverConsumerConfig(stream.ConsumerSettings{}, "site-a")
	assert.NotEqual(t, primary.Durable, failover.Durable)
	assert.NotEqual(t, primary.FilterSubjects, failover.FilterSubjects)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=inbox-worker`

Expected: FAIL — `undefined: buildFailoverConsumerConfig`.

- [ ] **Step 3: Write minimal implementation**

Add to `inbox-worker/main.go` next to `buildConsumerConfig`:

```go
// buildFailoverConsumerConfig returns the durable consumer config for the
// buddy-hosted INBOX-FAILOVER lane. Its durable name is distinct from the
// primary lane's so the two keep independent cursors, and its FilterSubjects
// scope it to the failover root.
func buildFailoverConsumerConfig(s stream.ConsumerSettings, siteID string) jetstream.ConsumerConfig {
	cc := stream.DurableConsumerDefaults(s)
	cc.Durable = "inbox-worker-failover"
	cc.FilterSubjects = []string{subject.FailoverInboxExternalAll(siteID)}
	return cc
}
```

Add to the `config` struct:

```go
	// BuddySiteID is the site whose NATS cluster hosts this site's standby
	// failover streams. Doubles as the expected StreamInfo.Cluster.Name for the
	// placement assertion — NATS cluster names match site IDs in this
	// deployment. Empty disables the failover lane entirely.
	BuddySiteID  string `env:"BUDDY_SITE_ID"  envDefault:""`
	BuddyNatsURL string `env:"BUDDY_NATS_URL" envDefault:""`
```

After the existing home-connection wiring in `main`, add:

```go
	// Buddy lane. ConnectBuddy never fails startup — a nil conn means no
	// failover lane and the service runs home-only.
	if bnc := natsutil.ConnectBuddy(ctx, cfg.BuddyNatsURL, cfg.NatsCredsFile,
		sdk.TracerProvider(), sdk.Propagator, sdk.Toggles.Trace); bnc != nil && cfg.BuddySiteID != "" {
		bjs, err := bnc.JetStream()
		if err != nil {
			slog.Warn("buddy jetstream init failed; running without the failover lane",
				"buddy_site_id", cfg.BuddySiteID, "error", err)
		} else if err := startFailoverLane(ctx, bjs, cfg, handler); err != nil {
			slog.Warn("failover lane unavailable", "buddy_site_id", cfg.BuddySiteID, "error", err)
		}
	}
```

Add `startFailoverLane`, which bootstraps or verifies the stream, asserts its
placement, and creates the consumer:

```go
// startFailoverLane binds the buddy-hosted INBOX-FAILOVER consumer. In
// production it verifies the stream exists AND is hosted by the configured
// buddy cluster — a standby stream placed on the cluster it exists to outlive
// resolves fine through the supercluster-wide JetStream API, so only the
// cluster name catches it.
func startFailoverLane(ctx context.Context, js jetstream.JetStream, cfg config, handler *Handler) error {
	failoverCfg := stream.InboxFailover(cfg.SiteID)

	if cfg.Bootstrap.Enabled {
		if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name:     failoverCfg.Name,
			Subjects: failoverCfg.Subjects,
		}); err != nil {
			return fmt.Errorf("create INBOX-FAILOVER stream: %w", err)
		}
	} else {
		s, err := js.Stream(ctx, failoverCfg.Name)
		if err != nil {
			return fmt.Errorf("verify INBOX-FAILOVER stream: %w", err)
		}
		info, err := s.Info(ctx)
		if err != nil {
			return fmt.Errorf("inspect INBOX-FAILOVER stream: %w", err)
		}
		if err := stream.CheckPlacement(info, cfg.BuddySiteID); err != nil {
			return fmt.Errorf("INBOX-FAILOVER placement: %w", err)
		}
	}

	cons, err := js.CreateOrUpdateConsumer(ctx, failoverCfg.Name,
		buildFailoverConsumerConfig(cfg.Consumer, cfg.SiteID))
	if err != nil {
		return fmt.Errorf("create INBOX-FAILOVER consumer: %w", err)
	}

	slog.Info("failover lane bound", "stream", failoverCfg.Name, "buddy_site_id", cfg.BuddySiteID)
	return startInboxConsumer(ctx, cons, handler)
}
```

Extract the existing two-lane pull loop that currently runs against `cons` in
`main` into `startInboxConsumer(ctx context.Context, cons jetstream.Consumer, handler *Handler) error`,
and call it for both the home and failover consumers. Register the buddy
connection's drain in the same `shutdown.Wait` hook list as the home one, using
`natsutil.Drain(ctx, bnc)`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=inbox-worker`

Expected: PASS.

- [ ] **Step 5: Verify it builds**

Run: `make build SERVICE=inbox-worker`

Expected: builds clean.

- [ ] **Step 6: Commit**

```bash
git add inbox-worker/main.go inbox-worker/bootstrap.go inbox-worker/main_test.go
git commit -m "feat(inbox-worker): consume the buddy-hosted failover inbox lane"
```

---

### Task 8: Compose file wiring for local dev

**Files:**
- Modify: `inbox-worker/deploy/docker-compose.yml`

**Interfaces:**
- Consumes: the config fields from Task 7.
- Produces: nothing consumed by later tasks.

Local dev runs a single NATS, so pointing `BUDDY_NATS_URL` at the same server
exercises the whole code path — stream creation, consumer binding, both lanes
draining — without a second container. Placement is not asserted because
`BOOTSTRAP_STREAMS=true` takes the create branch.

- [ ] **Step 1: Add the env vars**

In the `inbox-worker` service's `environment:` block, alongside the existing
`NATS_URL` and `BOOTSTRAP_STREAMS=true`:

```yaml
      # Dev points the buddy at the same NATS: one server, but the full failover
      # code path (stream create, consumer bind, dual-lane drain) still runs.
      # Placement is not asserted here — BOOTSTRAP_STREAMS=true takes the create
      # branch, and a single-server NATS reports no cluster at all.
      - BUDDY_SITE_ID=site-local
      - BUDDY_NATS_URL=nats://nats:4222
```

Match the existing `NATS_URL` host and port in that file rather than copying
this verbatim.

- [ ] **Step 2: Verify the service starts with both lanes**

Run: `docker compose -f inbox-worker/deploy/docker-compose.yml up --build -d`

Then: `docker compose -f inbox-worker/deploy/docker-compose.yml logs inbox-worker | grep "failover lane bound"`

Expected: one `failover lane bound` line naming `INBOX-FAILOVER-site-local`.

- [ ] **Step 3: Tear down**

Run: `docker compose -f inbox-worker/deploy/docker-compose.yml down -v`

- [ ] **Step 4: Commit**

```bash
git add inbox-worker/deploy/docker-compose.yml
git commit -m "chore(inbox-worker): wire the failover lane into local compose"
```

---

### Task 9: End-to-end integration test

**Files:**
- Modify: `inbox-worker/integration_test.go`

**Interfaces:**
- Consumes: everything from Tasks 1-7.
- Produces: nothing.

This is the task that proves the feature. It also **confirms which error value a
publish to a nonexistent stream actually returns**, which Task 4's helper matches
defensively.

- [ ] **Step 1: Write the failing test**

Add to `inbox-worker/integration_test.go`:

```go
// A federation event redirected to the buddy-hosted failover lane must land in
// the SAME database as one delivered through the primary lane — the whole point
// is that the down site's own worker does the work against its own store.
func TestFailoverLane_AppliesToSameStore(t *testing.T) {
	homeURL, buddyURL := testutil.NATSPair(t)
	db := testutil.MongoDB(t, "inboxfailover")
	ctx := context.Background()

	store := newMongoStore(db)
	require.NoError(t, store.EnsureIndexes(ctx))
	handler := NewHandler(store)

	homeJS := connectJS(t, homeURL)
	buddyJS := connectJS(t, buddyURL)

	primary := stream.Inbox("site-a")
	_, err := homeJS.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: primary.Name, Subjects: primary.Subjects,
	})
	require.NoError(t, err)

	failover := stream.InboxFailover("site-a")
	_, err = buddyJS.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: failover.Name, Subjects: failover.Subjects,
	})
	require.NoError(t, err)

	startLane(t, ctx, homeJS, primary.Name, buildConsumerConfig(stream.ConsumerSettings{}, "site-a"), handler)
	startLane(t, ctx, buddyJS, failover.Name, buildFailoverConsumerConfig(stream.ConsumerSettings{}, "site-a"), handler)

	// Same event shape on each lane, distinct rooms so the assertions are independent.
	publishStatusEvent(t, ctx, homeJS, subject.InboxExternal("site-a", "user_status_updated"), "alice", "home-status")
	publishStatusEvent(t, ctx, buddyJS, subject.FailoverInboxExternal("site-a", "user_status_updated"), "bob", "failover-status")

	require.Eventually(t, func() bool {
		return userStatus(t, ctx, db, "alice") == "home-status" &&
			userStatus(t, ctx, db, "bob") == "failover-status"
	}, 10*time.Second, 100*time.Millisecond,
		"both lanes must drain into the same store")
}

// Confirms the error a JetStream publish returns when no stream captures the
// subject. pkg/outbox.isNoResponders matches both candidates; this pins which
// one the client library actually produces so the helper stays honest.
func TestPublishToMissingStream_ReturnsNoResponders(t *testing.T) {
	homeURL, _ := testutil.NATSPair(t)
	js := connectJS(t, homeURL)

	_, err := js.Publish(context.Background(), "chat.inbox.nonexistent-site.external.member_added", []byte(`{}`))

	require.Error(t, err)
	assert.True(t,
		errors.Is(err, nats.ErrNoResponders) || errors.Is(err, jetstream.ErrNoStreamResponse),
		"publish to an uncaptured subject must be an unambiguous no-responders, got %v (%T)", err, err)
	t.Logf("publish-to-missing-stream error: %v (%T)", err, err)
}
```

Write the helpers `connectJS`, `startLane`, `publishStatusEvent`, and
`userStatus` in the same file if the existing integration test file does not
already provide equivalents — reuse whatever is there rather than duplicating.
`startLane` creates the consumer and runs `startInboxConsumer` in a goroutine
with a `t.Cleanup` that stops it.

- [ ] **Step 2: Run test to verify it fails**

Run: `make test-integration SERVICE=inbox-worker`

Expected: FAIL — before Task 7's wiring is exercised, the failover lane never
drains and the `Eventually` times out on `bob`.

- [ ] **Step 3: Make it pass**

If Tasks 1-7 are complete, no new production code is needed. Fix only test
wiring — helper signatures, consumer startup, cleanup ordering.

- [ ] **Step 4: Run the full integration suite**

Run: `make test-integration SERVICE=inbox-worker`

Expected: PASS. Note the logged error type from
`TestPublishToMissingStream_ReturnsNoResponders`; if it is neither candidate,
**stop and report it** — Task 4's fallback gate depends on matching it, and the
whole redirect is inert if the error does not match.

- [ ] **Step 5: Commit**

```bash
git add inbox-worker/integration_test.go
git commit -m "test(inbox-worker): cover the buddy failover lane end to end"
```

---

### Task 10: Documentation

**Files:**
- Modify: `docs/nats-subject-naming.md`
- Modify: `docs/architecture.md`

No `docs/client-api.md` change: nothing here is client-facing. That doc changes
in Plan 3.

- [ ] **Step 1: Document the failover subject root**

In `docs/nats-subject-naming.md`, add a section after the inbox/outbox
subjects describing:

- `chat.failover.inbox.{siteID}.external.{eventType}` — a peer's redirected
  federation forward when `{siteID}`'s own INBOX is unreachable. Captured by
  `INBOX-FAILOVER-{siteID}`, hosted on `{siteID}`'s buddy cluster.
- Why the root is `chat.failover.>` and not a token appended to `chat.inbox.>`:
  two streams in one account may not claim overlapping subject filters, and the
  constraint is enforced supercluster-wide.
- Why it is not under `chat.local.>`: that prefix is filtered from gateway
  interest advertisement, so a failover subject beneath it would never cross a
  gateway.

- [ ] **Step 2: Correct the stale architecture diagrams**

In `docs/architecture.md`:

- §3: delete the `BW-->>NATS: pub outbox.{site}.to.{dest}.*` line.
  `broadcast-worker` has no inbox/outbox code; cross-site message delivery is
  core NATS on `chat.room.{roomID}.>`.
- §3 and §4: delete the `FANOUT-{site}` stream and its edges.
  `pkg/stream/stream.go` has no `Fanout()`; `message-worker`,
  `broadcast-worker` and `notification-worker` all bind `MESSAGES-CANONICAL`
  through `stream.Resolve`. Redraw those three as consuming `CANON` directly.
- §4: add `INBOX-FAILOVER-{site}` as a standby stream fed by remote peers when
  `INBOX-{site}` is unreachable.

- [ ] **Step 3: Verify no other doc references the removed stream**

Run: `grep -rn "FANOUT" docs/`

Expected: no matches outside historical plan files under
`docs/superpowers/plans/`, which are point-in-time records and stay as-is.

- [ ] **Step 4: Commit**

```bash
git add docs/nats-subject-naming.md docs/architecture.md
git commit -m "docs: document the failover subject root and correct stale stream diagrams"
```

---

## Final Verification

- [ ] **Run the full unit suite:** `make test` — all packages pass with `-race`.
- [ ] **Run lint:** `make lint` — clean.
- [ ] **Run SAST:** `make sast` — no medium+ findings.
- [ ] **Run the affected integration suites:** `make test-integration SERVICE=inbox-worker` and `make test-integration SERVICE=outbox-worker`.
- [ ] **Check coverage against the 80% floor** for `pkg/outbox`, `pkg/stream`, `pkg/subject`: `go test -coverprofile=coverage.out ./pkg/outbox/... && go tool cover -func=coverage.out`.

## Staging Verification — cannot be covered by tests

The two-server harness is not a supercluster, so three things this plan depends
on are only verifiable against a real deployment. Do these before relying on the
feature:

1. **Subject-overlap enforcement is supercluster-wide.** Attempt to create a
   stream on one cluster whose subjects overlap a stream on another. Expect
   rejection. If it succeeds, the `chat.failover.>` root is unnecessary and the
   design simplifies considerably — report it rather than proceeding.
2. **Placement.** `nats stream info INBOX-FAILOVER-{site}` on each site;
   confirm the hosting cluster is that site's buddy.
3. **No-responders on a down cluster.** Stop a site's NATS and confirm a peer's
   forward returns no-responders rather than timing out. The redirect degrades
   silently to today's parking behavior if it times out instead.
