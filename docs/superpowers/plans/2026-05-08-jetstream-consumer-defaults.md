# JetStream Consumer Defaults Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Standardize JetStream durable consumer configuration across all worker services with a shared `pkg/stream` helper and per-service `MaxAckPending` recommendations.

**Architecture:** Add `DurableConsumerDefaults()` to `pkg/stream` returning a baseline `jetstream.ConsumerConfig` with project-wide defaults. Each worker service extracts an unexported `buildConsumerConfig` helper that overlays the service's own `Durable`, `MaxAckPending`, and any service-specific overrides on top of the defaults. Helpers are unit-tested; `main.go` uses them at consumer-creation sites.

**Tech Stack:** Go 1.25, `github.com/nats-io/nats.go/jetstream`, `testify` for assertions.

**Spec:** `docs/superpowers/specs/2026-05-08-jetstream-consumer-defaults-design.md`

**Branch:** `claude/jetstream-consumer-config-JTIKh` (already checked out)

---

## File Structure

| File | Action | Responsibility |
|---|---|---|
| `pkg/stream/consumer.go` | Create | `DurableConsumerDefaults()` + exported constants |
| `pkg/stream/consumer_test.go` | Create | Unit tests for the defaults helper |
| `message-gatekeeper/main.go` | Modify | Replace inline `ConsumerConfig` with helper |
| `message-gatekeeper/consumer_config_test.go` | Create | Unit test for service helper |
| `broadcast-worker/main.go` | Modify | Replace inline `ConsumerConfig` with helper |
| `broadcast-worker/consumer_config_test.go` | Create | Unit test for service helper |
| `message-worker/main.go` | Modify | Replace inline `ConsumerConfig`, drop `MaxRedeliver` |
| `message-worker/consumer_config_test.go` | Create | Unit test for service helper |
| `notification-worker/main.go` | Modify | Replace inline `ConsumerConfig` with helper |
| `notification-worker/consumer_config_test.go` | Create | Unit test for service helper |
| `room-worker/main.go` | Modify | Replace inline `ConsumerConfig` with helper |
| `room-worker/consumer_config_test.go` | Create | Unit test for service helper |
| `inbox-worker/main.go` | Modify | Replace inline `ConsumerConfig` with helper |
| `inbox-worker/consumer_config_test.go` | Create | Unit test for service helper |
| `search-sync-worker/main.go` | Modify | Replace inline `ConsumerConfig` with helper |
| `search-sync-worker/consumer_config_test.go` | Create | Unit test for service helper |

Each service places its `buildConsumerConfig` helper in the existing `main.go` (small enough to keep there; matches the project's flat layout). Tests live in a sibling `consumer_config_test.go` to keep the helper test isolated from the larger `bootstrap_test.go` / `handler_test.go` files.

---

## Task 1: Add `DurableConsumerDefaults` helper to `pkg/stream`

**Files:**
- Create: `pkg/stream/consumer.go`
- Create: `pkg/stream/consumer_test.go`

- [ ] **Step 1.1: Write the failing test**

Create `pkg/stream/consumer_test.go`:

```go
package stream_test

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/stream"
)

func TestDurableConsumerDefaults(t *testing.T) {
	cc := stream.DurableConsumerDefaults()

	assert.Equal(t, jetstream.AckExplicitPolicy, cc.AckPolicy, "AckPolicy")
	assert.Equal(t, 30*time.Second, cc.AckWait, "AckWait")
	assert.Equal(t, 5, cc.MaxDeliver, "MaxDeliver")
	assert.Equal(t, 512, cc.MaxWaiting, "MaxWaiting")
	assert.Equal(t, jetstream.DeliverNewPolicy, cc.DeliverPolicy, "DeliverPolicy")

	// Caller-owned fields are intentionally zero.
	assert.Empty(t, cc.Durable, "Durable must be set by caller")
	assert.Zero(t, cc.MaxAckPending, "MaxAckPending must be set by caller")
	assert.Empty(t, cc.FilterSubjects, "FilterSubjects must be set by caller if needed")
}

func TestDurableConsumerDefaultsConstants(t *testing.T) {
	assert.Equal(t, 30*time.Second, stream.DefaultAckWait)
	assert.Equal(t, 5, stream.DefaultMaxDeliver)
	assert.Equal(t, 512, stream.DefaultMaxWaiting)
}
```

- [ ] **Step 1.2: Run test to verify it fails**

Run: `make test SERVICE=pkg/stream`
Expected: FAIL — `undefined: stream.DurableConsumerDefaults`, `undefined: stream.DefaultAckWait`, etc.

- [ ] **Step 1.3: Write the helper**

Create `pkg/stream/consumer.go`:

```go
package stream

import (
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Project-wide defaults for durable JetStream consumers. Exported so
// individual services can reference them in tests and documentation.
const (
	DefaultAckWait    = 30 * time.Second
	DefaultMaxDeliver = 5
	DefaultMaxWaiting = 512 // NATS 2.10 default
)

// DurableConsumerDefaults returns a ConsumerConfig populated with the
// project-wide standard knobs for durable JetStream consumers.
//
// Callers MUST set Durable. Callers SHOULD set MaxAckPending sized for
// their service's pull concurrency, and FilterSubjects if they need to
// scope the consumer to a subset of the stream's subjects.
//
// DeliverPolicy is honored only at consumer creation. Updating an
// existing durable via js.CreateOrUpdateConsumer does not reset its
// cursor position.
func DurableConsumerDefaults() jetstream.ConsumerConfig {
	return jetstream.ConsumerConfig{
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       DefaultAckWait,
		MaxDeliver:    DefaultMaxDeliver,
		MaxWaiting:    DefaultMaxWaiting,
		DeliverPolicy: jetstream.DeliverNewPolicy,
	}
}
```

- [ ] **Step 1.4: Run test to verify it passes**

Run: `make test SERVICE=pkg/stream`
Expected: PASS for both `TestDurableConsumerDefaults` and `TestDurableConsumerDefaultsConstants`.

- [ ] **Step 1.5: Lint**

Run: `make lint`
Expected: clean.

- [ ] **Step 1.6: Commit**

```bash
git add pkg/stream/consumer.go pkg/stream/consumer_test.go
git commit -m "feat(pkg/stream): add DurableConsumerDefaults helper"
```

---

## Task 2: Apply defaults in `message-gatekeeper`

**Files:**
- Modify: `message-gatekeeper/main.go:95-103` (consumer creation)
- Create: `message-gatekeeper/consumer_config_test.go`

- [ ] **Step 2.1: Write the failing test**

Create `message-gatekeeper/consumer_config_test.go`:

```go
package main

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
)

func TestBuildConsumerConfig(t *testing.T) {
	cc := buildConsumerConfig()

	assert.Equal(t, "message-gatekeeper", cc.Durable)
	assert.Equal(t, 1000, cc.MaxAckPending)
	assert.Equal(t, jetstream.AckExplicitPolicy, cc.AckPolicy)
	assert.Equal(t, 30*time.Second, cc.AckWait)
	assert.Equal(t, 5, cc.MaxDeliver)
	assert.Equal(t, 512, cc.MaxWaiting)
	assert.Equal(t, jetstream.DeliverNewPolicy, cc.DeliverPolicy)
}
```

- [ ] **Step 2.2: Run test to verify it fails**

Run: `make test SERVICE=message-gatekeeper`
Expected: FAIL — `undefined: buildConsumerConfig`.

- [ ] **Step 2.3: Add the helper and rewire `main.go`**

Open `message-gatekeeper/main.go`. At line 95-103, the current code is:

```go
messagesCfg := stream.Messages(cfg.SiteID)
cons, err := js.CreateOrUpdateConsumer(ctx, messagesCfg.Name, jetstream.ConsumerConfig{
    Durable:   "message-gatekeeper",
    AckPolicy: jetstream.AckExplicitPolicy,
})
```

Replace with:

```go
messagesCfg := stream.Messages(cfg.SiteID)
cons, err := js.CreateOrUpdateConsumer(ctx, messagesCfg.Name, buildConsumerConfig())
```

Add this helper to the bottom of `message-gatekeeper/main.go` (after `main()`):

```go
// buildConsumerConfig returns the durable consumer config for
// message-gatekeeper. Centralized so it is unit-testable without NATS.
func buildConsumerConfig() jetstream.ConsumerConfig {
	cc := stream.DurableConsumerDefaults()
	cc.Durable = "message-gatekeeper"
	cc.MaxAckPending = 1000
	return cc
}
```

- [ ] **Step 2.4: Run test to verify it passes**

Run: `make test SERVICE=message-gatekeeper`
Expected: PASS for `TestBuildConsumerConfig`. All other unit tests in the package continue to pass.

- [ ] **Step 2.5: Verify the service still builds**

Run: `make build SERVICE=message-gatekeeper`
Expected: clean build, no errors.

- [ ] **Step 2.6: Lint**

Run: `make lint`
Expected: clean.

- [ ] **Step 2.7: Commit**

```bash
git add message-gatekeeper/main.go message-gatekeeper/consumer_config_test.go
git commit -m "feat(message-gatekeeper): apply standard durable consumer defaults"
```

---

## Task 3: Apply defaults in `broadcast-worker`

**Files:**
- Modify: `broadcast-worker/main.go:116-121` (consumer creation)
- Create: `broadcast-worker/consumer_config_test.go`

- [ ] **Step 3.1: Write the failing test**

Create `broadcast-worker/consumer_config_test.go`:

```go
package main

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
)

func TestBuildConsumerConfig(t *testing.T) {
	cc := buildConsumerConfig()

	assert.Equal(t, "broadcast-worker", cc.Durable)
	assert.Equal(t, 1000, cc.MaxAckPending)
	assert.Equal(t, jetstream.AckExplicitPolicy, cc.AckPolicy)
	assert.Equal(t, 30*time.Second, cc.AckWait)
	assert.Equal(t, 5, cc.MaxDeliver)
	assert.Equal(t, 512, cc.MaxWaiting)
	assert.Equal(t, jetstream.DeliverNewPolicy, cc.DeliverPolicy)
}
```

- [ ] **Step 3.2: Run test to verify it fails**

Run: `make test SERVICE=broadcast-worker`
Expected: FAIL — `undefined: buildConsumerConfig`.

- [ ] **Step 3.3: Add the helper and rewire `main.go`**

In `broadcast-worker/main.go` at line 116-121, the current code is:

```go
canonicalCfg := stream.MessagesCanonical(cfg.SiteID)

cons, err := js.CreateOrUpdateConsumer(ctx, canonicalCfg.Name, jetstream.ConsumerConfig{
    Durable:   "broadcast-worker",
    AckPolicy: jetstream.AckExplicitPolicy,
})
```

Replace with:

```go
canonicalCfg := stream.MessagesCanonical(cfg.SiteID)

cons, err := js.CreateOrUpdateConsumer(ctx, canonicalCfg.Name, buildConsumerConfig())
```

Add this helper to the bottom of `broadcast-worker/main.go`:

```go
// buildConsumerConfig returns the durable consumer config for
// broadcast-worker. Centralized so it is unit-testable without NATS.
func buildConsumerConfig() jetstream.ConsumerConfig {
	cc := stream.DurableConsumerDefaults()
	cc.Durable = "broadcast-worker"
	cc.MaxAckPending = 1000
	return cc
}
```

- [ ] **Step 3.4: Run test to verify it passes**

Run: `make test SERVICE=broadcast-worker`
Expected: PASS.

- [ ] **Step 3.5: Build and lint**

Run: `make build SERVICE=broadcast-worker && make lint`
Expected: clean.

- [ ] **Step 3.6: Commit**

```bash
git add broadcast-worker/main.go broadcast-worker/consumer_config_test.go
git commit -m "feat(broadcast-worker): apply standard durable consumer defaults"
```

---

## Task 4: Apply defaults in `message-worker` and remove `MaxRedeliver`

**Context:** `message-worker` has a custom `MaxRedeliver` config field (default `5`) referenced only in `main.go` lines 34 and 118. No deploy manifests set `MAX_REDELIVER`. Verified by `grep -rn "MAX_REDELIVER" /home/user/chat/` — only the spec doc and the field/usage. The prior code computed `MaxDeliver = MaxRedeliver + 1 = 6` (1 initial + 5 retries). The unified default `MaxDeliver = 5` (1 initial + 4 retries) is a deliberate 1-attempt reduction in retry budget — accepted as part of unifying the project standard.

**Files:**
- Modify: `message-worker/main.go:34` (remove `MaxRedeliver` config field)
- Modify: `message-worker/main.go:113-119` (consumer creation)
- Create: `message-worker/consumer_config_test.go`

- [ ] **Step 4.1: Write the failing test**

Create `message-worker/consumer_config_test.go`:

```go
package main

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
)

func TestBuildConsumerConfig(t *testing.T) {
	cc := buildConsumerConfig()

	assert.Equal(t, "message-worker", cc.Durable)
	assert.Equal(t, 500, cc.MaxAckPending)
	assert.Equal(t, jetstream.AckExplicitPolicy, cc.AckPolicy)
	assert.Equal(t, 30*time.Second, cc.AckWait)
	assert.Equal(t, 5, cc.MaxDeliver)
	assert.Equal(t, 512, cc.MaxWaiting)
	assert.Equal(t, jetstream.DeliverNewPolicy, cc.DeliverPolicy)
}
```

- [ ] **Step 4.2: Run test to verify it fails**

Run: `make test SERVICE=message-worker`
Expected: FAIL — `undefined: buildConsumerConfig`.

- [ ] **Step 4.3: Remove the `MaxRedeliver` config field**

In `message-worker/main.go` at line 34, remove this line entirely:

```go
	MaxRedeliver      int             `env:"MAX_REDELIVER"      envDefault:"5"`
```

- [ ] **Step 4.4: Add the helper and rewire `main.go`**

In `message-worker/main.go` at line 113-119, the current code is:

```go
canonicalCfg := stream.MessagesCanonical(cfg.SiteID)

cons, err := js.CreateOrUpdateConsumer(ctx, canonicalCfg.Name, jetstream.ConsumerConfig{
    Durable:    "message-worker",
    AckPolicy:  jetstream.AckExplicitPolicy,
    MaxDeliver: cfg.MaxRedeliver + 1, // initial delivery + MaxRedeliver retries
})
```

Replace with:

```go
canonicalCfg := stream.MessagesCanonical(cfg.SiteID)

cons, err := js.CreateOrUpdateConsumer(ctx, canonicalCfg.Name, buildConsumerConfig())
```

Add this helper to the bottom of `message-worker/main.go`:

```go
// buildConsumerConfig returns the durable consumer config for
// message-worker. Centralized so it is unit-testable without NATS.
func buildConsumerConfig() jetstream.ConsumerConfig {
	cc := stream.DurableConsumerDefaults()
	cc.Durable = "message-worker"
	cc.MaxAckPending = 500
	return cc
}
```

- [ ] **Step 4.5: Run tests to verify**

Run: `make test SERVICE=message-worker`
Expected: PASS for `TestBuildConsumerConfig`. All other tests continue to pass — confirm `MaxRedeliver` is not referenced in any test file.

If a test references `MaxRedeliver` (search with `grep -n MaxRedeliver message-worker/`), update it to drop the reference. The expected pre-edit grep output is empty outside `main.go`.

- [ ] **Step 4.6: Build and lint**

Run: `make build SERVICE=message-worker && make lint`
Expected: clean.

- [ ] **Step 4.7: Commit**

```bash
git add message-worker/main.go message-worker/consumer_config_test.go
git commit -m "feat(message-worker): apply standard consumer defaults, drop MaxRedeliver"
```

---

## Task 5: Apply defaults in `notification-worker`

**Files:**
- Modify: `notification-worker/main.go:100-105` (consumer creation)
- Create: `notification-worker/consumer_config_test.go`

- [ ] **Step 5.1: Write the failing test**

Create `notification-worker/consumer_config_test.go`:

```go
package main

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
)

func TestBuildConsumerConfig(t *testing.T) {
	cc := buildConsumerConfig()

	assert.Equal(t, "notification-worker", cc.Durable)
	assert.Equal(t, 500, cc.MaxAckPending)
	assert.Equal(t, jetstream.AckExplicitPolicy, cc.AckPolicy)
	assert.Equal(t, 30*time.Second, cc.AckWait)
	assert.Equal(t, 5, cc.MaxDeliver)
	assert.Equal(t, 512, cc.MaxWaiting)
	assert.Equal(t, jetstream.DeliverNewPolicy, cc.DeliverPolicy)
}
```

- [ ] **Step 5.2: Run test to verify it fails**

Run: `make test SERVICE=notification-worker`
Expected: FAIL — `undefined: buildConsumerConfig`.

- [ ] **Step 5.3: Add the helper and rewire `main.go`**

In `notification-worker/main.go` at line 100-105, the current code is:

```go
canonicalCfg := stream.MessagesCanonical(cfg.SiteID)

cons, err := js.CreateOrUpdateConsumer(ctx, canonicalCfg.Name, jetstream.ConsumerConfig{
    Durable:   "notification-worker",
    AckPolicy: jetstream.AckExplicitPolicy,
})
```

Replace with:

```go
canonicalCfg := stream.MessagesCanonical(cfg.SiteID)

cons, err := js.CreateOrUpdateConsumer(ctx, canonicalCfg.Name, buildConsumerConfig())
```

Add this helper to the bottom of `notification-worker/main.go`:

```go
// buildConsumerConfig returns the durable consumer config for
// notification-worker. Centralized so it is unit-testable without NATS.
func buildConsumerConfig() jetstream.ConsumerConfig {
	cc := stream.DurableConsumerDefaults()
	cc.Durable = "notification-worker"
	cc.MaxAckPending = 500
	return cc
}
```

- [ ] **Step 5.4: Run test, build, lint**

Run: `make test SERVICE=notification-worker && make build SERVICE=notification-worker && make lint`
Expected: PASS, clean build, clean lint.

- [ ] **Step 5.5: Commit**

```bash
git add notification-worker/main.go notification-worker/consumer_config_test.go
git commit -m "feat(notification-worker): apply standard durable consumer defaults"
```

---

## Task 6: Apply defaults in `room-worker`

**Files:**
- Modify: `room-worker/main.go:99-101` (consumer creation)
- Create: `room-worker/consumer_config_test.go`

- [ ] **Step 6.1: Write the failing test**

Create `room-worker/consumer_config_test.go`:

```go
package main

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
)

func TestBuildConsumerConfig(t *testing.T) {
	cc := buildConsumerConfig()

	assert.Equal(t, "room-worker", cc.Durable)
	assert.Equal(t, 200, cc.MaxAckPending)
	assert.Equal(t, jetstream.AckExplicitPolicy, cc.AckPolicy)
	assert.Equal(t, 30*time.Second, cc.AckWait)
	assert.Equal(t, 5, cc.MaxDeliver)
	assert.Equal(t, 512, cc.MaxWaiting)
	assert.Equal(t, jetstream.DeliverNewPolicy, cc.DeliverPolicy)
}
```

- [ ] **Step 6.2: Run test to verify it fails**

Run: `make test SERVICE=room-worker`
Expected: FAIL — `undefined: buildConsumerConfig`.

- [ ] **Step 6.3: Add the helper and rewire `main.go`**

In `room-worker/main.go` at line 99-101, the current code is:

```go
cons, err := js.CreateOrUpdateConsumer(ctx, streamCfg.Name, jetstream.ConsumerConfig{
    Durable: "room-worker", AckPolicy: jetstream.AckExplicitPolicy,
})
```

Replace with:

```go
cons, err := js.CreateOrUpdateConsumer(ctx, streamCfg.Name, buildConsumerConfig())
```

Add this helper to the bottom of `room-worker/main.go`:

```go
// buildConsumerConfig returns the durable consumer config for
// room-worker. Centralized so it is unit-testable without NATS.
func buildConsumerConfig() jetstream.ConsumerConfig {
	cc := stream.DurableConsumerDefaults()
	cc.Durable = "room-worker"
	cc.MaxAckPending = 200
	return cc
}
```

- [ ] **Step 6.4: Run test, build, lint**

Run: `make test SERVICE=room-worker && make build SERVICE=room-worker && make lint`
Expected: PASS, clean build, clean lint.

- [ ] **Step 6.5: Commit**

```bash
git add room-worker/main.go room-worker/consumer_config_test.go
git commit -m "feat(room-worker): apply standard durable consumer defaults"
```

---

## Task 7: Apply defaults in `inbox-worker`

**Files:**
- Modify: `inbox-worker/main.go:228-235` (consumer creation, preserving `FilterSubjects`)
- Create: `inbox-worker/consumer_config_test.go`

- [ ] **Step 7.1: Write the failing test**

Create `inbox-worker/consumer_config_test.go`:

```go
package main

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/subject"
)

func TestBuildConsumerConfig(t *testing.T) {
	siteID := "site-a"
	cc := buildConsumerConfig(siteID)

	assert.Equal(t, "inbox-worker", cc.Durable)
	assert.Equal(t, 100, cc.MaxAckPending)
	assert.Equal(t, []string{subject.InboxAggregateAll(siteID)}, cc.FilterSubjects)
	assert.Equal(t, jetstream.AckExplicitPolicy, cc.AckPolicy)
	assert.Equal(t, 30*time.Second, cc.AckWait)
	assert.Equal(t, 5, cc.MaxDeliver)
	assert.Equal(t, 512, cc.MaxWaiting)
	assert.Equal(t, jetstream.DeliverNewPolicy, cc.DeliverPolicy)
}
```

- [ ] **Step 7.2: Run test to verify it fails**

Run: `make test SERVICE=inbox-worker`
Expected: FAIL — `undefined: buildConsumerConfig`.

- [ ] **Step 7.3: Add the helper and rewire `main.go`**

In `inbox-worker/main.go` at line 228-235, the current code is:

```go
inboxCfg := stream.Inbox(cfg.SiteID)

// Local lane is reserved for search-sync-worker; scope to aggregate.> only.
cons, err := js.CreateOrUpdateConsumer(ctx, inboxCfg.Name, jetstream.ConsumerConfig{
    Durable:        "inbox-worker",
    AckPolicy:      jetstream.AckExplicitPolicy,
    FilterSubjects: []string{subject.InboxAggregateAll(cfg.SiteID)},
})
```

Replace with:

```go
inboxCfg := stream.Inbox(cfg.SiteID)

// Local lane is reserved for search-sync-worker; scope to aggregate.> only.
cons, err := js.CreateOrUpdateConsumer(ctx, inboxCfg.Name, buildConsumerConfig(cfg.SiteID))
```

Add this helper to the bottom of `inbox-worker/main.go`:

```go
// buildConsumerConfig returns the durable consumer config for
// inbox-worker. The site-scoped FilterSubjects keeps inbox-worker on the
// federated `aggregate.>` lane only; same-site direct publishes are
// reserved for search-sync-worker.
func buildConsumerConfig(siteID string) jetstream.ConsumerConfig {
	cc := stream.DurableConsumerDefaults()
	cc.Durable = "inbox-worker"
	cc.MaxAckPending = 100
	cc.FilterSubjects = []string{subject.InboxAggregateAll(siteID)}
	return cc
}
```

- [ ] **Step 7.4: Run test, build, lint**

Run: `make test SERVICE=inbox-worker && make build SERVICE=inbox-worker && make lint`
Expected: PASS, clean build, clean lint.

- [ ] **Step 7.5: Commit**

```bash
git add inbox-worker/main.go inbox-worker/consumer_config_test.go
git commit -m "feat(inbox-worker): apply standard durable consumer defaults"
```

---

## Task 8: Apply defaults in `search-sync-worker` (3 consumers)

**Context:** `search-sync-worker` creates one consumer per collection inside a loop. The helper takes the collection and `siteID` and preserves the existing `BackOff: [1s, 5s, 30s]` and per-collection `FilterSubjects`. With `MaxDeliver = 5` (from defaults) and 3 `BackOff` entries, the 4th and 5th retries reuse the last entry (30s) — this is intentional NATS behavior.

**Files:**
- Modify: `search-sync-worker/main.go:194-202` (consumer config construction inside the loop)
- Create: `search-sync-worker/consumer_config_test.go`

**Reference:** `Collection` is defined in `search-sync-worker/collection.go:14`. The relevant methods on it are `ConsumerName() string` and `FilterSubjects(siteID string) []string`. The package already has fake/stub collections in `handler_test.go` (`stubCollection`, `fanOutCollection`); we'll use a similar local fake in our new test file.

- [ ] **Step 8.1: Write the failing test**

Create `search-sync-worker/consumer_config_test.go`:

```go
package main

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
)

type fakeCollection struct {
	name    string
	filters []string
}

func (f fakeCollection) ConsumerName() string             { return f.name }
func (f fakeCollection) FilterSubjects(_ string) []string { return f.filters }

func TestBuildConsumerConfig(t *testing.T) {
	tests := []struct {
		name        string
		coll        fakeCollection
		siteID      string
		wantFilters []string
	}{
		{
			name:        "with filters",
			coll:        fakeCollection{name: "message-sync", filters: []string{"chat.msg.canonical.site-a.created"}},
			siteID:      "site-a",
			wantFilters: []string{"chat.msg.canonical.site-a.created"},
		},
		{
			name:        "without filters",
			coll:        fakeCollection{name: "spotlight-sync", filters: nil},
			siteID:      "site-a",
			wantFilters: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cc := buildConsumerConfig(tt.coll, tt.siteID)

			assert.Equal(t, tt.coll.name, cc.Durable)
			assert.Equal(t, 500, cc.MaxAckPending)
			assert.Equal(t, tt.wantFilters, cc.FilterSubjects)
			assert.Equal(t, []time.Duration{1 * time.Second, 5 * time.Second, 30 * time.Second}, cc.BackOff)
			assert.Equal(t, jetstream.AckExplicitPolicy, cc.AckPolicy)
			assert.Equal(t, 30*time.Second, cc.AckWait)
			assert.Equal(t, 5, cc.MaxDeliver)
			assert.Equal(t, 512, cc.MaxWaiting)
			assert.Equal(t, jetstream.DeliverNewPolicy, cc.DeliverPolicy)
		})
	}
}
```

`fakeCollection` satisfies enough of the `Collection` interface for `buildConsumerConfig` (which only calls `ConsumerName()` and `FilterSubjects()`). `buildConsumerConfig` accepts `Collection` as its parameter type, so the fake must implement the full `Collection` interface — but since Go interfaces are structural and `buildConsumerConfig` only invokes those two methods, define the helper to take a narrower interface:

```go
type consumerSource interface {
	ConsumerName() string
	FilterSubjects(siteID string) []string
}
```

Use `consumerSource` as the helper's parameter type. This keeps the helper testable without implementing the full `Collection` interface in tests, and the call site in `main.go` still passes a `Collection` value, which automatically satisfies `consumerSource`.

- [ ] **Step 8.2: Run test to verify it fails**

Run: `make test SERVICE=search-sync-worker`
Expected: FAIL — `undefined: buildConsumerConfig`, `undefined: consumerSource`.

- [ ] **Step 8.3: Add the helper and rewire the loop in `main.go`**

In `search-sync-worker/main.go` at line 194-202, the current code is:

```go
consumerCfg := jetstream.ConsumerConfig{
    Durable:   coll.ConsumerName(),
    AckPolicy: jetstream.AckExplicitPolicy,
    BackOff:   []time.Duration{1 * time.Second, 5 * time.Second, 30 * time.Second},
}
if filters := coll.FilterSubjects(cfg.SiteID); len(filters) > 0 {
    consumerCfg.FilterSubjects = filters
}
cons, err := js.CreateOrUpdateConsumer(ctx, streamCfg.Name, consumerCfg)
```

Replace with:

```go
cons, err := js.CreateOrUpdateConsumer(ctx, streamCfg.Name, buildConsumerConfig(coll, cfg.SiteID))
```

Add this helper plus the narrow `consumerSource` interface to the bottom of `search-sync-worker/main.go`:

```go
// consumerSource is the subset of Collection that buildConsumerConfig
// needs. Narrowing keeps the helper unit-testable with a small fake.
type consumerSource interface {
	ConsumerName() string
	FilterSubjects(siteID string) []string
}

// buildConsumerConfig returns the durable consumer config for one
// search-sync-worker collection. Custom BackOff is intentional: ES
// indexing benefits from progressive retries on transient failures.
// With MaxDeliver=5 from defaults and 3 BackOff entries, NATS reuses
// the last entry (30s) for the 4th and 5th retries — do not extend
// BackOff to length 5 to "fix" this; the reuse is the intended pattern.
func buildConsumerConfig(coll consumerSource, siteID string) jetstream.ConsumerConfig {
	cc := stream.DurableConsumerDefaults()
	cc.Durable = coll.ConsumerName()
	cc.MaxAckPending = 500
	cc.BackOff = []time.Duration{1 * time.Second, 5 * time.Second, 30 * time.Second}
	if filters := coll.FilterSubjects(siteID); len(filters) > 0 {
		cc.FilterSubjects = filters
	}
	return cc
}
```

- [ ] **Step 8.4: Run test, build, lint**

Run: `make test SERVICE=search-sync-worker && make build SERVICE=search-sync-worker && make lint`
Expected: PASS, clean build, clean lint.

- [ ] **Step 8.5: Commit**

```bash
git add search-sync-worker/main.go search-sync-worker/consumer_config_test.go
git commit -m "feat(search-sync-worker): apply standard consumer defaults"
```

---

## Task 9: Final verification

**Files:** none modified — verification only.

- [ ] **Step 9.1: Run the full unit test suite with race detector**

Run: `make test`
Expected: PASS for all packages.

- [ ] **Step 9.2: Run linter on the full repo**

Run: `make lint`
Expected: clean.

- [ ] **Step 9.3: Run integration tests for affected services**

Run integration tests for the services we modified. The Makefile supports per-service runs:

```bash
make test-integration SERVICE=message-gatekeeper
make test-integration SERVICE=broadcast-worker
make test-integration SERVICE=message-worker
make test-integration SERVICE=notification-worker
make test-integration SERVICE=room-worker
make test-integration SERVICE=inbox-worker
make test-integration SERVICE=search-sync-worker
```

Expected: PASS for each. Integration tests already exercise consumer creation with testcontainers; they should pass unchanged because:
- `AckPolicy` is unchanged.
- `AckWait = 30s` matches the prior NATS default.
- `MaxDeliver = 5` is permissive enough for any test.
- `MaxAckPending` is set well above each service's in-flight ceiling.
- `DeliverPolicy = DeliverNewPolicy` is honored at create-time only; testcontainer-spawned consumers are always fresh.

If any integration test fails, investigate the specific failure before proceeding — do NOT relax the defaults to make tests pass.

- [ ] **Step 9.4: Push the branch**

```bash
git push -u origin claude/jetstream-consumer-config-JTIKh
```

Expected: push succeeds; remote prints PR URL. Do NOT open a PR — the user will do that manually after reviewing the branch.

---

## Notes for the implementer

- **Order matters**: do Task 1 first; every other task imports `stream.DurableConsumerDefaults()`. Tasks 2-8 are independent of each other and may be done in any order, but follow the order listed for clean commit history.
- **Each service commit should be self-contained**: tests, helper, and call-site update in the same commit so each commit leaves the repo in a working state.
- **No deploy manifest changes needed**: `MAX_REDELIVER` was the only env var being removed and verification confirmed nothing else sets it.
- **No `docs/client-api.md` updates**: this change does not touch any client-facing handler.
- **`make generate` is NOT required**: no store interfaces change.
