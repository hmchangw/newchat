# Optional Stream Bootstrap — Part 1 (Message Pipeline Services)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make JetStream stream creation opt-in for the four message-pipeline services (`message-gatekeeper`, `broadcast-worker`, `message-worker`, `notification-worker`) by introducing a `BOOTSTRAP_STREAMS` env var per service. Default is `false` (prod-safe); local docker-compose sets `true`.

**Architecture:** Each service grows a `bootstrap.go` with a tiny `streamCreator` interface and a `bootstrapStreams(ctx, js, cfg) error` helper that no-ops when `Enabled=false`. `main.go` calls the helper instead of inlining `js.CreateOrUpdateStream`. The pattern matches `search-sync-worker`'s existing convention exactly. Per-service `deploy/docker-compose.yml` adds `BOOTSTRAP_STREAMS=true` so local dev is unchanged.

**Tech Stack:** Go 1.25, `github.com/nats-io/nats.go/jetstream`, `caarlos0/env`, `stretchr/testify`.

**Spec:** `docs/superpowers/specs/2026-04-27-optional-stream-bootstrap-design.md`

**Scope of Part 1:** 4 of the 7 services. Part 2 covers `room-worker`, `inbox-worker`, `room-service`, and the doc update.

---

## File Structure

For each of the four services, create:
- `<service>/bootstrap.go` — defines `bootstrapConfig` struct, `streamCreator` interface, `bootstrapStreams` helper.
- `<service>/bootstrap_test.go` — table-driven unit tests with a fake `streamCreator`.

Modify:
- `<service>/main.go` — add `Bootstrap bootstrapConfig \`envPrefix:"BOOTSTRAP_"\`` to `config`; replace inline `js.CreateOrUpdateStream` block with a call to `bootstrapStreams`.
- `<service>/deploy/docker-compose.yml` — add `BOOTSTRAP_STREAMS=true` under `environment:`.

The helper is service-local (same `package main`) — no shared package needed. Each service has different stream(s) to bootstrap, so the helper body differs.

---

## Task 1: message-gatekeeper (gates MESSAGES + MESSAGES_CANONICAL)

This is the reference implementation; it creates two streams, so it exercises the helper most fully.

**Files:**
- Create: `message-gatekeeper/bootstrap.go`
- Create: `message-gatekeeper/bootstrap_test.go`
- Modify: `message-gatekeeper/main.go` (config struct around line 23, stream creation block around lines 76-94)
- Modify: `message-gatekeeper/deploy/docker-compose.yml`

- [ ] **Step 1.1: Write the failing test**

Create `message-gatekeeper/bootstrap_test.go`:

```go
package main

import (
	"context"
	"errors"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeStreamCreator struct {
	created []string
	err     error
}

func (f *fakeStreamCreator) CreateOrUpdateStream(_ context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.created = append(f.created, cfg.Name)
	return nil, nil
}

func TestBootstrapStreams(t *testing.T) {
	tests := []struct {
		name        string
		enabled     bool
		creatorErr  error
		wantCreated []string
		wantErrSub  string
	}{
		{
			name:        "disabled - skips creation",
			enabled:     false,
			wantCreated: nil,
		},
		{
			name:        "enabled - creates MESSAGES and MESSAGES_CANONICAL",
			enabled:     true,
			wantCreated: []string{"MESSAGES_test", "MESSAGES_CANONICAL_test"},
		},
		{
			name:       "enabled - wraps creator error",
			enabled:    true,
			creatorErr: errors.New("nats down"),
			wantErrSub: "create MESSAGES stream",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeStreamCreator{err: tc.creatorErr}
			err := bootstrapStreams(context.Background(), fake, "test", tc.enabled)
			if tc.wantErrSub != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrSub)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantCreated, fake.created)
		})
	}
}
```

- [ ] **Step 1.2: Run the test to verify it fails**

Run: `make test SERVICE=message-gatekeeper`
Expected: FAIL with `undefined: bootstrapStreams` and `undefined: streamCreator` (or similar build error).

- [ ] **Step 1.3: Write the helper**

Create `message-gatekeeper/bootstrap.go`:

```go
package main

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/stream"
)

// bootstrapConfig groups every field that is ONLY meaningful when the
// service is being stood up in dev or integration tests against a NATS
// instance where the streams it consumes do not yet exist. In production
// streams are pre-provisioned by ops/IaC and Bootstrap.Enabled must remain
// false; the service only creates its own durable consumer.
type bootstrapConfig struct {
	// Enabled (BOOTSTRAP_STREAMS) toggles whether the service calls
	// CreateOrUpdateStream at startup for the streams it owns/consumes.
	// Leave false in production.
	Enabled bool `env:"STREAMS" envDefault:"false"`
}

// streamCreator is the minimal JetStream surface bootstrapStreams depends on.
// Kept service-local so we don't pollute pkg/ with a one-method type and so
// tests can inject a fake without mockgen.
type streamCreator interface {
	CreateOrUpdateStream(ctx context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error)
}

// bootstrapStreams creates the JetStream streams this service publishes to /
// consumes from. No-op when enabled is false (the production path) — streams
// are owned by ops/IaC. In dev/integration the local docker-compose sets
// BOOTSTRAP_STREAMS=true so a developer can stand the service up in isolation
// against a fresh NATS instance.
func bootstrapStreams(ctx context.Context, js streamCreator, siteID string, enabled bool) error {
	if !enabled {
		return nil
	}
	messagesCfg := stream.Messages(siteID)
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     messagesCfg.Name,
		Subjects: messagesCfg.Subjects,
	}); err != nil {
		return fmt.Errorf("create MESSAGES stream: %w", err)
	}
	canonicalCfg := stream.MessagesCanonical(siteID)
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     canonicalCfg.Name,
		Subjects: canonicalCfg.Subjects,
	}); err != nil {
		return fmt.Errorf("create MESSAGES_CANONICAL stream: %w", err)
	}
	return nil
}
```

- [ ] **Step 1.4: Run the test to verify it passes**

Run: `make test SERVICE=message-gatekeeper`
Expected: PASS for `TestBootstrapStreams` (3 subtests). Other tests still pass.

- [ ] **Step 1.5: Wire the helper into main.go and add config field**

In `message-gatekeeper/main.go`, modify the `config` struct (around line 23) by adding the `Bootstrap` field. Final shape:

```go
type config struct {
	NatsURL       string `env:"NATS_URL,required"`
	NatsCredsFile string `env:"NATS_CREDS_FILE" envDefault:""`
	SiteID        string `env:"SITE_ID,required"`
	MongoURI      string `env:"MONGO_URI,required"`
	MongoDB       string `env:"MONGO_DB"        envDefault:"chat"`
	MaxWorkers    int    `env:"MAX_WORKERS"     envDefault:"100"`

	Bootstrap bootstrapConfig `envPrefix:"BOOTSTRAP_"`
}
```

Replace lines 76-94 (the two `CreateOrUpdateStream` blocks plus the local config vars used only for those calls) with:

```go
	if err := bootstrapStreams(ctx, js, cfg.SiteID, cfg.Bootstrap.Enabled); err != nil {
		slog.Error("bootstrap streams failed", "error", err)
		os.Exit(1)
	}

	messagesCfg := stream.Messages(cfg.SiteID)
```

Note: `messagesCfg` is still needed below for the consumer `CreateOrUpdateConsumer(ctx, messagesCfg.Name, ...)` call on line 96, so keep that variable. The original `canonicalCfg` was only used for the now-deleted second `CreateOrUpdateStream` call — confirm it isn't referenced elsewhere; if it isn't, drop it. (Grep `canonicalCfg` within `main.go` to confirm before deleting.)

- [ ] **Step 1.6: Verify build and existing tests still pass**

Run: `make test SERVICE=message-gatekeeper`
Expected: PASS for all tests in the package, including `TestBootstrapStreams` and existing `handler_test.go` tests.

Run: `make lint`
Expected: no new findings.

- [ ] **Step 1.7: Update docker-compose for local dev**

Edit `message-gatekeeper/deploy/docker-compose.yml`. Add `BOOTSTRAP_STREAMS=true` to the `environment:` list. The list uses the dash-prefixed `KEY=value` form. Add a single new line, preserving existing entries:

```yaml
    environment:
      - NATS_URL=nats://nats:4222
      - NATS_CREDS_FILE=/etc/nats/backend.creds
      - SITE_ID=site-local
      - MONGO_URI=mongodb://mongodb:27017
      - MONGO_DB=chat
      - BOOTSTRAP_STREAMS=true
```

Read the file first to confirm the exact existing keys; insert `BOOTSTRAP_STREAMS=true` as the final entry under `environment:`. Do not modify any other field.

- [ ] **Step 1.8: Commit**

```bash
git add message-gatekeeper/bootstrap.go message-gatekeeper/bootstrap_test.go message-gatekeeper/main.go message-gatekeeper/deploy/docker-compose.yml
git commit -m "feat(message-gatekeeper): gate stream creation behind BOOTSTRAP_STREAMS"
```

---

## Task 2: broadcast-worker (gates MESSAGES_CANONICAL)

**Files:**
- Create: `broadcast-worker/bootstrap.go`
- Create: `broadcast-worker/bootstrap_test.go`
- Modify: `broadcast-worker/main.go` (config struct around line 26, stream creation block around lines 86-93)
- Modify: `broadcast-worker/deploy/docker-compose.yml`

- [ ] **Step 2.1: Write the failing test**

Create `broadcast-worker/bootstrap_test.go`:

```go
package main

import (
	"context"
	"errors"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeStreamCreator struct {
	created []string
	err     error
}

func (f *fakeStreamCreator) CreateOrUpdateStream(_ context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.created = append(f.created, cfg.Name)
	return nil, nil
}

func TestBootstrapStreams(t *testing.T) {
	tests := []struct {
		name        string
		enabled     bool
		creatorErr  error
		wantCreated []string
		wantErrSub  string
	}{
		{
			name:        "disabled - skips creation",
			enabled:     false,
			wantCreated: nil,
		},
		{
			name:        "enabled - creates MESSAGES_CANONICAL",
			enabled:     true,
			wantCreated: []string{"MESSAGES_CANONICAL_test"},
		},
		{
			name:       "enabled - wraps creator error",
			enabled:    true,
			creatorErr: errors.New("nats down"),
			wantErrSub: "create MESSAGES_CANONICAL stream",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeStreamCreator{err: tc.creatorErr}
			err := bootstrapStreams(context.Background(), fake, "test", tc.enabled)
			if tc.wantErrSub != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrSub)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantCreated, fake.created)
		})
	}
}
```

- [ ] **Step 2.2: Run the test to verify it fails**

Run: `make test SERVICE=broadcast-worker`
Expected: FAIL with `undefined: bootstrapStreams`.

- [ ] **Step 2.3: Write the helper**

Create `broadcast-worker/bootstrap.go`:

```go
package main

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/stream"
)

// bootstrapConfig groups every field that is ONLY meaningful when the
// service is being stood up in dev or integration tests against a NATS
// instance where the streams it consumes do not yet exist. In production
// streams are pre-provisioned by ops/IaC and Bootstrap.Enabled must remain
// false; the service only creates its own durable consumer.
type bootstrapConfig struct {
	// Enabled (BOOTSTRAP_STREAMS) toggles whether the service calls
	// CreateOrUpdateStream at startup for the streams it consumes.
	// Leave false in production.
	Enabled bool `env:"STREAMS" envDefault:"false"`
}

// streamCreator is the minimal JetStream surface bootstrapStreams depends on.
type streamCreator interface {
	CreateOrUpdateStream(ctx context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error)
}

// bootstrapStreams creates the JetStream streams this service consumes from.
// No-op when enabled is false (the production path) — streams are owned by
// ops/IaC. In dev/integration the local docker-compose sets
// BOOTSTRAP_STREAMS=true so a developer can stand the service up in isolation
// against a fresh NATS instance.
func bootstrapStreams(ctx context.Context, js streamCreator, siteID string, enabled bool) error {
	if !enabled {
		return nil
	}
	canonicalCfg := stream.MessagesCanonical(siteID)
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     canonicalCfg.Name,
		Subjects: canonicalCfg.Subjects,
	}); err != nil {
		return fmt.Errorf("create MESSAGES_CANONICAL stream: %w", err)
	}
	return nil
}
```

- [ ] **Step 2.4: Run the test to verify it passes**

Run: `make test SERVICE=broadcast-worker`
Expected: PASS for `TestBootstrapStreams` (3 subtests).

- [ ] **Step 2.5: Wire the helper into main.go and add config field**

In `broadcast-worker/main.go`, add to the `config` struct (around line 26):

```go
	Bootstrap bootstrapConfig `envPrefix:"BOOTSTRAP_"`
```

Place it as the last field, right before the closing `}` of `type config struct`.

Replace lines 86-93 (the `CreateOrUpdateStream` block plus the `canonicalCfg :=` line if it's only used for that call) with:

```go
	if err := bootstrapStreams(ctx, js, cfg.SiteID, cfg.Bootstrap.Enabled); err != nil {
		slog.Error("bootstrap streams failed", "error", err)
		os.Exit(1)
	}

	canonicalCfg := stream.MessagesCanonical(cfg.SiteID)
```

Keep `canonicalCfg :=` since `CreateOrUpdateConsumer(ctx, canonicalCfg.Name, ...)` on line 95 still needs it.

- [ ] **Step 2.6: Verify build and existing tests still pass**

Run: `make test SERVICE=broadcast-worker`
Expected: all tests pass (handler, integration build tag is excluded by default).

Run: `make lint`
Expected: no new findings.

- [ ] **Step 2.7: Update docker-compose for local dev**

Read `broadcast-worker/deploy/docker-compose.yml`. Append `- BOOTSTRAP_STREAMS=true` to the `environment:` list as the final entry, preserving all other keys. Do not touch `docker-compose.test.yml` (the `Enabled=false` default is correct for that environment unless explicit opt-in is desired; out of scope for this task).

- [ ] **Step 2.8: Commit**

```bash
git add broadcast-worker/bootstrap.go broadcast-worker/bootstrap_test.go broadcast-worker/main.go broadcast-worker/deploy/docker-compose.yml
git commit -m "feat(broadcast-worker): gate stream creation behind BOOTSTRAP_STREAMS"
```

---

## Task 3: message-worker (gates MESSAGES_CANONICAL)

**Files:**
- Create: `message-worker/bootstrap.go`
- Create: `message-worker/bootstrap_test.go`
- Modify: `message-worker/main.go` (config struct around line 26, stream creation block around lines 88-95)
- Modify: `message-worker/deploy/docker-compose.yml`

- [ ] **Step 3.1: Write the failing test**

Create `message-worker/bootstrap_test.go` with the same content as `broadcast-worker/bootstrap_test.go` from Task 2 Step 2.1. The expected stream name (`MESSAGES_CANONICAL_test`) and error substring (`create MESSAGES_CANONICAL stream`) match.

- [ ] **Step 3.2: Run the test to verify it fails**

Run: `make test SERVICE=message-worker`
Expected: FAIL with `undefined: bootstrapStreams`.

- [ ] **Step 3.3: Write the helper**

Create `message-worker/bootstrap.go` with the same content as `broadcast-worker/bootstrap.go` from Task 2 Step 2.3. The helper body (creates `MESSAGES_CANONICAL`) is identical.

- [ ] **Step 3.4: Run the test to verify it passes**

Run: `make test SERVICE=message-worker`
Expected: PASS for `TestBootstrapStreams` (3 subtests).

- [ ] **Step 3.5: Wire the helper into main.go and add config field**

In `message-worker/main.go`, add to the `config` struct (around line 26) as the last field:

```go
	Bootstrap bootstrapConfig `envPrefix:"BOOTSTRAP_"`
```

Replace lines 88-95 (the `CreateOrUpdateStream` block) with:

```go
	if err := bootstrapStreams(ctx, js, cfg.SiteID, cfg.Bootstrap.Enabled); err != nil {
		slog.Error("bootstrap streams failed", "error", err)
		os.Exit(1)
	}

	canonicalCfg := stream.MessagesCanonical(cfg.SiteID)
```

Keep `canonicalCfg :=` for the `CreateOrUpdateConsumer` call on line 97.

- [ ] **Step 3.6: Verify build and existing tests still pass**

Run: `make test SERVICE=message-worker`
Expected: all tests pass.

Run: `make lint`
Expected: no new findings.

- [ ] **Step 3.7: Update docker-compose for local dev**

Read `message-worker/deploy/docker-compose.yml`. Append `- BOOTSTRAP_STREAMS=true` to the `environment:` list as the final entry.

- [ ] **Step 3.8: Commit**

```bash
git add message-worker/bootstrap.go message-worker/bootstrap_test.go message-worker/main.go message-worker/deploy/docker-compose.yml
git commit -m "feat(message-worker): gate stream creation behind BOOTSTRAP_STREAMS"
```

---

## Task 4: notification-worker (gates MESSAGES_CANONICAL)

**Files:**
- Create: `notification-worker/bootstrap.go`
- Create: `notification-worker/bootstrap_test.go`
- Modify: `notification-worker/main.go` (config struct around line 26, stream creation block around lines 92-99)
- Modify: `notification-worker/deploy/docker-compose.yml`

- [ ] **Step 4.1: Write the failing test**

Create `notification-worker/bootstrap_test.go` with the same content as `broadcast-worker/bootstrap_test.go` from Task 2 Step 2.1. The expected stream name and error substring match.

- [ ] **Step 4.2: Run the test to verify it fails**

Run: `make test SERVICE=notification-worker`
Expected: FAIL with `undefined: bootstrapStreams`.

- [ ] **Step 4.3: Write the helper**

Create `notification-worker/bootstrap.go` with the same content as `broadcast-worker/bootstrap.go` from Task 2 Step 2.3.

- [ ] **Step 4.4: Run the test to verify it passes**

Run: `make test SERVICE=notification-worker`
Expected: PASS for `TestBootstrapStreams` (3 subtests).

- [ ] **Step 4.5: Wire the helper into main.go and add config field**

In `notification-worker/main.go`, add to the `config` struct (around line 26) as the last field:

```go
	Bootstrap bootstrapConfig `envPrefix:"BOOTSTRAP_"`
```

Replace lines 92-99 (the `CreateOrUpdateStream` block) with:

```go
	if err := bootstrapStreams(ctx, js, cfg.SiteID, cfg.Bootstrap.Enabled); err != nil {
		slog.Error("bootstrap streams failed", "error", err)
		os.Exit(1)
	}

	canonicalCfg := stream.MessagesCanonical(cfg.SiteID)
```

Keep `canonicalCfg :=` for the `CreateOrUpdateConsumer` call on line 101.

- [ ] **Step 4.6: Verify build and existing tests still pass**

Run: `make test SERVICE=notification-worker`
Expected: all tests pass.

Run: `make lint`
Expected: no new findings.

- [ ] **Step 4.7: Update docker-compose for local dev**

Read `notification-worker/deploy/docker-compose.yml`. Append `- BOOTSTRAP_STREAMS=true` to the `environment:` list as the final entry.

- [ ] **Step 4.8: Commit**

```bash
git add notification-worker/bootstrap.go notification-worker/bootstrap_test.go notification-worker/main.go notification-worker/deploy/docker-compose.yml
git commit -m "feat(notification-worker): gate stream creation behind BOOTSTRAP_STREAMS"
```

---

## Part 1 Wrap-Up

After all four tasks pass:

- [ ] **Run the full unit suite once**

Run: `make test`
Expected: every package passes including the four new `TestBootstrapStreams` instances.

- [ ] **Push branch**

```bash
git push -u origin claude/audit-message-streams-uvS7l
```

Proceed to Part 2 (`docs/superpowers/plans/2026-04-27-optional-stream-bootstrap-part2.md`) to handle `room-worker`, `inbox-worker`, `room-service`, and the `CLAUDE.md` doc update.
