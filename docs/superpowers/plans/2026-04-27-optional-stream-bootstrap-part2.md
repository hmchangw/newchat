# Optional Stream Bootstrap — Part 2 (Room/Inbox Services + Doc)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Finish gating JetStream stream creation across the remaining three services (`room-worker`, `inbox-worker`, `room-service`) and document the convention in `CLAUDE.md`.

**Architecture:** Same `bootstrap.go` + `bootstrap_test.go` pattern as Part 1. Each service gates its own `CreateOrUpdateStream` call(s) behind `cfg.Bootstrap.Enabled`. `CLAUDE.md` gets a short subsection under "JetStream Streams" so future services follow the convention.

**Tech Stack:** Go 1.25, `github.com/nats-io/nats.go/jetstream`, `caarlos0/env`, `stretchr/testify`.

**Spec:** `docs/superpowers/specs/2026-04-27-optional-stream-bootstrap-design.md`

**Prerequisite:** Part 1 (`docs/superpowers/plans/2026-04-27-optional-stream-bootstrap-part1.md`) is complete and the four message-pipeline services are gated.

---

## Task 5: room-worker (gates ROOMS)

**Files:**
- Create: `room-worker/bootstrap.go`
- Create: `room-worker/bootstrap_test.go`
- Modify: `room-worker/main.go` (config struct around line 23, stream creation block around lines 66-72)
- Modify: `room-worker/deploy/docker-compose.yml`

- [ ] **Step 5.1: Write the failing test**

Create `room-worker/bootstrap_test.go`:

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
			name:        "enabled - creates ROOMS",
			enabled:     true,
			wantCreated: []string{"ROOMS_test"},
		},
		{
			name:       "enabled - wraps creator error",
			enabled:    true,
			creatorErr: errors.New("nats down"),
			wantErrSub: "create ROOMS stream",
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

- [ ] **Step 5.2: Run the test to verify it fails**

Run: `make test SERVICE=room-worker`
Expected: FAIL with `undefined: bootstrapStreams`.

- [ ] **Step 5.3: Write the helper**

Create `room-worker/bootstrap.go`:

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

// bootstrapStreams creates the JetStream ROOMS stream this service consumes
// from. No-op when enabled is false (the production path) — streams are owned
// by ops/IaC. In dev/integration the local docker-compose sets
// BOOTSTRAP_STREAMS=true so a developer can stand the service up in isolation
// against a fresh NATS instance.
func bootstrapStreams(ctx context.Context, js streamCreator, siteID string, enabled bool) error {
	if !enabled {
		return nil
	}
	roomsCfg := stream.Rooms(siteID)
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     roomsCfg.Name,
		Subjects: roomsCfg.Subjects,
	}); err != nil {
		return fmt.Errorf("create ROOMS stream: %w", err)
	}
	return nil
}
```

- [ ] **Step 5.4: Run the test to verify it passes**

Run: `make test SERVICE=room-worker`
Expected: PASS for `TestBootstrapStreams` (3 subtests).

- [ ] **Step 5.5: Wire the helper into main.go and add config field**

In `room-worker/main.go`, add to the `config` struct (around line 23) as the last field:

```go
	Bootstrap bootstrapConfig `envPrefix:"BOOTSTRAP_"`
```

Replace lines 66-72 (the `CreateOrUpdateStream` block) with:

```go
	if err := bootstrapStreams(ctx, js, cfg.SiteID, cfg.Bootstrap.Enabled); err != nil {
		slog.Error("bootstrap streams failed", "error", err)
		os.Exit(1)
	}

	streamCfg := stream.Rooms(cfg.SiteID)
```

Keep `streamCfg :=` for `CreateOrUpdateConsumer(ctx, streamCfg.Name, ...)` on line 88.

- [ ] **Step 5.6: Verify build and existing tests still pass**

Run: `make test SERVICE=room-worker`
Expected: all tests pass.

Run: `make lint`
Expected: no new findings.

- [ ] **Step 5.7: Update docker-compose for local dev**

Read `room-worker/deploy/docker-compose.yml`. Append `- BOOTSTRAP_STREAMS=true` to the `environment:` list as the final entry.

- [ ] **Step 5.8: Commit**

```bash
git add room-worker/bootstrap.go room-worker/bootstrap_test.go room-worker/main.go room-worker/deploy/docker-compose.yml
git commit -m "feat(room-worker): gate stream creation behind BOOTSTRAP_STREAMS"
```

---

## Task 6: inbox-worker (gates INBOX)

Note: `inbox-worker` calls `CreateOrUpdateStream` with **only** the `Name` field (no `Subjects`). The stream's subjects are populated by the OUTBOX→INBOX cross-site sourcing config which is NOT set up by this service today. Preserve that exact shape — the helper only sets `Name`.

**Files:**
- Create: `inbox-worker/bootstrap.go`
- Create: `inbox-worker/bootstrap_test.go`
- Modify: `inbox-worker/main.go` (config struct around line 27, stream creation block around lines 148-154)
- Modify: `inbox-worker/deploy/docker-compose.yml`

- [ ] **Step 6.1: Write the failing test**

Create `inbox-worker/bootstrap_test.go`:

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
			name:        "enabled - creates INBOX",
			enabled:     true,
			wantCreated: []string{"INBOX_test"},
		},
		{
			name:       "enabled - wraps creator error",
			enabled:    true,
			creatorErr: errors.New("nats down"),
			wantErrSub: "create INBOX stream",
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

- [ ] **Step 6.2: Run the test to verify it fails**

Run: `make test SERVICE=inbox-worker`
Expected: FAIL with `undefined: bootstrapStreams`.

- [ ] **Step 6.3: Write the helper**

Create `inbox-worker/bootstrap.go`:

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

// bootstrapStreams creates the JetStream INBOX stream this service consumes
// from. The stream is created with Name only — no Subjects — because in
// production it is sourced from remote sites' OUTBOX streams via cross-site
// Sources + SubjectTransforms set up by ops/IaC. No-op when enabled is false.
func bootstrapStreams(ctx context.Context, js streamCreator, siteID string, enabled bool) error {
	if !enabled {
		return nil
	}
	inboxCfg := stream.Inbox(siteID)
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: inboxCfg.Name,
	}); err != nil {
		return fmt.Errorf("create INBOX stream: %w", err)
	}
	return nil
}
```

- [ ] **Step 6.4: Run the test to verify it passes**

Run: `make test SERVICE=inbox-worker`
Expected: PASS for `TestBootstrapStreams` (3 subtests).

- [ ] **Step 6.5: Wire the helper into main.go and add config field**

In `inbox-worker/main.go`, add to the `config` struct (around line 27) as the last field:

```go
	Bootstrap bootstrapConfig `envPrefix:"BOOTSTRAP_"`
```

Replace lines 148-154 (the `CreateOrUpdateStream` block) with:

```go
	if err := bootstrapStreams(ctx, js, cfg.SiteID, cfg.Bootstrap.Enabled); err != nil {
		slog.Error("bootstrap streams failed", "error", err)
		os.Exit(1)
	}

	inboxCfg := stream.Inbox(cfg.SiteID)
```

Keep `inboxCfg :=` for `CreateOrUpdateConsumer(ctx, inboxCfg.Name, ...)` on line 156.

- [ ] **Step 6.6: Verify build and existing tests still pass**

Run: `make test SERVICE=inbox-worker`
Expected: all tests pass.

Run: `make lint`
Expected: no new findings.

- [ ] **Step 6.7: Update docker-compose for local dev**

Read `inbox-worker/deploy/docker-compose.yml`. Append `- BOOTSTRAP_STREAMS=true` to the `environment:` list as the final entry.

- [ ] **Step 6.8: Commit**

```bash
git add inbox-worker/bootstrap.go inbox-worker/bootstrap_test.go inbox-worker/main.go inbox-worker/deploy/docker-compose.yml
git commit -m "feat(inbox-worker): gate stream creation behind BOOTSTRAP_STREAMS"
```

---

## Task 7: room-service (gates ROOMS, publisher-only)

`room-service` is publisher-only — it creates `ROOMS` for its own publishes and never sets up a consumer. The gate is identical in shape to `room-worker`, but lives in a different service.

**Files:**
- Create: `room-service/bootstrap.go`
- Create: `room-service/bootstrap_test.go`
- Modify: `room-service/main.go` (config struct around line 23, stream creation block around lines 81-87)
- Modify: `room-service/deploy/docker-compose.yml`

- [ ] **Step 7.1: Write the failing test**

Create `room-service/bootstrap_test.go` with the same content as `room-worker/bootstrap_test.go` from Task 5 Step 5.1. The expected stream name (`ROOMS_test`) and error substring (`create ROOMS stream`) match.

- [ ] **Step 7.2: Run the test to verify it fails**

Run: `make test SERVICE=room-service`
Expected: FAIL with `undefined: bootstrapStreams`.

- [ ] **Step 7.3: Write the helper**

Create `room-service/bootstrap.go` with the same content as `room-worker/bootstrap.go` from Task 5 Step 5.3.

- [ ] **Step 7.4: Run the test to verify it passes**

Run: `make test SERVICE=room-service`
Expected: PASS for `TestBootstrapStreams` (3 subtests).

- [ ] **Step 7.5: Wire the helper into main.go and add config field**

In `room-service/main.go`, add to the `config` struct (around line 23) as the last field:

```go
	Bootstrap bootstrapConfig `envPrefix:"BOOTSTRAP_"`
```

Replace lines 81-87 (the `CreateOrUpdateStream` block) with:

```go
	if err := bootstrapStreams(ctx, js, cfg.SiteID, cfg.Bootstrap.Enabled); err != nil {
		slog.Error("bootstrap streams failed", "error", err)
		os.Exit(1)
	}
```

Note: `room-service` does NOT call `CreateOrUpdateConsumer` after this block — it's publisher-only. The local `streamCfg :=` from line 81 was used only for the stream creation; if no other code below references `streamCfg`, drop the variable entirely. Grep `streamCfg` within `room-service/main.go` to confirm before deleting.

- [ ] **Step 7.6: Verify build and existing tests still pass**

Run: `make test SERVICE=room-service`
Expected: all tests pass.

Run: `make lint`
Expected: no new findings.

- [ ] **Step 7.7: Update docker-compose for local dev**

Read `room-service/deploy/docker-compose.yml`. Append `- BOOTSTRAP_STREAMS=true` to the `environment:` list as the final entry.

- [ ] **Step 7.8: Commit**

```bash
git add room-service/bootstrap.go room-service/bootstrap_test.go room-service/main.go room-service/deploy/docker-compose.yml
git commit -m "feat(room-service): gate stream creation behind BOOTSTRAP_STREAMS"
```

---

## Task 8: Document the convention in CLAUDE.md

**Files:**
- Modify: `CLAUDE.md` (the "JetStream Streams" subsection under "NATS & Messaging")

- [ ] **Step 8.1: Read the existing JetStream Streams section**

Read `CLAUDE.md` and locate the "JetStream Streams" subsection (under "Section 6: Project-Specific Patterns" → "NATS & Messaging"). Note its exact heading line and the line that follows it so the insertion is unambiguous.

- [ ] **Step 8.2: Append the convention paragraph**

Insert the following paragraph immediately after the existing bullet list in the "JetStream Streams" subsection (i.e., after the bullet for `INBOX_{siteID}`):

```markdown
- **Stream bootstrap is opt-in.** Services that consume from or publish to a stream MUST NOT create it in production — streams are owned by ops/IaC. Each consuming/publishing service's `config` includes `Bootstrap bootstrapConfig` (env prefix `BOOTSTRAP_`) with a single `Enabled` field tagged `env:"STREAMS" envDefault:"false"`. The service's `bootstrap.go` defines a `bootstrapStreams(ctx, js, siteID, enabled) error` helper that no-ops when `Enabled=false`. Local `deploy/docker-compose.yml` sets `BOOTSTRAP_STREAMS=true` so any service can stand up against a fresh NATS in dev. New services that interact with JetStream MUST follow this convention.
```

Do not modify any other section of `CLAUDE.md`.

- [ ] **Step 8.3: Verify the markdown still renders cleanly**

Run: `git diff CLAUDE.md`
Expected: a single addition under "JetStream Streams"; no other changes.

- [ ] **Step 8.4: Commit**

```bash
git add CLAUDE.md
git commit -m "docs(claude): document BOOTSTRAP_STREAMS convention"
```

---

## Task 9: Final integration verification

- [ ] **Step 9.1: Run the full unit suite**

Run: `make test`
Expected: every package passes, including the seven new `TestBootstrapStreams` instances (one per service).

- [ ] **Step 9.2: Run the linter**

Run: `make lint`
Expected: no findings.

- [ ] **Step 9.3: Verify default behavior is "do not bootstrap"**

Run:

```bash
grep -RhnE 'BOOTSTRAP_STREAMS|envDefault:"false"' message-gatekeeper/bootstrap.go broadcast-worker/bootstrap.go message-worker/bootstrap.go notification-worker/bootstrap.go room-worker/bootstrap.go inbox-worker/bootstrap.go room-service/bootstrap.go
```

Expected: every file shows `Enabled bool \`env:"STREAMS" envDefault:"false"\``. The default is `false` in every service.

- [ ] **Step 9.4: Verify local dev still bootstraps**

Run:

```bash
grep -nE 'BOOTSTRAP_STREAMS=true' message-gatekeeper/deploy/docker-compose.yml broadcast-worker/deploy/docker-compose.yml message-worker/deploy/docker-compose.yml notification-worker/deploy/docker-compose.yml room-worker/deploy/docker-compose.yml inbox-worker/deploy/docker-compose.yml room-service/deploy/docker-compose.yml
```

Expected: every compose file contains `BOOTSTRAP_STREAMS=true` exactly once.

- [ ] **Step 9.5: Verify no stray inline CreateOrUpdateStream calls remain**

Run:

```bash
grep -rn 'CreateOrUpdateStream' --include='*.go' | grep -v _test.go | grep -v 'pkg/'
```

Expected output: only the helper bodies in each `<service>/bootstrap.go` (8 lines total — message-gatekeeper has 2 calls, the rest 1 each) plus `search-sync-worker/main.go` (its existing gated call). No `main.go` outside `search-sync-worker` should appear.

- [ ] **Step 9.6: Push branch**

```bash
git push -u origin claude/audit-message-streams-uvS7l
```

---

## Self-Review Notes

- **Spec coverage:** All seven services from the spec table are gated (Tasks 1-7). `CLAUDE.md` updated (Task 8). Final verification confirms defaults and local-dev overrides (Task 9).
- **Type consistency:** Every service uses the same `streamCreator` interface signature, the same `bootstrapConfig` struct (single `Enabled` field), and the same `bootstrapStreams(ctx, js, siteID, enabled) error` signature. Stream names in tests use the literal `<NAME>_test` form because `pkg/stream`'s constructors append `_<siteID>` (siteID `"test"`).
- **Error wrapping:** Every helper wraps errors with `fmt.Errorf("create <STREAM> stream: %w", err)` matching CLAUDE.md's "describe what the current function was doing" rule.
- **No placeholders:** Every step contains the actual content. Tasks 3, 4, 7 reuse identically-shaped helpers from earlier tasks via explicit cross-references and a clear scope statement of what the helper body should be.
