# Inbox Stream Ownership Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `inbox-worker` the single owner of `INBOX_{siteID}` schema bootstrap (Name + Subjects), and delete the temporary `inboxBootstrapStreamConfig` scaffolding from `search-sync-worker` that was always intended to migrate.

**Architecture:** App code owns the stream's schema (`Name + Subjects`); ops/IaC owns the federation topology (`Sources + SubjectTransforms`). The `inbox-worker/bootstrap.go` helper is widened from "Name only" to "Name + Subjects". `search-sync-worker` stops touching INBOX entirely — its bootstrap loop skips the INBOX entry, and the `inboxBootstrapStreamConfig` function plus its dedicated unit-test file are deleted. `RemoteSiteIDs` config is removed from `search-sync-worker` since it was only consumed by the deleted function.

**Tech Stack:** Go 1.25, `github.com/nats-io/nats.go/jetstream`, `caarlos0/env`, `stretchr/testify`.

**Spec:** `docs/superpowers/specs/2026-04-27-inbox-stream-ownership-design.md`

**Branch:** `claude/audit-message-streams-uvS7l` (current; do NOT switch). Folds into PR #130 as a second squashed commit, layered on top of the existing `BOOTSTRAP_STREAMS` work.

---

## File Structure

| File | Action | Responsibility |
|---|---|---|
| `inbox-worker/bootstrap.go` | Modify | Helper now creates INBOX with `Name + Subjects` (was Name-only). Doc comments updated to reference the ownership rule. |
| `inbox-worker/bootstrap_test.go` | Modify | Assertion flips from "Subjects must be empty" to "Subjects equals `pkg/stream.Inbox(siteID).Subjects`". |
| `search-sync-worker/inbox_stream.go` | Modify | Delete `inboxBootstrapStreamConfig` function. Update `inboxMemberCollection` doc comment to reference inbox-worker as INBOX owner. |
| `search-sync-worker/inbox_stream_test.go` | Delete | Tests only the deleted function. |
| `search-sync-worker/main.go` | Modify | Remove `RemoteSiteIDs` field from `bootstrapConfig`. In the bootstrap loop, `continue` when the stream is INBOX (search-sync-worker no longer bootstraps it). Update related comments. |
| `CLAUDE.md` | Modify | Extend the existing "Stream bootstrap is opt-in" bullet with the explicit ownership table (app owns Name+Subjects; ops owns Sources+SubjectTransforms). |

`search-sync-worker/deploy/docker-compose.yml` is **not** modified — it continues to set `BOOTSTRAP_STREAMS=true` for the remaining bootstrapped streams (`MESSAGES_CANONICAL` via `messageCollection`).

---

## Task 1: Widen `inbox-worker/bootstrap.go` helper to include Subjects

**Files:**
- Modify: `inbox-worker/bootstrap_test.go`
- Modify: `inbox-worker/bootstrap.go`

- [ ] **Step 1.1: Update the failing assertion in the test**

The current test asserts `Subjects` is empty for the success case. Replace that with an assertion that `Subjects` matches `pkg/stream.Inbox(siteID).Subjects`. The new test code (replacing the existing `TestBootstrapStreams` function body) is:

```go
func TestBootstrapStreams(t *testing.T) {
	tests := []struct {
		name        string
		enabled     bool
		failOn      string
		failErr     error
		wantCreated []string
		wantErrSub  string
	}{
		{
			name:        "disabled - skips creation",
			enabled:     false,
			wantCreated: nil,
		},
		{
			name:        "enabled - creates INBOX with Name and Subjects",
			enabled:     true,
			wantCreated: []string{"INBOX_test"},
		},
		{
			name:       "enabled - wraps INBOX creator error",
			enabled:    true,
			failOn:     "INBOX_test",
			failErr:    errors.New("nats down"),
			wantErrSub: "create INBOX stream",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeStreamCreator{failOn: tc.failOn, failErr: tc.failErr}
			err := bootstrapStreams(context.Background(), fake, "test", tc.enabled)
			if tc.wantErrSub != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrSub)
				assert.ErrorIs(t, err, tc.failErr)
				return
			}
			require.NoError(t, err)
			require.Len(t, fake.created, len(tc.wantCreated))
			wantSubjects := stream.Inbox("test").Subjects
			for i, wantName := range tc.wantCreated {
				assert.Equal(t, wantName, fake.created[i].Name)
				// App owns the schema (Name + Subjects). Federation
				// (Sources + SubjectTransforms) belongs to ops/IaC and
				// must not appear here.
				assert.Equal(t, wantSubjects, fake.created[i].Subjects,
					"INBOX bootstrap must set Subjects from pkg/stream.Inbox")
				assert.Empty(t, fake.created[i].Sources,
					"federation Sources are owned by ops/IaC and must not be set in app code")
			}
		})
	}
}
```

This requires importing `"github.com/hmchangw/chat/pkg/stream"` in the test file. Verify the existing import block already has it; if not, add it.

- [ ] **Step 1.2: Run the test to verify it fails**

Run: `make test SERVICE=inbox-worker`
Expected: FAIL — the helper currently produces `cfg.Subjects == nil`, but the test now expects `["chat.inbox.test.*", "chat.inbox.test.aggregate.>"]`. The failure should mention the Subjects mismatch.

- [ ] **Step 1.3: Update the helper to pass Subjects**

In `inbox-worker/bootstrap.go`, the current helper body is:

```go
inboxCfg := stream.Inbox(siteID)
if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
    Name: inboxCfg.Name,
}); err != nil {
    return fmt.Errorf("create INBOX stream: %w", err)
}
```

Change it to:

```go
inboxCfg := stream.Inbox(siteID)
if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
    Name:     inboxCfg.Name,
    Subjects: inboxCfg.Subjects,
}); err != nil {
    return fmt.Errorf("create INBOX stream: %w", err)
}
```

Also replace the existing doc comment block above `bootstrapStreams` (the comment that says `Subjects is intentionally omitted...`) with an updated comment that reflects the new ownership rule:

```go
// bootstrapStreams creates the JetStream INBOX stream this service consumes
// from. No-op when enabled is false (the production path) — streams are owned
// by ops/IaC there. In dev/integration the local docker-compose sets
// BOOTSTRAP_STREAMS=true so a developer can stand the service up in isolation
// against a fresh NATS instance.
//
// Ownership rule: this helper sets only the stream schema (Name + Subjects)
// from pkg/stream.Inbox. Federation config (Sources + SubjectTransforms for
// cross-site OUTBOX→INBOX sourcing) belongs to ops/IaC and is layered on in
// production. App code never sets it.
```

- [ ] **Step 1.4: Run the test to verify it passes**

Run: `make test SERVICE=inbox-worker`
Expected: PASS for all 3 subtests of `TestBootstrapStreams`. Other existing tests still pass.

- [ ] **Step 1.5: Run lint**

Run: `make lint`
Expected: 0 issues.

- [ ] **Step 1.6: Commit**

```bash
git add inbox-worker/bootstrap.go inbox-worker/bootstrap_test.go
git commit -m "feat(inbox-worker): own INBOX schema bootstrap (Name + Subjects)"
```

---

## Task 2: Skip INBOX in `search-sync-worker` bootstrap loop and remove `RemoteSiteIDs`

**Files:**
- Modify: `search-sync-worker/main.go`

- [ ] **Step 2.1: Remove the `RemoteSiteIDs` field from `bootstrapConfig`**

In `search-sync-worker/main.go`, the current `bootstrapConfig` struct (around lines 31-42) is:

```go
type bootstrapConfig struct {
	// Enabled (BOOTSTRAP_STREAMS) toggles whether the worker calls
	// CreateOrUpdateStream at startup for each collection's stream. Leave
	// false in production.
	Enabled bool `env:"STREAMS" envDefault:"false"`
	// RemoteSiteIDs (BOOTSTRAP_REMOTE_SITE_IDS) lists the other sites whose
	// OUTBOX streams should be sourced into this site's INBOX when the
	// worker is creating it itself. Used to build the cross-site Sources +
	// SubjectTransforms config during bootstrap. Only consulted when
	// Enabled is true; unused in production.
	RemoteSiteIDs []string `env:"REMOTE_SITE_IDS" envSeparator:","`
}
```

Replace it with:

```go
type bootstrapConfig struct {
	// Enabled (BOOTSTRAP_STREAMS) toggles whether the worker calls
	// CreateOrUpdateStream at startup for each collection's stream. Leave
	// false in production. INBOX is intentionally excluded from this loop
	// — inbox-worker owns INBOX schema bootstrap.
	Enabled bool `env:"STREAMS" envDefault:"false"`
}
```

Also update the surrounding bootstrapConfig file-level comment block (lines 22-30) to remove the references to "ops-inbox-worker" hand-off — it's now done. Replace the existing block:

```go
// bootstrapConfig groups every field that is ONLY meaningful when the worker
// is being stood up in dev or integration tests without its normal upstream
// services. In production none of these fields should be set — streams are
// owned by their publisher services (message-gatekeeper for
// MESSAGES_CANONICAL, inbox-worker for INBOX) and search-sync-worker only
// manages its own durable consumers.
//
// Env vars in this group are all prefixed `BOOTSTRAP_` so they're easy to
// spot in deployment manifests and obvious to grep.
```

with:

```go
// bootstrapConfig groups every field that is ONLY meaningful when the worker
// is being stood up in dev or integration tests without its normal upstream
// services. In production Enabled must remain false — streams are owned by
// their publisher services (message-gatekeeper for MESSAGES_CANONICAL,
// inbox-worker for INBOX) and search-sync-worker only manages its own
// durable consumers.
//
// search-sync-worker NEVER bootstraps INBOX, even when Enabled=true; that
// stream's schema is owned by inbox-worker and its federation by ops/IaC.
//
// Env vars in this group are all prefixed `BOOTSTRAP_` so they're easy to
// spot in deployment manifests and obvious to grep.
```

- [ ] **Step 2.2: Skip INBOX in the bootstrap loop**

In `search-sync-worker/main.go` around lines 168-192, the current bootstrap loop is:

```go
	// Canonical INBOX stream name, used below to decide when to layer on
	// cross-site Sources + SubjectTransforms during bootstrap.
	inboxName := stream.Inbox(cfg.SiteID).Name

	for _, coll := range collections {
		streamCfg := coll.StreamConfig(cfg.SiteID)
		if cfg.Bootstrap.Enabled {
			bootstrapCfg := streamCfg
			// The INBOX stream is the only one that needs cross-site Sources
			// + SubjectTransforms. Collections return a minimal baseline
			// (name + local subjects from pkg/stream.Inbox) and the
			// bootstrap path layers on the federation config here, keeping
			// the cross-site topology out of the Collection type entirely.
			if streamCfg.Name == inboxName {
				bootstrapCfg = inboxBootstrapStreamConfig(cfg.SiteID, cfg.Bootstrap.RemoteSiteIDs)
			}
			if _, alreadyCreated := createdStreams[bootstrapCfg.Name]; !alreadyCreated {
				if _, err := js.CreateOrUpdateStream(ctx, bootstrapCfg); err != nil {
					slog.Error("create stream failed", "stream", bootstrapCfg.Name, "error", err)
					os.Exit(1)
				}
				createdStreams[bootstrapCfg.Name] = struct{}{}
				slog.Info("stream bootstrapped", "stream", bootstrapCfg.Name)
			}
		}
```

Replace with:

```go
	// INBOX is owned by inbox-worker — search-sync-worker is a pure consumer
	// of that stream. We compute its name to skip the bootstrap call below.
	inboxName := stream.Inbox(cfg.SiteID).Name

	for _, coll := range collections {
		streamCfg := coll.StreamConfig(cfg.SiteID)
		if cfg.Bootstrap.Enabled {
			// Skip INBOX entirely — inbox-worker owns its schema and
			// ops/IaC owns its federation. search-sync-worker only
			// creates a consumer below.
			if streamCfg.Name == inboxName {
				continue
			}
			if _, alreadyCreated := createdStreams[streamCfg.Name]; !alreadyCreated {
				if _, err := js.CreateOrUpdateStream(ctx, streamCfg); err != nil {
					slog.Error("create stream failed", "stream", streamCfg.Name, "error", err)
					os.Exit(1)
				}
				createdStreams[streamCfg.Name] = struct{}{}
				slog.Info("stream bootstrapped", "stream", streamCfg.Name)
			}
		}
```

Note three changes:
1. The `bootstrapCfg := streamCfg` and the if-branch that called `inboxBootstrapStreamConfig` are gone — `streamCfg` is now used directly.
2. A `continue` statement skips the INBOX entry before any creation.
3. The "Skip INBOX" comment replaces the old "The INBOX stream is the only one..." comment.

But notice that `continue` inside the `if cfg.Bootstrap.Enabled { ... }` block would skip the entire rest of the for-loop body for INBOX — including any consumer creation that follows after the `if cfg.Bootstrap.Enabled` block. **This is wrong** — search-sync-worker still needs to create its INBOX-bound consumers (spotlight, user-room) even when bootstrap is enabled. To preserve consumer creation, the `continue` must be inside a guard that only skips the bootstrap step, not the rest of the loop iteration. Restructure as:

```go
		streamCfg := coll.StreamConfig(cfg.SiteID)
		if cfg.Bootstrap.Enabled && streamCfg.Name != inboxName {
			if _, alreadyCreated := createdStreams[streamCfg.Name]; !alreadyCreated {
				if _, err := js.CreateOrUpdateStream(ctx, streamCfg); err != nil {
					slog.Error("create stream failed", "stream", streamCfg.Name, "error", err)
					os.Exit(1)
				}
				createdStreams[streamCfg.Name] = struct{}{}
				slog.Info("stream bootstrapped", "stream", streamCfg.Name)
			}
		}
```

This is the correct version — only the bootstrap block is gated by both `Enabled` and "not INBOX". Everything after the `if` block (consumer creation, etc.) still runs for every collection including INBOX-based ones.

- [ ] **Step 2.3: Verify the file compiles and existing tests still pass**

Run: `make test SERVICE=search-sync-worker`
Expected: all unit tests pass. The `inbox_stream_test.go` file may now fail with `undefined: inboxBootstrapStreamConfig` — that's expected; we delete the file in Task 3 and that resolves it.

If `make test SERVICE=search-sync-worker` fails ONLY with `undefined: inboxBootstrapStreamConfig` errors from `inbox_stream_test.go`, that's the expected state. Proceed to Task 3 to clear it.

- [ ] **Step 2.4: Commit**

```bash
git add search-sync-worker/main.go
git commit -m "feat(search-sync-worker): stop bootstrapping INBOX (owned by inbox-worker)"
```

---

## Task 3: Delete `inboxBootstrapStreamConfig` and its test file

**Files:**
- Modify: `search-sync-worker/inbox_stream.go`
- Delete: `search-sync-worker/inbox_stream_test.go`

- [ ] **Step 3.1: Delete the `inboxBootstrapStreamConfig` function and update the collection comment**

In `search-sync-worker/inbox_stream.go`, the file currently contains:
1. Package + imports
2. `inboxBootstrapStreamConfig` function (lines 14-53) — DELETE this entire block including its doc comment.
3. `inboxMemberCollection` type and methods (lines 55-78) — KEEP, but update the doc comment to remove the reference to `inboxBootstrapStreamConfig`.
4. `parseMemberEvent` function (lines 80+) — KEEP unchanged.

Specifically, the existing doc comment (lines 55-66) on `inboxMemberCollection` is:

```go
// inboxMemberCollection is the shared base for collections that index
// subscription lifecycle events (member_added, member_removed) off the
// INBOX stream. It centralizes stream config and subject filters so
// spotlight and user-room collections only need to implement the
// index-specific parts.
//
// The stream name + local subject pattern come straight from pkg/stream.Inbox
// so there's one canonical definition for every consumer of INBOX.
// Cross-site federation (Sources + SubjectTransforms) is a deployment
// concern owned by whichever service creates the INBOX stream and is
// layered on separately — see inboxBootstrapStreamConfig.
type inboxMemberCollection struct{}
```

Replace it with:

```go
// inboxMemberCollection is the shared base for collections that index
// subscription lifecycle events (member_added, member_removed) off the
// INBOX stream. It centralizes stream config and subject filters so
// spotlight and user-room collections only need to implement the
// index-specific parts.
//
// The stream name + local subject pattern come straight from pkg/stream.Inbox
// so there's one canonical definition for every consumer of INBOX.
// inbox-worker owns INBOX schema bootstrap; cross-site federation (Sources
// + SubjectTransforms) is owned by ops/IaC. search-sync-worker is a pure
// consumer of INBOX.
type inboxMemberCollection struct{}
```

Also drop the unused imports if they only existed for `inboxBootstrapStreamConfig`. Specifically check:
- `"fmt"` — used by `parseMemberEvent` (`fmt.Errorf`), KEEP.
- `"github.com/nats-io/nats.go/jetstream"` — used by `inboxMemberCollection.StreamConfig` return type, KEEP.
- `"github.com/hmchangw/chat/pkg/model"` — used by `parseMemberEvent`, KEEP.
- `"github.com/hmchangw/chat/pkg/stream"` — used by `inboxMemberCollection.StreamConfig`, KEEP.
- `"github.com/hmchangw/chat/pkg/subject"` — used by `inboxMemberCollection.FilterSubjects`, KEEP.
- `"encoding/json"` — used by `parseMemberEvent`, KEEP.

All imports stay.

- [ ] **Step 3.2: Delete the test file**

Run:

```bash
rm search-sync-worker/inbox_stream_test.go
```

This file's only contents test `inboxBootstrapStreamConfig`, which no longer exists. There are no other tests in this file to preserve.

- [ ] **Step 3.3: Verify build and tests pass**

Run: `make test SERVICE=search-sync-worker`
Expected: all tests pass. The previous `undefined: inboxBootstrapStreamConfig` error is gone because the test file referencing it is gone.

Run: `make lint`
Expected: 0 issues.

- [ ] **Step 3.4: Commit**

```bash
git add search-sync-worker/inbox_stream.go search-sync-worker/inbox_stream_test.go
git commit -m "refactor(search-sync-worker): delete inboxBootstrapStreamConfig scaffolding"
```

The `git add` for a deleted file works because Git tracks the deletion as a change to the staging area when the file is missing on disk and the path is staged.

---

## Task 4: Document the ownership rule in `CLAUDE.md`

**Files:**
- Modify: `CLAUDE.md`

- [ ] **Step 4.1: Read the existing "Stream bootstrap is opt-in" bullet**

The bullet is the last item under "JetStream Streams" (around line 218 after the Part 1 PR landed). It currently reads:

```
- **Stream bootstrap is opt-in.** Services that consume from or publish to a stream MUST NOT create it in production — streams are owned by ops/IaC. Each such service's `config` includes `Bootstrap bootstrapConfig` (env prefix `BOOTSTRAP_`) with a single `Enabled` field tagged `env:"STREAMS" envDefault:"false"`. The service's `bootstrap.go` defines a `bootstrapStreams(ctx, js, siteID, enabled) error` helper that no-ops when `Enabled=false`. Local `deploy/docker-compose.yml` sets `BOOTSTRAP_STREAMS=true` so any service can stand up against a fresh NATS in dev. New services that interact with JetStream MUST follow this convention.
```

- [ ] **Step 4.2: Append the ownership rule paragraph**

Replace the bullet above with:

```
- **Stream bootstrap is opt-in.** Services that consume from or publish to a stream MUST NOT create it in production — streams are owned by ops/IaC. Each such service's `config` includes `Bootstrap bootstrapConfig` (env prefix `BOOTSTRAP_`) with a single `Enabled` field tagged `env:"STREAMS" envDefault:"false"`. The service's `bootstrap.go` defines a `bootstrapStreams(ctx, js, siteID, enabled) error` helper that no-ops when `Enabled=false`. Local `deploy/docker-compose.yml` sets `BOOTSTRAP_STREAMS=true` so any service can stand up against a fresh NATS in dev. New services that interact with JetStream MUST follow this convention.
- **Stream bootstrap ownership.** When a service does bootstrap a stream in dev, the helper sets ONLY the stream's schema — `Name + Subjects` from `pkg/stream.<Stream>(siteID)`. Federation config (`Sources` + `SubjectTransforms` for cross-site sourcing) is owned by ops/IaC and MUST NOT appear in any service's `bootstrap.go`. INBOX has a single owning service (`inbox-worker`); other services that consume from INBOX (e.g., `search-sync-worker`) skip it in their bootstrap loop and rely on `inbox-worker` to create the stream.
```

- [ ] **Step 4.3: Verify diff is the single-bullet addition**

Run: `git diff CLAUDE.md`
Expected: one new bullet appended under the "Stream bootstrap is opt-in" bullet, no other changes.

- [ ] **Step 4.4: Commit**

```bash
git add CLAUDE.md
git commit -m "docs(claude): document INBOX ownership and federation boundary"
```

---

## Task 5: Final integration verification

- [ ] **Step 5.1: Run the full unit suite**

Run: `make test`
Expected: every package green, including `inbox-worker` and `search-sync-worker`.

- [ ] **Step 5.2: Run the full linter**

Run: `make lint`
Expected: 0 issues.

- [ ] **Step 5.3: Run search-sync-worker integration tests**

Run: `make test-integration SERVICE=search-sync-worker`
Expected: all integration tests pass. These tests use the helper at `search-sync-worker/inbox_integration_test.go:28-36` (`createInboxStream`) which already creates INBOX with `Name + Subjects` only (no Sources). The deletion of `inboxBootstrapStreamConfig` does not affect them.

If integration tests are slow (testcontainers spins up Elasticsearch + NATS), this may take several minutes — that's expected.

- [ ] **Step 5.4: Verify no stray references to deleted symbols**

Run:

```bash
grep -rn 'inboxBootstrapStreamConfig\|RemoteSiteIDs\|BOOTSTRAP_REMOTE_SITE_IDS' --include='*.go' --include='*.md' --include='*.yml' /home/user/chat 2>/dev/null
```

Expected: only matches in the historical spec/plan documents under `docs/superpowers/specs/` and `docs/superpowers/plans/` (the original Part 2 plan doc references them in its example snippets — those are point-in-time records and are not edited). NO matches in `*.go` files outside of `docs/`. NO matches in `deploy/docker-compose.yml` files.

If any stray reference appears in a `.go` file, fix it before proceeding.

- [ ] **Step 5.5: Verify INBOX bootstrap is now Name+Subjects everywhere**

Run:

```bash
grep -nE 'stream\.Inbox\(|Subjects:.*inboxCfg\.Subjects' /home/user/chat/inbox-worker/bootstrap.go
```

Expected: confirms `inboxCfg := stream.Inbox(siteID)` is followed by `Subjects: inboxCfg.Subjects` in the `CreateOrUpdateStream` call.

---

## Self-Review Notes

- **Spec coverage:** Each section of the spec maps to a task:
  - "Code changes → inbox-worker/bootstrap.go" → Task 1
  - "Code changes → inbox-worker/bootstrap_test.go" → Task 1
  - "Code changes → search-sync-worker/inbox_stream.go" → Task 3
  - "Code changes → search-sync-worker/inbox_stream_test.go (deleted)" → Task 3
  - "Code changes → search-sync-worker/main.go" → Task 2
  - "Code changes → CLAUDE.md" → Task 4
  - "Test strategy" → Task 5 (integration verification)
- **Type consistency:** The helper signature `bootstrapStreams(ctx, js, siteID, enabled) error` is unchanged. The `streamCreator` interface is unchanged. The only structural change is `RemoteSiteIDs` removal from `search-sync-worker`'s `bootstrapConfig` — verified consistent across Tasks 2 and the deleted file in Task 3.
- **Placeholder scan:** No TBDs, no "implement later", no skipped error handling. All code blocks are complete.
