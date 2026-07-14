# Remove unused `Subscription.AvatarURL` field — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Delete the dead `Subscription.AvatarURL` field and its three downstream references so the codebase carries no stale avatar state ahead of the new `avatar-service`.

**Architecture:** Pure removal. The field has no writer anywhere, is `omitempty` + always empty (never on the wire), and is read by no client or frontend. Removing it is a clean, behavior-preserving deletion across model, projection, test, and API doc.

**Tech Stack:** Go 1.25, MongoDB driver v2, testify. Spec: `docs/superpowers/specs/2026-06-22-remove-subscription-avatarurl-design.md`.

## Global Constraints

- Use `make` targets only — never raw `go` commands.
- Go 1.25; `json`/`bson` tags are camelCase.
- Client-facing change: `docs/client-api.md` MUST be updated in the same commit as the code.
- Single atomic commit on branch `ds-feat/remove-old-avatar-info`.
- Removal-shaped TDD: no new red test; discipline is "suite stays green and stops referencing the deleted field."

---

### Task 1: Remove the dead `Subscription.AvatarURL` field

**Files:**
- Modify: `pkg/model/model_test.go` (`TestSubscriptionBaseMetadata_RoundTrip`, lines 3870, 3884, 3890)
- Modify: `pkg/model/subscription.go:77`
- Modify: `user-service/mongorepo/subscriptions.go:142`
- Modify: `docs/client-api.md:507`

**Interfaces:**
- Consumes: nothing.
- Produces: nothing. `model.Subscription` loses its `AvatarURL` field; no task or consumer depends on it.

- [ ] **Step 1: Stop the test from referencing `AvatarURL` (test-first for a removal)**

In `pkg/model/model_test.go`, `TestSubscriptionBaseMetadata_RoundTrip`:

Remove the struct-literal field (line 3870):
```go
		HasGroupMention: true,
		HasUnread:       true,
		AvatarURL:       "https://cdn/avatar.png",   // <-- delete this line
		FavoritedAt:     &favoritedAt,
		UpdatedAt:       &updatedAt,
```
becomes:
```go
		HasGroupMention: true,
		HasUnread:       true,
		FavoritedAt:     &favoritedAt,
		UpdatedAt:       &updatedAt,
```

Remove the wire assertion (line 3884):
```go
	assert.Equal(t, true, raw["hasGroupMention"])
	assert.Equal(t, true, raw["hasUnread"])
	assert.Equal(t, "https://cdn/avatar.png", raw["avatarUrl"])   // <-- delete this line
```
becomes:
```go
	assert.Equal(t, true, raw["hasGroupMention"])
	assert.Equal(t, true, raw["hasUnread"])
```

Drop `"avatarUrl"` from the omitempty-key list (line 3890):
```go
	for _, k := range []string{"avatarUrl", "favoritedAt", "updatedAt"} {
```
becomes:
```go
	for _, k := range []string{"favoritedAt", "updatedAt"} {
```

- [ ] **Step 2: Confirm the test still passes (field still present, no longer asserted)**

Run: `make test SERVICE=pkg/model`
(expands to `go test -race ./pkg/model/...`)
Expected: PASS — `TestSubscriptionBaseMetadata_RoundTrip` green. This is the safe-refactor checkpoint before the field is deleted.

- [ ] **Step 3: Delete the `AvatarURL` field from the model**

In `pkg/model/subscription.go`, delete line 77. The preceding comment (lines 74-76) describes the metadata group + `UpdatedAt` only and never named `AvatarURL`, so leave it unchanged.

```go
	// Subscription-level metadata persisted on the Mongo subscriptions document.
	// UpdatedAt is a nullable pointer so a creating writer that doesn't stamp it
	// (e.g. room-worker's $setOnInsert) never persists a zero-time placeholder.
	AvatarURL   string     `json:"avatarUrl,omitempty"   bson:"avatarUrl,omitempty"`   // <-- delete this line
	FavoritedAt *time.Time `json:"favoritedAt,omitempty" bson:"favoritedAt,omitempty"`
```
becomes:
```go
	// Subscription-level metadata persisted on the Mongo subscriptions document.
	// UpdatedAt is a nullable pointer so a creating writer that doesn't stamp it
	// (e.g. room-worker's $setOnInsert) never persists a zero-time placeholder.
	FavoritedAt *time.Time `json:"favoritedAt,omitempty" bson:"favoritedAt,omitempty"`
```

- [ ] **Step 4: Delete the projection entry in user-service**

In `user-service/mongorepo/subscriptions.go`, `subscriptionProjection`, delete line 142:
```go
		"externalAccess":     1,
		"avatarUrl":          1,   // <-- delete this line
		"favoritedAt":        1,
```
becomes:
```go
		"externalAccess":     1,
		"favoritedAt":        1,
```

- [ ] **Step 5: Verify both packages build**

Run: `make build SERVICE=user-service`
Expected: success (no reference to the removed field remains).

- [ ] **Step 6: Run the affected package tests**

Run: `make test SERVICE=pkg/model`
Run: `make test SERVICE=user-service`
Expected: PASS for both.

- [ ] **Step 7: Remove the API-doc row**

In `docs/client-api.md`, delete the Subscription field-table row (line 507):
```markdown
| `room` | [SubscriptionRoom](#subscriptionroom) | Optional. Room-derived view (read-time enrichment; user-service endpoints only). |
| `avatarUrl` | string | Optional. Subscription avatar URL. |
| `favoritedAt` | RFC3339 timestamp | Optional. When the user favorited the room. |
```
becomes:
```markdown
| `room` | [SubscriptionRoom](#subscriptionroom) | Optional. Room-derived view (read-time enrichment; user-service endpoints only). |
| `favoritedAt` | RFC3339 timestamp | Optional. When the user favorited the room. |
```

- [ ] **Step 8: Lint**

Run: `make lint`
Expected: clean (no new findings).

- [ ] **Step 9: Full unit-test sweep**

Run: `make test`
Expected: PASS across the repo (no other package referenced the field).

- [ ] **Step 10: Commit**

```bash
git add pkg/model/subscription.go pkg/model/model_test.go \
        user-service/mongorepo/subscriptions.go docs/client-api.md
git commit -m "$(cat <<'EOF'
refactor: remove unused Subscription.AvatarURL field

The field had no writer anywhere, was omitempty + always empty (never
serialized), and was read by no client or frontend. Removed across the
model, the user-service read projection, the model roundtrip test, and
the client-api Subscription field table, ahead of the new avatar-service.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01TVw5qkYeE4GKiT1iRZxrRs
EOF
)"
```

---

## Pre-push note (not part of the commit task)

Before any push, per CLAUDE.md run `make sast` (blocking CI gate, fail on medium+). A pure field deletion is not expected to surface findings, but the gate is mandatory. Push only when the user asks.
