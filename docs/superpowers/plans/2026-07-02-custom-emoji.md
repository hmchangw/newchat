# Custom Emoji Management Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the full site-scoped custom-emoji lifecycle to media-service — REST upload (`PUT /emoji/v1/{siteID}/{shortcode}`), REST image serving with cross-site 307 (`GET /emoji/v1/{siteID}/{shortcode}`), and NATS request-reply list/delete — writing to the `custom_emojis` collection the shipped reaction validator already reads.

**Architecture:** media-service (flat `package main`, Gin) gains a NATS side: `natsutil.Connect` + `natsrouter` with queue group `media-service`. Blobs go in the existing MinIO bucket under `emoji/` keys with the avatar invariant *doc exists ⟺ object exists* (Put blob → upsert doc; delete doc → best-effort delete blob). Shared plumbing lands first (pkg/emoji canonicalizer, pkg/model wire types, pkg/subject builders, pkg/errcode reasons), then store, then handlers, then wiring, then integration tests and docs.

**Tech Stack:** Go 1.25, Gin, `go.mongodb.org/mongo-driver/v2`, `minio-go/v7`, `pkg/natsrouter` over `pkg/natsutil` (`*otelnats.Conn`), `go.uber.org/mock`, testify, testcontainers via `pkg/testutil`.

**Design spec:** `docs/superpowers/specs/2026-07-02-custom-emoji-design.md` (approved).

## Global Constraints

- TDD Red-Green-Refactor for every task; tests first, confirm they fail, then implement.
- Never run raw `go` commands where a make target exists: `make test SERVICE=<dir>`, `make lint`, `make fmt`, `make generate SERVICE=media-service`, `make test-integration SERVICE=media-service`. (Exception per CLAUDE.md: coverage uses `go test -coverprofile`.)
- Shortcode wire format: `^[a-z0-9_+-]{1,32}$` after NFC — must stay byte-identical to `pkg/emoji.Validator` semantics.
- Env config only, via `caarlos0/env`: `NATS_URL` (required), `NATS_CREDS_FILE` (optional), `EMOJI_MAX_UPLOAD_BYTES` default `262144`, `EMOJI_MAX_DIMENSION` default `512`.
- Mongo: explicit projections on every find; no `$lookup`; collection `custom_emojis`; unique index `siteId_shortcode_unique` on `(siteId, shortcode)`.
- Errors: Tier-1 named constructors (`errcode.BadRequest/NotFound/Conflict`) + `WithReason` only where the FE branches; bare wrapped `fmt.Errorf("…: %w", err)` for infra failures; never log AND return the same error.
- Every commit message ends with exactly these two trailer lines:
  ```
  Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_0172WdvqXaCgCyG5gZMdbqtd
  ```
- Branch: `ds-feat/customized_emoji`. Never push elsewhere. `git push -u origin ds-feat/customized_emoji` (retry up to 4× with 2s/4s/8s/16s backoff on network failure only).
- A pre-commit hook runs lint + tests; fix failures rather than bypassing.
- Coverage floor 80%, target 90%+ on handlers.

---

### Task 1: pkg/emoji — exported `Canonicalize` + `IsStandard`

**Files:**
- Modify: `pkg/emoji/emoji.go`
- Test: `pkg/emoji/emoji_test.go`

**Interfaces:**
- Consumes: existing unexported `shortcodeRe`, `standardEmoji` map, `ErrInvalidShortcode`.
- Produces: `func Canonicalize(shortcode string) (string, error)` and `func IsStandard(shortcode string) bool` in package `emoji` — Tasks 7, 8, 10 call both.

- [ ] **Step 1: Write the failing tests**

Append to `pkg/emoji/emoji_test.go` (package `emoji_test`; `strings`, `testify` already imported):

```go
func TestCanonicalize(t *testing.T) {
	cases := []struct {
		name    string
		in      string
		want    string
		wantErr bool
	}{
		{"valid ascii", "party_parrot", "party_parrot", false},
		{"valid plus", "+1", "+1", false},
		{"boundary 32", strings.Repeat("a", 32), strings.Repeat("a", 32), false},
		{"empty", "", "", true},
		{"uppercase", "Party", "", true},
		{"wrapped in colons", ":party:", "", true},
		{"too long 33", strings.Repeat("a", 33), "", true},
		{"over byte cap", strings.Repeat("a", 1024), "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := emoji.Canonicalize(tc.in)
			if tc.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, emoji.ErrInvalidShortcode)
				assert.Empty(t, got)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestIsStandard(t *testing.T) {
	assert.True(t, emoji.IsStandard("thumbsup"))
	assert.True(t, emoji.IsStandard("+1"))
	assert.True(t, emoji.IsStandard("heart"))
	assert.False(t, emoji.IsStandard("acme_party"))
	assert.False(t, emoji.IsStandard(""))
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=pkg/emoji`
Expected: FAIL — compile error `undefined: emoji.Canonicalize` / `emoji.IsStandard`.

- [ ] **Step 3: Implement — extract lines 45–58 of `Validate` into `Canonicalize`, add `IsStandard`, refactor `Validate`**

In `pkg/emoji/emoji.go`, add above `Validate`:

```go
// Canonicalize returns the NFC-canonical form of a bare shortcode, or
// ErrInvalidShortcode when it fails the input-length cap or wire-format regex.
// Callers MUST use the returned string — not the raw input — for any storage
// key or wire echo, because storage-key equality is byte-exact.
func Canonicalize(shortcode string) (string, error) {
	// Cap input bytes before NFC so a pathological input can't allocate a large output buffer.
	const maxInputBytes = 256
	if len(shortcode) > maxInputBytes {
		return "", fmt.Errorf("canonicalize shortcode (%d bytes): %w", len(shortcode), ErrInvalidShortcode)
	}

	// IsNormalString skips the allocating transform on already-NFC inputs (ASCII always is).
	if !norm.NFC.IsNormalString(shortcode) {
		shortcode = norm.NFC.String(shortcode)
	}

	if !shortcodeRe.MatchString(shortcode) {
		return "", fmt.Errorf("canonicalize shortcode %q: %w", shortcode, ErrInvalidShortcode)
	}
	return shortcode, nil
}

// IsStandard reports whether an already-canonical shortcode is one of the
// built-in standard emoji (gemoji set). Custom emoji colliding with these are
// permanently shadowed by the validator, so uploads should reject them.
func IsStandard(shortcode string) bool {
	_, ok := standardEmoji[shortcode]
	return ok
}
```

Replace the body of `Validate` (keep its doc comment) with:

```go
func (v *Validator) Validate(ctx context.Context, siteID, shortcode string) (string, error) {
	canonical, err := Canonicalize(shortcode)
	if err != nil {
		return "", err
	}
	shortcode = canonical

	// Built-in standard emoji are accepted without a custom-store lookup; the
	// custom_emojis collection is an additive per-site extension (issue #382).
	if IsStandard(shortcode) {
		return shortcode, nil
	}

	ok, err := v.lookup.CustomEmojiExists(ctx, siteID, shortcode)
	if err != nil {
		return "", fmt.Errorf("lookup custom emoji %q for site %q: %w", shortcode, siteID, err)
	}
	if !ok {
		return "", fmt.Errorf("validate shortcode %q: %w", shortcode, ErrUnknownShortcode)
	}
	return shortcode, nil
}
```

(Existing tests only assert `errors.Is(err, ErrInvalidShortcode)`, so the message-prefix change from "validate" to "canonicalize" is safe.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=pkg/emoji`
Expected: PASS — all existing `Validate` tests plus the two new functions.

- [ ] **Step 5: Commit**

```bash
git add pkg/emoji/emoji.go pkg/emoji/emoji_test.go
git commit -m "$(cat <<'EOF'
feat(emoji): export Canonicalize and IsStandard helpers

Extract the shortcode length-cap + NFC + regex block from
Validator.Validate into an exported Canonicalize, and expose the
standard-emoji set via IsStandard, so media-service upload/delete can
share the exact validation semantics. Zero behavior change to Validate.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_0172WdvqXaCgCyG5gZMdbqtd
EOF
)"
```

---

### Task 2: pkg/model — extend `CustomEmoji`, add wire types

**Files:**
- Modify: `pkg/model/custom_emoji.go`
- Test: `pkg/model/model_test.go` (existing tests at lines ~3273–3301)

**Interfaces:**
- Produces: extended `model.CustomEmoji` (new fields `UpdatedBy string`, `UpdatedAt int64`, `MinioKey string`, `ContentType string`, `Size int64`, `ETag string`); new types `model.EmojiEntry`, `model.EmojiListResponse`, `model.EmojiDeleteRequest`, `model.EmojiDeleteResponse`. Tasks 6–10, 12, 13 depend on these exact names.

- [ ] **Step 1: Write the failing tests**

In `pkg/model/model_test.go`, replace `TestCustomEmojiRoundtrip` and `TestCustomEmojiBSON` with versions exercising the new fields (non-zero values so `reflect.DeepEqual` catches tag typos), and add round-trips for the wire types:

```go
func TestCustomEmojiRoundtrip(t *testing.T) {
	e := model.CustomEmoji{
		ID:          "site-a:acme_party",
		SiteID:      "site-a",
		Shortcode:   "acme_party",
		ImageURL:    "/emoji/v1/site-a/acme_party",
		CreatedBy:   "alice",
		CreatedAt:   1747800000000,
		UpdatedBy:   "bob",
		UpdatedAt:   1747900000000,
		MinioKey:    "emoji/acme_party",
		ContentType: "image/gif",
		Size:        20480,
		ETag:        "abc123",
	}
	var dst model.CustomEmoji
	roundTrip(t, &e, &dst)
	assert.Equal(t, "acme_party", dst.Shortcode)
	assert.Equal(t, "emoji/acme_party", dst.MinioKey)
}

func TestCustomEmojiBSON(t *testing.T) {
	e := model.CustomEmoji{
		ID:          "site-a:acme_party",
		SiteID:      "site-a",
		Shortcode:   "acme_party",
		ImageURL:    "/emoji/v1/site-a/acme_party",
		CreatedBy:   "alice",
		CreatedAt:   1747800000000,
		UpdatedBy:   "bob",
		UpdatedAt:   1747900000000,
		MinioKey:    "emoji/acme_party",
		ContentType: "image/gif",
		Size:        20480,
		ETag:        "abc123",
	}
	data, err := bson.Marshal(&e)
	require.NoError(t, err)
	var dst model.CustomEmoji
	require.NoError(t, bson.Unmarshal(data, &dst))
	assert.Equal(t, e, dst)
}

func TestEmojiListResponseRoundtrip(t *testing.T) {
	src := model.EmojiListResponse{Emojis: []model.EmojiEntry{{
		Shortcode:   "acme_party",
		ImageURL:    "/emoji/v1/site-a/acme_party",
		ContentType: "image/png",
		ETag:        "abc123",
		CreatedBy:   "alice",
		UpdatedAt:   1747900000000,
	}}}
	roundTrip(t, &src, &model.EmojiListResponse{})
}

func TestEmojiDeleteRequestRoundtrip(t *testing.T) {
	src := model.EmojiDeleteRequest{Shortcode: "acme_party"}
	roundTrip(t, &src, &model.EmojiDeleteRequest{})
}

func TestEmojiDeleteResponseRoundtrip(t *testing.T) {
	src := model.EmojiDeleteResponse{Shortcode: "acme_party", Deleted: true}
	roundTrip(t, &src, &model.EmojiDeleteResponse{})
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=pkg/model`
Expected: FAIL — compile error `unknown field UpdatedBy` / `undefined: model.EmojiEntry`.

- [ ] **Step 3: Implement**

Replace `pkg/model/custom_emoji.go` with:

```go
package model

// CustomEmoji is a site-scoped custom reaction emoji; Shortcode is stored bare
// (no wrapping colons). Written by media-service (upload/delete); read by the
// pkg/emoji validator via an existence check. Doc exists ⟺ MinIO object exists.
type CustomEmoji struct {
	// ID is the deterministic "{siteID}:{shortcode}" document key.
	ID        string `json:"id"        bson:"_id"`
	SiteID    string `json:"siteId"    bson:"siteId"`
	Shortcode string `json:"shortcode" bson:"shortcode"`
	// ImageURL is the canonical relative serve path "/emoji/v1/{siteID}/{shortcode}".
	ImageURL  string `json:"imageUrl"  bson:"imageUrl"`
	CreatedBy string `json:"createdBy" bson:"createdBy"`
	CreatedAt int64  `json:"createdAt" bson:"createdAt"`
	// UpdatedBy/UpdatedAt track the last upload; CreatedBy/CreatedAt are
	// preserved on overwrite (audit).
	UpdatedBy   string `json:"updatedBy"   bson:"updatedBy"`
	UpdatedAt   int64  `json:"updatedAt"   bson:"updatedAt"`
	// MinioKey is "emoji/{siteID}/{shortcode}" — site-scoped: shortcodes are only unique per site.
	MinioKey    string `json:"minioKey"    bson:"minioKey"`
	ContentType string `json:"contentType" bson:"contentType"`
	Size        int64  `json:"size"        bson:"size"`
	ETag        string `json:"etag"        bson:"etag"`
}

// EmojiEntry is the wire shape of one emoji in EmojiListResponse.
type EmojiEntry struct {
	Shortcode   string `json:"shortcode"   bson:"shortcode"`
	ImageURL    string `json:"imageUrl"    bson:"imageUrl"`
	ContentType string `json:"contentType" bson:"contentType"`
	ETag        string `json:"etag"        bson:"etag"`
	CreatedBy   string `json:"createdBy"   bson:"createdBy"`
	UpdatedAt   int64  `json:"updatedAt"   bson:"updatedAt"`
}

// EmojiListResponse is the reply to chat.user.{account}.request.emoji.{siteID}.list.
type EmojiListResponse struct {
	Emojis []EmojiEntry `json:"emojis" bson:"emojis"`
}

// EmojiDeleteRequest is the body of chat.user.{account}.request.emoji.{siteID}.delete.
type EmojiDeleteRequest struct {
	Shortcode string `json:"shortcode" bson:"shortcode"`
}

// EmojiDeleteResponse is the reply to a successful emoji delete.
type EmojiDeleteResponse struct {
	Shortcode string `json:"shortcode" bson:"shortcode"`
	Deleted   bool   `json:"deleted"   bson:"deleted"`
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=pkg/model`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/model/custom_emoji.go pkg/model/model_test.go
git commit -m "$(cat <<'EOF'
feat(model): extend CustomEmoji with blob fields, add emoji wire types

New fields (updatedBy/updatedAt/minioKey/contentType/size/etag) carry
the MinIO-backed lifecycle media-service writes; existing readers only
project _id so the extension is inert for them. EmojiEntry/List/Delete
types are the NATS request-reply payloads.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_0172WdvqXaCgCyG5gZMdbqtd
EOF
)"
```

---

### Task 3: pkg/subject — emoji list/delete builders

**Files:**
- Modify: `pkg/subject/subject.go` (append after the search-service block, ~line 705)
- Test: `pkg/subject/subject_test.go` (add rows to `TestSubjectBuilders`)

**Interfaces:**
- Produces: `subject.EmojiList(account, siteID string) string`, `subject.EmojiListPattern(siteID string) string`, `subject.EmojiDelete(account, siteID string) string`, `subject.EmojiDeletePattern(siteID string) string`. Tasks 10–12 depend on these exact names.

- [ ] **Step 1: Write the failing tests**

Add four rows to the `tests` table in `TestSubjectBuilders` in `pkg/subject/subject_test.go`:

```go
		{"EmojiList", subject.EmojiList("alice", "site-a"),
			"chat.user.alice.request.emoji.site-a.list"},
		{"EmojiListPattern", subject.EmojiListPattern("site-a"),
			"chat.user.{account}.request.emoji.site-a.list"},
		{"EmojiDelete", subject.EmojiDelete("alice", "site-a"),
			"chat.user.alice.request.emoji.site-a.delete"},
		{"EmojiDeletePattern", subject.EmojiDeletePattern("site-a"),
			"chat.user.{account}.request.emoji.site-a.delete"},
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=pkg/subject`
Expected: FAIL — compile error `undefined: subject.EmojiList`.

- [ ] **Step 3: Implement**

Append to `pkg/subject/subject.go` after the search-service builders:

```go
// --- custom emoji (media-service) ---

// EmojiList builds the concrete subject for listing a site's custom emoji.
func EmojiList(account, siteID string) string {
	return fmt.Sprintf("chat.user.%s.request.emoji.%s.list", account, siteID)
}

// EmojiListPattern is the natsrouter pattern for the emoji list RPC. siteID is
// baked in so each site's media-service only serves its own emoji set —
// clients target the room's origin site.
func EmojiListPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.emoji.%s.list", siteID)
}

// EmojiDelete builds the concrete subject for deleting a custom emoji.
func EmojiDelete(account, siteID string) string {
	return fmt.Sprintf("chat.user.%s.request.emoji.%s.delete", account, siteID)
}

// EmojiDeletePattern is the natsrouter pattern for the emoji delete RPC.
func EmojiDeletePattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.emoji.%s.delete", siteID)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=pkg/subject`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/subject/subject.go pkg/subject/subject_test.go
git commit -m "$(cat <<'EOF'
feat(subject): add custom-emoji list/delete subject builders

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_0172WdvqXaCgCyG5gZMdbqtd
EOF
)"
```

---

### Task 4: pkg/errcode — emoji reasons

**Files:**
- Create: `pkg/errcode/codes_emoji.go`
- Test: `pkg/errcode/codes_test.go` (append to `allReasons`)

**Interfaces:**
- Produces: `errcode.EmojiWrongCluster Reason = "emoji_wrong_cluster"`, `errcode.EmojiShortcodeReserved Reason = "emoji_shortcode_reserved"`. Task 7 depends on both.

- [ ] **Step 1: Write the failing test**

In `pkg/errcode/codes_test.go`, append the two new reasons to the `allReasons` slice (keep list order grouped by service, add at the end):

```go
	EmojiWrongCluster,
	EmojiShortcodeReserved,
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=pkg/errcode`
Expected: FAIL — compile error `undefined: EmojiWrongCluster`.

- [ ] **Step 3: Implement**

Create `pkg/errcode/codes_emoji.go`:

```go
package errcode

// Reasons emitted by media-service custom-emoji endpoints.
const (
	// EmojiWrongCluster signals an emoji upload reached a cluster that does not
	// own the target siteID; the response message names the correct domain to
	// retry against.
	EmojiWrongCluster Reason = "emoji_wrong_cluster"

	// EmojiShortcodeReserved signals the shortcode collides with a built-in
	// standard emoji: the reaction validator resolves standard names first, so
	// a custom emoji under that name could never be used.
	EmojiShortcodeReserved Reason = "emoji_shortcode_reserved"
)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=pkg/errcode`
Expected: PASS — `TestReasons_SnakeCase` and `TestReasons_Unique` cover the new values.

- [ ] **Step 5: Commit**

```bash
git add pkg/errcode/codes_emoji.go pkg/errcode/codes_test.go
git commit -m "$(cat <<'EOF'
feat(errcode): add emoji_wrong_cluster and emoji_shortcode_reserved reasons

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_0172WdvqXaCgCyG5gZMdbqtd
EOF
)"
```

---

### Task 5: media-service config — NATS + emoji limits

**Files:**
- Modify: `media-service/config.go`
- Test: `media-service/config_test.go`

**Interfaces:**
- Produces: `cfg.NatsURL string`, `cfg.NatsCredsFile string`, `cfg.EmojiMaxUploadBytes int64`, `cfg.EmojiMaxDimension int`. Tasks 7, 8, 11 depend on these exact field names.

- [ ] **Step 1: Write the failing test**

Append to `media-service/config_test.go`:

```go
func TestConfig_EmojiAndNATSDefaults(t *testing.T) {
	t.Setenv("SITE_ID", "s1")
	t.Setenv("CLUSTER_DOMAINS", `[{"siteID":"s1","domain":"http://localhost:8080"}]`)
	t.Setenv("EMPLOYEE_PHOTO_BASE_URL", "https://photos.example.com")
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")
	t.Setenv("MINIO_ENDPOINT", "localhost:9000")
	t.Setenv("MINIO_ACCESS_KEY", "k")
	t.Setenv("MINIO_SECRET_KEY", "s")
	t.Setenv("NATS_URL", "nats://localhost:4222")

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	assert.Equal(t, "nats://localhost:4222", cfg.NatsURL)
	assert.Empty(t, cfg.NatsCredsFile)
	assert.Equal(t, int64(262144), cfg.EmojiMaxUploadBytes)
	assert.Equal(t, 512, cfg.EmojiMaxDimension)
}

func TestConfig_NATSURLRequired(t *testing.T) {
	t.Setenv("SITE_ID", "s1")
	t.Setenv("CLUSTER_DOMAINS", `[]`)
	t.Setenv("EMPLOYEE_PHOTO_BASE_URL", "https://photos.example.com")
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")
	t.Setenv("MINIO_ENDPOINT", "localhost:9000")
	t.Setenv("MINIO_ACCESS_KEY", "k")
	t.Setenv("MINIO_SECRET_KEY", "s")

	_, err := env.ParseAs[config]()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "NATS_URL")
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=media-service`
Expected: FAIL — compile error `cfg.NatsURL undefined` (first test) — the second test would also fail because `NATS_URL` is not yet required.

- [ ] **Step 3: Implement**

In `media-service/config.go`, add to the `config` struct (after the `MinioBucket` field):

```go
	NatsURL       string `env:"NATS_URL,required"`
	NatsCredsFile string `env:"NATS_CREDS_FILE"`
```

and after `CacheMaxAgeSeconds`:

```go
	// Custom-emoji upload limits. Bytes cap the raw body; dimension caps the
	// decoded width AND height independently.
	EmojiMaxUploadBytes int64 `env:"EMOJI_MAX_UPLOAD_BYTES" envDefault:"262144"`
	EmojiMaxDimension   int   `env:"EMOJI_MAX_DIMENSION" envDefault:"512"`
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=media-service`
Expected: PASS (main.go doesn't parse config in tests; existing tests unaffected).

- [ ] **Step 5: Commit**

```bash
git add media-service/config.go media-service/config_test.go
git commit -m "$(cat <<'EOF'
feat(media-service): add NATS and emoji-limit config

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_0172WdvqXaCgCyG5gZMdbqtd
EOF
)"
```

---

### Task 6: media-service store — `emojiStore` interface, Mongo impl, blob `Delete`

**Files:**
- Modify: `media-service/store.go`, `media-service/store_mongo.go`, `media-service/minio.go`, `media-service/handler_test.go` (fakeBlobStore only)
- Create: `media-service/emoji_integration_test.go` (store-level tests)
- Regenerate: `media-service/mock_store_test.go` (via `make generate SERVICE=media-service`)

**Interfaces:**
- Consumes: `model.CustomEmoji` (Task 2).
- Produces:
  - `emojiStore` interface: `EmojiDoc(ctx, siteID, shortcode string) (*model.CustomEmoji, bool, error)`, `ListEmojis(ctx, siteID string) ([]model.CustomEmoji, error)`, `UpsertEmoji(ctx context.Context, e *model.CustomEmoji) error`, `DeleteEmoji(ctx, siteID, shortcode string) (minioKey string, found bool, err error)` — implemented by `*mongoStore`, mocked as `MockemojiStore`.
  - `blobStore` gains `Delete(ctx context.Context, key string) error`; `fakeBlobStore` gains `Delete` + `deleteErr` field.
  - `(*mongoStore).EnsureEmojiIndexes(ctx) error` (concrete method, not on the interface).

- [ ] **Step 1: Write the failing integration tests**

Create `media-service/emoji_integration_test.go`:

```go
//go:build integration

package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/testutil"
)

func emojiFixture(siteID, shortcode, by string, at int64) *model.CustomEmoji {
	return &model.CustomEmoji{
		ID:          siteID + ":" + shortcode,
		SiteID:      siteID,
		Shortcode:   shortcode,
		ImageURL:    "/emoji/v1/" + siteID + "/" + shortcode,
		CreatedBy:   by,
		CreatedAt:   at,
		UpdatedBy:   by,
		UpdatedAt:   at,
		MinioKey:    "emoji/" + shortcode,
		ContentType: "image/png",
		Size:        128,
		ETag:        "etag-" + shortcode,
	}
}

func TestMongoStore_EmojiCRUD(t *testing.T) {
	ctx := context.Background()
	db := testutil.MongoDB(t, "emoji")
	st := newMongoStore(db)
	require.NoError(t, st.EnsureEmojiIndexes(ctx))

	// Insert.
	require.NoError(t, st.UpsertEmoji(ctx, emojiFixture("s1", "party", "alice", 1000)))

	// EmojiDoc projects minioKey + etag.
	doc, found, err := st.EmojiDoc(ctx, "s1", "party")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, "emoji/party", doc.MinioKey)
	assert.Equal(t, "etag-party", doc.ETag)

	// Miss.
	_, found, err = st.EmojiDoc(ctx, "s1", "nope")
	require.NoError(t, err)
	assert.False(t, found)

	// Overwrite preserves createdBy/createdAt, updates the rest, no dup docs.
	over := emojiFixture("s1", "party", "bob", 2000)
	over.ContentType = "image/gif"
	require.NoError(t, st.UpsertEmoji(ctx, over))
	n, err := db.Collection("custom_emojis").CountDocuments(ctx, bson.M{"siteId": "s1", "shortcode": "party"})
	require.NoError(t, err)
	assert.EqualValues(t, 1, n)
	var full model.CustomEmoji
	require.NoError(t, db.Collection("custom_emojis").
		FindOne(ctx, bson.M{"_id": "s1:party"}).Decode(&full))
	assert.Equal(t, "alice", full.CreatedBy)
	assert.EqualValues(t, 1000, full.CreatedAt)
	assert.Equal(t, "bob", full.UpdatedBy)
	assert.EqualValues(t, 2000, full.UpdatedAt)
	assert.Equal(t, "image/gif", full.ContentType)

	// List is sorted by shortcode and projects the wire fields.
	require.NoError(t, st.UpsertEmoji(ctx, emojiFixture("s1", "aaa_first", "alice", 1000)))
	require.NoError(t, st.UpsertEmoji(ctx, emojiFixture("s2", "other_site", "alice", 1000)))
	list, err := st.ListEmojis(ctx, "s1")
	require.NoError(t, err)
	require.Len(t, list, 2)
	assert.Equal(t, "aaa_first", list[0].Shortcode)
	assert.Equal(t, "party", list[1].Shortcode)
	assert.Equal(t, "alice", list[0].CreatedBy)
	assert.Empty(t, list[0].MinioKey, "list must not project minioKey")

	// Delete returns the minioKey; second delete reports not found.
	key, found, err := st.DeleteEmoji(ctx, "s1", "party")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, "emoji/party", key)
	_, found, err = st.DeleteEmoji(ctx, "s1", "party")
	require.NoError(t, err)
	assert.False(t, found)
}

func TestMongoStore_EnsureEmojiIndexes_Idempotent(t *testing.T) {
	ctx := context.Background()
	db := testutil.MongoDB(t, "emojiidx")
	st := newMongoStore(db)
	require.NoError(t, st.EnsureEmojiIndexes(ctx))
	require.NoError(t, st.EnsureEmojiIndexes(ctx))
}

func TestMinioBlobStore_Delete(t *testing.T) {
	ctx := context.Background()
	client, bucket := testutil.MinIO(t, "emoji")
	bs := newMinioBlobStore(client, bucket)

	_, err := bs.Put(ctx, "emoji/party", bytesReader("PNG"), 3, "image/png")
	require.NoError(t, err)
	require.NoError(t, bs.Delete(ctx, "emoji/party"))
	_, _, err = bs.Get(ctx, "emoji/party")
	assert.ErrorIs(t, err, errBlobNotFound)

	// Deleting a missing key is a no-op (S3 semantics).
	assert.NoError(t, bs.Delete(ctx, "emoji/missing"))
}
```

Add the tiny helper at the bottom of the same file:

```go
func bytesReader(s string) *strings.Reader { return strings.NewReader(s) }
```

(and add `"strings"` to the imports).

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test-integration SERVICE=media-service` (requires Docker)
Expected: FAIL — compile errors: `st.EnsureEmojiIndexes undefined`, `st.UpsertEmoji undefined`, `bs.Delete undefined`.
If Docker is unavailable in the environment, compile-check instead with `go vet ./media-service/` — it must fail with the same undefined symbols.

- [ ] **Step 3: Implement store interface + Mongo + blob Delete**

In `media-service/store.go`, append below `avatarStore`:

```go
// emojiStore is the custom-emoji data access this service needs. It reads and
// writes the site-local custom_emojis collection — the same collection the
// pkg/emoji reaction validator (history-service) reads existence from.
type emojiStore interface {
	// EmojiDoc returns one emoji doc projecting only what the serve path needs
	// (minioKey, etag). found=false when the emoji is not registered.
	EmojiDoc(ctx context.Context, siteID, shortcode string) (*model.CustomEmoji, bool, error)
	// ListEmojis returns a site's emoji sorted by shortcode, projecting only
	// the wire fields (shortcode, imageUrl, contentType, etag, createdBy, updatedAt).
	ListEmojis(ctx context.Context, siteID string) ([]model.CustomEmoji, error)
	// UpsertEmoji inserts or overwrites by (siteId, shortcode). createdBy and
	// createdAt are set on insert only; all blob fields update on overwrite.
	UpsertEmoji(ctx context.Context, e *model.CustomEmoji) error
	// DeleteEmoji removes the doc and returns its minioKey so the caller can
	// clean up the blob. found=false when no such emoji exists.
	DeleteEmoji(ctx context.Context, siteID, shortcode string) (minioKey string, found bool, err error)
}
```

In `media-service/store_mongo.go`:
- add `customEmojis *mongo.Collection` to the `mongoStore` struct and `customEmojis: db.Collection("custom_emojis"),` to `newMongoStore`;
- append:

```go
// EnsureEmojiIndexes creates the (siteId, shortcode) unique index; idempotent.
// Mirrors history-service's CustomEmojiRepo.EnsureIndexes (same index name) so
// either service can start first.
func (s *mongoStore) EnsureEmojiIndexes(ctx context.Context) error {
	_, err := s.customEmojis.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "siteId", Value: 1}, {Key: "shortcode", Value: 1}},
		Options: options.Index().SetUnique(true).SetName("siteId_shortcode_unique"),
	})
	if err != nil {
		return fmt.Errorf("ensure custom_emojis indexes: %w", err)
	}
	return nil
}

func (s *mongoStore) EmojiDoc(ctx context.Context, siteID, shortcode string) (*model.CustomEmoji, bool, error) {
	var e model.CustomEmoji
	err := s.customEmojis.FindOne(ctx, bson.M{"siteId": siteID, "shortcode": shortcode},
		options.FindOne().SetProjection(bson.M{"minioKey": 1, "etag": 1})).Decode(&e)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("find custom emoji: %w", err)
	}
	return &e, true, nil
}

func (s *mongoStore) ListEmojis(ctx context.Context, siteID string) ([]model.CustomEmoji, error) {
	cur, err := s.customEmojis.Find(ctx, bson.M{"siteId": siteID},
		options.Find().
			SetProjection(bson.M{"shortcode": 1, "imageUrl": 1, "contentType": 1, "etag": 1, "createdBy": 1, "updatedAt": 1}).
			SetSort(bson.D{{Key: "shortcode", Value: 1}}))
	if err != nil {
		return nil, fmt.Errorf("list custom emojis: %w", err)
	}
	var out []model.CustomEmoji
	if err := cur.All(ctx, &out); err != nil {
		return nil, fmt.Errorf("decode custom emojis: %w", err)
	}
	return out, nil
}

func (s *mongoStore) UpsertEmoji(ctx context.Context, e *model.CustomEmoji) error {
	filter := bson.M{"siteId": e.SiteID, "shortcode": e.Shortcode}
	update := bson.M{
		"$set": bson.M{
			"imageUrl":    e.ImageURL,
			"updatedBy":   e.UpdatedBy,
			"updatedAt":   e.UpdatedAt,
			"minioKey":    e.MinioKey,
			"contentType": e.ContentType,
			"size":        e.Size,
			"etag":        e.ETag,
		},
		"$setOnInsert": bson.M{
			"_id":       e.ID,
			"siteId":    e.SiteID,
			"shortcode": e.Shortcode,
			"createdBy": e.CreatedBy,
			"createdAt": e.CreatedAt,
		},
	}
	if _, err := s.customEmojis.UpdateOne(ctx, filter, update, options.UpdateOne().SetUpsert(true)); err != nil {
		return fmt.Errorf("upsert custom emoji: %w", err)
	}
	return nil
}

func (s *mongoStore) DeleteEmoji(ctx context.Context, siteID, shortcode string) (string, bool, error) {
	var e model.CustomEmoji
	err := s.customEmojis.FindOneAndDelete(ctx, bson.M{"siteId": siteID, "shortcode": shortcode},
		options.FindOneAndDelete().SetProjection(bson.M{"minioKey": 1})).Decode(&e)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("delete custom emoji: %w", err)
	}
	return e.MinioKey, true, nil
}
```

In `media-service/minio.go`, add to the `blobStore` interface:

```go
	Delete(ctx context.Context, key string) error
```

and implement:

```go
func (m *minioBlobStore) Delete(ctx context.Context, key string) error {
	if err := m.client.RemoveObject(ctx, m.bucket, key, minio.RemoveObjectOptions{}); err != nil {
		return fmt.Errorf("remove object: %w", err)
	}
	return nil
}
```

In `media-service/handler_test.go`, add a `deleteErr error` field to `fakeBlobStore` and:

```go
func (f *fakeBlobStore) Delete(_ context.Context, key string) error {
	if f.deleteErr != nil {
		return f.deleteErr
	}
	delete(f.objects, key)
	delete(f.info, key)
	return nil
}
```

- [ ] **Step 4: Regenerate mocks**

Run: `make generate SERVICE=media-service`
Expected: `mock_store_test.go` now also contains `MockemojiStore` / `NewMockemojiStore`. Do not hand-edit.

- [ ] **Step 5: Run tests to verify they pass**

Run: `make test SERVICE=media-service` (unit compile + existing tests)
Run: `make test-integration SERVICE=media-service` (if Docker available)
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add media-service/store.go media-service/store_mongo.go media-service/minio.go media-service/handler_test.go media-service/mock_store_test.go media-service/emoji_integration_test.go
git commit -m "$(cat <<'EOF'
feat(media-service): emoji store (custom_emojis CRUD) and blob delete

UpsertEmoji preserves createdBy/createdAt via $setOnInsert; DeleteEmoji
returns the minioKey for blob cleanup; list/serve queries use precise
projections. EnsureEmojiIndexes mirrors history-service's unique index.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_0172WdvqXaCgCyG5gZMdbqtd
EOF
)"
```

---

### Task 7: HTTP upload handler — `PUT /emoji/v1/:siteID/:shortcode`

**Files:**
- Create: `media-service/emoji_upload.go`
- Modify: `media-service/handler.go` (handler struct + constructor), `media-service/routes.go`, `media-service/main.go` (constructor call only), `media-service/handler_test.go` (test-router helper)
- Test: `media-service/emoji_upload_test.go`

**Interfaces:**
- Consumes: `emoji.Canonicalize`/`emoji.IsStandard` (Task 1), `model.CustomEmoji` (Task 2), `errcode.EmojiWrongCluster`/`EmojiShortcodeReserved` (Task 4), `cfg.EmojiMaxUploadBytes`/`cfg.EmojiMaxDimension` (Task 5), `emojiStore.UpsertEmoji` + `blobStore.Put` (Task 6).
- Produces: `(*handler).HandleEmojiUpload(c *gin.Context)`; helpers `emojiObjectKey(siteID, shortcode string) string`, `emojiDocID(siteID, shortcode) string`, `emojiImagePath(siteID, shortcode) string`; `handler` struct gains `emojis emojiStore`, `newHandler(store avatarStore, emojis emojiStore, blobs blobStore, cfg *config) *handler`; test helper `newEmojiTestRouter(t) (*gin.Engine, *MockavatarStore, *MockemojiStore, *fakeBlobStore)`. Tasks 8–12 depend on all of these.

- [ ] **Step 1: Update the handler struct, constructor, and test router (mechanical prerequisite)**

In `media-service/handler.go`:

```go
type handler struct {
	store    avatarStore
	emojis   emojiStore
	blobs    blobStore
	cfg      config
	eidCache *eidCache
}

func newHandler(store avatarStore, emojis emojiStore, blobs blobStore, cfg *config) *handler {
	return &handler{
		store:    store,
		emojis:   emojis,
		blobs:    blobs,
		cfg:      *cfg,
		eidCache: newEIDCache(store, cfg.EIDCacheCapacity, cfg.EIDCacheTTL),
	}
}
```

In `media-service/main.go` change the construction line to `h := newHandler(store, store, blobs, &cfg)` (`*mongoStore` implements both interfaces).

In `media-service/handler_test.go`, convert `newTestRouter` into a thin wrapper so the ~30 existing call sites stay untouched:

```go
func newTestRouter(t *testing.T) (*gin.Engine, *MockavatarStore, *fakeBlobStore) {
	r, store, _, blobs := newEmojiTestRouter(t)
	return r, store, blobs
}

func newEmojiTestRouter(t *testing.T) (*gin.Engine, *MockavatarStore, *MockemojiStore, *fakeBlobStore) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	ctrl := gomock.NewController(t)
	store := NewMockavatarStore(ctrl)
	emojis := NewMockemojiStore(ctrl)
	blobs := &fakeBlobStore{}
	h := newHandler(store, emojis, blobs, &config{
		SiteID:               "s1",
		EmployeePhotoBaseURL: "https://photos.example.com",
		CacheMaxAgeSeconds:   3600,
		MinioBucket:          "avatars",
		ClusterDomains:       clusterDomains{byID: map[string]string{"s2": "https://avatar-s2"}}, // keep the original s2 value — existing avatar tests assert on it
		MaxUploadBytes:       1048576,
		EmojiMaxUploadBytes:  262144,
		EmojiMaxDimension:    512,
		EIDCacheCapacity:     1000,
		EIDCacheTTL:          time.Minute,
	})
	r := gin.New()
	registerRoutes(r, h)
	return r, store, emojis, blobs
}
```

Run `make test SERVICE=media-service` — existing tests must still PASS before continuing.

- [ ] **Step 2: Write the failing tests**

Create `media-service/emoji_upload_test.go`:

```go
package main

import (
	"bytes"
	"image"
	"image/color/palette"
	"image/gif"
	"image/png"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/model"
)

func pngSized(t *testing.T, w, h int) []byte {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, png.Encode(&buf, image.NewRGBA(image.Rect(0, 0, w, h))))
	return buf.Bytes()
}

func gifAnimated(t *testing.T) []byte {
	t.Helper()
	g := &gif.GIF{}
	for i := 0; i < 2; i++ {
		g.Image = append(g.Image, image.NewPaletted(image.Rect(0, 0, 2, 2), palette.Plan9))
		g.Delay = append(g.Delay, 10)
	}
	var buf bytes.Buffer
	require.NoError(t, gif.EncodeAll(&buf, g))
	return buf.Bytes()
}

func TestEmojiUpload_Success_StoresBlobThenUpsertsDoc(t *testing.T) {
	r, _, emojis, blobs := newEmojiTestRouter(t)
	emojis.EXPECT().UpsertEmoji(gomock.Any(), gomock.Any()).DoAndReturn(func(_ any, e *model.CustomEmoji) error {
		assert.Equal(t, "s1:party", e.ID)
		assert.Equal(t, "s1", e.SiteID)
		assert.Equal(t, "party", e.Shortcode)
		assert.Equal(t, "/emoji/v1/s1/party", e.ImageURL)
		assert.Equal(t, "emoji/party", e.MinioKey)
		assert.Equal(t, "image/png", e.ContentType)
		assert.Equal(t, "alice", e.CreatedBy)
		assert.Equal(t, "alice", e.UpdatedBy)
		assert.NotEmpty(t, e.ETag)
		assert.Positive(t, e.UpdatedAt)
		return nil
	})
	w := httptest.NewRecorder()
	r.ServeHTTP(w, putReq("/emoji/v1/s1/party?uploader=alice", pngSized(t, 2, 2), "image/png"))
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "nosniff", w.Header().Get("X-Content-Type-Options"))
	body := w.Body.String()
	assert.Contains(t, body, `"shortcode":"party"`)
	assert.Contains(t, body, `"etag":"etag-emoji/party"`)
	assert.Contains(t, body, `"contentType":"image/png"`)
	_, ok := blobs.objects["emoji/party"]
	assert.True(t, ok, "object stored before the doc")
}

func TestEmojiUpload_AnimatedGIF_Accepted(t *testing.T) {
	r, _, emojis, _ := newEmojiTestRouter(t)
	emojis.EXPECT().UpsertEmoji(gomock.Any(), gomock.Any()).DoAndReturn(func(_ any, e *model.CustomEmoji) error {
		assert.Equal(t, "image/gif", e.ContentType)
		return nil
	})
	w := httptest.NewRecorder()
	r.ServeHTTP(w, putReq("/emoji/v1/s1/dance?uploader=alice", gifAnimated(t), "image/gif"))
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestEmojiUpload_InvalidShortcode_400(t *testing.T) {
	r, _, _, _ := newEmojiTestRouter(t)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, putReq("/emoji/v1/s1/BadName", pngSized(t, 2, 2), "image/png"))
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestEmojiUpload_ReservedStandardShortcode_400(t *testing.T) {
	r, _, _, _ := newEmojiTestRouter(t)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, putReq("/emoji/v1/s1/thumbsup", pngSized(t, 2, 2), "image/png"))
	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "emoji_shortcode_reserved")
}

func TestEmojiUpload_WrongCluster_409(t *testing.T) {
	r, _, _, _ := newEmojiTestRouter(t)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, putReq("/emoji/v1/s2/party", pngSized(t, 2, 2), "image/png"))
	assert.Equal(t, http.StatusConflict, w.Code)
	assert.Contains(t, w.Body.String(), "emoji_wrong_cluster")
	assert.Contains(t, w.Body.String(), "https://avatar-s2")
}

func TestEmojiUpload_NotAnImage_400(t *testing.T) {
	r, _, _, _ := newEmojiTestRouter(t)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, putReq("/emoji/v1/s1/party", []byte("<svg></svg>"), "image/svg+xml"))
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestEmojiUpload_OversizeBytes_400(t *testing.T) {
	r, _, _, _ := newEmojiTestRouter(t)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, putReq("/emoji/v1/s1/party", make([]byte, 262144+1), "image/png"))
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestEmojiUpload_OversizeDimensions_400(t *testing.T) {
	r, _, _, _ := newEmojiTestRouter(t)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, putReq("/emoji/v1/s1/party", pngSized(t, 513, 513), "image/png"))
	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "512x512")
}

func TestEmojiUpload_BlobPutError_500(t *testing.T) {
	r, _, _, blobs := newEmojiTestRouter(t)
	blobs.putErr = assert.AnError
	w := httptest.NewRecorder()
	r.ServeHTTP(w, putReq("/emoji/v1/s1/party", pngSized(t, 2, 2), "image/png"))
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestEmojiUpload_UpsertError_500(t *testing.T) {
	r, _, emojis, _ := newEmojiTestRouter(t)
	emojis.EXPECT().UpsertEmoji(gomock.Any(), gomock.Any()).Return(assert.AnError)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, putReq("/emoji/v1/s1/party", pngSized(t, 2, 2), "image/png"))
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}
```

(`putReq` already exists in `upload_test.go`, same package.)

- [ ] **Step 3: Run tests to verify they fail**

Run: `make test SERVICE=media-service`
Expected: FAIL — 404s (route not registered) / compile error `undefined: newEmojiTestRouter` resolved in Step 1, so the failures are HTTP 404 vs expected codes.

- [ ] **Step 4: Implement**

Create `media-service/emoji_upload.go`:

```go
package main

import (
	"bytes"
	"fmt"
	"image"
	"io"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	_ "image/gif" // register the GIF decoder (jpeg/png registered in upload.go)

	"github.com/hmchangw/chat/pkg/emoji"
	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
	"github.com/hmchangw/chat/pkg/model"
)

// emojiObjectKey is the MinIO key for an emoji blob; the bucket is shared with
// avatars, so the prefix namespaces it. siteID is included so the key carries
// the emoji's full identity — shortcodes are only unique per site.
func emojiObjectKey(siteID, shortcode string) string {
	return "emoji/" + siteID + "/" + shortcode
}

// emojiDocID is the deterministic custom_emojis document _id.
func emojiDocID(siteID, shortcode string) string { return siteID + ":" + shortcode }

// emojiImagePath is the canonical relative serve path, stored in imageUrl and
// used as the cross-cluster redirect target. Both tokens are
// charset-validated, so no escaping is needed.
func emojiImagePath(siteID, shortcode string) string {
	return "/emoji/v1/" + siteID + "/" + shortcode
}

// emojiUploadResponse is the 200 body on a successful upload.
type emojiUploadResponse struct {
	Shortcode   string `json:"shortcode"`
	ETag        string `json:"etag"`
	ContentType string `json:"contentType"`
	Size        int64  `json:"size"`
	UpdatedAt   int64  `json:"updatedAt"`
}

func (h *handler) HandleEmojiUpload(c *gin.Context) {
	ctx := c.Request.Context()
	c.Set("avatar_kind", "emoji")
	siteID := c.Param("siteID")

	shortcode, err := emoji.Canonicalize(c.Param("shortcode"))
	if err != nil {
		c.Set("avatar_outcome", "error")
		errhttp.Write(ctx, c, errcode.BadRequest("invalid emoji shortcode"))
		return
	}
	if emoji.IsStandard(shortcode) {
		c.Set("avatar_outcome", "error")
		errhttp.Write(ctx, c, errcode.BadRequest(
			"shortcode collides with a built-in standard emoji",
			errcode.WithReason(errcode.EmojiShortcodeReserved)))
		return
	}
	if siteID != h.cfg.SiteID {
		c.Set("avatar_outcome", "error")
		errhttp.Write(ctx, c, errcode.Conflict(
			fmt.Sprintf("emoji belongs to another cluster; upload to %s", h.cfg.clusterBaseURL(siteID)),
			errcode.WithReason(errcode.EmojiWrongCluster)))
		return
	}

	// Size cap before reading the body.
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, h.cfg.EmojiMaxUploadBytes)
	raw, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.Set("avatar_outcome", "error")
		errhttp.Write(ctx, c, errcode.BadRequest("upload too large or unreadable"))
		return
	}

	// Decode to confirm a real PNG/JPEG/GIF; animated GIFs decode as their
	// first frame, which is what the dimension check applies to.
	img, format, err := image.Decode(bytes.NewReader(raw))
	if err != nil || (format != "png" && format != "jpeg" && format != "gif") {
		c.Set("avatar_outcome", "error")
		errhttp.Write(ctx, c, errcode.BadRequest("body is not a valid PNG, JPEG, or GIF image"))
		return
	}
	if b := img.Bounds(); b.Dx() > h.cfg.EmojiMaxDimension || b.Dy() > h.cfg.EmojiMaxDimension {
		c.Set("avatar_outcome", "error")
		errhttp.Write(ctx, c, errcode.BadRequest(
			fmt.Sprintf("image exceeds %dx%d", h.cfg.EmojiMaxDimension, h.cfg.EmojiMaxDimension)))
		return
	}
	contentType := "image/" + format

	// Store the object FIRST, then upsert the doc (doc exists ⟺ object exists).
	key := emojiObjectKey(siteID, shortcode)
	etag, err := h.blobs.Put(ctx, key, bytes.NewReader(raw), int64(len(raw)), contentType)
	if err != nil {
		c.Set("avatar_outcome", "error")
		errhttp.Write(ctx, c, fmt.Errorf("store emoji object: %w", err))
		return
	}
	now := time.Now().UTC().UnixMilli()
	uploader := c.Query("uploader") // v1: unauthenticated, audit-only (§7 client-api)
	e := &model.CustomEmoji{
		ID:          emojiDocID(siteID, shortcode),
		SiteID:      siteID,
		Shortcode:   shortcode,
		ImageURL:    emojiImagePath(siteID, shortcode),
		CreatedBy:   uploader,
		CreatedAt:   now,
		UpdatedBy:   uploader,
		UpdatedAt:   now,
		MinioKey:    key,
		ContentType: contentType,
		Size:        int64(len(raw)),
		ETag:        etag,
	}
	if err := h.emojis.UpsertEmoji(ctx, e); err != nil {
		c.Set("avatar_outcome", "error")
		errhttp.Write(ctx, c, fmt.Errorf("upsert emoji doc: %w", err))
		return
	}
	c.Set("avatar_outcome", "upload")
	c.Header("X-Content-Type-Options", "nosniff")
	c.JSON(http.StatusOK, emojiUploadResponse{
		Shortcode:   shortcode,
		ETag:        e.ETag,
		ContentType: e.ContentType,
		Size:        e.Size,
		UpdatedAt:   e.UpdatedAt,
	})
}
```

In `media-service/routes.go` add:

```go
	r.PUT("/emoji/v1/:siteID/:shortcode", h.HandleEmojiUpload) // v1: no auth; ?uploader= is audit-only
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `make test SERVICE=media-service`
Expected: PASS — all new upload tests plus every pre-existing avatar test.

- [ ] **Step 6: Commit**

```bash
git add media-service/emoji_upload.go media-service/handler.go media-service/routes.go media-service/main.go media-service/handler_test.go media-service/emoji_upload_test.go
git commit -m "$(cat <<'EOF'
feat(media-service): custom emoji upload endpoint

PUT /emoji/v1/{siteID}/{shortcode} with upsert semantics: canonical
shortcode validation shared with the reaction validator, reserved-name
rejection, wrong-cluster 409, byte + dimension caps (env-configurable),
PNG/JPEG/GIF (incl. animated), blob-then-doc write ordering.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_0172WdvqXaCgCyG5gZMdbqtd
EOF
)"
```

---

### Task 8: HTTP serve handler — `GET /emoji/v1/:siteID/:shortcode`

**Files:**
- Create: `media-service/emoji_serve.go`
- Modify: `media-service/routes.go`
- Test: `media-service/emoji_serve_test.go`

**Interfaces:**
- Consumes: `emojiStore.EmojiDoc`, `blobStore.Get`, `emoji.Canonicalize`, `emojiImagePath`, `h.setImageCacheHeaders` (existing, `handler.go:38`), `errBlobNotFound` (existing, `minio.go:13`).
- Produces: `(*handler).HandleEmojiGet(c *gin.Context)`.

- [ ] **Step 1: Write the failing tests**

Create `media-service/emoji_serve_test.go`:

```go
package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/model"
)

func TestEmojiGet_LocalHit_Streams(t *testing.T) {
	r, _, emojis, blobs := newEmojiTestRouter(t)
	emojis.EXPECT().EmojiDoc(gomock.Any(), "s1", "party").
		Return(&model.CustomEmoji{MinioKey: "emoji/party", ETag: "e1"}, true, nil)
	blobs.objects = map[string][]byte{"emoji/party": []byte("GIF!")}
	blobs.info = map[string]blobInfo{"emoji/party": {Size: 4, ContentType: "image/gif", ETag: "e1"}}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/emoji/v1/s1/party", nil))
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "image/gif", w.Header().Get("Content-Type"))
	assert.Equal(t, "GIF!", w.Body.String())
	assert.Contains(t, w.Header().Get("Cache-Control"), "max-age=3600")
	assert.Equal(t, "nosniff", w.Header().Get("X-Content-Type-Options"))
}

func TestEmojiGet_NotModified_304(t *testing.T) {
	r, _, emojis, _ := newEmojiTestRouter(t)
	emojis.EXPECT().EmojiDoc(gomock.Any(), "s1", "party").
		Return(&model.CustomEmoji{MinioKey: "emoji/party", ETag: "e1"}, true, nil)
	req := httptest.NewRequest(http.MethodGet, "/emoji/v1/s1/party", nil)
	req.Header.Set("If-None-Match", "e1")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusNotModified, w.Code)
	assert.Empty(t, w.Body.String())
}

func TestEmojiGet_LocalMiss_404(t *testing.T) {
	r, _, emojis, _ := newEmojiTestRouter(t)
	emojis.EXPECT().EmojiDoc(gomock.Any(), "s1", "nope").Return(nil, false, nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/emoji/v1/s1/nope", nil))
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestEmojiGet_RemoteSite_307(t *testing.T) {
	r, _, _, _ := newEmojiTestRouter(t)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/emoji/v1/s2/party", nil))
	assert.Equal(t, http.StatusTemporaryRedirect, w.Code)
	assert.Equal(t, "https://avatar-s2/emoji/v1/s2/party", w.Header().Get("Location"))
}

func TestEmojiGet_UnknownSite_404(t *testing.T) {
	r, _, _, _ := newEmojiTestRouter(t)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/emoji/v1/s9/party", nil))
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestEmojiGet_InvalidShortcode_400(t *testing.T) {
	r, _, _, _ := newEmojiTestRouter(t)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/emoji/v1/s1/Bad%20Name", nil))
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestEmojiGet_StoreError_500(t *testing.T) {
	r, _, emojis, _ := newEmojiTestRouter(t)
	emojis.EXPECT().EmojiDoc(gomock.Any(), "s1", "party").Return(nil, false, assert.AnError)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/emoji/v1/s1/party", nil))
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestEmojiGet_BlobMissing_404(t *testing.T) {
	r, _, emojis, _ := newEmojiTestRouter(t)
	emojis.EXPECT().EmojiDoc(gomock.Any(), "s1", "party").
		Return(&model.CustomEmoji{MinioKey: "emoji/party", ETag: "e1"}, true, nil)
	// fakeBlobStore with no seeded object returns errBlobNotFound.
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/emoji/v1/s1/party", nil))
	assert.Equal(t, http.StatusNotFound, w.Code)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=media-service`
Expected: FAIL — 404 (route missing) where 200/307/etc. expected.

- [ ] **Step 3: Implement**

Create `media-service/emoji_serve.go`:

```go
package main

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/emoji"
	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
)

// HandleEmojiGet serves a custom emoji image. The path carries the full
// identity (siteID, shortcode): a request for a remote site 307-redirects to
// the owning cluster, which always resolves locally — no redirect loop is
// possible, so no fwd guard is needed (unlike avatars, whose paths lack site
// identity). There is no generated default: unknown emoji are 404s.
func (h *handler) HandleEmojiGet(c *gin.Context) {
	ctx := c.Request.Context()
	c.Set("avatar_kind", "emoji")
	siteID := c.Param("siteID")

	shortcode, err := emoji.Canonicalize(c.Param("shortcode"))
	if err != nil {
		c.Set("avatar_outcome", "error")
		errhttp.Write(ctx, c, errcode.BadRequest("invalid emoji shortcode"))
		return
	}

	if siteID != h.cfg.SiteID {
		base := h.cfg.clusterBaseURL(siteID)
		if base == "" {
			c.Set("avatar_outcome", "error")
			errhttp.Write(ctx, c, errcode.NotFound("unknown site"))
			return
		}
		c.Set("avatar_outcome", "redirect")
		c.Redirect(http.StatusTemporaryRedirect, base+emojiImagePath(siteID, shortcode))
		return
	}

	e, found, err := h.emojis.EmojiDoc(ctx, siteID, shortcode)
	if err != nil {
		c.Set("avatar_outcome", "error")
		errhttp.Write(ctx, c, err)
		return
	}
	if !found {
		c.Set("avatar_outcome", "error")
		errhttp.Write(ctx, c, errcode.NotFound("emoji not found"))
		return
	}

	h.setImageCacheHeaders(c, e.ETag)
	if m := c.GetHeader("If-None-Match"); m != "" && m == e.ETag {
		c.Set("avatar_outcome", "304")
		c.Status(http.StatusNotModified)
		return
	}
	rc, info, err := h.blobs.Get(ctx, e.MinioKey)
	if errors.Is(err, errBlobNotFound) {
		// doc⟺object invariant briefly broken (concurrent delete): treat as gone.
		c.Set("avatar_outcome", "error")
		errhttp.Write(ctx, c, errcode.NotFound("emoji not found"))
		return
	}
	if err != nil {
		c.Set("avatar_outcome", "error")
		errhttp.Write(ctx, c, err)
		return
	}
	defer rc.Close()
	c.Set("avatar_outcome", "stream")
	c.DataFromReader(http.StatusOK, info.Size, info.ContentType, rc, nil)
}
```

In `media-service/routes.go` add (above the PUT):

```go
	r.GET("/emoji/v1/:siteID/:shortcode", h.HandleEmojiGet)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=media-service`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add media-service/emoji_serve.go media-service/routes.go media-service/emoji_serve_test.go
git commit -m "$(cat <<'EOF'
feat(media-service): custom emoji image endpoint with cross-site redirect

GET /emoji/v1/{siteID}/{shortcode}: local docs stream from MinIO with
ETag/304 and the shared image cache headers; remote siteIDs 307 to the
owning cluster (path carries full identity, so no fwd loop guard);
unknown site or emoji is 404 — emoji have no generated default.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_0172WdvqXaCgCyG5gZMdbqtd
EOF
)"
```

---

### Task 9: NATS list handler

**Files:**
- Create: `media-service/emoji_nats.go`
- Test: `media-service/emoji_nats_test.go`

**Interfaces:**
- Consumes: `emojiStore.ListEmojis`, `model.EmojiListResponse`/`EmojiEntry`, `natsrouter.Context` (`natsrouter.NewContext(map[string]string{...})` in tests).
- Produces: `(*handler).HandleEmojiList(c *natsrouter.Context) (*model.EmojiListResponse, error)`. Task 10 adds the delete handler to the same file; Task 11 registers both.

- [ ] **Step 1: Write the failing tests**

Create `media-service/emoji_nats_test.go`:

```go
package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
)

func newNATSTestHandler(t *testing.T) (*handler, *MockemojiStore, *fakeBlobStore) {
	t.Helper()
	ctrl := gomock.NewController(t)
	store := NewMockavatarStore(ctrl)
	emojis := NewMockemojiStore(ctrl)
	blobs := &fakeBlobStore{}
	h := newHandler(store, emojis, blobs, &config{
		SiteID:           "s1",
		EIDCacheCapacity: 1000,
		EIDCacheTTL:      time.Minute,
	})
	return h, emojis, blobs
}

func natsCtx() *natsrouter.Context {
	return natsrouter.NewContext(map[string]string{"account": "alice"})
}

func TestHandleEmojiList_Success(t *testing.T) {
	h, emojis, _ := newNATSTestHandler(t)
	emojis.EXPECT().ListEmojis(gomock.Any(), "s1").Return([]model.CustomEmoji{
		{Shortcode: "aaa", ImageURL: "/emoji/v1/s1/aaa", ContentType: "image/png", ETag: "e1", CreatedBy: "alice", UpdatedAt: 1000},
		{Shortcode: "bbb", ImageURL: "/emoji/v1/s1/bbb", ContentType: "image/gif", ETag: "e2", CreatedBy: "bob", UpdatedAt: 2000},
	}, nil)

	resp, err := h.HandleEmojiList(natsCtx())
	require.NoError(t, err)
	require.Len(t, resp.Emojis, 2)
	assert.Equal(t, model.EmojiEntry{
		Shortcode: "aaa", ImageURL: "/emoji/v1/s1/aaa", ContentType: "image/png",
		ETag: "e1", CreatedBy: "alice", UpdatedAt: 1000,
	}, resp.Emojis[0])
	assert.Equal(t, "bbb", resp.Emojis[1].Shortcode)
}

func TestHandleEmojiList_Empty(t *testing.T) {
	h, emojis, _ := newNATSTestHandler(t)
	emojis.EXPECT().ListEmojis(gomock.Any(), "s1").Return(nil, nil)

	resp, err := h.HandleEmojiList(natsCtx())
	require.NoError(t, err)
	assert.NotNil(t, resp.Emojis, "empty set must marshal as [], not null")
	assert.Empty(t, resp.Emojis)
}

func TestHandleEmojiList_StoreError(t *testing.T) {
	h, emojis, _ := newNATSTestHandler(t)
	emojis.EXPECT().ListEmojis(gomock.Any(), "s1").Return(nil, assert.AnError)

	_, err := h.HandleEmojiList(natsCtx())
	require.Error(t, err)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=media-service`
Expected: FAIL — compile error `h.HandleEmojiList undefined`.

- [ ] **Step 3: Implement**

Create `media-service/emoji_nats.go`:

```go
package main

import (
	"fmt"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
)

// HandleEmojiList replies with this site's full custom-emoji set. The subject
// carries the target siteID, so the supercluster routes each request to the
// owning site's media-service — this handler only ever serves its own site.
func (h *handler) HandleEmojiList(c *natsrouter.Context) (*model.EmojiListResponse, error) {
	list, err := h.emojis.ListEmojis(c, h.cfg.SiteID)
	if err != nil {
		return nil, fmt.Errorf("list custom emojis: %w", err)
	}
	entries := make([]model.EmojiEntry, 0, len(list))
	for _, e := range list {
		entries = append(entries, model.EmojiEntry{
			Shortcode:   e.Shortcode,
			ImageURL:    e.ImageURL,
			ContentType: e.ContentType,
			ETag:        e.ETag,
			CreatedBy:   e.CreatedBy,
			UpdatedAt:   e.UpdatedAt,
		})
	}
	return &model.EmojiListResponse{Emojis: entries}, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=media-service`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add media-service/emoji_nats.go media-service/emoji_nats_test.go
git commit -m "$(cat <<'EOF'
feat(media-service): NATS emoji list handler

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_0172WdvqXaCgCyG5gZMdbqtd
EOF
)"
```

---

### Task 10: NATS delete handler + registration helper

**Files:**
- Modify: `media-service/emoji_nats.go`
- Test: `media-service/emoji_nats_test.go`

**Interfaces:**
- Consumes: `emojiStore.DeleteEmoji`, `blobStore.Delete`, `emoji.Canonicalize`, `subject.EmojiListPattern`/`EmojiDeletePattern` (Task 3).
- Produces: `(*handler).HandleEmojiDelete(c *natsrouter.Context, req model.EmojiDeleteRequest) (*model.EmojiDeleteResponse, error)`; `registerEmojiNATS(r *natsrouter.Router, h *handler, siteID string)`. Task 11 calls `registerEmojiNATS`.

- [ ] **Step 1: Write the failing tests**

Append to `media-service/emoji_nats_test.go`:

```go
func TestHandleEmojiDelete_Success_RemovesDocThenBlob(t *testing.T) {
	h, emojis, blobs := newNATSTestHandler(t)
	blobs.objects = map[string][]byte{"emoji/party": []byte("x")}
	blobs.info = map[string]blobInfo{"emoji/party": {}}
	emojis.EXPECT().DeleteEmoji(gomock.Any(), "s1", "party").Return("emoji/party", true, nil)

	resp, err := h.HandleEmojiDelete(natsCtx(), model.EmojiDeleteRequest{Shortcode: "party"})
	require.NoError(t, err)
	assert.Equal(t, &model.EmojiDeleteResponse{Shortcode: "party", Deleted: true}, resp)
	_, ok := blobs.objects["emoji/party"]
	assert.False(t, ok, "blob removed after the doc")
}

func TestHandleEmojiDelete_NotFound(t *testing.T) {
	h, emojis, _ := newNATSTestHandler(t)
	emojis.EXPECT().DeleteEmoji(gomock.Any(), "s1", "nope").Return("", false, nil)

	_, err := h.HandleEmojiDelete(natsCtx(), model.EmojiDeleteRequest{Shortcode: "nope"})
	require.Error(t, err)
	assert.ErrorContains(t, err, "emoji not found")
}

func TestHandleEmojiDelete_InvalidShortcode(t *testing.T) {
	h, _, _ := newNATSTestHandler(t)

	_, err := h.HandleEmojiDelete(natsCtx(), model.EmojiDeleteRequest{Shortcode: "Bad Name"})
	require.Error(t, err)
	assert.ErrorContains(t, err, "invalid emoji shortcode")
}

func TestHandleEmojiDelete_StoreError(t *testing.T) {
	h, emojis, _ := newNATSTestHandler(t)
	emojis.EXPECT().DeleteEmoji(gomock.Any(), "s1", "party").Return("", false, assert.AnError)

	_, err := h.HandleEmojiDelete(natsCtx(), model.EmojiDeleteRequest{Shortcode: "party"})
	require.Error(t, err)
}

func TestHandleEmojiDelete_BlobDeleteFailure_StillSucceeds(t *testing.T) {
	h, emojis, blobs := newNATSTestHandler(t)
	blobs.deleteErr = assert.AnError
	emojis.EXPECT().DeleteEmoji(gomock.Any(), "s1", "party").Return("emoji/party", true, nil)

	resp, err := h.HandleEmojiDelete(natsCtx(), model.EmojiDeleteRequest{Shortcode: "party"})
	require.NoError(t, err)
	assert.True(t, resp.Deleted)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=media-service`
Expected: FAIL — compile error `h.HandleEmojiDelete undefined`.

- [ ] **Step 3: Implement**

Append to `media-service/emoji_nats.go` (add `"log/slog"`, `"github.com/hmchangw/chat/pkg/emoji"`, `"github.com/hmchangw/chat/pkg/errcode"`, `"github.com/hmchangw/chat/pkg/subject"` to its imports):

```go
// HandleEmojiDelete removes one custom emoji. Anyone may delete (v1); the
// authenticated caller comes from the JWT-enforced {account} subject token.
func (h *handler) HandleEmojiDelete(c *natsrouter.Context, req model.EmojiDeleteRequest) (*model.EmojiDeleteResponse, error) {
	shortcode, err := emoji.Canonicalize(req.Shortcode)
	if err != nil {
		return nil, errcode.BadRequest("invalid emoji shortcode")
	}

	// Doc first: once it is gone the emoji is invisible everywhere; the blob
	// delete below is best-effort because an orphaned object is unreachable.
	minioKey, found, err := h.emojis.DeleteEmoji(c, h.cfg.SiteID, shortcode)
	if err != nil {
		return nil, fmt.Errorf("delete custom emoji: %w", err)
	}
	if !found {
		return nil, errcode.NotFound("emoji not found")
	}
	if err := h.blobs.Delete(c, minioKey); err != nil {
		slog.WarnContext(c, "emoji blob delete failed; doc already removed",
			"shortcode", shortcode, "key", minioKey, "error", err)
	}
	return &model.EmojiDeleteResponse{Shortcode: shortcode, Deleted: true}, nil
}

// registerEmojiNATS wires the emoji request-reply endpoints; panics on
// subscription failure (fatal at startup, matching natsrouter semantics).
func registerEmojiNATS(r *natsrouter.Router, h *handler, siteID string) {
	natsrouter.RegisterNoBody(r, subject.EmojiListPattern(siteID), h.HandleEmojiList)
	natsrouter.Register(r, subject.EmojiDeletePattern(siteID), h.HandleEmojiDelete)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=media-service`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add media-service/emoji_nats.go media-service/emoji_nats_test.go
git commit -m "$(cat <<'EOF'
feat(media-service): NATS emoji delete handler and RPC registration

Delete removes the custom_emojis doc first, then best-effort removes
the blob (a doc-less object is unreachable). registerEmojiNATS wires
list (RegisterNoBody) and delete (Register) on the site-baked patterns.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_0172WdvqXaCgCyG5gZMdbqtd
EOF
)"
```

---

### Task 11: main.go NATS wiring + deploy config

**Files:**
- Modify: `media-service/main.go`, `media-service/deploy/docker-compose.yml`

**Interfaces:**
- Consumes: `natsutil.Connect(url, credsFile)` → `*otelnats.Conn`; `natsrouter.New(nc, "media-service")` + `.Use(natsrouter.Recovery(), natsrouter.RequestID(), natsrouter.Logging())`; `registerEmojiNATS` (Task 10); `(*mongoStore).EnsureEmojiIndexes` (Task 6).
- Produces: a running HTTP+NATS media-service.

- [ ] **Step 1: Modify `media-service/main.go`**

Add imports `"github.com/hmchangw/chat/pkg/natsrouter"` and `"github.com/hmchangw/chat/pkg/natsutil"`. After the store is built (`store := newMongoStore(...)`), add:

```go
	if err := store.EnsureEmojiIndexes(ctx); err != nil {
		return fmt.Errorf("ensure emoji indexes: %w", err)
	}
```

After `blobs := newMinioBlobStore(...)` and before the Gin setup, add:

```go
	nc, err := natsutil.Connect(cfg.NatsURL, cfg.NatsCredsFile)
	if err != nil {
		return fmt.Errorf("connect nats: %w", err)
	}
```

Change the handler construction (already done in Task 7) and after it add:

```go
	router := natsrouter.New(nc, "media-service")
	router.Use(natsrouter.Recovery(), natsrouter.RequestID(), natsrouter.Logging())
	registerEmojiNATS(router, h, cfg.SiteID)
```

Replace the shutdown block with (ordering: drain in-flight NATS handlers → drain the connection → stop HTTP; Mongo disconnect stays in its existing `defer`):

```go
	go shutdown.Wait(ctx, 25*time.Second,
		func(ctx context.Context) error { return router.Shutdown(ctx) },
		func(_ context.Context) error { return nc.Drain() },
		func(ctx context.Context) error {
			slog.Info("shutting down media-service")
			return srv.Shutdown(ctx)
		},
	)
```

- [ ] **Step 2: Update `media-service/deploy/docker-compose.yml`**

Add to the `environment:` list:

```yaml
      - NATS_URL=nats://nats:4222
      - EMOJI_MAX_UPLOAD_BYTES=262144
      - EMOJI_MAX_DIMENSION=512
```

(No creds in local dev — `NATS_CREDS_FILE` stays unset, matching `natsutil.Connect`'s no-creds path.)

- [ ] **Step 3: Verify compilation and tests**

Run: `make build SERVICE=media-service` — expected: builds cleanly.
Run: `make test SERVICE=media-service` — expected: PASS (unit tests never exercise `run()`).
Run: `make lint` — expected: clean.

- [ ] **Step 4: Commit**

```bash
git add media-service/main.go media-service/deploy/docker-compose.yml
git commit -m "$(cat <<'EOF'
feat(media-service): wire NATS router and emoji index bootstrap

media-service becomes HTTP+NATS: natsutil.Connect (fail-fast), a
natsrouter on queue group media-service serving the emoji RPCs, and
shutdown ordered router.Shutdown -> nc.Drain -> srv.Shutdown.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_0172WdvqXaCgCyG5gZMdbqtd
EOF
)"
```

---

### Task 12: End-to-end integration tests (Mongo + MinIO + NATS)

**Files:**
- Modify: `media-service/integration_test.go` (TestMain prewarm), `media-service/emoji_integration_test.go`

**Interfaces:**
- Consumes: `testutil.NATS(t)`, `testutil.EnsureNATS`, `otelnats.Connect(url)` (`github.com/Marz32onE/instrumentation-go/otel-nats/otelnats` — the pattern used by `room-service/integration_test.go:1561`), `subject.EmojiList`/`EmojiDelete` concrete builders, `registerEmojiNATS`.

- [ ] **Step 1: Extend TestMain prewarm**

In `media-service/integration_test.go` change:

```go
func TestMain(m *testing.M) {
	testutil.RunTestsWithPrewarm(m, testutil.EnsureMongo, testutil.EnsureMinIO, testutil.EnsureNATS)
}
```

- [ ] **Step 2: Write the failing end-to-end test**

Append to `media-service/emoji_integration_test.go` (extend imports with `"encoding/json"`, `"time"`, `"github.com/Marz32onE/instrumentation-go/otel-nats/otelnats"`, `"github.com/hmchangw/chat/pkg/natsrouter"`, `"github.com/hmchangw/chat/pkg/subject"`):

```go
func TestEmojiNATS_EndToEnd(t *testing.T) {
	ctx := context.Background()
	db := testutil.MongoDB(t, "emojirpc")
	client, bucket := testutil.MinIO(t, "emojirpc")
	st := newMongoStore(db)
	require.NoError(t, st.EnsureEmojiIndexes(ctx))
	bs := newMinioBlobStore(client, bucket)

	nc, err := otelnats.Connect(testutil.NATS(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = nc.Drain() })

	h := newHandler(st, st, bs, &config{
		SiteID:           "s1",
		EIDCacheCapacity: 10,
		EIDCacheTTL:      time.Minute,
	})
	router := natsrouter.New(nc, "media-service-"+t.Name())
	router.Use(natsrouter.Recovery(), natsrouter.RequestID(), natsrouter.Logging())
	registerEmojiNATS(router, h, "s1")
	t.Cleanup(func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = router.Shutdown(sctx)
	})

	// Seed one emoji through the store (doc + blob).
	_, err = bs.Put(ctx, "emoji/party", bytesReader("GIF!"), 4, "image/gif")
	require.NoError(t, err)
	require.NoError(t, st.UpsertEmoji(ctx, emojiFixture("s1", "party", "alice", 1000)))

	// List over real NATS request-reply.
	msg, err := nc.Request(subject.EmojiList("alice", "s1"), nil, 5*time.Second)
	require.NoError(t, err)
	var list model.EmojiListResponse
	require.NoError(t, json.Unmarshal(msg.Data, &list))
	require.Len(t, list.Emojis, 1)
	assert.Equal(t, "party", list.Emojis[0].Shortcode)
	assert.Equal(t, "/emoji/v1/s1/party", list.Emojis[0].ImageURL)

	// Delete over real NATS request-reply.
	body, err := json.Marshal(model.EmojiDeleteRequest{Shortcode: "party"})
	require.NoError(t, err)
	msg, err = nc.Request(subject.EmojiDelete("alice", "s1"), body, 5*time.Second)
	require.NoError(t, err)
	var del model.EmojiDeleteResponse
	require.NoError(t, json.Unmarshal(msg.Data, &del))
	assert.True(t, del.Deleted)

	// Doc and blob are both gone.
	_, found, err := st.EmojiDoc(ctx, "s1", "party")
	require.NoError(t, err)
	assert.False(t, found)
	_, _, err = bs.Get(ctx, "emoji/party")
	assert.ErrorIs(t, err, errBlobNotFound)

	// Second delete returns the not_found envelope.
	msg, err = nc.Request(subject.EmojiDelete("alice", "s1"), body, 5*time.Second)
	require.NoError(t, err)
	assert.Contains(t, string(msg.Data), `"not_found"`)
}
```

- [ ] **Step 3: Run to verify state (new test must run and pass; if it fails, fix the wiring, not the test)**

Run: `make test-integration SERVICE=media-service`
Expected: PASS — including the pre-existing avatar integration tests.

- [ ] **Step 4: Commit**

```bash
git add media-service/integration_test.go media-service/emoji_integration_test.go
git commit -m "$(cat <<'EOF'
test(media-service): end-to-end emoji NATS request-reply integration test

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_0172WdvqXaCgCyG5gZMdbqtd
EOF
)"
```

---

### Task 13: docs/client-api.md

**Files:**
- Modify: `docs/client-api.md`

Four edits. Mirror surrounding formatting exactly (field tables, `#####` heading levels, `---` rules between endpoints).

- [ ] **Step 1: Add `### 3.5 media-service` after the end of `### 3.4 user-service` (before `## 4.`)**

```markdown
### 3.5 media-service

| RPC subject | Method |
|---|---|
| `chat.user.{account}.request.emoji.{siteID}.list` | [`emoji.list`](#emojilist--list-a-sites-custom-emoji) |
| `chat.user.{account}.request.emoji.{siteID}.delete` | [`emoji.delete`](#emojidelete--delete-a-custom-emoji) |

#### `emoji.list` — list a site's custom emoji

**Subject:** `chat.user.{account}.request.emoji.{siteID}.list`
**Reply subject:** auto-generated `_INBOX.>` (NATS request/reply)

**Auth:** the `{account}` in the subject is the authenticated identity. `{siteID}` is the **target site whose emoji set you want** — for a room's emoji picker, pass the room's origin `siteId` (a room's usable custom emoji are always its origin site's set; see [React to Message](#react-to-message)). The supercluster routes the request to that site's media-service.

##### Request body

None — send an empty payload.

##### Success response

| Field | Type | Notes |
|---|---|---|
| `emojis` | [EmojiEntry](#emojientry)[] | Sorted by `shortcode`. `[]` when the site has none. |

###### EmojiEntry

| Field | Type | Notes |
|---|---|---|
| `shortcode` | string | Bare shortcode (no colons), `^[a-z0-9_+-]{1,32}$`. |
| `imageUrl` | string | Relative serve path `/emoji/v1/{siteID}/{shortcode}` — resolve against your media-service base URL; append `?v={etag}` to cache-bust. |
| `contentType` | string | `image/png`, `image/jpeg`, or `image/gif` (GIFs may be animated). |
| `etag` | string | Storage ETag of the current image. |
| `createdBy` | string | Account that first uploaded the shortcode (audit; unauthenticated in v1). |
| `updatedAt` | number | Epoch ms (UTC) of the last upload. |

```json
{
  "emojis": [
    {
      "shortcode": "acme_party",
      "imageUrl": "/emoji/v1/site-a/acme_party",
      "contentType": "image/gif",
      "etag": "9a0364b9e99bb480dd25e1f0284c8555",
      "createdBy": "alice",
      "updatedAt": 1746518900000
    }
  ]
}
```

##### Error response

See [Error envelope](#6-error-envelope-reference).

| Code | Reason |
|---|---|
| `internal` | Store failure. |

##### Triggered events — success path

`None — reply only.`

---

#### `emoji.delete` — delete a custom emoji

**Subject:** `chat.user.{account}.request.emoji.{siteID}.delete`
**Reply subject:** auto-generated `_INBOX.>` (NATS request/reply)

**Auth:** the `{account}` in the subject is the authenticated identity. Any authenticated user may delete (v1). `{siteID}` targets the owning site.

##### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `shortcode` | string | yes | Bare shortcode of the emoji to delete. |

```json
{ "shortcode": "acme_party" }
```

##### Success response

| Field | Type | Notes |
|---|---|---|
| `shortcode` | string | Canonical (NFC) form of the deleted shortcode. |
| `deleted` | boolean | Always `true` on success. |

```json
{ "shortcode": "acme_party", "deleted": true }
```

Existing reactions that reference the deleted shortcode are not rewritten; the reaction validator stops accepting new uses within its cache TTL (≤ 60 s by default).

##### Error response

See [Error envelope](#6-error-envelope-reference).

| Code | Reason |
|---|---|
| `bad_request` | `shortcode` is missing or fails `^[a-z0-9_+-]{1,32}$` after NFC. |
| `not_found` | No custom emoji with that shortcode on this site. |
| `internal` | Store failure. |

##### Triggered events — success path

`None — reply only.`
```

- [ ] **Step 2: Add two REST endpoints to `## 7. Media Service` (after the `PUT /avatar/v1/bot/:botName` subsection, separated by `---`)**

```markdown
---

### GET /emoji/v1/:siteID/:shortcode

Serves a custom emoji image. The path carries the emoji's full identity — `(siteID, shortcode)` — because shortcodes are only unique per site. Clients build this URL from the room's origin `siteId` plus the shortcode (see [`emoji.list`](#emojilist--list-a-sites-custom-emoji)); append `?v={etag}` to cache-bust after re-uploads.

#### Decision logic

- `{siteID}` owned by another cluster → `307` redirect to that cluster's media-service, same path. The target always resolves locally (the path names it as owner), so there is no redirect loop and no `fwd` guard.
- `{siteID}` not in this cluster's domain map → `404`.
- Local and registered → streams the stored image with `ETag` / `Cache-Control` / `X-Content-Type-Options: nosniff`; honours `If-None-Match` with `304`.
- Local and unknown → `404`. Custom emoji have **no generated default** (unlike avatars).
- Malformed shortcode → `400`.

#### Response

`200` with the image bytes (`image/png`, `image/jpeg`, or `image/gif`), or `304`/`307`/`400`/`404` per above.

---

### PUT /emoji/v1/:siteID/:shortcode

Uploads (or replaces — PUT is an upsert) a custom emoji. v1: no auth; the optional `?uploader={account}` query parameter is recorded for audit only. `{siteID}` must be **this** cluster's site — it declares intent so a misrouted upload fails loudly instead of writing to the wrong site's set.

#### Request

Raw image bytes as the body. PNG, JPEG, or GIF (animated GIFs are stored and served verbatim). Limits (env-configurable): body ≤ `EMOJI_MAX_UPLOAD_BYTES` (default 256 KiB), width/height ≤ `EMOJI_MAX_DIMENSION` (default 512).

#### Response

##### Success response (`200`)

| Field | Type | Notes |
|---|---|---|
| `shortcode` | string | Canonical (NFC) shortcode. |
| `etag` | string | New storage ETag — use as the `?v=` cache-buster. |
| `contentType` | string | Detected type (`image/png`, `image/jpeg`, `image/gif`). |
| `size` | int | Stored size in bytes. |
| `updatedAt` | number | Epoch ms (UTC). |

```json
{
  "shortcode": "acme_party",
  "etag": "9a0364b9e99bb480dd25e1f0284c8555",
  "contentType": "image/gif",
  "size": 20480,
  "updatedAt": 1746518900000
}
```

##### Errors

| Status | Reason |
|---|---|
| `400` | Malformed shortcode; body not a valid PNG/JPEG/GIF; body or dimensions over the limits. |
| `400` + reason `emoji_shortcode_reserved` | Shortcode collides with a built-in standard emoji (would be permanently shadowed). |
| `409` + reason `emoji_wrong_cluster` | `{siteID}` is owned by another cluster; the message names the domain to retry against. |

A newly uploaded emoji becomes reactable once the reaction validator's per-site cache expires (≤ 60 s by default).
```

- [ ] **Step 3: Add the reasons to the §6 reason catalog table**

Append rows (columns: reason | category | where-emitted):

```markdown
| `emoji_wrong_cluster` | conflict | media-service `PUT /emoji/v1/…` (upload sent to a non-owning cluster) |
| `emoji_shortcode_reserved` | bad_request | media-service `PUT /emoji/v1/…` (shortcode collides with a built-in standard emoji) |
```

- [ ] **Step 4: Update the TOC**

Mirror the existing TOC entries: add `emoji.list` / `emoji.delete` under the §3 block (next to the other 3.x services) and `GET /emoji/v1/:siteID/:shortcode` / `PUT /emoji/v1/:siteID/:shortcode` under the §7 block, using the same auto-anchor link style as the neighbouring lines.

- [ ] **Step 5: Commit**

```bash
git add docs/client-api.md
git commit -m "$(cat <<'EOF'
docs(client-api): document custom emoji RPCs and REST endpoints

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_0172WdvqXaCgCyG5gZMdbqtd
EOF
)"
```

---

### Task 14: Final verification and push

**Files:** none new.

- [ ] **Step 1: Full lint + format check**

Run: `make fmt && make lint`
Expected: no diffs, no findings. Fix anything reported.

- [ ] **Step 2: Full unit test suite**

Run: `make test`
Expected: PASS across the repo (the pkg/ changes touch shared code — history-service tests must stay green).

- [ ] **Step 3: Coverage check on the changed packages**

Run:
```bash
go test -race -coverprofile=/tmp/cover.out ./media-service/... ./pkg/emoji/... ./pkg/model/... ./pkg/subject/... ./pkg/errcode/...
go tool cover -func=/tmp/cover.out | tail -20
```
Expected: media-service total ≥ 80%; emoji handlers ≥ 90%. If short, add unit cases for the uncovered branches (most likely `emoji_serve.go` error paths).

- [ ] **Step 4: Integration tests (if Docker available)**

Run: `make test-integration SERVICE=media-service`
Expected: PASS.

- [ ] **Step 5: SAST**

Run: `make sast`
Expected: clean at medium+. If gosec flags the image decode or file paths, evaluate: fix real findings; suppress genuine false positives with `// #nosec <RULE> -- reason` directly above the line.

- [ ] **Step 6: Push**

```bash
git push -u origin ds-feat/customized_emoji
```
Retry up to 4× with 2s/4s/8s/16s backoff on network failure only.

---

## Coverage of the spec (traceability)

| Spec section | Task(s) |
|---|---|
| §2 Data model (`CustomEmoji` extension, doc⟺object invariant) | 2, 6, 7, 10 |
| §3a Upload (validation chain, upsert, wrong-cluster, `?uploader=`) | 1, 4, 5, 7 |
| §3b Serve (ETag/304, cross-site 307, no default, unknown site 404) | 8 |
| §3c List (RegisterNoBody, wire fields, sorted, `[]`) | 2, 3, 9, 11 |
| §3d Delete (canonicalize, doc-first, best-effort blob, not_found) | 3, 6, 10, 11 |
| §4 Architecture (NATS wiring, shutdown order, store iface, config) | 5, 6, 11 |
| §5 Consistency & caching (validator TTL documented) | 13 |
| §6 Error reasons | 4, 13 |
| §7 Testing (unit matrix, integration e2e, pkg tests, coverage) | 1–2, 6–10, 12, 14 |
| §8 Documentation (`client-api.md`) | 13 |
