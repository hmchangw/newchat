# Spotlight Org Sync Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `spotlight-org` collection to `search-sync-worker` that consumes `hr.sync.{siteID}.employees.upsert` envelopes from `hr-syncer` and maintains the `spotlightorg-{siteID}` ES index keyed by `sectId` via doc-merge upserts.

**Architecture:** New `spotlightOrgCollection` implementing the existing `Collection` interface, alongside a shared `HRSyncEvent` envelope in `pkg/model`, a new `HR_SYNC_{siteID}` stream definition, new subject builders, shared template helpers (`indexTopology`, `customAnalyzerSettings`), and a worker-wide `DEV_MODE` toggle reused from the project-wide env var already used by `auth-service` and `chat-frontend`.

**Tech Stack:** Go 1.25, NATS JetStream (`nats.go/jetstream`), Elasticsearch 8.11, `compress/gzip`, `caarlos0/env` for config, `stretchr/testify` for assertions, `testcontainers-go` for integration tests.

**Spec:** `docs/superpowers/specs/2026-05-11-spotlight-org-sync-design.md`

---

## File Structure

**New files**
- `pkg/model/employee.go` — minimal `Employee` struct with `SectID` + the nine org fields. Owns the consumer-side contract; Mat can extend with non-org fields.
- `pkg/model/hrsync.go` — `HRSyncEvent` envelope.
- `search-sync-worker/spotlight_org.go` — new collection, projection struct, BuildAction, template body.
- `search-sync-worker/spotlight_org_test.go` — unit tests.

**Modified files**
- `pkg/stream/stream.go` — add `HRSync(siteID)`.
- `pkg/stream/stream_test.go` — assert `HRSync`.
- `pkg/subject/subject.go` — add `HRSyncEmployeesUpsert`, `HRSyncUsersUpsert`.
- `pkg/subject/subject_test.go` — assert new subjects.
- `pkg/model/model_test.go` — roundtrip tests for `Employee` + `HRSyncEvent`.
- `search-sync-worker/template.go` — add `indexTopology` + `customAnalyzerSettings` helpers.
- `search-sync-worker/messages.go` — thread `devMode` into `messageCollection` + `messageTemplateBody`.
- `search-sync-worker/messages_test.go` — `devMode=true` subtest.
- `search-sync-worker/spotlight.go` — thread `devMode`, switch to `customAnalyzerSettings`, add `token_chars`, drop stale comment.
- `search-sync-worker/spotlight_test.go` — update constructor calls, add `devMode=true` subtest.
- `search-sync-worker/user_room.go` — thread `devMode`.
- `search-sync-worker/user_room_test.go` — update constructor calls, add `devMode=true` subtest.
- `search-sync-worker/main.go` — new config fields, default index name, skip HR_SYNC bootstrap, wire new collection.
- `search-sync-worker/integration_test.go` — add `TestSearchSync_SpotlightOrg_Integration`.
- `search-sync-worker/consumer_config_test.go` — extend if it enumerates collections.
- `search-sync-worker/deploy/docker-compose.yml` — `DEV_MODE=${DEV_MODE:-true}`, optional `SPOTLIGHT_ORG_INDEX`.

---

## Task 1 (REMOVED): ~~Add `model.Employee`~~

**Status: superseded.** The original Task 1 added a `pkg/model.Employee`
struct, but this would conflict on merge with the internal repo's
already-existing `pkg/model/employee.go` (which defines a fuller
`Employee` plus an `Org` type for that repo's other consumers).

Replacement: the consumer-side projection moves into
`search-sync-worker/spotlight_org.go` as `SpotlightOrgIndex` (defined
in Task 11). One struct serves three roles — unmarshal target for the
wire payload, document body on ES write, and source of truth for the
ES mapping. No public `pkg/model.Employee` is introduced in this PR.

Commits `d9199cf` and `bd92d63` have been reverted (in a single combined
revert commit). Skip Task 1 when executing.

---

## Task 2: Add `model.HRSyncEvent` envelope

Mirrors `OutboxEvent` in shape: small fixed metadata + opaque payload bytes the consumer types per subject. The `Gzip` flag lets dev tooling skip compression without changing the consumer.

**Files:**
- Create: `pkg/model/hrsync.go`
- Modify: `pkg/model/model_test.go`

- [ ] **Step 1: Write the failing test**

Append to `pkg/model/model_test.go`:

```go
func TestHRSyncEventJSON(t *testing.T) {
	src := model.HRSyncEvent{
		Timestamp: 1735689600000,
		BatchID:   "0192a4f7-8c2d-7c9a-abcd-e0123456789f",
		Gzip:      true,
		Payload:   json.RawMessage(`[{"sectId":"S001"}]`),
	}
	var dst model.HRSyncEvent
	roundTrip(t, &src, &dst)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=pkg/model 2>&1 | tail -10`
Expected: FAIL with `undefined: model.HRSyncEvent`.

- [ ] **Step 3: Create `pkg/model/hrsync.go`**

```go
package model

import "encoding/json"

// HRSyncEvent is the envelope `hr-syncer` publishes on every
// `hr.sync.*` subject. Mirrors OutboxEvent: small fixed metadata plus
// an opaque payload the consumer types per-subject.
//
// Payload typing:
//   - `hr.sync.{siteID}.employees.upsert` → []Employee
//   - `hr.sync.{siteID}.users.upsert`     → []User (future)
//
// Gzip lets dev tooling publish uncompressed without changing the
// consumer. Timestamp is set at the publish site in milliseconds since
// epoch; consumers reject `Timestamp <= 0`. BatchID is a UUIDv7 used
// for end-to-end tracing of one cron run.
type HRSyncEvent struct {
	Timestamp int64           `json:"timestamp"`
	BatchID   string          `json:"batchId"`
	Gzip      bool            `json:"gzip"`
	Payload   json.RawMessage `json:"payload"`
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=pkg/model 2>&1 | tail -10`
Expected: PASS.

- [ ] **Step 5: Lint and commit**

```bash
make lint
git add pkg/model/hrsync.go pkg/model/model_test.go
git commit -m "feat(model): add HRSyncEvent envelope for hr-syncer publishes"
```

---

## Task 3: Add `stream.HRSync`

Site-scoped stream definition. `search-sync-worker` consumes from this stream but does NOT own its schema — `hr-syncer` does. Following the same pattern as `INBOX_{siteID}` (owned by `inbox-worker`).

**Files:**
- Modify: `pkg/stream/stream.go`
- Modify: `pkg/stream/stream_test.go`

- [ ] **Step 1: Write the failing test**

Append the row in the `TestStreamConfigs` table in `pkg/stream/stream_test.go`:

```go
{"HRSync", stream.HRSync(siteID), "HR_SYNC_site-a", "hr.sync.site-a.>"},
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=pkg/stream 2>&1 | tail -10`
Expected: FAIL with `undefined: stream.HRSync`.

- [ ] **Step 3: Add `HRSync` to `pkg/stream/stream.go`**

Append after the `Inbox` function:

```go
// HRSync returns the canonical config for the `HR_SYNC_{siteID}` stream
// that carries HR account sync events published daily by `hr-syncer`
// (e.g., `hr.sync.{siteID}.employees.upsert`,
// `hr.sync.{siteID}.users.upsert`). Schema is owned by `hr-syncer`;
// consumers like `search-sync-worker` must skip this stream in their
// bootstrap loop the same way they skip INBOX (owned by inbox-worker).
func HRSync(siteID string) Config {
	return Config{
		Name:     fmt.Sprintf("HR_SYNC_%s", siteID),
		Subjects: []string{fmt.Sprintf("hr.sync.%s.>", siteID)},
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=pkg/stream 2>&1 | tail -10`
Expected: PASS for the new `HRSync` subtest.

- [ ] **Step 5: Lint and commit**

```bash
make lint
git add pkg/stream/stream.go pkg/stream/stream_test.go
git commit -m "feat(stream): add HR_SYNC stream definition"
```

---

## Task 4: Add HR sync subject builders

Two builders so the future `users.upsert` consumer can reuse the same pattern. Filter subjects are passed to `jetstream.ConsumerConfig.FilterSubjects`.

**Files:**
- Modify: `pkg/subject/subject.go`
- Modify: `pkg/subject/subject_test.go`

- [ ] **Step 1: Write the failing test**

Append to `pkg/subject/subject_test.go`:

```go
func TestHRSyncEmployeesUpsert(t *testing.T) {
	got := subject.HRSyncEmployeesUpsert("site-a")
	assert.Equal(t, "hr.sync.site-a.employees.upsert", got)
}

func TestHRSyncUsersUpsert(t *testing.T) {
	got := subject.HRSyncUsersUpsert("site-a")
	assert.Equal(t, "hr.sync.site-a.users.upsert", got)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=pkg/subject 2>&1 | tail -10`
Expected: FAIL with `undefined: subject.HRSyncEmployeesUpsert`.

- [ ] **Step 3: Add builders to `pkg/subject/subject.go`**

Append after the `MsgCanonicalDeleted` function:

```go
// HRSyncEmployeesUpsert returns the subject for batch HR-account
// upsert events. Consumed by `search-sync-worker`'s spotlight-org
// collection.
func HRSyncEmployeesUpsert(siteID string) string {
	return fmt.Sprintf("hr.sync.%s.employees.upsert", siteID)
}

// HRSyncUsersUpsert returns the subject for batch HR-user upsert
// events. Consumed by a separate service that maintains the `users`
// MongoDB collection (out of scope for this worker).
func HRSyncUsersUpsert(siteID string) string {
	return fmt.Sprintf("hr.sync.%s.users.upsert", siteID)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=pkg/subject 2>&1 | tail -10`
Expected: PASS.

- [ ] **Step 5: Lint and commit**

```bash
make lint
git add pkg/subject/subject.go pkg/subject/subject_test.go
git commit -m "feat(subject): add HR sync subject builders"
```

---

## Task 5: Add `indexTopology` template helper

Shared helper so every template's dev-mode toggle is centralized. Prod values stay configurable per collection; dev mode collapses uniformly to 1/0.

**Files:**
- Modify: `search-sync-worker/template.go`
- Create: `search-sync-worker/template_test.go` (new — `template.go` doesn't have its own test file today; assertions for the existing reflect helper are split across `*_test.go` files)

- [ ] **Step 1: Write the failing test**

Create `search-sync-worker/template_test.go`:

```go
package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIndexTopology_Prod(t *testing.T) {
	shards, replicas := indexTopology(4, 2, false)
	assert.Equal(t, 4, shards)
	assert.Equal(t, 2, replicas)
}

func TestIndexTopology_Dev(t *testing.T) {
	// Dev collapses every input to 1/0 regardless of prod values.
	shards, replicas := indexTopology(4, 2, true)
	assert.Equal(t, 1, shards)
	assert.Equal(t, 0, replicas)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=search-sync-worker 2>&1 | tail -10`
Expected: FAIL with `undefined: indexTopology`.

- [ ] **Step 3: Add `indexTopology` to `search-sync-worker/template.go`**

Append to the end of `template.go`:

```go
// indexTopology returns the (shards, replicas) pair an ES index
// template should declare. Prod values vary by collection — pass them
// in. In dev mode every template collapses to 1/0 so a single
// DEV_MODE toggle gives every index a fast local footprint without
// per-template env vars.
func indexTopology(prodShards, prodReplicas int, devMode bool) (int, int) {
	if devMode {
		return 1, 0
	}
	return prodShards, prodReplicas
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=search-sync-worker 2>&1 | tail -10`
Expected: PASS.

- [ ] **Step 5: Lint and commit**

```bash
make lint
git add search-sync-worker/template.go search-sync-worker/template_test.go
git commit -m "feat(search-sync-worker): add indexTopology helper for DEV_MODE"
```

---

## Task 6: Add `customAnalyzerSettings` template helper

Shared analyzer block both spotlight templates use. Includes the `token_chars` setting on the whitespace tokenizer (verified to be accepted on ES 8.11).

**Files:**
- Modify: `search-sync-worker/template.go`
- Modify: `search-sync-worker/template_test.go`

- [ ] **Step 1: Write the failing test**

Append to `search-sync-worker/template_test.go`:

```go
func TestCustomAnalyzerSettings_Shape(t *testing.T) {
	got := customAnalyzerSettings()

	analyzer := got["analyzer"].(map[string]any)
	custom := analyzer["custom_analyzer"].(map[string]any)
	assert.Equal(t, "custom", custom["type"])
	assert.Equal(t, "custom_tokenizer", custom["tokenizer"])
	assert.Equal(t, []string{"lowercase"}, custom["filter"])

	tokenizer := got["tokenizer"].(map[string]any)
	tok := tokenizer["custom_tokenizer"].(map[string]any)
	assert.Equal(t, "whitespace", tok["type"])
	assert.Equal(t, []string{"letter", "digit", "punctuation", "symbol"}, tok["token_chars"])
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=search-sync-worker 2>&1 | tail -10`
Expected: FAIL with `undefined: customAnalyzerSettings`.

- [ ] **Step 3: Add `customAnalyzerSettings` to `search-sync-worker/template.go`**

Append after `indexTopology`:

```go
// customAnalyzerSettings returns the `analysis` block shared by the
// spotlight (room-typeahead) and spotlight-org (section-typeahead)
// templates. A whitespace tokenizer with a permissive `token_chars`
// set (letter, digit, punctuation, symbol) feeds a lowercase-folding
// `custom_analyzer`. Returning a fresh map per call prevents aliasing
// if a caller mutates the result.
func customAnalyzerSettings() map[string]any {
	return map[string]any{
		"analyzer": map[string]any{
			"custom_analyzer": map[string]any{
				"type":      "custom",
				"tokenizer": "custom_tokenizer",
				"filter":    []string{"lowercase"},
			},
		},
		"tokenizer": map[string]any{
			"custom_tokenizer": map[string]any{
				"type":        "whitespace",
				"token_chars": []string{"letter", "digit", "punctuation", "symbol"},
			},
		},
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=search-sync-worker 2>&1 | tail -10`
Expected: PASS.

- [ ] **Step 5: Lint and commit**

```bash
make lint
git add search-sync-worker/template.go search-sync-worker/template_test.go
git commit -m "feat(search-sync-worker): add customAnalyzerSettings helper"
```

---

## Task 7: Refactor `spotlightTemplateBody` to use shared analyzer + add `token_chars`

The existing spotlight template inlines its analyzer block and has a stale comment claiming `token_chars` is rejected by ES. Per the user's verification on ES 8.11, drop the comment and route through `customAnalyzerSettings()`.

**Files:**
- Modify: `search-sync-worker/spotlight.go:130-168`
- Modify: `search-sync-worker/spotlight_test.go` (no behavior change expected — existing assertions should still pass)

- [ ] **Step 1: Write the failing test**

Append to `search-sync-worker/spotlight_test.go`:

```go
// TestSpotlightTemplateBody_HasTokenChars locks in that the spotlight
// template adopts the shared analyzer (which carries token_chars).
// Before this change the spotlight tokenizer had no token_chars set.
func TestSpotlightTemplateBody_HasTokenChars(t *testing.T) {
	body := spotlightTemplateBody("spotlight-site-a-v1-chat")
	var parsed map[string]any
	require.NoError(t, json.Unmarshal(body, &parsed))

	tmpl := parsed["template"].(map[string]any)
	settings := tmpl["settings"].(map[string]any)
	analysis := settings["analysis"].(map[string]any)
	tokenizer := analysis["tokenizer"].(map[string]any)
	custom := tokenizer["custom_tokenizer"].(map[string]any)
	tokenChars, ok := custom["token_chars"].([]any)
	require.True(t, ok, "custom_tokenizer must declare token_chars after the refactor")
	assert.ElementsMatch(t, []any{"letter", "digit", "punctuation", "symbol"}, tokenChars)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=search-sync-worker -run TestSpotlightTemplateBody_HasTokenChars 2>&1 | tail -10`
Expected: FAIL — current template doesn't declare `token_chars`.

- [ ] **Step 3: Refactor `spotlightTemplateBody`**

Replace the body of `spotlightTemplateBody` in `search-sync-worker/spotlight.go` (lines 130-168):

```go
// spotlightTemplateBody builds the ES index template for the spotlight
// (room-typeahead) collection. Analyzer config is shared with the
// spotlight-org template via customAnalyzerSettings(). The
// `index_patterns` field is set to the exact configured index name so
// a custom SPOTLIGHT_INDEX value still receives the correct mapping.
func spotlightTemplateBody(indexName string) json.RawMessage {
	tmpl := map[string]any{
		"index_patterns": []string{indexName},
		"template": map[string]any{
			"settings": map[string]any{
				"index": map[string]any{
					"number_of_shards":   3,
					"number_of_replicas": 1,
				},
				"analysis": customAnalyzerSettings(),
			},
			"mappings": map[string]any{
				"dynamic":    false,
				"properties": esPropertiesFromStruct[SpotlightSearchIndex](),
			},
		},
	}
	data, _ := json.Marshal(tmpl)
	return data
}
```

(Note: `devMode` is deferred to Task 10 — this task is purely the analyzer extraction so the diff stays reviewable.)

- [ ] **Step 4: Run all spotlight tests to verify nothing broke**

Run: `make test SERVICE=search-sync-worker 2>&1 | tail -20`
Expected: PASS — including the new `TestSpotlightTemplateBody_HasTokenChars` and all existing spotlight tests.

- [ ] **Step 5: Lint and commit**

```bash
make lint
git add search-sync-worker/spotlight.go search-sync-worker/spotlight_test.go
git commit -m "refactor(search-sync-worker): spotlight template uses shared analyzer"
```

---

## Task 8: Thread `devMode` into `messageCollection`

Adds `devMode bool` to the constructor and template-body signature. Existing prod behavior (4 shards, 2 replicas) preserved when `devMode=false`.

**Files:**
- Modify: `search-sync-worker/messages.go`
- Modify: `search-sync-worker/messages_test.go`

- [ ] **Step 1: Write the failing test**

Append to `search-sync-worker/messages_test.go`:

```go
func TestMessageTemplateBody_DevMode(t *testing.T) {
	t.Run("prod", func(t *testing.T) {
		body := messageTemplateBody("messages-site-a-v1", false)
		var parsed map[string]any
		require.NoError(t, json.Unmarshal(body, &parsed))
		idx := parsed["template"].(map[string]any)["settings"].(map[string]any)["index"].(map[string]any)
		assert.Equal(t, float64(4), idx["number_of_shards"])
		assert.Equal(t, float64(2), idx["number_of_replicas"])
	})
	t.Run("dev", func(t *testing.T) {
		body := messageTemplateBody("messages-site-a-v1", true)
		var parsed map[string]any
		require.NoError(t, json.Unmarshal(body, &parsed))
		idx := parsed["template"].(map[string]any)["settings"].(map[string]any)["index"].(map[string]any)
		assert.Equal(t, float64(1), idx["number_of_shards"])
		assert.Equal(t, float64(0), idx["number_of_replicas"])
	})
}
```

Also update every existing call site in `messages_test.go` from `messageTemplateBody(prefix)` → `messageTemplateBody(prefix, false)`, and from `newMessageCollection(prefix)` → `newMessageCollection(prefix, false)`.

- [ ] **Step 2: Run test to verify build error / failures**

Run: `make test SERVICE=search-sync-worker 2>&1 | tail -20`
Expected: build error — `messageTemplateBody` is currently 1-arg.

- [ ] **Step 3: Update `search-sync-worker/messages.go`**

Modify the struct, constructor, `TemplateBody`, and `messageTemplateBody`:

```go
type messageCollection struct {
	indexPrefix string
	devMode     bool
}

func newMessageCollection(indexPrefix string, devMode bool) *messageCollection {
	return &messageCollection{indexPrefix: indexPrefix, devMode: devMode}
}
```

```go
func (c *messageCollection) TemplateBody() json.RawMessage {
	return messageTemplateBody(c.indexPrefix, c.devMode)
}
```

```go
func messageTemplateBody(prefix string, devMode bool) json.RawMessage {
	shards, replicas := indexTopology(4, 2, devMode)
	tmpl := map[string]any{
		"index_patterns": []string{fmt.Sprintf("%s-*", prefix)},
		"template": map[string]any{
			"settings": map[string]any{
				"index": map[string]any{
					"number_of_shards":   shards,
					"number_of_replicas": replicas,
					"refresh_interval":   "30s",
				},
				"analysis": map[string]any{
					"analyzer": map[string]any{
						"custom_analyzer": map[string]any{
							"type":        "custom",
							"tokenizer":   "underscore_preserving",
							"filter":      []string{"underscore_subword", "cjk_bigram", "lowercase"},
							"char_filter": []string{"html_strip"},
						},
					},
					"tokenizer": map[string]any{
						"underscore_preserving": map[string]any{
							"type":    "pattern",
							"pattern": `[\s,;!?()\[\]{}"'<>]+`,
						},
					},
					"filter": map[string]any{
						"underscore_subword": map[string]any{
							"type":                 "word_delimiter_graph",
							"split_on_case_change": false,
							"split_on_numerics":    false,
							"preserve_original":    true,
						},
					},
				},
			},
			"mappings": map[string]any{
				"dynamic":    false,
				"properties": messageTemplateProperties(),
			},
		},
	}
	data, _ := json.Marshal(tmpl)
	return data
}
```

Also update `main.go` temporarily to keep the build green: `newMessageCollection(cfg.MsgIndexPrefix, false)`. The proper wiring (passing `cfg.DevMode`) is Task 14.

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=search-sync-worker 2>&1 | tail -20`
Expected: PASS — including the new dev-mode subtest.

- [ ] **Step 5: Lint and commit**

```bash
make lint
git add search-sync-worker/messages.go search-sync-worker/messages_test.go search-sync-worker/main.go
git commit -m "feat(search-sync-worker): thread DEV_MODE into message template"
```

---

## Task 9: Thread `devMode` into `spotlightCollection`

Same mechanical refactor as Task 8.

**Files:**
- Modify: `search-sync-worker/spotlight.go`
- Modify: `search-sync-worker/spotlight_test.go`

- [ ] **Step 1: Write the failing test**

Append to `search-sync-worker/spotlight_test.go`:

```go
func TestSpotlightTemplateBody_DevMode(t *testing.T) {
	t.Run("prod", func(t *testing.T) {
		body := spotlightTemplateBody("spotlight-site-a-v1-chat", false)
		var parsed map[string]any
		require.NoError(t, json.Unmarshal(body, &parsed))
		idx := parsed["template"].(map[string]any)["settings"].(map[string]any)["index"].(map[string]any)
		assert.Equal(t, float64(3), idx["number_of_shards"])
		assert.Equal(t, float64(1), idx["number_of_replicas"])
	})
	t.Run("dev", func(t *testing.T) {
		body := spotlightTemplateBody("spotlight-site-a-v1-chat", true)
		var parsed map[string]any
		require.NoError(t, json.Unmarshal(body, &parsed))
		idx := parsed["template"].(map[string]any)["settings"].(map[string]any)["index"].(map[string]any)
		assert.Equal(t, float64(1), idx["number_of_shards"])
		assert.Equal(t, float64(0), idx["number_of_replicas"])
	})
}
```

Update every call to `newSpotlightCollection(idx)` → `newSpotlightCollection(idx, false)`, and `spotlightTemplateBody(idx)` → `spotlightTemplateBody(idx, false)` in this file and in the Task 7 test you just added.

- [ ] **Step 2: Run test to verify build error**

Run: `make test SERVICE=search-sync-worker 2>&1 | tail -20`
Expected: build error.

- [ ] **Step 3: Update `search-sync-worker/spotlight.go`**

```go
type spotlightCollection struct {
	inboxMemberCollection
	indexName string
	devMode   bool
}

func newSpotlightCollection(indexName string, devMode bool) *spotlightCollection {
	return &spotlightCollection{indexName: indexName, devMode: devMode}
}
```

```go
func (c *spotlightCollection) TemplateBody() json.RawMessage {
	return spotlightTemplateBody(c.indexName, c.devMode)
}
```

```go
func spotlightTemplateBody(indexName string, devMode bool) json.RawMessage {
	shards, replicas := indexTopology(3, 1, devMode)
	tmpl := map[string]any{
		"index_patterns": []string{indexName},
		"template": map[string]any{
			"settings": map[string]any{
				"index": map[string]any{
					"number_of_shards":   shards,
					"number_of_replicas": replicas,
				},
				"analysis": customAnalyzerSettings(),
			},
			"mappings": map[string]any{
				"dynamic":    false,
				"properties": esPropertiesFromStruct[SpotlightSearchIndex](),
			},
		},
	}
	data, _ := json.Marshal(tmpl)
	return data
}
```

Update `main.go`: `newSpotlightCollection(cfg.SpotlightIndex, false)`.

- [ ] **Step 4: Run tests**

Run: `make test SERVICE=search-sync-worker 2>&1 | tail -20`
Expected: PASS.

- [ ] **Step 5: Lint and commit**

```bash
make lint
git add search-sync-worker/spotlight.go search-sync-worker/spotlight_test.go search-sync-worker/main.go
git commit -m "feat(search-sync-worker): thread DEV_MODE into spotlight template"
```

---

## Task 10: Thread `devMode` into `userRoomCollection`

Same mechanical refactor.

**Files:**
- Modify: `search-sync-worker/user_room.go`
- Modify: `search-sync-worker/user_room_test.go`

- [ ] **Step 1: Write the failing test**

Append to `search-sync-worker/user_room_test.go`:

```go
func TestUserRoomTemplateBody_DevMode(t *testing.T) {
	t.Run("prod", func(t *testing.T) {
		body := userRoomTemplateBody("user-room-site-a", false)
		var parsed map[string]any
		require.NoError(t, json.Unmarshal(body, &parsed))
		idx := parsed["template"].(map[string]any)["settings"].(map[string]any)["index"].(map[string]any)
		assert.Equal(t, float64(1), idx["number_of_shards"])
		assert.Equal(t, float64(1), idx["number_of_replicas"])
	})
	t.Run("dev", func(t *testing.T) {
		body := userRoomTemplateBody("user-room-site-a", true)
		var parsed map[string]any
		require.NoError(t, json.Unmarshal(body, &parsed))
		idx := parsed["template"].(map[string]any)["settings"].(map[string]any)["index"].(map[string]any)
		assert.Equal(t, float64(1), idx["number_of_shards"])
		assert.Equal(t, float64(0), idx["number_of_replicas"])
	})
}
```

Update every `newUserRoomCollection(idx)` → `newUserRoomCollection(idx, false)` and `userRoomTemplateBody(idx)` → `userRoomTemplateBody(idx, false)`.

- [ ] **Step 2: Run test**

Run: `make test SERVICE=search-sync-worker 2>&1 | tail -20`
Expected: build error.

- [ ] **Step 3: Update `search-sync-worker/user_room.go`**

```go
type userRoomCollection struct {
	inboxMemberCollection
	indexName string
	devMode   bool
}

func newUserRoomCollection(indexName string, devMode bool) *userRoomCollection {
	return &userRoomCollection{indexName: indexName, devMode: devMode}
}
```

```go
func (c *userRoomCollection) TemplateBody() json.RawMessage {
	return userRoomTemplateBody(c.indexName, c.devMode)
}
```

```go
func userRoomTemplateBody(indexName string, devMode bool) json.RawMessage {
	shards, replicas := indexTopology(1, 1, devMode)
	tmpl := map[string]any{
		"index_patterns": []string{indexName},
		"template": map[string]any{
			"settings": map[string]any{
				"index": map[string]any{
					"number_of_shards":   shards,
					"number_of_replicas": replicas,
				},
			},
			"mappings": map[string]any{
				"dynamic": false,
				"properties": map[string]any{
					"userAccount": map[string]any{"type": "keyword"},
					"rooms": map[string]any{
						"type": "text",
						"fields": map[string]any{
							"keyword": map[string]any{"type": "keyword", "ignore_above": 256},
						},
					},
					"restrictedRooms": map[string]any{"type": "flattened"},
					"roomTimestamps":  map[string]any{"type": "flattened"},
					"createdAt":       map[string]any{"type": "date"},
					"updatedAt":       map[string]any{"type": "date"},
				},
			},
		},
	}
	data, _ := json.Marshal(tmpl)
	return data
}
```

Update `main.go`: `newUserRoomCollection(cfg.UserRoomIndex, false)`.

- [ ] **Step 4: Run tests**

Run: `make test SERVICE=search-sync-worker 2>&1 | tail -20`
Expected: PASS.

- [ ] **Step 5: Lint and commit**

```bash
make lint
git add search-sync-worker/user_room.go search-sync-worker/user_room_test.go search-sync-worker/main.go
git commit -m "feat(search-sync-worker): thread DEV_MODE into user-room template"
```

---

## Task 11: Scaffold `spotlightOrgCollection` (metadata, no BuildAction yet)

Create the type, constructor, and the four metadata methods (`StreamConfig`, `ConsumerName`, `FilterSubjects`, `TemplateName`, `TemplateBody`). `BuildAction` is stubbed to return an error so the build succeeds; real logic lands in Tasks 12-13.

**Files:**
- Create: `search-sync-worker/spotlight_org.go`
- Create: `search-sync-worker/spotlight_org_test.go`

- [ ] **Step 1: Write the failing metadata test**

Create `search-sync-worker/spotlight_org_test.go`:

```go
package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSpotlightOrgCollection_Metadata(t *testing.T) {
	coll := newSpotlightOrgCollection("spotlightorg-site-a", false)

	assert.Equal(t, "spotlight-org-sync", coll.ConsumerName())
	assert.Equal(t, "spotlight_org_template", coll.TemplateName())

	cfg := coll.StreamConfig("site-a")
	assert.Equal(t, "HR_SYNC_site-a", cfg.Name)
	assert.Equal(t, []string{"hr.sync.site-a.>"}, cfg.Subjects)
	assert.Empty(t, cfg.Sources)

	filters := coll.FilterSubjects("site-a")
	assert.Equal(t, []string{"hr.sync.site-a.employees.upsert"}, filters)
}

func TestSpotlightOrgTemplateBody_Prod(t *testing.T) {
	coll := newSpotlightOrgCollection("spotlightorg-site-a", false)
	body := coll.TemplateBody()
	require.NotNil(t, body)

	var parsed map[string]any
	require.NoError(t, json.Unmarshal(body, &parsed))

	patterns := parsed["index_patterns"].([]any)
	assert.Equal(t, "spotlightorg-site-a", patterns[0])

	tmpl := parsed["template"].(map[string]any)
	idx := tmpl["settings"].(map[string]any)["index"].(map[string]any)
	assert.Equal(t, float64(3), idx["number_of_shards"])
	assert.Equal(t, float64(1), idx["number_of_replicas"])

	mappings := tmpl["mappings"].(map[string]any)
	assert.Equal(t, false, mappings["dynamic"])
	props := mappings["properties"].(map[string]any)
	for _, f := range []string{
		"sectId", "sectTCName", "sectName", "sectDescription",
		"deptId", "deptTCName", "deptName", "deptDescription", "divisionId",
	} {
		field, ok := props[f].(map[string]any)
		require.True(t, ok, "missing property: %s", f)
		assert.Equal(t, "search_as_you_type", field["type"], "field %s wrong type", f)
		assert.Equal(t, "custom_analyzer", field["analyzer"], "field %s wrong analyzer", f)
	}
}

func TestSpotlightOrgTemplateBody_Dev(t *testing.T) {
	coll := newSpotlightOrgCollection("spotlightorg-site-a", true)
	body := coll.TemplateBody()
	var parsed map[string]any
	require.NoError(t, json.Unmarshal(body, &parsed))
	idx := parsed["template"].(map[string]any)["settings"].(map[string]any)["index"].(map[string]any)
	assert.Equal(t, float64(1), idx["number_of_shards"])
	assert.Equal(t, float64(0), idx["number_of_replicas"])
}
```

- [ ] **Step 2: Run test to verify build error**

Run: `make test SERVICE=search-sync-worker 2>&1 | tail -10`
Expected: build error — `undefined: newSpotlightOrgCollection`.

- [ ] **Step 3: Create `search-sync-worker/spotlight_org.go`**

```go
package main

import (
	"encoding/json"
	"errors"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/searchengine"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
)

// spotlightOrgCollection implements Collection for the spotlight-org
// search index. One document per `sectId` carries the nine org fields
// projected from each HR account row in the batched payload. The doc
// ID is the sectId itself — many employees collapse to one document
// via dedup in BuildAction.
//
// The wire-side row type and the ES doc projection are the same struct
// (SpotlightOrgIndex below), keeping the consumer loosely coupled to
// hr-syncer's own internal Employee/Org types without taking a public
// dependency on a pkg/model.Employee that would conflict on merge with
// the internal repo's existing one.
//
// The HR_SYNC_{siteID} stream is owned by `hr-syncer`; this collection
// is a pure consumer. main.go skips HR_SYNC in its bootstrap loop the
// same way it skips INBOX.
type spotlightOrgCollection struct {
	indexName string
	devMode   bool
}

func newSpotlightOrgCollection(indexName string, devMode bool) *spotlightOrgCollection {
	return &spotlightOrgCollection{indexName: indexName, devMode: devMode}
}

func (c *spotlightOrgCollection) StreamConfig(siteID string) jetstream.StreamConfig {
	cfg := stream.HRSync(siteID)
	return jetstream.StreamConfig{Name: cfg.Name, Subjects: cfg.Subjects}
}

func (c *spotlightOrgCollection) ConsumerName() string {
	return "spotlight-org-sync"
}

func (c *spotlightOrgCollection) FilterSubjects(siteID string) []string {
	return []string{subject.HRSyncEmployeesUpsert(siteID)}
}

func (c *spotlightOrgCollection) TemplateName() string {
	return "spotlight_org_template"
}

func (c *spotlightOrgCollection) TemplateBody() json.RawMessage {
	return spotlightOrgTemplateBody(c.indexName, c.devMode)
}

// BuildAction is implemented in stages — see Tasks 12-13 of the plan.
// The stub returns an error so a misconfigured wiring fails loudly
// rather than silently dropping HR sync events.
func (c *spotlightOrgCollection) BuildAction(data []byte) ([]searchengine.BulkAction, error) {
	return nil, errors.New("spotlightOrgCollection.BuildAction: not yet implemented")
}

// SpotlightOrgIndex serves three roles in the consumer: unmarshal
// target for the wire-side row, document body on ES write, and source
// of truth for the ES mapping via esPropertiesFromStruct. Every field
// is `omitempty` `string` so absent values serialize away and
// doc-merge upsert preserves the stored value rather than overwriting
// with empty. Fields not in this struct are silently ignored by the
// json decoder — hr-syncer is free to publish additional fields.
type SpotlightOrgIndex struct {
	SectID          string `json:"sectId,omitempty"          es:"search_as_you_type,custom_analyzer"`
	SectTCName      string `json:"sectTCName,omitempty"      es:"search_as_you_type,custom_analyzer"`
	SectName        string `json:"sectName,omitempty"        es:"search_as_you_type,custom_analyzer"`
	SectDescription string `json:"sectDescription,omitempty" es:"search_as_you_type,custom_analyzer"`
	DeptID          string `json:"deptId,omitempty"          es:"search_as_you_type,custom_analyzer"`
	DeptTCName      string `json:"deptTCName,omitempty"      es:"search_as_you_type,custom_analyzer"`
	DeptName        string `json:"deptName,omitempty"        es:"search_as_you_type,custom_analyzer"`
	DeptDescription string `json:"deptDescription,omitempty" es:"search_as_you_type,custom_analyzer"`
	DivisionID      string `json:"divisionId,omitempty"      es:"search_as_you_type,custom_analyzer"`
}

func spotlightOrgTemplateBody(indexName string, devMode bool) json.RawMessage {
	shards, replicas := indexTopology(3, 1, devMode)
	tmpl := map[string]any{
		"index_patterns": []string{indexName},
		"template": map[string]any{
			"settings": map[string]any{
				"index": map[string]any{
					"number_of_shards":   shards,
					"number_of_replicas": replicas,
				},
				"analysis": customAnalyzerSettings(),
			},
			"mappings": map[string]any{
				"dynamic":    false,
				"properties": esPropertiesFromStruct[SpotlightOrgIndex](),
			},
		},
	}
	data, _ := json.Marshal(tmpl)
	return data
}
```

- [ ] **Step 4: Run metadata tests to verify pass**

Run: `make test SERVICE=search-sync-worker 2>&1 | tail -20`
Expected: PASS for metadata + template tests.

- [ ] **Step 5: Lint and commit**

```bash
make lint
git add search-sync-worker/spotlight_org.go search-sync-worker/spotlight_org_test.go
git commit -m "feat(search-sync-worker): scaffold spotlight-org collection metadata"
```

---

## Task 12: Implement `BuildAction` happy path + dedup

Parse the envelope (uncompressed only for now — gzip lands in Task 13), unmarshal `[]SpotlightOrgIndex` (defined in Task 11; the wire-side row and the ES doc projection are the same struct), dedup by `SectID`, emit one `_update` per unique sectId with `doc_as_upsert:true`.

**Files:**
- Modify: `search-sync-worker/spotlight_org.go`
- Modify: `search-sync-worker/spotlight_org_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `search-sync-worker/spotlight_org_test.go`:

```go
import (
	"github.com/hmchangw/chat/pkg/model"
)

// makeHRSyncEvent builds a plaintext (gzip=false) HRSyncEvent containing
// the given employees. Used by every BuildAction test that doesn't
// exercise the compression path.
func makeHRSyncEvent(t *testing.T, ts int64, employees []SpotlightOrgIndex) []byte {
	t.Helper()
	payload, err := json.Marshal(employees)
	require.NoError(t, err)
	evt := model.HRSyncEvent{
		Timestamp: ts,
		BatchID:   "b-1",
		Gzip:      false,
		Payload:   payload,
	}
	data, err := json.Marshal(evt)
	require.NoError(t, err)
	return data
}

func TestSpotlightOrg_BuildAction_HappyPath(t *testing.T) {
	coll := newSpotlightOrgCollection("spotlightorg-site-a", false)
	data := makeHRSyncEvent(t, 1735689600000, []SpotlightOrgIndex{
		{SectID: "S1", SectName: "Eng", DeptID: "D1", DeptName: "Tech"},
		{SectID: "S2", SectName: "Sales", DeptID: "D2", DeptName: "Biz"},
	})

	actions, err := coll.BuildAction(data)
	require.NoError(t, err)
	require.Len(t, actions, 2)

	docIDs := map[string]bool{}
	for _, a := range actions {
		assert.Equal(t, searchengine.ActionUpdate, a.Action)
		assert.Equal(t, "spotlightorg-site-a", a.Index)
		assert.Equal(t, int64(0), a.Version, "ActionUpdate must not use external versioning")
		docIDs[a.DocID] = true

		var body map[string]any
		require.NoError(t, json.Unmarshal(a.Doc, &body))
		assert.Equal(t, true, body["doc_as_upsert"])
		assert.Contains(t, body, "doc")
	}
	assert.True(t, docIDs["S1"])
	assert.True(t, docIDs["S2"])
}

// TestSpotlightOrg_BuildAction_DedupBySectID covers many-employees-one-section:
// 100 employees sharing the same sectId must collapse to a single ES action.
// The kept row is the LAST occurrence so the most recent in-batch update wins.
func TestSpotlightOrg_BuildAction_DedupBySectID(t *testing.T) {
	coll := newSpotlightOrgCollection("spotlightorg-site-a", false)
	data := makeHRSyncEvent(t, 1, []SpotlightOrgIndex{
		{SectID: "S1", SectName: "Engineering"},
		{SectID: "S1", SectName: "Engineering Renamed"},
		{SectID: "S2", SectName: "Sales"},
	})

	actions, err := coll.BuildAction(data)
	require.NoError(t, err)
	require.Len(t, actions, 2)

	var s1Body map[string]any
	for _, a := range actions {
		if a.DocID == "S1" {
			require.NoError(t, json.Unmarshal(a.Doc, &s1Body))
		}
	}
	require.NotNil(t, s1Body)
	doc := s1Body["doc"].(map[string]any)
	assert.Equal(t, "Engineering Renamed", doc["sectName"], "last-wins on dedup")
}

func TestSpotlightOrg_BuildAction_EmptySectIDsSkipped(t *testing.T) {
	coll := newSpotlightOrgCollection("spotlightorg-site-a", false)
	data := makeHRSyncEvent(t, 1, []SpotlightOrgIndex{
		{SectID: "", SectName: "no-section"},
		{SectID: "S1", SectName: "Eng"},
		{SectID: "", DeptID: "D9"},
	})

	actions, err := coll.BuildAction(data)
	require.NoError(t, err)
	require.Len(t, actions, 1)
	assert.Equal(t, "S1", actions[0].DocID)
}

func TestSpotlightOrg_BuildAction_AllEmptySectIDs(t *testing.T) {
	coll := newSpotlightOrgCollection("spotlightorg-site-a", false)
	data := makeHRSyncEvent(t, 1, []SpotlightOrgIndex{
		{SectName: "no-section-1"},
		{SectName: "no-section-2"},
	})

	actions, err := coll.BuildAction(data)
	require.NoError(t, err)
	assert.Nil(t, actions, "all empty sectIds → zero actions, handler acks JS msg without ES write")
}

func TestSpotlightOrg_BuildAction_EmptyEmployees(t *testing.T) {
	coll := newSpotlightOrgCollection("spotlightorg-site-a", false)
	data := makeHRSyncEvent(t, 1, []SpotlightOrgIndex{})

	actions, err := coll.BuildAction(data)
	require.NoError(t, err)
	assert.Nil(t, actions)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=search-sync-worker -run TestSpotlightOrg_BuildAction 2>&1 | tail -20`
Expected: FAIL — stub returns an error.

- [ ] **Step 3: Replace the `BuildAction` stub in `search-sync-worker/spotlight_org.go`**

```go
import (
	"encoding/json"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/searchengine"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
)
```

```go
// BuildAction parses an HR sync envelope, dedupes employees by SectID
// (keeping the last occurrence per sectId), and emits one ES `_update`
// per unique sectId with `doc_as_upsert:true`. Doc-merge means absent
// fields on the projection preserve the stored value rather than
// overwriting with empty strings — partial-field events from
// `hr-syncer` work without a painless script.
//
// Returns (nil, nil) for empty employee arrays or batches where every
// row has an empty SectID; the handler then acks the JS message with
// no ES write.
func (c *spotlightOrgCollection) BuildAction(data []byte) ([]searchengine.BulkAction, error) {
	var envelope model.HRSyncEvent
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, fmt.Errorf("unmarshal hr-sync envelope: %w", err)
	}
	if envelope.Timestamp <= 0 {
		return nil, fmt.Errorf("build spotlight-org action: missing timestamp")
	}

	var employees []SpotlightOrgIndex
	if err := json.Unmarshal(envelope.Payload, &employees); err != nil {
		return nil, fmt.Errorf("unmarshal hr-sync employees: %w", err)
	}
	if len(employees) == 0 {
		return nil, nil
	}

	// Dedup by SectID keeping the LAST occurrence — within one batch
	// the most recent update wins. Empty SectID rows are skipped
	// silently (employees not yet assigned to a section).
	deduped := make(map[string]SpotlightOrgIndex, len(employees))
	order := make([]string, 0, len(employees))
	for _, emp := range employees {
		if emp.SectID == "" {
			continue
		}
		if _, seen := deduped[emp.SectID]; !seen {
			order = append(order, emp.SectID)
		}
		deduped[emp.SectID] = emp
	}
	if len(deduped) == 0 {
		return nil, nil
	}

	actions := make([]searchengine.BulkAction, 0, len(deduped))
	for _, sectID := range order {
		row := deduped[sectID]
		body, err := buildSpotlightOrgUpdateBody(row)
		if err != nil {
			return nil, err
		}
		actions = append(actions, searchengine.BulkAction{
			Action: searchengine.ActionUpdate,
			Index:  c.indexName,
			DocID:  sectID,
			Doc:    body,
			// No Version: ActionUpdate must not use external versioning.
			// handler.go::isBulkItemSuccess depends on this.
		})
	}
	return actions, nil
}

// buildSpotlightOrgUpdateBody wraps the row in an ES `_update` body
// with `doc_as_upsert:true`. The row is already the projection — the
// json `omitempty` discipline guarantees absent fields don't appear
// in the body and therefore don't overwrite stored values on the
// merge. Errors here are theoretical (the inputs are plain strings),
// but we return the wrapped error to keep the call site explicit.
func buildSpotlightOrgUpdateBody(row SpotlightOrgIndex) (json.RawMessage, error) {
	body := map[string]any{
		"doc":           row,
		"doc_as_upsert": true,
	}
	data, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal spotlight-org update body: %w", err)
	}
	return data, nil
}
```

Also delete the now-unused `errors` import.

- [ ] **Step 4: Run tests**

Run: `make test SERVICE=search-sync-worker -run TestSpotlightOrg 2>&1 | tail -20`
Expected: PASS.

- [ ] **Step 5: Lint and commit**

```bash
make lint
git add search-sync-worker/spotlight_org.go search-sync-worker/spotlight_org_test.go
git commit -m "feat(search-sync-worker): spotlight-org BuildAction happy path + dedup"
```

---

## Task 13: Implement gzip decompression + partial-field test + error cases

Add the `compress/gzip` branch and lock in the remaining behavior.

**Files:**
- Modify: `search-sync-worker/spotlight_org.go`
- Modify: `search-sync-worker/spotlight_org_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `search-sync-worker/spotlight_org_test.go`:

```go
import (
	"bytes"
	"compress/gzip"
)

// makeHRSyncEventGzip mirrors makeHRSyncEvent but gzip-compresses the
// employee payload. Used to exercise the consumer's decompression path.
func makeHRSyncEventGzip(t *testing.T, ts int64, employees []SpotlightOrgIndex) []byte {
	t.Helper()
	raw, err := json.Marshal(employees)
	require.NoError(t, err)
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	_, err = gz.Write(raw)
	require.NoError(t, err)
	require.NoError(t, gz.Close())
	evt := model.HRSyncEvent{
		Timestamp: ts,
		BatchID:   "b-1",
		Gzip:      true,
		Payload:   buf.Bytes(),
	}
	data, err := json.Marshal(evt)
	require.NoError(t, err)
	return data
}

func TestSpotlightOrg_BuildAction_Gzip(t *testing.T) {
	coll := newSpotlightOrgCollection("spotlightorg-site-a", false)
	data := makeHRSyncEventGzip(t, 1, []SpotlightOrgIndex{
		{SectID: "S1", SectName: "Engineering"},
	})

	actions, err := coll.BuildAction(data)
	require.NoError(t, err)
	require.Len(t, actions, 1)
	assert.Equal(t, "S1", actions[0].DocID)

	var body map[string]any
	require.NoError(t, json.Unmarshal(actions[0].Doc, &body))
	doc := body["doc"].(map[string]any)
	assert.Equal(t, "Engineering", doc["sectName"])
}

// TestSpotlightOrg_BuildAction_PartialFields locks in the partial-update
// contract: an Employee carrying only SectID + SectName must produce
// an ES `doc` body containing ONLY those two keys. Other org fields
// must be absent so doc-merge preserves their stored values.
func TestSpotlightOrg_BuildAction_PartialFields(t *testing.T) {
	coll := newSpotlightOrgCollection("spotlightorg-site-a", false)
	data := makeHRSyncEvent(t, 1, []SpotlightOrgIndex{
		{SectID: "S1", SectName: "Engineering"},
	})

	actions, err := coll.BuildAction(data)
	require.NoError(t, err)
	require.Len(t, actions, 1)

	var body map[string]any
	require.NoError(t, json.Unmarshal(actions[0].Doc, &body))
	doc := body["doc"].(map[string]any)

	assert.Equal(t, "S1", doc["sectId"])
	assert.Equal(t, "Engineering", doc["sectName"])
	for _, absent := range []string{
		"sectTCName", "sectDescription",
		"deptId", "deptTCName", "deptName", "deptDescription", "divisionId",
	} {
		_, present := doc[absent]
		assert.False(t, present, "doc must not carry %s when Employee did not set it", absent)
	}
}

func TestSpotlightOrg_BuildAction_Errors(t *testing.T) {
	coll := newSpotlightOrgCollection("spotlightorg-site-a", false)

	t.Run("malformed envelope", func(t *testing.T) {
		_, err := coll.BuildAction([]byte("{invalid"))
		assert.Error(t, err)
	})

	t.Run("missing timestamp", func(t *testing.T) {
		data := makeHRSyncEvent(t, 0, []SpotlightOrgIndex{{SectID: "S1"}})
		_, err := coll.BuildAction(data)
		assert.Error(t, err)
	})

	t.Run("malformed employees payload", func(t *testing.T) {
		evt := model.HRSyncEvent{
			Timestamp: 1,
			Payload:   json.RawMessage(`not json`),
		}
		data, _ := json.Marshal(evt)
		_, err := coll.BuildAction(data)
		assert.Error(t, err)
	})

	t.Run("corrupt gzip header", func(t *testing.T) {
		evt := model.HRSyncEvent{
			Timestamp: 1,
			Gzip:      true,
			Payload:   []byte("not gzip"),
		}
		data, _ := json.Marshal(evt)
		_, err := coll.BuildAction(data)
		assert.Error(t, err)
	})
}
```

- [ ] **Step 2: Run tests to verify failures**

Run: `make test SERVICE=search-sync-worker -run TestSpotlightOrg_BuildAction_Gzip 2>&1 | tail -10`
Expected: FAIL — the implementation doesn't yet handle `Gzip=true`.

- [ ] **Step 3: Add gzip decompression to `BuildAction`**

In `search-sync-worker/spotlight_org.go`, update the imports:

```go
import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/searchengine"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
)
```

Replace the body of `BuildAction` between the envelope unmarshal and the employees unmarshal:

```go
	payload := []byte(envelope.Payload)
	if envelope.Gzip {
		decompressed, err := gunzipBytes(payload)
		if err != nil {
			return nil, fmt.Errorf("decompress hr-sync payload: %w", err)
		}
		payload = decompressed
	}

	var employees []SpotlightOrgIndex
	if err := json.Unmarshal(payload, &employees); err != nil {
		return nil, fmt.Errorf("unmarshal hr-sync employees: %w", err)
	}
```

Append the helper:

```go
// gunzipBytes returns the gzip-decompressed contents of b. A corrupt
// header or truncated stream returns an error; the caller turns that
// into a NAK so JetStream retries — a transient publisher hiccup
// should not silently drop the batch.
func gunzipBytes(b []byte) ([]byte, error) {
	gr, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	defer gr.Close()
	return io.ReadAll(gr)
}
```

- [ ] **Step 4: Run all spotlight-org tests**

Run: `make test SERVICE=search-sync-worker -run TestSpotlightOrg 2>&1 | tail -20`
Expected: PASS.

- [ ] **Step 5: Lint and commit**

```bash
make lint
git add search-sync-worker/spotlight_org.go search-sync-worker/spotlight_org_test.go
git commit -m "feat(search-sync-worker): spotlight-org gzip + partial-field handling"
```

---

## Task 14: Wire `DEV_MODE` config + `SPOTLIGHT_ORG_INDEX` + HR_SYNC skip + collection registration

Final assembly in `main.go` — adds config fields, defaults the index name, skips HR_SYNC bootstrap, and wires the new collection alongside the existing three (now also passing the real `cfg.DevMode`).

**Files:**
- Modify: `search-sync-worker/main.go`
- Modify: `search-sync-worker/consumer_config_test.go` (if it enumerates collections by name)

- [ ] **Step 1: Check whether `consumer_config_test.go` needs updating**

Run: `grep -n "spotlight\|user-room\|message-sync" /home/user/chat/search-sync-worker/consumer_config_test.go`

If the file enumerates collection consumer names, add `"spotlight-org-sync"` to whatever list it asserts. If it uses a generic fake, no change needed. (This file is a one-off check, not a TDD step — adjust based on what you see.)

- [ ] **Step 2: Update `search-sync-worker/main.go::config`**

Add the two new env vars to the `config` struct alongside the existing ones:

```go
SpotlightOrgIndex string `env:"SPOTLIGHT_ORG_INDEX" envDefault:""`
DevMode           bool   `env:"DEV_MODE"            envDefault:"false"`
```

- [ ] **Step 3: Default the index name in `main`**

After the existing `SpotlightIndex` default (around line 91), add:

```go
if cfg.SpotlightOrgIndex == "" {
    cfg.SpotlightOrgIndex = fmt.Sprintf("spotlightorg-%s", cfg.SiteID)
}
```

- [ ] **Step 4: Extend the bootstrap skip list**

Modify the section around line 176-184 from:

```go
// INBOX is owned by inbox-worker — see the skip in the loop below.
inboxName := stream.Inbox(cfg.SiteID).Name
```

to:

```go
// INBOX is owned by inbox-worker; HR_SYNC is owned by hr-syncer.
// search-sync-worker is a pure consumer of both and must not create
// their schemas.
inboxName  := stream.Inbox(cfg.SiteID).Name
hrSyncName := stream.HRSync(cfg.SiteID).Name
```

And modify the condition (around line 184) from:

```go
if cfg.Bootstrap.Enabled && streamCfg.Name != inboxName {
```

to:

```go
if cfg.Bootstrap.Enabled && streamCfg.Name != inboxName && streamCfg.Name != hrSyncName {
```

- [ ] **Step 5: Wire the new collection and pass `cfg.DevMode` to all four**

Replace the `collections := []Collection{...}` slice (around line 137-141):

```go
collections := []Collection{
    newMessageCollection(cfg.MsgIndexPrefix, cfg.DevMode),
    newSpotlightCollection(cfg.SpotlightIndex, cfg.DevMode),
    newSpotlightOrgCollection(cfg.SpotlightOrgIndex, cfg.DevMode),
    newUserRoomCollection(cfg.UserRoomIndex, cfg.DevMode),
}
```

Update the trailing `slog.Info` (around line 219-225) to include the new index:

```go
slog.Info("search-sync-worker running",
    "site", cfg.SiteID,
    "msgPrefix", cfg.MsgIndexPrefix,
    "spotlightIndex", cfg.SpotlightIndex,
    "spotlightOrgIndex", cfg.SpotlightOrgIndex,
    "userRoomIndex", cfg.UserRoomIndex,
    "devMode", cfg.DevMode,
    "collections", len(collections),
)
```

- [ ] **Step 6: Build, lint, run all unit tests**

Run: `make build SERVICE=search-sync-worker && make lint && make test SERVICE=search-sync-worker 2>&1 | tail -20`
Expected: build succeeds, lint clean, all tests pass.

- [ ] **Step 7: Commit**

```bash
git add search-sync-worker/main.go search-sync-worker/consumer_config_test.go
git commit -m "feat(search-sync-worker): wire spotlight-org collection and DEV_MODE"
```

---

## Task 15: Integration test

End-to-end test in real testcontainers — publishes a gzipped `HRSyncEvent` to a fresh `HR_SYNC_{siteID}` stream and asserts the resulting ES docs.

**Files:**
- Modify: `search-sync-worker/integration_test.go`

- [ ] **Step 1: Read the existing integration test setup**

Run: `grep -n "func setup\|TestSearchSync\|func Test" /home/user/chat/search-sync-worker/integration_test.go | head -20`

Identify the existing setup helpers (likely `setupNATS`, `setupES`, or a single composite). Match their style for the new test.

- [ ] **Step 2: Add the test to `search-sync-worker/integration_test.go`**

Append to the file (keep the existing `//go:build integration` tag intact at the top):

```go
// TestSearchSync_SpotlightOrg_Integration drives the new collection end
// to end: publish a gzipped HRSyncEvent to a fresh HR_SYNC stream, let
// the worker run briefly, and verify the resulting documents.
//
// Doc-merge is the key behavior under test: a second event carrying
// only SectID + SectName must NOT clear the other stored fields.
func TestSearchSync_SpotlightOrg_Integration(t *testing.T) {
	const siteID = "site-int"
	const indexName = "spotlightorg-site-int"

	ctx := context.Background()

	// Reuse the existing setup helpers. Adjust names to match what's
	// already in this file.
	nc, js := setupNATS(t, ctx)
	engine, esURL := setupSearchEngine(t, ctx)

	// Create HR_SYNC stream (the test owns it because hr-syncer isn't
	// running; in prod hr-syncer creates it).
	hrCfg := stream.HRSync(siteID)
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     hrCfg.Name,
		Subjects: hrCfg.Subjects,
	})
	require.NoError(t, err)

	// Upsert the template once before any docs land so the mapping
	// applies to the very first index.
	coll := newSpotlightOrgCollection(indexName, true)
	require.NoError(t, engine.UpsertTemplate(ctx, coll.TemplateName(), coll.TemplateBody()))

	// Build the durable consumer and the handler exactly as main.go
	// would, then drive runConsumer in a goroutine bounded by stopCh.
	consumerCfg := buildConsumerConfig(stream.DefaultConsumerSettings(), coll, siteID)
	cons, err := js.CreateOrUpdateConsumer(ctx, hrCfg.Name, consumerCfg)
	require.NoError(t, err)
	handler := NewHandler(&engineAdapter{engine: engine}, coll, 500)
	stopCh := make(chan struct{})
	doneCh := make(chan struct{})
	go runConsumer(ctx, cons, handler, 100, 500, time.Second, stopCh, doneCh)
	t.Cleanup(func() {
		close(stopCh)
		<-doneCh
	})

	// Publish event #1 with full org info for two sections.
	subj := subject.HRSyncEmployeesUpsert(siteID)
	publishHRSyncEvent(t, nc, subj, time.Now().UnixMilli(), []SpotlightOrgIndex{
		{SectID: "S1", SectName: "Engineering", DeptID: "D1", DeptName: "Tech"},
		{SectID: "S2", SectName: "Sales", DeptID: "D2", DeptName: "Biz"},
	})

	// Poll until both docs land. ES is eventually consistent; refresh
	// or a short retry loop is needed.
	require.Eventually(t, func() bool {
		s1 := getESDoc(t, esURL, indexName, "S1")
		s2 := getESDoc(t, esURL, indexName, "S2")
		return s1 != nil && s2 != nil &&
			s1["sectName"] == "Engineering" && s2["sectName"] == "Sales"
	}, 30*time.Second, 500*time.Millisecond, "first batch did not land")

	// Publish event #2 — partial update for S1 with only SectName changed.
	// The doc-merge contract says deptId/deptName must survive.
	publishHRSyncEvent(t, nc, subj, time.Now().UnixMilli(), []SpotlightOrgIndex{
		{SectID: "S1", SectName: "Engineering Renamed"},
	})

	require.Eventually(t, func() bool {
		s1 := getESDoc(t, esURL, indexName, "S1")
		return s1 != nil &&
			s1["sectName"] == "Engineering Renamed" &&
			s1["deptId"] == "D1" &&
			s1["deptName"] == "Tech"
	}, 30*time.Second, 500*time.Millisecond, "doc-merge did not preserve untouched fields")
}

// publishHRSyncEvent builds a gzipped HRSyncEvent and publishes it to
// the given subject. The test owns the encoding so it can also exercise
// the worker's decompression path.
func publishHRSyncEvent(t *testing.T, nc *nats.Conn, subj string, ts int64, employees []SpotlightOrgIndex) {
	t.Helper()
	raw, err := json.Marshal(employees)
	require.NoError(t, err)
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	_, err = gz.Write(raw)
	require.NoError(t, err)
	require.NoError(t, gz.Close())
	envelope := model.HRSyncEvent{
		Timestamp: ts,
		BatchID:   fmt.Sprintf("test-%d", ts),
		Gzip:      true,
		Payload:   buf.Bytes(),
	}
	data, err := json.Marshal(envelope)
	require.NoError(t, err)
	require.NoError(t, nc.Publish(subj, data))
}

// getESDoc fetches a document by ID via ES HTTP _source endpoint and
// returns the parsed _source. Returns nil when the doc isn't there
// yet (404) so the caller's Eventually polling loop works cleanly.
func getESDoc(t *testing.T, esURL, index, docID string) map[string]any {
	t.Helper()
	resp, err := http.Get(fmt.Sprintf("%s/%s/_source/%s", esURL, index, docID))
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode == 404 {
		return nil
	}
	if resp.StatusCode != 200 {
		return nil
	}
	var doc map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		return nil
	}
	return doc
}
```

Add any missing imports to the file's import block: `bytes`, `compress/gzip`, `net/http`, `time`, `github.com/hmchangw/chat/pkg/model`, `github.com/hmchangw/chat/pkg/stream`, `github.com/hmchangw/chat/pkg/subject`.

**Note**: the helper names `setupNATS`, `setupSearchEngine`, and the type returned by the latter may differ in your file. Match the existing names; the structure of the test is what matters. If `setupSearchEngine` returns only an engine and not a URL, add a second helper that returns the container's mapped URL.

- [ ] **Step 3: Run the integration test**

Run: `make test-integration SERVICE=search-sync-worker 2>&1 | tail -30`
Expected: PASS (initial run pulls Docker images — may take 1-2 minutes).

- [ ] **Step 4: Lint and commit**

```bash
make lint
git add search-sync-worker/integration_test.go
git commit -m "test(search-sync-worker): integration test for spotlight-org sync"
```

---

## Task 16: Docker compose

Local dev now wants `DEV_MODE=true` (collapses every template to 1/0) and an optional `SPOTLIGHT_ORG_INDEX` override.

**Files:**
- Modify: `search-sync-worker/deploy/docker-compose.yml`

- [ ] **Step 1: Update the env block**

Replace the `environment` list in `search-sync-worker/deploy/docker-compose.yml` to add `DEV_MODE`:

```yaml
    environment:
      - NATS_URL=nats://nats:4222
      - NATS_CREDS_FILE=/etc/nats/backend.creds
      - SITE_ID=site-local
      - SEARCH_URL=http://elasticsearch:9200
      - SEARCH_BACKEND=elasticsearch
      - MSG_INDEX_PREFIX=messages-site-local-v1
      - BOOTSTRAP_STREAMS=true
      - DEV_MODE=${DEV_MODE:-true}
      # SPOTLIGHT_ORG_INDEX defaults to spotlightorg-${SITE_ID} when unset.
      # Override here if you need a different name for parallel test setups.
```

- [ ] **Step 2: Verify YAML parses**

Run: `docker compose -f search-sync-worker/deploy/docker-compose.yml config 2>&1 | tail -20`
Expected: parses cleanly, shows `DEV_MODE: "true"` in the rendered output.

- [ ] **Step 3: Commit**

```bash
git add search-sync-worker/deploy/docker-compose.yml
git commit -m "chore(search-sync-worker): enable DEV_MODE in local docker-compose"
```

---

## Task 17: Final verification + push

- [ ] **Step 1: Full unit test pass with race detector**

Run: `make test 2>&1 | tail -20`
Expected: all packages PASS with `-race`.

- [ ] **Step 2: Full integration test pass**

Run: `make test-integration SERVICE=search-sync-worker 2>&1 | tail -20`
Expected: PASS.

- [ ] **Step 3: Lint clean**

Run: `make lint 2>&1 | tail -10`
Expected: no issues.

- [ ] **Step 4: Verify coverage on the new file**

Run: `cd search-sync-worker && go test -coverprofile=/tmp/cov.out -run TestSpotlightOrg && go tool cover -func=/tmp/cov.out | grep spotlight_org`
Expected: ≥80% per CLAUDE.md, target ≥90%.

- [ ] **Step 5: Push the branch**

```bash
git push -u origin claude/spotlight-org-sync-design-fS2qh
```

(Per the branch policy: do NOT create a PR unless the user asks.)

---

## Self-review notes

Spec coverage check:
- §3.1 Stream definition → Task 3
- §3.2 Envelope → Task 2
- §3.3 Inner payload + Employee model → Task 1
- §4.1 Collection scaffold → Task 11
- §4.2 BuildAction flow → Tasks 12 + 13
- §4.3 Doc-merge upsert rationale → covered in Task 12 implementation
- §4.4 Fan-out — relies on existing handler.go logic, no new code; the dedup test asserts the per-batch action count
- §5.1 SpotlightOrgIndex struct → Task 11
- §5.2 Shared analyzer (+ updating existing spotlight) → Tasks 6, 7
- §5.3 Topology + dev mode → Task 5, applied in Tasks 8-11
- §6 Config & wiring → Task 14
- §7 Files touched — every entry mapped to a task
- §8 Testing — unit Tasks 11-13, dev-mode subtests Tasks 8-10, integration Task 15
- §9 Local dev — Task 16
