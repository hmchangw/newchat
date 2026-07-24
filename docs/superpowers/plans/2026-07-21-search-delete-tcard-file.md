# Search delete/edit correctness + tcard & file search — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> Spec: `docs/superpowers/specs/2026-07-21-search-delete-tcard-file-design.md` (binding).
> This plan targets the spec's FINAL design (verbatim `cardData`; no reconcile tooling —
> see Task 5's removal note). The branch already implements it; the plan doubles as the
> audit reference — compare shipped code against each task.

**Goal:** Deleted/edited messages propagate to Elasticsearch, and file-attachment + tcard data become searchable — with zero RPC/API flow changes.

**Architecture:** Event-driven, three seams: `history-service` enriches the `.updated` canonical events it already publishes; `search-sync-worker` maps canonical events to externally-versioned ES actions (full-replace on create/update, delete on delete, skip slim events) and indexes the new fields; `search-service` widens its `multi_match` and returns render payloads on hits.

**Tech Stack:** Go 1.25, NATS JetStream, Elasticsearch via `pkg/searchengine`, `go.uber.org/mock`, testify, testcontainers via `pkg/testutil`.

## Global Constraints

- TDD Red-Green-Refactor for every behavior change; run tests via `make test SERVICE=<name>` (race detector included) — never raw `go` commands. Sole exception: coverage runs use raw `go test -coverprofile` per CLAUDE.md §Coverage (the wrapper has no coverage mode).
- No RPC/API contract changes; every `SearchMessage` addition is `omitempty` (additive).
- Integration tests: `//go:build integration`, containers from `pkg/testutil`, per-test isolation.
- Errors wrapped with context (`fmt.Errorf("…: %w", err)`); `log/slog` JSON only; never log message/card bodies.
- ES ordering invariant: every index/delete action carries `evt.Timestamp` as external version (spec "Ordering invariant").
- Client-facing response changes must update `docs/client-api.md` + `docs/client-api/request-reply.md` in the same PR (CLAUDE.md rule).
- Branch `claude/search-deletion-tcard-file-dwnnnk`; commit per task; push with `git push -u origin <branch>`.

---

### Task 1: search-sync-worker — event allowlist + attachment/card ES fields

**Files:**
- Modify: `search-sync-worker/messages.go` (BuildAction skip logic ~line 91; `MessageSearchIndex` ~line 129; `newMessageSearchIndex` ~line 145)
- Test: `search-sync-worker/messages_test.go`
- Test (integration): `search-sync-worker/integration_test.go`, `search-sync-worker/testdata/events.json`

**Interfaces:**
- Consumes: `model.MessageEvent` (`pkg/model/event.go:30`), `cassandra.DecodeAttachments(raw [][]byte) ([]Attachment, int)` (`pkg/model/cassandra/attachment.go:48`), `cassandra.Card{Template string, Data []byte}`.
- Produces (Task 4 depends on these exact JSON names): searched ES doc fields `attachmentText` (one string: titles + descriptions joined), `cardData` (`text,custom_analyzer`); render-only fields `attachments` (full `Attachment` objects) and `card` (template + data), both `object`/`enabled:false`; helper `actionableEvent(e model.EventType) bool`.

- [ ] **Step 1: Write the failing tests** (append to `messages_test.go`; add `"github.com/hmchangw/chat/pkg/model/cassandra"` to imports)

```go
// Slim events (no content) must never take the full-doc-replace upsert path:
// a pinned/unpinned event would wipe content (and attachment/card fields) from
// the indexed document, and an unpin after delete would resurrect a stub doc.
func TestMessageCollection_BuildAction_SlimEventsSkipped(t *testing.T) {
	coll := newMessageCollection("msgs-v1", time.Time{}, false)

	mkEvent := func(eventType model.EventType) []byte {
		evt := model.MessageEvent{
			Event: eventType,
			Message: model.Message{
				ID: "m1", RoomID: "r1", UserID: "u1", UserAccount: "alice",
				CreatedAt: time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC),
			},
			SiteID: "site-a", Timestamp: 100,
		}
		data, err := json.Marshal(evt)
		require.NoError(t, err)
		return data
	}

	tests := []struct {
		name  string
		event model.EventType
	}{
		{"pinned skipped", model.EventPinned},
		{"unpinned skipped", model.EventUnpinned},
		{"thread_reply_added skipped", model.EventThreadReplyAdded},
		{"unknown future type skipped", model.EventType("archived")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actions, err := coll.BuildAction(mkEvent(tt.event))
			require.NoError(t, err)
			assert.Empty(t, actions, "event %q must not produce an ES action", tt.event)
		})
	}
}

func TestBuildDocument_AttachmentFields(t *testing.T) {
	ts := time.Date(2026, 1, 15, 10, 30, 0, 0, time.UTC)

	mkBlob := func(t *testing.T, a cassandra.Attachment) []byte {
		b, err := json.Marshal(a)
		require.NoError(t, err)
		return b
	}

	t.Run("titles, descriptions and file types are indexed", func(t *testing.T) {
		evt := &model.MessageEvent{
			Event: model.EventCreated,
			Message: model.Message{
				ID: "m1", RoomID: "r1", UserID: "u1", UserAccount: "alice",
				Content: "see attached", CreatedAt: ts,
				Attachments: [][]byte{
					mkBlob(t, cassandra.Attachment{ID: "f1", Title: "q3-report.pdf", Description: "Quarterly numbers", FileType: "application/pdf"}),
					mkBlob(t, cassandra.Attachment{ID: "f2", Title: "team.png", FileType: "image/png"}),
				},
			},
			SiteID: "site-a", Timestamp: 100,
		}
		var doc map[string]any
		require.NoError(t, json.Unmarshal(buildDocument(evt), &doc))
		assert.Equal(t, "q3-report.pdf Quarterly numbers team.png", doc["attachmentText"])
		atts := doc["attachments"].([]any) // full objects ride along, render-only
		require.Len(t, atts, 2)
		assert.Equal(t, "q3-report.pdf", atts[0].(map[string]any)["title"])
	})

	t.Run("malformed blob is skipped, valid ones kept", func(t *testing.T) {
		evt := &model.MessageEvent{
			Event: model.EventCreated,
			Message: model.Message{
				ID: "m1", RoomID: "r1", UserID: "u1", UserAccount: "alice",
				Content: "x", CreatedAt: ts,
				Attachments: [][]byte{
					[]byte("{not json"),
					mkBlob(t, cassandra.Attachment{ID: "f1", Title: "ok.txt", FileType: "text/plain"}),
				},
			},
			SiteID: "site-a", Timestamp: 100,
		}
		var doc map[string]any
		require.NoError(t, json.Unmarshal(buildDocument(evt), &doc))
		assert.Equal(t, "ok.txt", doc["attachmentText"])
	})

	t.Run("no attachments omits the fields", func(t *testing.T) {
		evt := &model.MessageEvent{
			Event: model.EventCreated,
			Message: model.Message{
				ID: "m1", RoomID: "r1", UserID: "u1", UserAccount: "alice",
				Content: "x", CreatedAt: ts,
			},
			SiteID: "site-a", Timestamp: 100,
		}
		var doc map[string]any
		require.NoError(t, json.Unmarshal(buildDocument(evt), &doc))
		for _, key := range []string{"attachmentText", "attachments"} {
			_, present := doc[key]
			assert.False(t, present, "%s should be omitted when there are no attachments", key)
		}
	})
}

func TestBuildDocument_CardFields(t *testing.T) {
	ts := time.Date(2026, 1, 15, 10, 30, 0, 0, time.UTC)

	t.Run("card template and stringified card data are indexed", func(t *testing.T) {
		data := `{"type":"AdaptiveCard","body":[{"type":"TextBlock","text":"Expense request from Bob"},{"title":"Amount","value":"$120"}]}`
		evt := &model.MessageEvent{
			Event: model.EventCreated,
			Message: model.Message{
				ID: "m1", RoomID: "r1", UserID: "u1", UserAccount: "alice",
				CreatedAt: ts,
				Card: &cassandra.Card{
					Template: "expense-approval-v1",
					Data:     []byte(data),
				},
				CardAction: &cassandra.CardAction{
					Verb: "approve", Text: "Approve the expense", DisplayText: "Bob approved",
				},
			},
			SiteID: "site-a", Timestamp: 100,
		}
		var doc map[string]any
		require.NoError(t, json.Unmarshal(buildDocument(evt), &doc))
		assert.Equal(t, data, doc["cardData"], "card data is indexed verbatim as text")
		card := doc["card"].(map[string]any) // full object rides along, render-only
		assert.Equal(t, "expense-approval-v1", card["template"])
	})

	t.Run("no card omits the fields", func(t *testing.T) {
		evt := &model.MessageEvent{
			Event: model.EventCreated,
			Message: model.Message{
				ID: "m1", RoomID: "r1", UserID: "u1", UserAccount: "alice",
				Content: "x", CreatedAt: ts,
			},
			SiteID: "site-a", Timestamp: 100,
		}
		var doc map[string]any
		require.NoError(t, json.Unmarshal(buildDocument(evt), &doc))
		for _, key := range []string{"card", "cardData"} {
			_, present := doc[key]
			assert.False(t, present, "%s should be omitted when there is no card", key)
		}
	})

	t.Run("card with empty data indexes template only", func(t *testing.T) {
		evt := &model.MessageEvent{
			Event: model.EventCreated,
			Message: model.Message{
				ID: "m1", RoomID: "r1", UserID: "u1", UserAccount: "alice",
				CreatedAt: ts,
				Card:      &cassandra.Card{Template: "welcome-v1"},
			},
			SiteID: "site-a", Timestamp: 100,
		}
		var doc map[string]any
		require.NoError(t, json.Unmarshal(buildDocument(evt), &doc))
		assert.Equal(t, "welcome-v1", doc["card"].(map[string]any)["template"])
		_, present := doc["cardData"]
		assert.False(t, present, "empty card data should be omitted")
	})
}
```

- [ ] **Step 2: Run to verify failure**

Run: `make test SERVICE=search-sync-worker`
Expected: FAIL — SlimEventsSkipped subtests get 1 action instead of 0; `doc["attachmentText"]`/`doc["cardData"]` are `<nil>`.

- [ ] **Step 3: Implement in `messages.go`**

Replace the `EventReacted` skip in `BuildAction` with:

```go
	// Only full-content events may touch the index. Slim events (reacted,
	// pinned, unpinned, thread_reply_added, and any future type) carry no
	// content: letting them take the full-doc-replace upsert path would wipe
	// content/attachment/card fields — and an unpin after a delete would
	// resurrect a stub document. The unstamped "" event is the legacy replay
	// form of created and stays indexable.
	if !actionableEvent(evt.Event) {
		return nil, nil
	}
```

Add below the `--- Message-specific internals ---` marker:

```go
// actionableEvent reports whether an event type produces a bulk action at
// all — index/replace (created, updated, legacy "") or delete (deleted).
func actionableEvent(e model.EventType) bool {
	switch e {
	case model.EventCreated, model.EventUpdated, model.EventDeleted, "":
		return true
	default:
		return false
	}
}
```

Extend `MessageSearchIndex` (below `TShow`):

```go
	// Searched attachment/tcard projections (ES maps arrays implicitly).
	// CardData is the card's data doc indexed verbatim — no expansion.
	AttachmentText string `json:"attachmentText,omitempty" es:"text,custom_analyzer"`
	CardData               string   `json:"cardData,omitempty"               es:"text,custom_analyzer"`

	// Render payloads stored as-is (never indexed) so search hits can be
	// rendered on the frontend without a history-service lookup.
	Attachments []cassandra.Attachment `json:"attachments,omitempty" es:"object_disabled"`
	Card        *cassandra.Card        `json:"card,omitempty"        es:"object_disabled"`
```

(`es:"object_disabled"` is a template.go extension expanding to `{"type":"object","enabled":false}` — stored in `_source`, never indexed.)

Populate at the end of `newMessageSearchIndex` (before `return doc`; add `pkg/model/cassandra` import):

```go
	// Lenient decode: a malformed blob is skipped by DecodeAttachments; one
	// bad attachment must not block indexing the rest of the message.
	attachments, _ := cassandra.DecodeAttachments(evt.Message.Attachments)
	doc.Attachments = attachments
	var attachmentText []string
	for i := range attachments {
		a := &attachments[i]
		if a.Title != "" {
			attachmentText = append(attachmentText, a.Title)
		}
		if a.Description != "" {
			attachmentText = append(attachmentText, a.Description)
		}
	}
	doc.AttachmentText = strings.Join(attachmentText, " ")

	if evt.Message.Card != nil {
		doc.Card = evt.Message.Card
		doc.CardData = string(evt.Message.Card.Data)
	}
```

(Loop indexes by pointer — gocritic `rangeValCopy` fires on the 248-byte `Attachment` value copy.)

- [ ] **Step 4: Run to verify pass**

Run: `make test SERVICE=search-sync-worker`
Expected: `ok github.com/hmchangw/chat/search-sync-worker` — new tests pass, `TestMessageCollection_BuildAction_ReactedSkipped` still passes (reacted remains skipped via the allowlist).

- [ ] **Step 5: Extend the integration fixture** (`testdata/events.json` + `integration_test.go`)

Append to `events.json` (after the existing deleted event; attachment blob is base64 of `{"title":"q3-report.pdf","description":"Quarterly numbers","fileType":"application/pdf"}`):

```json
{
  "event": "created",
  "message": {
    "id": "msg-006", "roomId": "room-2", "userId": "u1", "userAccount": "alice",
    "content": "see the attached report", "createdAt": "2026-02-11T09:00:00Z",
    "attachments": ["eyJ0aXRsZSI6InEzLXJlcG9ydC5wZGYiLCJkZXNjcmlwdGlvbiI6IlF1YXJ0ZXJseSBudW1iZXJzIiwiZmlsZVR5cGUiOiJhcHBsaWNhdGlvbi9wZGYifQ=="],
    "card": {
      "template": "expense-approval-v1",
      "data": "eyJ0eXBlIjoiQWRhcHRpdmVDYXJkIiwiYm9keSI6W3sidHlwZSI6IlRleHRCbG9jayIsInRleHQiOiJFeHBlbnNlIHJlcXVlc3QgZnJvbSBCb2IifV19"
    },
    "cardAction": {"verb": "approve", "displayText": "Approved by Alice"}
  },
  "siteId": "site-test", "timestamp": 1000008
},
{
  "event": "pinned",
  "message": {"id": "msg-001", "roomId": "room-1", "userId": "u1", "userAccount": "alice", "createdAt": "2026-01-15T10:30:00Z"},
  "siteId": "site-test", "timestamp": 1000009
},
{
  "event": "unpinned",
  "message": {"id": "msg-002", "roomId": "room-1", "userId": "u2", "userAccount": "bob", "createdAt": "2026-01-15T10:31:00Z"},
  "siteId": "site-test", "timestamp": 1000010
}
```

In `TestSearchSyncIntegration`: route `EventPinned`/`EventUnpinned` to `subject.MsgCanonicalPinned/Unpinned`; bump doc-count expectations (total 5, February 3); assert msg-001 keeps `"hello world (edited)"` after the pin, msg-002 stays absent after the unpin, msg-006 carries the searched projections + the full `attachments`/`card` objects + `cardData` containing `"Expense request from Bob"`; add `searchHitsMultiField` (multi_match `bool_prefix`/`AND` over `["content","attachmentText","cardData"]`) and assert one hit each for `"q3-report.pdf"`, `"Quarterly"`, `"Expense request"`.

- [ ] **Step 6: Verify + commit**

Run: `go vet -tags integration ./search-sync-worker/ && make test SERVICE=search-sync-worker` → PASS. Where Docker is available: `make test-integration SERVICE=search-sync-worker` → `ok` (~35–50s).

```bash
git add search-sync-worker
git commit -m "search-sync-worker: skip slim events; index attachment + tcard fields"
```

---

### Task 2: pkg/searchengine — UpdateMapping + startup mapping push

**Files:**
- Modify: `pkg/searchengine/searchengine.go` (interface), `pkg/searchengine/adapter.go`
- Modify: `search-sync-worker/collection.go` (interface), `search-sync-worker/messages.go`, `search-sync-worker/inbox_stream.go`, `search-sync-worker/spotlight_org.go` (no-op stubs), `search-sync-worker/main.go` (startup loop after template upsert)
- Test: `pkg/searchengine/adapter_test.go`, `pkg/searchengine/integration_test.go`, `search-sync-worker/messages_test.go`, `search-sync-worker/handler_test.go` (test-stub collections gain the method)

**Interfaces:**
- Produces: `SearchEngine.UpdateMapping(ctx context.Context, indexPattern string, body json.RawMessage) error`; `Collection.MappingUpdate() (indexPattern string, body json.RawMessage)` — empty pattern/nil body = no update. Only `messageCollection` returns a real update.

- [ ] **Step 1: Write the failing tests**

`pkg/searchengine/adapter_test.go`:

```go
func TestAdapter_UpdateMapping(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		var capturedBody string
		ft := &fakeTransport{handler: func(req *http.Request) (*http.Response, error) {
			assert.Equal(t, http.MethodPut, req.Method)
			assert.Equal(t, "/messages-site1-*/_mapping", req.URL.Path)
			assert.Equal(t, "true", req.URL.Query().Get("allow_no_indices"))
			assert.Equal(t, "true", req.URL.Query().Get("ignore_unavailable"))
			assert.Equal(t, "application/json", req.Header.Get("Content-Type"))
			body, _ := io.ReadAll(req.Body)
			capturedBody = string(body)
			return jsonResponse(200, `{"acknowledged": true}`), nil
		}}
		a := newAdapter(ft)
		body := json.RawMessage(`{"properties":{"cardData":{"type":"text"}}}`)
		err := a.UpdateMapping(context.Background(), "messages-site1-*", body)
		require.NoError(t, err)
		assert.JSONEq(t, string(body), capturedBody)
	})

	t.Run("error status", func(t *testing.T) {
		ft := &fakeTransport{handler: func(req *http.Request) (*http.Response, error) {
			return jsonResponse(400, `{"error":"mapper_parsing_exception"}`), nil
		}}
		a := newAdapter(ft)
		err := a.UpdateMapping(context.Background(), "messages-*", json.RawMessage(`{}`))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "400")
	})

	t.Run("empty pattern rejected", func(t *testing.T) {
		a := newAdapter(&fakeTransport{handler: func(req *http.Request) (*http.Response, error) {
			t.Fatal("no request expected for empty pattern")
			return nil, nil
		}})
		err := a.UpdateMapping(context.Background(), "", json.RawMessage(`{}`))
		assert.Error(t, err)
	})
}
```

`search-sync-worker/messages_test.go`:

```go
// Index templates only apply to indices created after the template change, so
// the collection must expose an additive mapping update for existing monthly
// indices — otherwise new fields stay unmapped (dynamic:false drops them)
// until the next month rolls over.
func TestMessageCollection_MappingUpdate(t *testing.T) {
	coll := newMessageCollection("messages-site1-v1", time.Time{}, false)
	pattern, body := coll.MappingUpdate()
	assert.Equal(t, "messages-site1-*", pattern, "pattern must strip the version suffix like the template's index_patterns")
	require.NotNil(t, body)

	var parsed struct {
		Properties map[string]any `json:"properties"`
	}
	require.NoError(t, json.Unmarshal(body, &parsed))
	assert.Contains(t, parsed.Properties, "attachmentText")
	assert.Contains(t, parsed.Properties, "cardData")
	assert.Contains(t, parsed.Properties, "content", "full property set keeps the update idempotent")
	// Render payloads are stored but never indexed: object + enabled:false.
	for _, key := range []string{"attachments", "card"} {
		prop := parsed.Properties[key].(map[string]any)
		assert.Equal(t, "object", prop["type"], key)
		assert.Equal(t, false, prop["enabled"], key)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `make test SERVICE=pkg/searchengine && make test SERVICE=search-sync-worker`
Expected: build FAIL — `a.UpdateMapping undefined`, `coll.MappingUpdate undefined`.

- [ ] **Step 3: Implement**

`pkg/searchengine/adapter.go` (beside `PutScript`):

```go
// UpdateMapping PUTs an additive mapping body onto every index matching the
// pattern. Index templates only apply at index creation, so new fields must
// also be pushed onto already-existing indices. `allow_no_indices` +
// `ignore_unavailable` make the call a no-op on a fresh cluster.
func (a *httpAdapter) UpdateMapping(ctx context.Context, indexPattern string, body json.RawMessage) error {
	if indexPattern == "" {
		return fmt.Errorf("update mapping: index pattern required")
	}
	path := fmt.Sprintf("/%s/_mapping?allow_no_indices=true&ignore_unavailable=true", indexPattern)
	resp, err := a.do(ctx, "indices.put_mapping", indexPattern, func(ctx context.Context) (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, path, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		return req, nil
	})
	if err != nil {
		return fmt.Errorf("update mapping: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return fmt.Errorf("update mapping: status %d, read body: %w", resp.StatusCode, readErr)
		}
		return fmt.Errorf("update mapping: status %d, body: %s", resp.StatusCode, respBody)
	}
	return nil
}
```

Interface method on `SearchEngine` (`searchengine.go`, beside `PutScript`) with the same doc comment. `search-sync-worker/collection.go` — add to `Collection`:

```go
	// MappingUpdate returns an index pattern and an additive mapping body
	// (`{"properties": ...}`) to PUT onto already-existing indices at startup.
	// Index templates only apply to indices created afterwards, so collections
	// with rolling (e.g. monthly) indices must push schema additions onto the
	// live ones too. Empty pattern / nil body means no update is needed.
	MappingUpdate() (indexPattern string, body json.RawMessage)
```

`messages.go`:

```go
// MappingUpdate pushes the full (idempotent) property set onto existing
// monthly indices; the same pattern the template targets.
func (c *messageCollection) MappingUpdate() (string, json.RawMessage) {
	// Error discarded: input is a static map of literals, marshal cannot fail.
	body, _ := json.Marshal(map[string]any{"properties": messageTemplateProperties()})
	return searchindex.IndexPattern(c.indexPrefix), body
}
```

No-op stubs: `inboxMemberCollection` (covers spotlight + user-room) and `spotlightOrgCollection` return `"", nil` (pinned by `TestSpotlightCollection_MappingUpdate_NoOp` / `TestSpotlightOrg_MappingUpdate_NoOp`); the stub collections in `handler_test.go` gain one-line `MappingUpdate` methods. `main.go`, after the template-upsert loop, calls an extracted `pushMappings` helper (unit-tested in `main_test.go` with a fake engine — pushes only collections with a mapping, propagates engine errors):

```go
	if err := pushMappings(ctx, engine, collections); err != nil {
		slog.Error("update index mapping failed", "error", err)
		os.Exit(1)
	}

// pushMappings PUTs each collection's additive mapping onto its existing
// indices — templates govern only new indices, so without this new fields
// stay unmapped (dynamic:false) until the next monthly rollover.
func pushMappings(ctx context.Context, engine searchengine.SearchEngine, collections []Collection) error {
	for _, coll := range collections {
		pattern, body := coll.MappingUpdate()
		if pattern == "" || body == nil {
			continue
		}
		if err := engine.UpdateMapping(ctx, pattern, body); err != nil {
			return fmt.Errorf("update mapping %s: %w", pattern, err)
		}
		slog.Info("index mapping updated", "pattern", pattern)
	}
	return nil
}
```

- [ ] **Step 4: Integration case** (`pkg/searchengine/integration_test.go`)

```go
func TestUpdateMapping_AppliesToExistingIndex(t *testing.T) {
	esURL := testutil.Elasticsearch(t)
	index := testutil.ElasticsearchIndex(t, "searchenginemap")

	ctx := context.Background()
	engine, err := New(ctx, Config{Backend: "elasticsearch", URL: esURL})
	require.NoError(t, err)

	// Materialize the index by writing a doc, then push a new field mapping.
	_, err = engine.Bulk(ctx, []BulkAction{{
		Action: ActionIndex, Index: index, DocID: "d1", Version: 1,
		Doc: json.RawMessage(`{"content":"x"}`),
	}})
	require.NoError(t, err)

	err = engine.UpdateMapping(ctx, index, json.RawMessage(`{"properties":{"cardData":{"type":"text"}}}`))
	require.NoError(t, err)

	mapping, err := engine.GetIndexMapping(ctx, index)
	require.NoError(t, err)
	assert.Contains(t, string(mapping), `"cardData"`)

	// A pattern matching nothing must be a no-op, not an error.
	err = engine.UpdateMapping(ctx, "no-such-index-pattern-*", json.RawMessage(`{"properties":{"x":{"type":"keyword"}}}`))
	assert.NoError(t, err)
}
```

- [ ] **Step 5: Verify + commit**

Run: `make test SERVICE=pkg/searchengine && make test SERVICE=search-sync-worker && go vet -tags integration ./pkg/searchengine/` → PASS.

```bash
git add pkg/searchengine search-sync-worker
git commit -m "searchengine: UpdateMapping API; push message mapping onto existing indices"
```

---

### Task 3: history-service — enrich `.updated` canonical events

**Files:**
- Modify: `history-service/internal/service/messages.go` (EditMessage canonical event, ~line 421)
- Modify: `history-service/internal/service/migration.go` (MigrationEditMessage, ~line 24)
- Test: `history-service/internal/service/messages_test.go`, `history-service/internal/service/migration_test.go`

**Interfaces:**
- Consumes: Task 1's full-doc replace semantics; `models.Message` (= `cassandra.Message`) with decrypted `Attachments [][]byte`, `Card`, `CardAction` (cassrepo read path applies `atrest.ApplyDecryptedFields`).
- Produces: `.updated` events carrying `Attachments`/`Card` (`CardAction` deliberately excluded — no `.updated` consumer reads it); migration edit resolves the row via `s.msgReader.GetMessageByID` before updating (matching the migration delete path).

- [ ] **Step 1: Write the failing tests**

`messages_test.go`:

```go
// The .updated event is a full-doc replace for search-sync-worker: it must
// carry the message's attachments and card, or the re-index wipes those
// fields from the search document.
func TestHistoryService_EditMessage_CarriesAttachmentsAndCard(t *testing.T) {
	svc, msgs, subs, pub, _ := newService(t)
	c := testContext()

	attachments := [][]byte{[]byte(`{"title":"q3.pdf","description":"numbers","fileType":"application/pdf"}`)}
	card := &models.Card{Template: "expense-v1", Data: []byte(`{"text":"hi"}`)}
	cardAction := &models.CardAction{Verb: "approve", DisplayText: "Approved by Ann"}

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	hydrated := &models.Message{
		MessageID:   "msg-1",
		RoomID:      "r1",
		Sender:      models.Participant{Account: "u1", ID: "u1-id"},
		CreatedAt:   time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC),
		Msg:         "original content",
		Attachments: attachments,
		Card:        card,
		CardAction:  cardAction,
	}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "msg-1").Return(hydrated, nil)
	msgs.EXPECT().UpdateMessageContent(gomock.Any(), hydrated, "updated content", gomock.Any()).Return(nil)

	pub.EXPECT().
		Publish(gomock.Any(), subject.MsgCanonicalUpdated("site-test"), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, data []byte, _ string) error {
			var evt model.MessageEvent
			require.NoError(t, json.Unmarshal(data, &evt))
			assert.Equal(t, attachments, evt.Message.Attachments)
			require.NotNil(t, evt.Message.Card)
			assert.Equal(t, card.Template, evt.Message.Card.Template)
			assert.Equal(t, card.Data, evt.Message.Card.Data)
			// CardAction stays off the wire: no .updated consumer reads it,
			// and its Data blob would inflate every edit event.
			assert.Nil(t, evt.Message.CardAction)
			return nil
		})

	_, err := svc.EditMessage(c, "site-test", models.EditMessageRequest{
		MessageID: "msg-1",
		NewMsg:    "updated content",
	})
	require.NoError(t, err)
}
```

`migration_test.go` — extend `TestHistoryService_MigrationEditMessage_Success` with a `GetMessageByID` expectation returning a hydrated row (sender `bob`/`bob-id`, attachments, card `legacy-card-v1`) and assert the published event carries `UserAccount`/`UserID`/`Attachments`/`Card`; change `_AbsentRowRetries` to expect `GetMessageByID → (nil, nil)` with `UpdateMessageContent` `Times(0)` and a NotFound error; add `GetMessageByID` to `_WriterError`. Guard tests: `_AlreadyDeletedAcksOK` (deleted row → OK ack, no update, no publish), `_RoomMismatchRejected` (row owned by another room → NotFound, no update), `_ReaderErrorPropagates`, `_RowVanishesRetries` (writer returns wrapped `ErrMessageNotFound` → retryable NotFound).

- [ ] **Step 2: Run to verify failure**

Run: `make test SERVICE=history-service/internal/service`
Expected: 4 FAILs (missing fields on published events; missing mock expectations).

- [ ] **Step 3: Implement**

`messages.go` — add two lines to the `canonicalEvt.Message` literal in `EditMessage` (and extend the comment: the full doc is required or re-index wipes fields). `CardAction` is deliberately NOT carried — no `.updated` consumer reads it:

```go
			Attachments:                  msg.Attachments,
			Card:                         msg.Card,
```

`migration.go` — `MigrationEditMessage` resolves the row first (replaces the locator-only flow):

```go
	// Resolve the full row first (like the delete path): the .updated event is
	// a full-doc replace for search-sync-worker, so it must carry sender,
	// attachments and card — a slim locator-only event would wipe those fields
	// from the search document. The resolved row is also the more accurate
	// UPDATE locator (thread/pin routing fields present).
	msg, err := s.msgReader.GetMessageByID(c, req.MessageID)
	if err != nil {
		return nil, fmt.Errorf("migration edit %q: %w", req.MessageID, err)
	}
	if msg == nil {
		// Edit-before-insert race: the row isn't persisted yet. Surface a
		// retryable NotFound (4xx, Nak) so the benign race doesn't log as 5xx.
		return nil, errcode.NotFound("message not yet persisted, retry")
	}
	// Edit-after-delete replay: ack idempotently — updating would republish
	// .updated with a fresh version and resurrect the doc in search.
	if msg.Deleted {
		return &model.MigrationAck{OK: true}, nil
	}
	// Locator sanity: a transformer bug with a wrong RoomID must not edit
	// whatever row happens to own the message ID.
	if req.RoomID != "" && msg.RoomID != req.RoomID {
		return nil, errcode.NotFound("message not found in room")
	}

	if err := s.msgWriter.UpdateMessageContent(c, msg, req.Content, req.EditedAt); err != nil {
		// Row vanished between read and update (hard-missing on the
		// cipher-path read) — benign, retryable.
		if errors.Is(err, cassrepo.ErrMessageNotFound) {
			return nil, errcode.NotFound("message row missing, retry")
		}
		return nil, fmt.Errorf("migration edit message %s: %w", req.MessageID, err)
	}
```

and build the event from `msg` (ID/RoomID/CreatedAt/Sender fields/Attachments/Card/ThreadParent fields/TShow — no CardAction) with `Content: req.Content`, `EditedAt`/`UpdatedAt` = `req.EditedAt`.

- [ ] **Step 4: Verify + commit**

Run: `make test SERVICE=history-service` → all `ok`.

```bash
git add history-service
git commit -m "history-service: carry attachments/card (and sender) on .updated canonical events"
```

---

### Task 4: search-service — query + response

**Files:**
- Modify: `search-service/query_messages.go` (multi_match fields, ~line 46), `search-service/response.go` (`messageSearchHit`, `toSearchMessage`), `pkg/model/search.go` (`SearchMessage`)
- Modify: `docs/client-api.md` (search.messages section), `docs/client-api/request-reply.md`
- Test: `search-service/query_messages_test.go`, `search-service/response_test.go`, `pkg/model/model_test.go`, `search-service/integration_messages_test.go`

**Interfaces:**
- Consumes: Task 1's ES field names.
- Produces: `model.SearchMessage` gains `Attachments []Attachment` and `Card *Card` (aliases of the cassandra types), both `omitempty` — the message's render payloads mirrored as-is (same wire shape as history reads) so hits render without a follow-up history load.

- [ ] **Step 1: Write the failing tests**

`query_messages_test.go`:

```go
// One query box searches message text, file names/descriptions and tcard
// data together.
func TestBuildMessageQuery_SearchesAttachmentAndCardFields(t *testing.T) {
	req := model.SearchMessagesRequest{Query: "report", Size: 25}
	raw, err := buildMessageQuery(req, "alice", nil, 365*24*time.Hour, "user-room")
	require.NoError(t, err)

	q := parseQuery(t, raw)
	musts := q["query"].(map[string]any)["bool"].(map[string]any)["must"].([]any)
	require.Len(t, musts, 1)
	mm := musts[0].(map[string]any)["multi_match"].(map[string]any)
	assert.Equal(t, "report", mm["query"])
	assert.Equal(t, "bool_prefix", mm["type"])
	assert.Equal(t, "AND", mm["operator"])
	assert.Equal(t,
		[]any{"content", "attachmentText", "cardData"},
		mm["fields"])
}
```

`response_test.go`:

```go
// Full attachment objects and the card ride the hit so the client can render
// the result (file row, tcard) without a history-service lookup.
func TestToSearchMessage_AttachmentAndCardFieldsCopied(t *testing.T) {
	hit := messageSearchHit{
		MessageID:   "m1",
		RoomID:      "r1",
		UserAccount: "alice",
		Attachments: []model.Attachment{
			{ID: "f1", Title: "q3-report.pdf", Description: "Quarterly numbers", FileType: "application/pdf", TitleLink: "api/v1/file/rooms/r1/file/f1", TitleLinkDownload: true},
			{ID: "f2", Title: "team.png", FileType: "image/png"},
		},
		Card: &model.Card{Template: "expense-approval-v1", Data: []byte(`{"amount":42}`)},
	}
	got := toSearchMessage(&hit)
	require.Len(t, got.Attachments, 2)
	assert.Equal(t, "q3-report.pdf", got.Attachments[0].Title)
	require.NotNil(t, got.Card)
	assert.Equal(t, "expense-approval-v1", got.Card.Template)
}

// A text-only hit must not grow attachment/card keys on the wire.
func TestToSearchMessage_AttachmentAndCardOmittedWhenAbsent(t *testing.T) {
	hit := messageSearchHit{MessageID: "m1", RoomID: "r1", UserAccount: "alice", Content: "hello"}
	got := toSearchMessage(&hit)

	data, err := json.Marshal(got)
	require.NoError(t, err)
	var raw map[string]any
	require.NoError(t, json.Unmarshal(data, &raw))
	for _, key := range []string{"attachments", "card"} {
		_, present := raw[key]
		assert.False(t, present, "%s should be omitted when empty", key)
	}
}
```

`pkg/model/model_test.go`: extend `TestSearchMessageJSON` "full" case with `Attachments`/`Card` + a subtest asserting they're omitted when empty.

- [ ] **Step 2: Run to verify failure**

Run: `make test SERVICE=search-service && make test SERVICE=pkg/model`
Expected: build FAIL — `SearchMessage has no field Attachments` etc.

- [ ] **Step 3: Implement**

`pkg/model/attachment.go` — add `Card = cassandra.Card` to the alias block. `pkg/model/search.go` — append to `SearchMessage`:

```go
	// Render payloads mirrored as-is from the message (same wire shape as
	// history reads) so the client can render hits without a second lookup.
	Attachments []Attachment `json:"attachments,omitempty"`
	Card        *Card        `json:"card,omitempty"`
```

`response.go` — mirror both fields on `messageSearchHit` (same JSON tags) and copy them in `toSearchMessage`. `query_messages.go`:

```go
							// Message text plus file-attachment names/descriptions
							// and tcard data — one query box covers all of them.
							"fields": []string{"content", "attachmentText", "cardData"},
```

Docs: `client-api.md` search.messages — "Searched fields" paragraph (content, attachmentText, cardData verbatim), response JSON example + `attachments`/`card` field-table rows (linking the shared [Attachment]/[MessageCard] schemas) with omission rules; `request-reply.md` — same fields in the compact list.

- [ ] **Step 4: End-to-end guarantee test** (`integration_messages_test.go`, real ES + real NATS)

Seed with the worker's exact externally-versioned write shapes and search through the real RPC — this pins the spec's ordering invariant:

```go
	// m1: created (v=1000) then edited (full replace, v=2000, new content + attachment).
	// m2: created (v=1000) then deleted (v=2000).
	// Then a stale replay of m2's create (v=1000) MUST 409 against the tombstone.
	staleResults := mustBulk(searchengine.BulkAction{Action: searchengine.ActionIndex, Index: index, DocID: "m2", Version: 1000,
		Doc: msgDoc("m2", "secret that must vanish", nil)})
	require.Equal(t, http.StatusConflict, staleResults[0].Status, "stale replay must hit the external-version tombstone")
```

Subtests: `search("edited words")` → exactly m1 with post-edit content; `search("original words")` → 0 hits; `search("secret")` → 0 hits; `search("q3")` → m1 via `attachmentText`. The index MUST be created from a production-equivalent template (`messageTestTemplate()` extended with the new fields) — ES dynamic mapping types `roomId` as `text`, which silently breaks the terms-lookup room gate. Seed the caller's user-room doc for the terms lookup.

- [ ] **Step 5: Verify + commit**

Run: `make test SERVICE=search-service && make test SERVICE=pkg/model && go vet -tags integration ./search-service/` → PASS. With Docker: `make test-integration SERVICE=search-service` → `ok` (~100s).

```bash
git add search-service pkg/model docs/client-api.md docs/client-api/request-reply.md
git commit -m "search-service: search + return attachment/tcard fields"
```

---

### Task 5: search-sync-worker — reconcile mode (REMOVED by owner decision)

> **2026-07-22:** A fully-built `-reconcile` one-shot mode (search_after ES scan →
> Cassandra `messages_by_id` verification → bulk delete, with dry-run default,
> `RECONCILE_MIN_AGE` grace window and bounded-concurrency checks) was removed from
> the branch. Rationale: pre-pipeline deletions are out of scope, and once a
> `.deleted` event is published the worker's Nak/retry makes the ES delete
> at-least-once. The accepted residual gap — a publish-side drop after the Cassandra
> soft-delete commits — is recorded in the spec's "Stale deleted docs" decision.

---


### Task 6: Full verification

- [ ] **Step 1:** `make lint` → `0 issues.`
- [ ] **Step 2:** `make test` → every package `ok`, no FAIL lines.
- [ ] **Step 3:** `make sast-gosec` → no findings (govulncheck/semgrep in CI if the sandbox blocks their feeds).
- [ ] **Step 4:** Integration suites (Docker required): `make test-integration SERVICE=search-sync-worker` (~35–50s), `make test-integration SERVICE=search-service` (~100s), `make test-integration SERVICE=history-service` (~80s), `make test-integration SERVICE=pkg/searchengine` (~20s) → all `ok`.
- [ ] **Step 5:** Coverage spot-check (raw `go test` per the Global Constraints exception): `go test -coverprofile=cov.out ./search-sync-worker/ ./search-service/ ./history-service/internal/service/ && go tool cover -func=cov.out | grep total` — new functions ≥80% (handlers in `history-service/internal/service` sit >90%).
- [ ] **Step 6:** Push: `git push -u origin claude/search-deletion-tcard-file-dwnnnk`. No PR unless requested.

## Rollout notes

- Deploy `search-sync-worker` first — startup pushes the new mapping onto existing monthly indices; docs indexed before the deploy lack the new fields until edited/replayed (spec limitation).
- `search-service` deploy order-independent: query references fields that may be empty; response additions are `omitempty`.
- Documents deleted before this deploy keep their search docs (owner decision — no reconcile tooling; see spec "Stale deleted docs").
