# Spotlight / user-room indexing: implementation spec

**Companion to:** `docs/design/2026-08-07-search-rooms-empty-results.md` (the investigation).
This document is self-contained — every claim carries a `file:line` so it can be verified
without reading the investigation or the conversation that produced it.

**Purpose:** specify the fix for a confirmed defect in which Teams-migrated room
memberships never reach either Elasticsearch search index.

---

## 0. Confidence — read this before validating anything else

Two different things are described below, and they carry different weight:

| | Status | How a reviewer checks it |
|---|---|---|
| **The defect** (§2) | **Confirmed** by code reading | Statically, from this repo alone. No cluster needed. |
| **That this defect causes the reported test-server symptom** | **Not confirmed** | Requires cluster access (§8). It is *a* cause of empty search results; whether it is *the* one on that server is open. |

The defect is worth fixing on its own merits regardless of how §8 resolves. Do not let
this document imply the test server is diagnosed — it is not.

**Reported symptom:** `chat.user.{account}.request.search.{siteID}.rooms` replies
`{"rooms": []}` on the test server.

---

## 1. Background: how a room becomes searchable

`search.rooms` is served entirely from the ES *spotlight* index — one document per
`(account, room)` pair (`search-service/handler.go:162`, `search-service/query_rooms.go`).
Documents get there by exactly two routes:

1. **Live:** `search-sync-worker` consumes `member_added` / `member_removed` from the
   site's INBOX stream and writes one doc per account
   (`search-sync-worker/spotlight.go:55-100`).
2. **Backfill:** `data-migration/es-index-migrator` reads Mongo `subscriptions` and writes
   the same docs (`data-migration/es-index-migrator/spotlightaction.go:19-48`).

Nothing else writes spotlight. A membership that produces no INBOX event, or an event the
worker cannot decode, is invisible to room search until a backfill runs.

The INBOX **internal lane** (`chat.inbox.{siteID}.internal.{eventType}`) is a
search-indexing feed only — `inbox-worker` deliberately does not consume it, because the
originating service already applied the change to the local DB
(`pkg/subject/subject.go:274-281`). So search-sync-worker is the *sole* consumer, and a
malformed internal-lane publish has no second reader to notice it.

---

## 2. The defect

### 2.1 Producer publishes an unwrapped payload

`room-worker/teamsroomcreate.go:242-261` — `federateTeamsMembership`, reached from the
Teams-migration room-create path (`teamsroomcreate.go:175,178`):

```go
evt := model.InboxMemberEvent{
	RoomID:    room.ID,
	RoomName:  room.Name,
	RoomType:  room.Type,
	SiteID:    h.siteID,
	Accounts:  accounts,
	Timestamp: acceptedAt.UnixMilli(),
}
payload, err := json.Marshal(evt)
if err != nil {
	return fmt.Errorf("marshal membership event: %w", err)
}
seed := fmt.Sprintf("%s:%s:%d", room.ID, eventType, acceptedAt.UnixMilli())
if err := h.publish(ctx, subject.InboxInternal(h.siteID, eventType), payload, …); err != nil {
```

`payload` is the **inner** event. Every other internal-lane publisher wraps it in an
`model.InboxEvent` envelope first — compare `finishCreateRoom`
(`room-worker/handler.go:1809-1817`):

```go
internalEvt := model.InboxEvent{
	Type:       model.InboxMemberAdded,
	SiteID:     room.SiteID,
	DestSiteID: room.SiteID,
	Payload:    innerData,          // ← the inner event goes HERE
	Timestamp:  now.UnixMilli(),
}
internalData, _ := json.Marshal(internalEvt)
h.publish(ctx, subject.InboxInternal(room.SiteID, model.InboxMemberAdded), internalData, …)
```

The **cross-site** branch of `federateTeamsMembership` is correct: it passes the inner
payload to `h.federate` (`teamsroomcreate.go:277`), which delegates to `outbox.Publish`
(`room-worker/handler.go:320-322`) and that builds the envelope. Only the **local** branch
skips it. Same function, two lanes, one wrapped and one not.

### 2.2 Consumer requires the envelope

`search-sync-worker/inbox_stream.go:65-78`:

```go
func parseMemberEvent(data []byte) (*model.InboxEvent, *model.InboxMemberEvent, error) {
	var evt model.InboxEvent
	if err := json.Unmarshal(data, &evt); err != nil { … }
	if evt.Timestamp <= 0 {
		return nil, nil, fmt.Errorf("parse member event: missing timestamp")
	}
	var payload model.InboxMemberEvent
	if err := json.Unmarshal(evt.Payload, &payload); err != nil { … }
	return &evt, &payload, nil
}
```

### 2.3 Why the existing guard does not catch it

Decoding a bare `InboxMemberEvent` into `InboxEvent` half-succeeds, because the two
structs share two JSON keys:

| `InboxEvent` field (`pkg/model/event.go:187-193`) | JSON key | Value after decoding a bare `InboxMemberEvent` (`event.go:106-115`) |
|---|---|---|
| `Timestamp int64` | `timestamp` | **the real timestamp** — present on both structs |
| `SiteID string` | `siteId` | the real site id — present on both structs |
| `Type InboxEventType` | `type` | `""` — absent from the inner struct |
| `DestSiteID string` | `destSiteId` | `""` — absent |
| `Payload []byte` | `payload` | **`nil`** — absent |

`roomId`, `roomName`, `roomType`, `accounts`, `joinedAt`, `historySharedSince` are simply
ignored (Go's default is to skip unknown fields).

So `evt.Timestamp > 0` and the **one validation in the function passes**. Execution reaches
`json.Unmarshal(nil, &payload)` → `unexpected end of JSON input`.

Even if the payload had decoded, `evt.Type == ""` would fall through
`spotlightCollection.BuildAction`'s switch to
`default: unsupported event type ""` (`search-sync-worker/spotlight.go:95-97`).

### 2.4 The message is then dropped, not retried

`search-sync-worker/handler.go:90-95`:

```go
actions, err := h.collection.BuildAction(data)
if err != nil {
	slog.Error("build action", "error", err)
	natsutil.Ack(msg, "build action failed")   // Ack ⇒ no redelivery, no DLQ
	return
}
```

The event is acknowledged and discarded. There is an ERROR log, but no retry and nothing
downstream ever observes the loss.

### 2.5 Blast radius: two indexes, not one

`parseMemberEvent` is shared by both INBOX-consuming collections:

| Collection | Call site | Index | User-visible effect |
|---|---|---|---|
| `spotlightCollection` | `search-sync-worker/spotlight.go:56` | spotlight | `search.rooms` omits the room |
| `userRoomCollection` | `search-sync-worker/user_room.go:48` | user-room-mv | restricted-room access map is missing entries; consumed by `search-service/handler.go:224-261` (`loadRestricted` → `GetUserRoomDoc`) for **message** search |

One producer bug degrades both room search and message search for every
Teams-migrated room.

### 2.6 Why CI is green

`room-worker/teamsroomcreate_test.go:40-59` — the assertion helper:

```go
raw := p.data
if strings.Contains(p.subj, "outbox") {   // outbox: unwrap OutboxEvent → InboxEvent → Payload
	…
	raw = ie.Payload
}
var e model.InboxMemberEvent
require.NoError(t, json.Unmarshal(raw, &e))
```

For the **internal** subject it unmarshals `p.data` directly into `InboxMemberEvent` — it
asserts the shape the producer happens to emit, not the shape the consumer requires. The
test therefore encodes the same mistake and passes. `TestProcessTeamsRoomCreate_AddOnly`
(`teamsroomcreate_test.go:105-121`) is green today against broken behaviour.

---

## 3. The fix

Three changes, one PR. Scope is deliberately narrow: correct the producer, make the
contract enforceable, and fix the test that permitted it.

### F1 — wrap the internal-lane publish (`room-worker/teamsroomcreate.go`)

```go
	payload, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal membership event: %w", err)
	}
	// The internal lane carries the InboxEvent envelope, matching every other
	// InboxInternal publisher; search-sync-worker decodes evt.Payload.
	internalData, err := json.Marshal(model.InboxEvent{
		Type:       eventType,
		SiteID:     h.siteID,
		DestSiteID: h.siteID,
		Payload:    payload,
		Timestamp:  acceptedAt.UnixMilli(),
	})
	if err != nil {
		return fmt.Errorf("marshal internal inbox envelope: %w", err)
	}
	seed := fmt.Sprintf("%s:%s:%d", room.ID, eventType, acceptedAt.UnixMilli())
	if err := h.publish(ctx, subject.InboxInternal(h.siteID, eventType), internalData,
		natsutil.InboxDedupID(ctx, h.siteID, seed)); err != nil {
		return fmt.Errorf("local inbox publish: %w", err)
	}
```

`DestSiteID: h.siteID` mirrors `finishCreateRoom` — for a local-origin event, origin and
destination are the same site.

**Leave the federated branch (`teamsroomcreate.go:263-280`) untouched.** It correctly passes
the inner `siteData` to `h.federate`, which wraps. Wrapping there too would double-wrap and
break cross-site federation — this is the most likely way to get the fix wrong.

### F2 — make the contract enforceable (`search-sync-worker/inbox_stream.go:70`)

```go
	if evt.Timestamp <= 0 {
		return nil, nil, fmt.Errorf("parse member event: missing timestamp")
	}
	if evt.Type == "" {
		return nil, nil, fmt.Errorf("parse member event: missing event type (unwrapped payload?)")
	}
	if len(evt.Payload) == 0 {
		return nil, nil, fmt.Errorf("parse member event: missing payload envelope")
	}
```

Rationale: the sole existing guard is satisfiable by a wrongly-shaped message (§2.3). These
two turn a silent misdecode into a named failure. They are cheap and they are what makes
the next producer that gets this wrong fail legibly.

### F3 — fix the test that allowed it (`room-worker/teamsroomcreate_test.go:40-59`)

Decode the internal-lane message through the envelope, same as the outbox branch. Then add
a **round-trip contract test** — producer and consumer live in the same Go module, so the
bytes one emits can be fed directly to the other:

```go
// The internal-lane bytes room-worker publishes must decode through the exact
// helper search-sync-worker uses. Guards the envelope contract end to end.
func TestFederateTeamsMembership_InternalLaneDecodesAsInboxEvent(t *testing.T) { … }
```

A shape assertion in one service cannot catch a cross-service contract break; only the
round trip can. This is the change that actually prevents recurrence — F2 makes the failure
loud, F3 makes it impossible to merge.

---

## 4. TDD plan

Per `CLAUDE.md` §4 (Red-Green-Refactor, mandatory). Verify Red before writing any
implementation.

| Step | Action | Expected |
|---|---|---|
| 1 | Write the F3 round-trip test against current `main` | **RED** — `unexpected end of JSON input` |
| 2 | Write `parseMemberEvent` table tests: bare inner event, missing `type`, empty `payload`, valid envelope | **RED** for the first three (they currently fail with the wrong error, or pass the guard) |
| 3 | Apply F2 | Step-2 cases GREEN; step-1 still RED |
| 4 | Apply F1 | Step-1 GREEN |
| 5 | Update the existing `membershipEvents` helper | `TestProcessTeamsRoomCreate_*` GREEN |
| 6 | `make lint && make test && make sast` | clean |

Coverage: both touched files must stay ≥80% (`CLAUDE.md` §4); `room-worker` and
`search-sync-worker` are core business logic and should target 90%+.

**Do not skip step 1's Red.** If the round-trip test passes before F1, it is not exercising
the internal lane and needs rewriting.

---

## 5. Compliance checklist

- [ ] No store interfaces changed → `make generate` not required
- [ ] No client-facing handler changed (`chat.user.…` subjects untouched) → **no
      `docs/client-api.md` update required** for this PR. Note this does *not* hold for the
      §6 item on DM names, which changes a `pkg/model` event struct and therefore requires
      `docs/client-api.md` **and** both derived views in the same PR.
- [ ] No new dependencies
- [ ] Errors wrapped with context, no bare `err` (`CLAUDE.md` §3)
- [ ] Branch → PR; never merge to `master` directly
- [ ] `make sast` clean (blocking CI gate)

---

## 6. Out of scope — separate PRs

These are confirmed defects in the same subsystem, deliberately excluded to keep this PR
reviewable. Full evidence in the investigation doc.

| # | Location | Defect |
|---|---|---|
| A | `room-worker/handler.go:1335-1340` | `resolveRoomName` returns `""` for DM/botDM, so every live-path DM is indexed with an empty `roomName` and can never match. The backfill uses `sub.Name` (`es-index-migrator/spotlightaction.go:30`), so the two writers of the same index disagree. |
| B | `room-worker/handler.go:2381-2384` | `publishSyncDMInbox` returns early for a same-site counterpart → server-created same-site DMs emit no INBOX event at all; cross-site ones notify only the remote member. |
| C | `search-sync-worker/inbox_stream.go:35-37` | Filters cover only `member_added`/`member_removed`; `room_renamed` never updates spotlight, so renamed rooms keep stale names indefinitely. |
| D | `search-service/integration_rooms_test.go:60-91` | Every room-search integration test builds the index *without* the production analyzer, so it runs against `standard` while production uses `custom_analyzer` (whitespace + lowercase, and — unlike the messages template at `pkg/searchindex/template.go:81` — **no `cjk_bigram`**). The failing seam is untested. |
| E | `search-sync-worker/inbox_stream.go:48-50` | `MappingUpdate()` is a no-op for spotlight, and `UpsertTemplate` targets `_index_template` (`pkg/searchengine/adapter.go:193`) which applies only at index creation. An index created before its template keeps ES dynamic mappings forever — including `userAccount` as `text`, which silently breaks the `term` access filter at `search-service/query_rooms.go:30`. |

**Recommended order: D first.** Rebuilding the test fixture from
`searchindex.SpotlightTemplateBody` converts the analyzer questions from speculation into
failing tests, which should be resolved before E changes mappings or a backfill rewrites
documents.

---

## 7. Data recovery — no code change substitutes for this

Documents already dropped stay missing after the fix; the producer only emits on *new*
membership events. Rebuild with `data-migration/es-index-migrator`, which reads Mongo
`subscriptions` and repopulates spotlight and user-room
(`data-migration/es-index-migrator/runner.go:81-110`).

Sequence: **fix → deploy → resolve D and E → backfill.** The migrator writes into whatever
mapping already exists and inherits defects C and D, so running it before those are settled
produces documents that must be rebuilt again.

---

## 8. What this spec does *not* establish

Open, and requiring cluster access:

1. **Whether the Teams migration ever ran on the test server.** If those rooms were created
   through the normal `room.create` path, this defect is dormant there and the symptom has
   another cause. Check: `kubectl logs deploy/search-sync-worker | grep -i 'build action'` —
   `unexpected end of JSON input` or `unsupported event type ""` confirms it fired.
2. **Whether the search-sync-worker pod is healthy at all.** A missing required env var, or
   the `INBOX_{site}` → `INBOX-{site}` rename in commit `6685001` (#181) meeting an
   un-renamed stream, makes `CreateOrUpdateConsumer` fail and `os.Exit(1)`
   (`search-sync-worker/main.go:290-298`) — producing an identical empty result with no
   defect in this code path.
3. **Whether the two services agree on the index name.** search-service reads
   `SEARCH_SPOTLIGHT_INDEX` (`main.go:74` under `envPrefix:"SEARCH_"` at `main.go:90`);
   search-sync-worker writes `SPOTLIGHT_INDEX` (`main.go:47`). Two names, one value. The
   startup line `search-sync-worker running` (`main.go:327`) prints the resolved
   `spotlightIndex` — compare it against search-service's configuration.
4. **Whether the failure is total or partial.** "Empty for every user and every query"
   points at 1-3; "empty for some queries" points at D (analyzer) or C (stale names).
   Nobody has established which.

Point 1 in particular decides whether this PR resolves the reported incident or merely
removes one of several ways to reproduce it.

---

## 9. Verification after deploy

1. Trigger a Teams-migration room create.
2. `search-sync-worker` logs **no** `build action` error (success is silent — every `slog`
   call in `search-sync-worker/handler.go` is an ERROR, so absence is the pass condition).
3. Query ES directly, bypassing search-service:
   ```bash
   curl -s "$ES/spotlight-*/_search" -H 'Content-Type: application/json' \
     -d '{"query":{"term":{"roomId":"<migrated-room-id>"}}}' | jq '.hits.total.value'
   ```
   Expect one document per member account.
4. `search.rooms` returns the room for a member of it.
5. Confirm cross-site federation still works — a remote member's home site must also
   receive the membership. This is the regression F1 could plausibly introduce if the
   federated branch were wrapped by mistake (§3, F1).

Allow ~6s between steps 1 and 3: `BULK_FLUSH_INTERVAL` defaults to 5s
(`search-sync-worker/main.go:72`) and the spotlight template sets no `refresh_interval`, so
ES uses its 1s default.
