# subscription.update appInfo enrichment — `description` + `appViewUrl`

**Date:** 2026-08-13
**Status:** Draft — awaiting review
**Scope:** Add two fields to `CounterpartAppInfo` on the `subscription.update`
`added` event for botDM rooms, populated from the app document room-worker
already reads. Backend + docs + tests only; no new RPC, no frontend code in
this PR.

## Problem

When a user subscribes to a bot for the first time, `user-service` creates a
botDM room and `room-worker` fans out a `subscription.update` event with
`action: "added"`. The human member's copy carries `appInfo`
(`CounterpartAppInfo`), which the event exists to provide: the documented
intent sits on the sibling `hrInfo` row (`docs/client-api.md:1312`) — the
counterpart record is sent "so the client can render the new sidebar row
without a `subscription.list` refetch" — and `appInfo` is its botDM
counterpart, same purpose.

Today `appInfo` carries only `{id, name, assistantName}`
(`pkg/model/event.go:98-102`). The frontend also needs the app's
`description` and `appViewUrl` to render the bot row and its app view. Both
fields already exist on the stored app document and on `model.App`
(`pkg/model/app.go:7-8`), and a `subscription.list` refetch already returns
them (via `AppSubscriptionFromApp`, used at
`user-service/service/subscriptions.go:98`) — only the live event omits them.
So the event-driven sidebar row is missing data that the very next refetch
would have, which is exactly the gap the counterpart fields were added to
close.

### Verified current state

The FE claim "name + assistant were already added" is confirmed, with one
naming nuance: the fields are `name` and `assistantName` (not a full
`assistant` object — that exists only on `model.App`).

| Fact | Where |
|---|---|
| `CounterpartAppInfo{ID, Name, AssistantName}` — no `description`, no `appViewUrl` | `pkg/model/event.go:98-102` |
| Single production constructor: `&model.CounterpartAppInfo{ID: app.ID, Name: app.Name, AssistantName: cp}` | `room-worker/handler.go:2129` (`resolveSubUpdateCounterpart`) |
| `GetApp` projects only `{"name": 1, "_id": 1}`; `Assistant` deliberately unprojected | `room-worker/store_mongo.go:211-226` |
| `model.App` has `Description string` and `AppViewURL map[string]string`, both with real `bson` tags | `pkg/model/app.go:7-8` |
| `apps` collection is provisioned upstream; this repo only reads it (room-service/user-service create indexes, nothing writes documents) — both fields optional (`omitempty`) on the doc | `pkg/model/app.go:3`, repo-wide grep: no non-test writers |
| Event is core-NATS ephemeral (msgID `""`), per-user subject `chat.user.{account}.event.subscription.update`, marshaled with `encoding/json` (room-worker is not a sonic service) | `room-worker/main.go:211-219`, `handler.go:109-114`, `pkg/subject/subject.go:257-261` |
| `appInfo` fires only on the `added` fan-out at botDM room creation — reached via user-service first-time subscribe (a re-subscribe just flips a Mongo flag and publishes nothing) or the client `room.create` dm path; JetStream redelivery can re-emit it (at-least-once). Cold either way. | `user-service/service/apps.go:12-50`, `room-service/handler.go:246-300` |
| `appInfo` never rides OUTBOX/INBOX — the federated reuse of this struct's bytes (`role_updated` → `InboxRoleUpdated`) has two producers, room-service and the oplog migration transformer, and neither sets counterpart fields | `room-service/handler.go:796-806, 820-826`, `data-migration/oplog-collections-transformer/subscriptions.go:253-264`, `inbox-worker/handler.go:268` |

## Approaches considered

### A. Backend enriches the event (recommended)

Add the two fields to `CounterpartAppInfo`, widen room-worker's `GetApp`
projection by two fields, and copy them at the one construction site.

- **Pros:** No extra query — the same single `GetApp` read that already runs
  per event returns two more projected fields. The event becomes
  self-sufficient, matching its documented purpose and the data
  `subscription.list` already serves, so the event-rendered row and the
  refetched row agree. No FE dependency to land it; additive optional JSON
  fields are wire-safe in a mixed-version fleet (every decoder on the path is
  lenient — no `DisallowUnknownFields`, TS parses structurally; no consumer
  hashes or dedups this payload). The path is cold: two events per botDM
  room creation.
- **Cons:** Event payload grows slightly (an app description plus a small URL
  map). Three test files and two doc files must be touched (enumerated below).

### B. Frontend fetches app details after the event

Leave the event as-is; FE fetches `description`/`appViewUrl` when a botDM
`added` event arrives.

- **Pros:** Zero backend change; event payload stays minimal.
- **Cons:** There is **no app-by-id RPC** to call. The client surfaces that
  return these fields today are the nested `app` object on `subscription.list`
  botDM rows, `apps.list` (paginated full catalog, no id filter),
  and Search Apps (substring match on `app.name`, non-empty query required —
  which cannot address the known nameless-app case, and the endpoint is
  flagged prototype / not subscription-scoped). So FE would either page the
  catalog, do a fragile name search and match `id` client-side, or refetch
  `subscription.list` — the exact round trip this event exists to avoid. Doing
  B "properly" means BE building a new `app.get(appId)` RPC first, which is
  strictly more backend work than approach A. Plus one extra round trip of
  sidebar-render latency per new botDM, duplicated enrichment logic in FE, and
  a cross-team dependency for a two-field gap.

**Decision: A.** The enrichment costs ~5 production lines and zero additional
queries; B costs either a new RPC or a workaround that contradicts the event's
design.

## Design (approach A)

### 1. Model — `pkg/model/event.go`

```go
// CounterpartAppInfo is the botDM counterpart's app record on a subscription.update.
// AssistantName is the bot account the app answers on.
type CounterpartAppInfo struct {
	ID            string            `json:"id"`
	Name          string            `json:"name"`
	AssistantName string            `json:"assistantName"`
	Description   string            `json:"description,omitempty"`
	AppViewURL    map[string]string `json:"appViewUrl,omitempty"`
}
```

Decisions:

- **`AppViewURL` is `map[string]string`**, mirroring `model.App` — the wire
  type FE already receives from `apps.list` / Search Apps (URLs keyed by view
  name, e.g. `{"main": "…"}`). Shipping a different shape on the event than on
  the list would force two FE parsers for the same concept (the doc already
  warns about exactly that trap on `CounterpartHRInfo`).
- **Both fields `omitempty`**, unlike the existing three: they are optional on
  the upstream-provisioned app document (`omitempty` in `model.App` both
  directions), so absent data is omitted rather than shipped as `""` / `{}`.
  This also keeps the pinned exact-JSON assertion for a minimal app
  (`TestCounterpartAppInfoJSON`'s 3-key `JSONEq`) truthful.
- No `bson` tags — the struct is wire-only, matching the existing fields.

### 2. Store — `room-worker/store_mongo.go`

Widen the projection in `GetApp` from `{"name": 1, "_id": 1}` to:

```go
options.FindOne().SetProjection(bson.M{"name": 1, "_id": 1, "description": 1, "appViewUrl": 1})
```

and update the function's doc comment (it currently says "Projects name +
_id"). Keep `assistant` unprojected — `AssistantName` stays derived from the
queried account (`cp`), per the comment at `handler.go:2127-2128`. Per the
CLAUDE.md precise-projection rule, we extend the projection rather than copy
room-service's projection-less `GetApp`.

The `SubscriptionStore` interface signature is unchanged, so **no
`make generate`** — mocks reference `*model.App` opaquely.

### 3. Handler — `room-worker/handler.go:2129`

```go
appInfo := &model.CounterpartAppInfo{
	ID: app.ID, Name: app.Name, AssistantName: cp,
	Description: app.Description, AppViewURL: app.AppViewURL,
}
```

All fallback behavior (ErrAppNotFound → nil appInfo, other errors → warn +
nil, nameless app → roomName falls back but appInfo ships) is unchanged; a
nameless app now ships its `description`/`appViewUrl` too, if present.

### 4. Docs (same PR, per CLAUDE.md event-struct rule)

- `docs/client-api.md:1419-1425` — add two rows to the `CounterpartAppInfo`
  table:
  - `description` — string — Optional. App description. Omitted when the app
    document has none.
  - `appViewUrl` — map<string, string> — Optional. App-view URLs keyed by view
    name (e.g. `{"main": "..."}`). Omitted when the app document has none.
  (Wording mirrors the existing `App` schema rows at client-api.md:4337-4338.)
- `docs/client-api.md:1372-1381` — extend the botDM `added` JSON example's
  `appInfo` object with both fields.
- `docs/client-api/events.md:112` — the `appInfo` row inlines the field list
  in prose; update `{id, name, assistantName}` to include the two new fields
  and their optionality.
- `docs/client-api/request-reply.md` — no reference to `CounterpartAppInfo`;
  untouched. Note: the `appInfo` prose at request-reply.md:1348-1351 is
  `MessageAppInfo` (the search-hit sender schema), a different struct that
  coincidentally shares the 3-field shape — do **not** "update" it. Since
  CLAUDE.md names both derived views in the same-PR rule, the PR description
  should state this no-reference justification explicitly. The `last synced`
  marker in the derived views is left as-is (recent PRs edit the views without
  touching it).

### 5. Tests (TDD: red first, then the ~5 production lines)

| File | Change |
|---|---|
| `pkg/model/model_test.go` | Extend `TestCounterpartAppInfoJSON`: round-trip a fully-populated struct (both new fields); keep the minimal-app `JSONEq` pinning that zero-valued new fields are **omitted** (assertion unchanged — that's the point of `omitempty`), but rewrite the comment above it (line 1336, "No field is omitempty…"), which becomes false once two of five fields are; add a populated-case `JSONEq` for the exact 5-key wire shape. Extend `TestSubscriptionUpdateEventCounterpartJSON`'s "botDM carries appInfo" case: populate the case's `AppInfo` with both new fields **and** add `wantKeys` for `"description"` / `"appViewUrl"` (with `omitempty`, `wantKeys` alone would never go green). |
| `room-worker/handler_test.go` | `TestHandler_resolveSubUpdateCounterpart`: add a table case where `GetApp` returns `Description` + `AppViewURL` and `wantApp` carries them; existing cases keep proving zero-values pass through as omitted. Update one end-to-end path (`TestServerCreateDM_BotDM_SetsCounterpartAppInfo` or `TestProcessCreateRoom_BotDM_HasIsSubscribed`) so the mocked `GetApp` returns the new fields and the decoded human-side event asserts them. Three sites mock `GetApp` (≈3225-3227, 7093, 7145; the third belongs to `TestServerCreateDM_BotDM_SetsAppNameForHuman`, which asserts RoomName only and stays green) — refresh the two stale comments pinning "the real store's `{"name":1}` projection" (3226, 7144). |
| `room-worker/integration_test.go` | `TestMongoStore_GetApp_Integration`: seed the inserted app with `Description` and `AppViewURL`; assert both come back projected. **Keep** `assert.Nil(t, app.Assistant)` — assistant stays unprojected — and update the comment at line 2401 ("Both projected fields feed appInfo"), since the projection now feeds four. |

No new test files; no mock regeneration. Coverage: the touched functions are
already covered; the new table cases keep the changed lines at 100%.

### 6. Compatibility and rollout

- Additive optional JSON fields; every consumer decodes leniently. Old clients
  ignore the new keys; new clients treat absence as "no data" (same as an app
  doc without the fields). Mixed-version fleet safe; no ordering constraint
  between deploying room-worker and FE adopting the fields.
- No federation impact: `appInfo` is only set by room-worker's `added`
  fan-out, which is core-NATS direct to the user's subject; the one federated
  reuse of this struct (`role_updated`) never carries counterpart fields.
- FE note (out of scope here): `chat-frontend/src/api/types.ts`'s
  `SubscriptionUpdateEvent` mirror predates the counterpart fields entirely —
  it has no `hrInfo`/`appInfo`/`roomName`. Adding them (now including the two
  new fields) is part of FE's consumption work, not this PR.

## Non-goals

- No `app.get(appId)` RPC.
- No change to room-service's `publishSubscriptionUpdate` (it never sets
  counterpart fields) or to any federation payload.
- No change to `AssistantName` sourcing; `assistant` stays unprojected.
- No FE/TypeScript changes in this PR.

## Verification

`make test SERVICE=room-worker`, `make test` (covers `pkg/model`),
`make test-integration SERVICE=room-worker`, `make lint`, `make sast` before
push. Docs-sync rule satisfied by §4 landing in the same PR.
