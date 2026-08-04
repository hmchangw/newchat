# Search message response: sender/room enrichment redesign

**Date:** 2026-07-31
**Service:** `search-service` (`search.messages` RPC)
**Builds on:** PR #160 (removed the fat `appInfo` object from `room`)

## Goal

Reshape the `sender` and `room` enrichment objects on each `search.messages`
hit:

- `sender` carries a compact `hr` object for human senders and a compact
  `appInfo` object for bot senders. The server-composed `displayName` is
  removed — the client renders the name.
- `room` regains an `appInfo` object for `botDM` rooms (compact, not the old
  fat `AppSubscription`), including an `isSubscribed` flag from the caller's
  own subscription row. `dm` rooms keep `hrInfo`, rekeyed to `chineseName`.
- No new queries: the existing three batched Mongo lookups and the per-site
  room-name RPC fan-out are reused; only projections change.

## Wire schema

New compact types in `pkg/model/search.go`:

```go
// MessageHRInfo is the compact HR record on search sender/room objects.
type MessageHRInfo struct {
	Account     string `json:"account"`
	ChineseName string `json:"chineseName,omitempty"`
	EngName     string `json:"engName,omitempty"`
}

// MessageAppInfo is the compact app record on search sender/room objects.
// IsSubscribed is set only on room.appInfo (botDM): explicit true/false there,
// absent on sender.appInfo.
type MessageAppInfo struct {
	ID            string `json:"id"`
	Name          string `json:"name"`
	AssistantName string `json:"assistantName"`
	IsSubscribed  *bool  `json:"isSubscribed,omitempty"`
}

type MessageSender struct {
	Account string          `json:"account"`
	HR      *MessageHRInfo  `json:"hr,omitempty"`      // human senders only
	AppInfo *MessageAppInfo `json:"appInfo,omitempty"` // bot senders only
}

type MessageRoom struct {
	ID      string          `json:"id"`
	Name    string          `json:"name,omitempty"`
	Type    RoomType        `json:"type,omitempty"`
	HRInfo  *MessageHRInfo  `json:"hrInfo,omitempty"`  // dm rooms only
	AppInfo *MessageAppInfo `json:"appInfo,omitempty"` // botDM rooms only
}
```

Breaking changes (search response only; other payloads untouched):

- `sender.displayName` removed.
- `room.hrInfo` switches from `SubscriptionHRInfo` (chinese name under
  `"name"`) to `MessageHRInfo` (`"chineseName"`).

### Example payloads

Human sender, dm room:

```json
{
  "sender": {
    "account": "alice",
    "hr": {"account": "alice", "chineseName": "愛麗絲", "engName": "Alice Wang"}
  },
  "room": {
    "id": "r1", "name": "Bob Chan 陳", "type": "dm",
    "hrInfo": {"account": "bob", "chineseName": "陳", "engName": "Bob Chan"}
  }
}
```

Bot sender, botDM room:

```json
{
  "sender": {
    "account": "weather.site-a.bot",
    "appInfo": {"id": "app-1", "name": "Weather App", "assistantName": "weather.site-a.bot"}
  },
  "room": {
    "id": "r2", "name": "Weather App", "type": "botDM",
    "appInfo": {"id": "app-1", "name": "Weather App", "assistantName": "weather.site-a.bot", "isSubscribed": true}
  }
}
```

Channel (default): `room = {id, name, type}` — unchanged.

## Enrichment rules (`search-service/enrich.go`)

Per hit, all best-effort (a lookup miss degrades the field, never fails the
response):

| Case | Result |
|---|---|
| Sender, human | `hr` from the batched users lookup; miss → `{account}` only |
| Sender, bot (`model.IsBot`) | `appInfo{id,name,assistantName}` from the batched apps lookup; miss → `{account}` only |
| Room, `dm` | `name` = combined counterpart display name (fallback counterpart account); `hrInfo` in new shape; users miss → `name` = counterpart account, no `hrInfo` |
| Room, `botDM` | `name` = `app.Name` (fallback subscription name); `appInfo` with `IsSubscribed` (pointer, always set) from the caller's subscription row; apps miss → no `appInfo`, `name` = subscription name |
| Room, channel/unknown | `{id, name, type}` via the room-name RPC — unchanged |

`isSubscribed` semantics: the `isSubscribed` boolean on the caller's own
subscription document for the botDM room (true while actively subscribed,
false after unsubscribing while the room row remains).

## Store changes (`search-service`)

- `SubscriptionsByRoomIDs`: projection adds `isSubscribed`; `SubscriptionMeta`
  gains `IsSubscribed bool`.
- `AppsByAssistantNames`: projection becomes `{_id, name, assistant.name}`
  (adds `_id` for `appInfo.id`; everything else still excluded), and the
  method returns a dedicated `AppRef{ID, Name, AssistantName}` projection
  struct (mirroring `SubscriptionMeta`/`HRUser`) instead of hollow
  `model.App` values.
- No new store methods; no new queries.

## Docs

- `docs/client-api.md`: rewrite the `MessageSender` / `MessageRoom` tables,
  add `MessageHRInfo` / `MessageAppInfo` tables, update the JSON example.
- `docs/client-api/request-reply.md`: mirror the schema line.

## Testing (TDD)

- Unit (`enrich_test.go`): human sender → `hr`; bot sender → `appInfo`; dm
  room `hrInfo` new keys; botDM room `appInfo` with `isSubscribed` true and
  false; apps-miss botDM (no `appInfo`, fallback name); users-miss dm;
  channel unchanged; marshaled JSON contains no `displayName`, and no
  `isSubscribed` under `sender.appInfo`.
- Model (`pkg/model/model_test.go`): round-trip + omitempty for the new
  types.
- Integration (`integration_enrich_test.go`): apps projection returns `_id` +
  `name` + `assistant.name` only; subscriptions projection captures
  `isSubscribed`.

## Out of scope

- Any change to `SubscriptionHRInfo` or DM-subscription payloads elsewhere.
- Any change to search indexing, room-name RPC, or other search RPCs
  (`search.rooms`, `search.apps`, ...).
