# user-service API Doc Refinement (`client-api.md` §3.4) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use `- [ ]` checkboxes.

**Goal:** Bring `docs/client-api.md` §3.4 (user-service) fully in line with the API-doc guidelines, every field verified against the Go structs, federation events removed, zero bare `object`/`array` types.

**Architecture:** Documentation-only edits to `docs/client-api.md` (§3.0 shared schemas + §3.4 user-service). No code. Verification = grep checks + field-by-field struct comparison. Ground truth lives in the Go structs listed below.

**Tech Stack:** Markdown; Go structs in `user-service/models/`, `pkg/model/`.

---

## Ground truth (Go structs — the single source of truth)

**Requests** (`user-service/models/`): `StatusGetByNameRequest{name}`, `StatusSetRequest{text, isShow?}`, `SubscriptionListRequest{type, favorite?, updatedWithinDays?}`, `GetChannelsRequest{membersContain?, accountNames?}`, `GetDMRequest{accountName}`, `GetByRoomIDRequest{roomId}`, `CountRequest{unread?}`, `SetAppSubscriptionRequest{appId, subscribed}`, apps.list = no body.

**Responses**: `StatusView{account, statusText, statusIsShow, chineseName?, engName?}`, `SubscriptionListResponse{subscriptions:[]Subscription, total}`, `DMResponse{subscription:DMSubscription}`, `CountResponse{count}`, `AppsListResponse{apps:[]AppListItem, total}`, `OKResponse{success}`.

**`pkg/model.Subscription`** json tags: `id, u(SubscriptionUser), roomId, siteId, roles([]Role string), name, roomType, isSubscribed?, historySharedSince?, joinedAt, lastSeenAt?, hasMention, threadUnread?, alert, muted, favorite, restricted?, externalAccess?, userCount?, lastMsgAt?, lastMsgId?`. (`?` = `omitempty`.) `SubscriptionUser{id, account, isBot}`.

**`pkg/model.SubscriptionHRInfo`** (getDM `hrInfo`): `{account, name, engName}` — all present when parent set. NOTE: distinct from §3.0 `HrInfo{engName, chineseName}`.

**`pkg/model.DMSubscription`** = embedded `*Subscription` (flattens) + `hrInfo?(SubscriptionHRInfo)`.

**`pkg/model.App`**: `id, name, description?, avatarUrl?, assistant?(AppAssistant), channelTab?(AppChannelTab), sponsors?([]AppSponsor)`. `AppListItem` = App flattened + `isSubscribed`.
**`AppAssistant`**: `{enabled, name, settingsUrl?}` — enabled+name always present.
**`AppChannelTab`**: `{enabled, default, name, url}`, `url` = `{default:string}` (`AppChannelTabURL`).
**`AppSponsor`**: `{name, phone}`.

---

## Task 1: §3.0 shared-schema repairs (referenced by §3.4)

**Files:** Modify `docs/client-api.md` §3.0 (lines ~221–342).

- [ ] **Step 1.1 — Add the 3 read-time enrichment fields to the §3.0 `Subscription` table** (after `externalAccess`):

```
| `userCount` | integer | Optional. Member count. Local rooms only; omitted for cross-site subscriptions. |
| `lastMsgAt` | RFC3339 timestamp | Optional. Time of the room's last message (read-time enrichment). |
| `lastMsgId` | string | Optional. Last message ID. Local rooms only; omitted for cross-site subscriptions. |
```

- [ ] **Step 1.2 — Fix §3.0 `AppAssistant`:** change `enabled` from "Optional." to always-present (struct field is non-omitempty):

```
| `enabled` | boolean | Whether the assistant is enabled. |
```

- [ ] **Step 1.3 — Add a new §3.0 `AppChannelTab` schema** (after `AppAssistant`):

```
#### AppChannelTab

An app's channel-tab configuration (`App.channelTab`).

| Field | Type | Notes |
|---|---|---|
| `enabled` | boolean | Whether the tab is enabled. |
| `default` | boolean | Whether the tab appears by default in every channel. |
| `name` | string | Tab display name. |
| `url` | [AppChannelTabURL](#appchanneltaburl) | Tab URL template. |

#### AppChannelTabURL

| Field | Type | Notes |
|---|---|---|
| `default` | string | Canonical URL template with literal `${roomId}` / `${siteId}` placeholders. |
```

- [ ] **Step 1.4 — Add a new §3.0 `AppSponsor` schema** (after AppChannelTabURL):

```
#### AppSponsor

| Field | Type | Notes |
|---|---|---|
| `name` | string | Sponsor name. |
| `phone` | string | Sponsor phone. |
```

- [ ] **Step 1.5 — Add a new §3.0 `SubscriptionHRInfo` schema** (after `HrInfo`), distinct from `HrInfo`:

```
#### SubscriptionHRInfo

The `hrInfo` field on a [DMSubscription](#subscription) — the DM counterpart's HR record.

| Field | Type | Notes |
|---|---|---|
| `account` | string | Counterpart's account. |
| `name` | string | Counterpart's display name. |
| `engName` | string | Counterpart's English name. |
```

- [ ] **Step 1.6 — Update the §3.0 table of contents / shared-schema list** if one enumerates the schemas (search the TOC at top of file for the §3.0 schema names; add `AppChannelTab`, `AppChannelTabURL`, `AppSponsor`, `SubscriptionHRInfo` if the others are listed). If §3.0 schemas aren't individually in the TOC, skip.

- [ ] **Step 1.7 — Verify:** `grep -nE '\| (userCount|lastMsgAt|lastMsgId) \|' docs/client-api.md` shows the new Subscription rows; the 3 new schema headers exist. Commit:

```bash
git add docs/client-api.md
git commit -m "docs(client-api): §3.0 schema repairs — Subscription enrichment fields, AppChannelTab/Sponsor/SubscriptionHRInfo"
```

---

## Task 2: §3.4 status endpoints + remove federation events

**Files:** Modify `docs/client-api.md` §3.4 status section (lines ~3011–3132).

- [ ] **Step 2.1 — Add a one-line intro note** to §3.4 (after the "Migration note" block, ~line 3022): `> **Events:** these endpoints emit no client-facing events. (`status.set` triggers a server-side cross-site federation update, which is internal and not delivered to clients.)`

- [ ] **Step 2.2 — Delete the entire `status.set` "Triggered events — success path" block** (currently lines ~3108–3130, the `UserStatusUpdated` payload table + JSON + the publish-errors sentence).

- [ ] **Step 2.3 — Trim `status.set` intro prose** (~line 3074): replace the sentence referencing the outbox event with a terse statement, e.g. `Sets the calling user's status and returns the updated status view.`

- [ ] **Step 2.4 — Verify `status.getByName` + `status.set` fields against `StatusView` / `StatusSetRequest` / `StatusGetByNameRequest`:** confirm `statusIsShow` always-present (✓ matches struct), `text` ≤512 bytes note, `isShow?` optional, `chineseName?`/`engName?` optional. Fix any mismatch. Both already carry JSON examples — confirm present.

- [ ] **Step 2.5 — Verify + commit:** `grep -n 'UserStatusUpdated\|Triggered events' docs/client-api.md` returns NOTHING within §3.4 (lines 3011–3510).

```bash
git add docs/client-api.md
git commit -m "docs(client-api): §3.4 remove federation event from status.set; verify status fields"
```

---

## Task 3: §3.4 subscription endpoints — eliminate `object`, reference §3.0

**Files:** Modify `docs/client-api.md` §3.4 subscription.* (lines ~3134–3408).

- [ ] **Step 3.1 — `subscription.list` success response:** REPLACE the inline "Subscription fields (key fields …)" partial table (the one containing `u | object | {…}`) with a single reference line:

```
`subscriptions` is an array of [Subscription](#subscription) records (full schema in §3.0), room-info-enriched per the behavior below.
```

Keep the existing "Enrichment behavior" bullet list and the JSON example (the example is valid — leave it). Keep the response field table (`subscriptions | array<[Subscription](#subscription)>`, `total | integer`).

- [ ] **Step 3.2 — `subscription.getDM` success response:** REPLACE the `hrInfo | object | {…}` table with a reference. Set the response as:

```
| `subscription` | [DMSubscription](#subscription) | The enriched DM subscription. |

`DMSubscription` is a [Subscription](#subscription) plus one extra top-level field:

| Field | Type | Notes |
|---|---|---|
| `hrInfo` | [SubscriptionHRInfo](#subscriptionhrinfo) | Optional. DM counterpart's HR record. Present when available. |
```

Leave the JSON example (valid).

- [ ] **Step 3.3 — `subscription.getByRoomID` / `subscription.getChannels` / `subscription.count`:** confirm they reference `Subscription`/the list shape and carry no bare `object`. `getByRoomID` already uses `Subscription[]` (fine — optionally linkify to `[Subscription](#subscription)`). `count` returns `{count:integer}` (fine). Verify request fields against `GetByRoomIDRequest{roomId}`, `GetChannelsRequest{membersContain?, accountNames?}`, `CountRequest{unread?}`.

- [ ] **Step 3.4 — Verify request fields** for `subscription.list` (`type`, `favorite?`, `updatedWithinDays?`) match `SubscriptionListRequest`; confirm `total` typed `integer` (not `number`) consistently OR match the file's prevailing convention — **check what §3.1–3.3 use** (`number` vs `integer`) and follow it; do not introduce a new convention.

- [ ] **Step 3.5 — Verify + commit:** `awk 'NR>=3134 && NR<=3408 && /\| object \|/' docs/client-api.md` returns NOTHING.

```bash
git add docs/client-api.md
git commit -m "docs(client-api): §3.4 subscriptions — reference §3.0 schemas, drop object types"
```

---

## Task 4: §3.4 apps endpoints + final sweep

**Files:** Modify `docs/client-api.md` §3.4 apps (lines ~3410–3510).

- [ ] **Step 4.1 — `subscription.setAppSubscription`:** verify request `{appId, subscribed}` vs `SetAppSubscriptionRequest`; response `{success:boolean}` vs `OKResponse`. JSON example present. No `object`. (Likely already clean — confirm.)

- [ ] **Step 4.2 — `apps.list` success response:** REPLACE the `AppListItem` table's bare types. Target table (matching `App` + `isSubscribed`, all `omitempty` → Optional):

```
`AppListItem` is a flattened [App] record plus `isSubscribed`:

| Field | Type | Notes |
|---|---|---|
| `id` | string | App ID. |
| `name` | string | App display name. |
| `description` | string | Optional. App description. |
| `avatarUrl` | string | Optional. App avatar image URL. |
| `assistant` | [AppAssistant](#appassistant) | Optional. The app's bot assistant. |
| `channelTab` | [AppChannelTab](#appchanneltab) | Optional. Channel-tab configuration. |
| `sponsors` | [AppSponsor](#appsponsor)[] | Optional. App sponsor list. |
| `isSubscribed` | boolean | Whether the calling user is subscribed to this app's bot. |
```

Leave the JSON example (valid; optionally extend with `channelTab`/`sponsors` if desired — not required).

- [ ] **Step 4.3 — Full §3.4 sweep:** `awk 'NR>=3011 && NR<=3520 && /\| object \|/' docs/client-api.md` returns NOTHING; `awk 'NR>=3011 && NR<=3520 && /\| array \|/' docs/client-api.md` returns NOTHING (bare `array` without element type).

- [ ] **Step 4.4 — Confirm every §3.4 success response has a JSON example:** all 9 endpoints have an `##### Success response` followed by a ```json block (getChannels reuses subscription.list's shape — ensure it either shows or explicitly links an example).

- [ ] **Step 4.5 — Commit:**

```bash
git add docs/client-api.md
git commit -m "docs(client-api): §3.4 apps.list — explicit AppAssistant/AppChannelTab/AppSponsor types; final object-type sweep"
```

---

## Self-review checklist (run before final review agents)
- Every §3.4 field table cross-checked field-name + type + optionality vs the structs above.
- No `| object |` and no bare `| array |` anywhere in §3.0 additions or §3.4.
- Every §3.4 success response has a JSON example.
- No `UserStatusUpdated` / "Triggered events" remains in §3.4.
- `type integer` vs `number`: matches the file's prevailing convention.
- Prose minimal; matches §3.1–3.3 style.
