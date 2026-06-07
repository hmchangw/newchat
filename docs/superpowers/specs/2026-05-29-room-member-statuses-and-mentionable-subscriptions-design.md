# Room Member Statuses and Mentionable Subscriptions — Design

> **Drift from shipped implementation.** This spec was written before the
> final design decisions landed. The following items diverge from what
> shipped — treat the code, tests, and `docs/client-api.md` as the source
> of truth and use the snippets/tables below only as starting points:
>
> - **Bot/admin classification was split.** The spec describes a single
>   `botPattern` regex `(\.bot$|^p_)` used Go-side and Mongo-side. The
>   shipped code splits this into `botAccountRegex = `\.bot$`` (assistant
>   bots) and `platformAdminRegex = `^p_`` (platform-admin / webhook
>   accounts). Only `.bot` accounts classify as `optionType="app"`;
>   `p_` accounts are filtered out at the `$match` stage and never
>   surface in `subscription.mentionable` results.
> - **Membership probe runs in parallel with `GetRoom`.** The pseudocode
>   does both reads sequentially; the shipped code uses a
>   `requireMembershipAndGetRoom` helper (sync.WaitGroup) with
>   membership-error precedence.
> - **Default `subscription.mentionable` limit is 3, not 1.**

## 1. Goal

Add two new NATS request/reply RPCs to `room-service`:

1. **`member.statuses`** — return a bounded list of room members with their display names and presence status. Used by clients to render a "people in this room" panel with live status text.
2. **`subscription.mentionable`** — return a bounded list of room subscriptions (users and apps) filtered by a substring against a searchable keyword built from account/name/app fields. Used by mention autocomplete (`@…`) in the message composer.

Both RPCs share the existing per-room, per-site subject convention and reuse `room-service`'s existing membership-check, error-sanitization, and Mongo aggregation patterns.

## 2. Scope

**In scope**
- Two new handlers in `room-service` (`handler.go`) registered via `RegisterCRUD`.
- Two new store methods (`store.go` interface, `store_mongo.go` implementation).
- Two new subject builders + wildcards in `pkg/subject/subject.go`.
- New request/response types in `pkg/model/member.go`.
- Two new fields on `User` (`StatusIsShow`, `StatusText`) — no backfill of existing user documents.
- Reuse the existing `botPattern` regex (`\.bot$|^p_`) as the single source of truth for "is this a bot/app account?", exposed both Go-side and inside the Mongo pipeline.
- New `docs/client-api.md` sections for both RPCs.
- Unit tests (handler) and integration tests (store) following the repo's TDD rule.

**Out of scope**
- Backfilling existing user documents with default `StatusIsShow` / `StatusText` values — new docs include them, legacy docs decode the zero value, which is the response contract for un-set users.
- Cross-site federation — both RPCs are scoped to the room's owning site; cross-site mention is a separate concern.
- Pagination via offset — both RPCs return at most `limit` results in a single response; no cursor.

## 3. Architecture

### 3.1 Service placement

Both handlers live in `room-service` next to `handleListMembers` because:
- The membership check (`store.GetSubscription(ctx, account, roomID)`) is already there.
- The subscriptions/users/apps Mongo collections are already wired into `MongoStore`.
- The existing `sanitizeError` + `natsutil.ReplyError`/`natsutil.ReplyJSON` plumbing covers the reply shape.

### 3.2 Subject convention

| RPC | Concrete subject | Wildcard (QueueSubscribe) |
|---|---|---|
| Feature 1 | `chat.user.{account}.request.room.{roomID}.{siteID}.member.statuses` | `chat.user.*.request.room.*.{siteID}.member.statuses` |
| Feature 2 | `chat.user.{account}.request.room.{roomID}.{siteID}.subscription.mentionable` | `chat.user.*.request.room.*.{siteID}.subscription.mentionable` |

Both subjects share the 5th-token structure (`room`) and are parseable by the existing `subject.ParseUserRoomSubject(subj)` helper. `{siteID}` must be the room's origin site — same rule as every other room-scoped RPC.

### 3.3 Bot vs platform-admin detection — split regex sources

> **Implementation note:** the final design splits the original combined
> rule into two narrower predicates. `.bot`-suffixed accounts are assistant
> bots; `p_`-prefixed accounts are platform-admin / webhook accounts and
> are a separate classification (not bots). This makes the wire-shape and
> mention-autocomplete contracts more honest about what each class means.

`room-service/helper.go` exposes two package-level constants so the Mongo
pipelines can reference the same patterns the Go-side predicates use:

```go
const botAccountRegex    = `\.bot$`
const platformAdminRegex = `^p_`
```

Go-side, the equivalents are the methods on `model.User`:

```go
func (u *User) IsBot() bool          { return strings.HasSuffix(u.Account, ".bot") }
func (u *User) IsPlatformAdmin() bool { return strings.HasPrefix(u.Account, "p_") }
```

The Feature 2 pipeline uses `botAccountRegex` in its `$regexMatch` stage
to mark `app` rows in the discriminated union, and `platformAdminRegex` in
a `$not` clause to hide platform admins from mention autocomplete entirely.
`TestBotAndAdminPredicate_GoAndMongoAgree_Integration` locks Go-side and
Mongo-side classification in agreement on a probe set covering `.bot`/`p_`/
look-alikes/case-sensitivity edges.

## 4. Model changes (`pkg/model/`)

### 4.1 `user.go` — add two fields

```go
type User struct {
    ID           string `json:"id"           bson:"_id"`
    Account      string `json:"account"      bson:"account"`
    SiteID       string `json:"siteId"       bson:"siteId"`
    SectID       string `json:"sectId"       bson:"sectId"`
    SectName     string `json:"sectName"     bson:"sectName"`
    SectTCName   string `json:"sectTCName"   bson:"sectTCName"`
    DeptID       string `json:"deptId"       bson:"deptId"`
    DeptName     string `json:"deptName"     bson:"deptName"`
    DeptTCName   string `json:"deptTCName"   bson:"deptTCName"`
    EngName      string `json:"engName"      bson:"engName"`
    ChineseName  string `json:"chineseName"  bson:"chineseName"`
    EmployeeID   string `json:"employeeId"   bson:"employeeId"`
    StatusIsShow bool   `json:"statusIsShow" bson:"statusIsShow"` // NEW
    StatusText   string `json:"statusText"   bson:"statusText"`   // NEW
}
```

Legacy documents that pre-date this change decode as `StatusIsShow=false`, `StatusText=""`. That is the response contract for users whose status has never been set.

### 4.2 `member.go` — append new types

```go
// --- Feature 1 ---
type ListMemberStatusesRequest struct {
    Limit *int `json:"limit,omitempty"` // nil → default 3
}

type MemberStatus struct {
    Account      string `json:"account"      bson:"account"`
    EngName      string `json:"engName"      bson:"engName"`
    ChineseName  string `json:"chineseName"  bson:"chineseName"`
    StatusIsShow bool   `json:"statusIsShow" bson:"statusIsShow"`
    StatusText   string `json:"statusText"   bson:"statusText"`
}

type ListMemberStatusesResponse struct {
    Members []MemberStatus `json:"members"`
}

// --- Feature 2 ---
type MentionableSubscriptionsRequest struct {
    Limit  *int   `json:"limit,omitempty"`  // nil → min(3, room.UserCount + room.AppCount)
    Filter string `json:"filter,omitempty"` // empty → match all
}

type MentionableHRInfo struct {
    EngName     string `json:"engName"     bson:"engName"`
    ChineseName string `json:"chineseName" bson:"chineseName"`
}

type MentionableAppAssistant struct {
    Name string `json:"name" bson:"name"`
}

type MentionableApp struct {
    Name      string                  `json:"name"      bson:"name"`
    Assistant MentionableAppAssistant `json:"assistant" bson:"assistant"`
}

type MentionableSubscription struct {
    OptionType string             `json:"optionType" bson:"optionType"` // "user" | "app"
    UserID     string             `json:"userId"     bson:"userId"`
    Account    string             `json:"account"    bson:"account"`
    SiteID     string             `json:"siteId"     bson:"siteId"` // "" for apps
    HRInfo     *MentionableHRInfo `json:"hrInfo,omitempty" bson:"hrInfo,omitempty"`
    App        *MentionableApp    `json:"app,omitempty"    bson:"app,omitempty"`
}

type MentionableSubscriptionsResponse struct {
    Subscriptions []MentionableSubscription `json:"subscriptions"`
}
```

The pointer + `omitempty` pattern on `HRInfo` and `App` makes the union discriminated on the wire: user rows marshal without an `app` key; app rows marshal without an `hrInfo` key.

## 5. Subject builders (`pkg/subject/subject.go`)

Append next to `MemberList` / `MemberListWildcard`:

```go
func MemberStatuses(account, roomID, siteID string) string {
    return fmt.Sprintf("chat.user.%s.request.room.%s.%s.member.statuses", account, roomID, siteID)
}
func MemberStatusesWildcard(siteID string) string {
    return fmt.Sprintf("chat.user.*.request.room.*.%s.member.statuses", siteID)
}

func MentionableSubscriptions(account, roomID, siteID string) string {
    return fmt.Sprintf("chat.user.%s.request.room.%s.%s.subscription.mentionable", account, roomID, siteID)
}
func MentionableSubscriptionsWildcard(siteID string) string {
    return fmt.Sprintf("chat.user.*.request.room.*.%s.subscription.mentionable", siteID)
}
```

No new parser is required — `subject.ParseUserRoomSubject(subj)` already extracts `account` and `roomID` from both shapes.

## 6. Feature 1 — `GetRoomMemberStatuses`

### 6.1 Handler flow (`room-service/handler.go`)

```go
func (h *Handler) natsListMemberStatuses(m otelnats.Msg) {
    ctx := wrappedCtx(m)
    resp, err := h.handleListMemberStatuses(ctx, m.Msg.Subject, m.Msg.Data)
    if err != nil {
        slog.Error("list member statuses failed", "error", err)
        natsutil.ReplyError(m.Msg, sanitizeError(err))
        return
    }
    natsutil.ReplyJSON(m.Msg, resp)
}

func (h *Handler) handleListMemberStatuses(ctx context.Context, subj string, data []byte) (model.ListMemberStatusesResponse, error) {
    requesterAccount, roomID, ok := subject.ParseUserRoomSubject(subj)
    if !ok {
        return model.ListMemberStatusesResponse{}, fmt.Errorf("invalid member-statuses subject")
    }

    // Membership check — sentinel maps to "only room members can perform this action" via sanitizeError.
    if _, err := h.store.GetSubscription(ctx, requesterAccount, roomID); err != nil {
        if errors.Is(err, model.ErrSubscriptionNotFound) {
            return model.ListMemberStatusesResponse{}, errNotRoomMember
        }
        return model.ListMemberStatusesResponse{}, fmt.Errorf("check room membership: %w", err)
    }

    var req model.ListMemberStatusesRequest
    if len(data) > 0 {
        if err := json.Unmarshal(data, &req); err != nil {
            return model.ListMemberStatusesResponse{}, fmt.Errorf("invalid request: %w", err)
        }
    }
    limit := 3
    if req.Limit != nil {
        limit = *req.Limit
    }

    room, err := h.store.GetRoom(ctx, roomID)
    if err != nil {
        return model.ListMemberStatusesResponse{}, fmt.Errorf("get room: %w", err)
    }
    if limit <= 0 || limit > room.UserCount {
        return model.ListMemberStatusesResponse{}, errMemberStatusesLimitInvalid
    }

    members, err := h.store.ListMemberStatuses(ctx, roomID, limit)
    if err != nil {
        return model.ListMemberStatusesResponse{}, fmt.Errorf("list member statuses: %w", err)
    }
    return model.ListMemberStatusesResponse{Members: members}, nil
}
```

Registration in `RegisterCRUD`:

```go
if _, err := nc.QueueSubscribe(subject.MemberStatusesWildcard(h.siteID), queue, h.natsListMemberStatuses); err != nil {
    return fmt.Errorf("subscribe member statuses: %w", err)
}
```

### 6.2 Store method

```go
// In RoomStore interface
ListMemberStatuses(ctx context.Context, roomID string, limit int) ([]model.MemberStatus, error)
```

```go
func (s *MongoStore) ListMemberStatuses(ctx context.Context, roomID string, limit int) ([]model.MemberStatus, error) {
    pipeline := mongo.Pipeline{
        {{Key: "$match", Value: bson.M{"roomId": roomID}}},
        {{Key: "$lookup", Value: bson.M{
            "from": "users",
            "let":  bson.M{"acct": "$u.account"},
            "pipeline": bson.A{
                bson.M{"$match": bson.M{"$expr": bson.M{"$eq": bson.A{"$account", "$$acct"}}}},
                bson.M{"$limit": 1},
                bson.M{"$project": bson.M{
                    "_id":          0,
                    "account":      1,
                    "engName":      1,
                    "chineseName":  1,
                    "statusIsShow": 1,
                    "statusText":   1,
                }},
            },
            "as": "user",
        }}},
        {{Key: "$unwind", Value: bson.M{"path": "$user", "preserveNullAndEmptyArrays": false}}},
        {{Key: "$replaceWith", Value: "$user"}},
        {{Key: "$limit", Value: int64(limit)}},
    }
    cursor, err := s.subscriptions.Aggregate(ctx, pipeline)
    if err != nil {
        return nil, fmt.Errorf("aggregate member statuses for %q: %w", roomID, err)
    }
    defer cursor.Close(ctx)
    members := []model.MemberStatus{}
    if err := cursor.All(ctx, &members); err != nil {
        return nil, fmt.Errorf("decode member statuses for %q: %w", roomID, err)
    }
    return members, nil
}
```

Ordering: `$limit` runs AFTER `$unwind`/`$replaceWith` so the wire contract ("up to `limit` live rows") holds even when the room contains orphan subscriptions whose user document has been hard-deleted. Pre-join `$limit` would silently under-deliver in that case. `preserveNullAndEmptyArrays: false` drops a subscription whose user document has been deleted, rather than returning a half-populated `{account: "", engName: "", …}` row.

## 7. Feature 2 — `GetMentionableSubscriptions`

### 7.1 Handler flow

```go
func (h *Handler) handleListMentionableSubscriptions(ctx context.Context, subj string, data []byte) (model.MentionableSubscriptionsResponse, error) {
    requesterAccount, roomID, ok := subject.ParseUserRoomSubject(subj)
    if !ok {
        return model.MentionableSubscriptionsResponse{}, fmt.Errorf("invalid mentionable-subscriptions subject")
    }

    if _, err := h.store.GetSubscription(ctx, requesterAccount, roomID); err != nil {
        if errors.Is(err, model.ErrSubscriptionNotFound) {
            return model.MentionableSubscriptionsResponse{}, errNotRoomMember
        }
        return model.MentionableSubscriptionsResponse{}, fmt.Errorf("check room membership: %w", err)
    }

    var req model.MentionableSubscriptionsRequest
    if len(data) > 0 {
        if err := json.Unmarshal(data, &req); err != nil {
            return model.MentionableSubscriptionsResponse{}, fmt.Errorf("invalid request: %w", err)
        }
    }
    limit := 3
    if req.Limit != nil {
        limit = *req.Limit
    }

    room, err := h.store.GetRoom(ctx, roomID)
    if err != nil {
        return model.MentionableSubscriptionsResponse{}, fmt.Errorf("get room: %w", err)
    }
    if limit <= 0 || limit > room.UserCount+room.AppCount {
        return model.MentionableSubscriptionsResponse{}, errMentionableLimitInvalid
    }

    // QuoteMeta escapes regex metacharacters; empty filter stays empty (matches everything).
    escapedFilter := regexp.QuoteMeta(req.Filter)

    subs, err := h.store.ListMentionableSubscriptions(ctx, roomID, requesterAccount, escapedFilter, limit)
    if err != nil {
        return model.MentionableSubscriptionsResponse{}, fmt.Errorf("list mentionable subscriptions: %w", err)
    }
    return model.MentionableSubscriptionsResponse{Subscriptions: subs}, nil
}
```

Registration in `RegisterCRUD`:

```go
if _, err := nc.QueueSubscribe(subject.MentionableSubscriptionsWildcard(h.siteID), queue, h.natsListMentionableSubscriptions); err != nil {
    return fmt.Errorf("subscribe mentionable subscriptions: %w", err)
}
```

### 7.2 Store method

```go
// In RoomStore interface
ListMentionableSubscriptions(ctx context.Context, roomID, excludeAccount, escapedFilter string, limit int) ([]model.MentionableSubscription, error)
```

Single-pipeline implementation:

```go
func (s *MongoStore) ListMentionableSubscriptions(
    ctx context.Context, roomID, excludeAccount, escapedFilter string, limit int,
) ([]model.MentionableSubscription, error) {
    pipeline := mongo.Pipeline{
        // 1. Filter the room's subscriptions, excluding the caller and any
        // platform-admin / webhook accounts (`p_` prefix). Hiding platform
        // admins keeps them from appearing as `@`-mention targets.
        {{Key: "$match", Value: bson.M{
            "roomId": roomID,
            "u.account": bson.M{
                "$ne":  excludeAccount,
                "$not": bson.M{"$regex": platformAdminRegex},
            },
        }}},
        // 2. Join the users collection by u.account.
        {{Key: "$lookup", Value: bson.M{
            "from": "users",
            "let":  bson.M{"acct": "$u.account"},
            "pipeline": bson.A{
                bson.M{"$match": bson.M{"$expr": bson.M{"$eq": bson.A{"$account", "$$acct"}}}},
                bson.M{"$limit": 1},
                bson.M{"$project": bson.M{
                    "_id": 0, "account": 1, "engName": 1, "chineseName": 1, "siteId": 1,
                }},
            },
            "as": "_users",
        }}},
        // 3. Join the apps collection where apps.assistant.name == u.account.
        {{Key: "$lookup", Value: bson.M{
            "from": "apps",
            "let":  bson.M{"acct": "$u.account"},
            "pipeline": bson.A{
                bson.M{"$match": bson.M{"$expr": bson.M{"$eq": bson.A{"$assistant.name", "$$acct"}}}},
                bson.M{"$limit": 1},
                bson.M{"$project": bson.M{
                    "_id": 0, "name": 1, "assistant.name": 1,
                }},
            },
            "as": "_apps",
        }}},
        // 4. Classify (isApp via the shared bot regex) and unwrap singleton lookup arrays.
        {{Key: "$addFields", Value: bson.M{
            "isApp":   bson.M{"$regexMatch": bson.M{"input": "$u.account", "regex": botAccountRegex}},
            "userDoc": bson.M{"$arrayElemAt": bson.A{"$_users", 0}},
            "appDoc":  bson.M{"$arrayElemAt": bson.A{"$_apps", 0}},
        }}},
        // 5. Build the dash-joined searchable keyword.
        {{Key: "$addFields", Value: bson.M{
            "keyword": bson.M{"$concat": bson.A{
                bson.M{"$ifNull": bson.A{"$u.account", ""}}, "-",
                bson.M{"$ifNull": bson.A{"$userDoc.engName", ""}}, "-",
                bson.M{"$ifNull": bson.A{"$userDoc.chineseName", ""}}, "-",
                bson.M{"$ifNull": bson.A{"$appDoc.name", ""}}, "-",
                bson.M{"$ifNull": bson.A{"$appDoc.assistant.name", ""}},
            }},
        }}},
        // 6. Case-insensitive substring match.
        {{Key: "$match", Value: bson.M{
            "keyword": bson.M{"$regex": escapedFilter, "$options": "i"},
        }}},
        // 7. Cap the result set.
        {{Key: "$limit", Value: int64(limit)}},
        // 8. Project the final wire shape; $$REMOVE strips the branch that doesn't apply.
        {{Key: "$project", Value: bson.M{
            "_id":        0,
            "optionType": bson.M{"$cond": bson.A{"$isApp", "app", "user"}},
            "userId":     "$u._id",
            "account":    "$u.account",
            "siteId": bson.M{"$cond": bson.A{
                "$isApp",
                "",
                bson.M{"$ifNull": bson.A{"$userDoc.siteId", ""}},
            }},
            "hrInfo": bson.M{"$cond": bson.A{
                "$isApp",
                "$$REMOVE",
                bson.M{
                    "engName":     bson.M{"$ifNull": bson.A{"$userDoc.engName", ""}},
                    "chineseName": bson.M{"$ifNull": bson.A{"$userDoc.chineseName", ""}},
                },
            }},
            "app": bson.M{"$cond": bson.A{
                "$isApp",
                bson.M{
                    "name": bson.M{"$ifNull": bson.A{"$appDoc.name", ""}},
                    "assistant": bson.M{
                        "name": bson.M{"$ifNull": bson.A{"$appDoc.assistant.name", ""}},
                    },
                },
                "$$REMOVE",
            }},
        }}},
    }

    cursor, err := s.subscriptions.Aggregate(ctx, pipeline)
    if err != nil {
        return nil, fmt.Errorf("aggregate mentionable subscriptions for %q: %w", roomID, err)
    }
    defer cursor.Close(ctx)
    subs := []model.MentionableSubscription{}
    if err := cursor.All(ctx, &subs); err != nil {
        return nil, fmt.Errorf("decode mentionable subscriptions for %q: %w", roomID, err)
    }
    return subs, nil
}
```

Edge-case handling baked into the pipeline:
- A subscription whose `u.account` ends in `.bot` but has no matching app document yields `app: {name: "", assistant: {name: ""}}` rather than null leaves.
- A non-bot subscription whose user document is missing yields `hrInfo: {engName: "", chineseName: ""}` and `siteId: ""`.
- An empty `escapedFilter` ("") matches every keyword by Mongo `$regex` semantics, so the empty-filter default is a no-op rather than a special case.

## 8. Errors and sanitization (`room-service/helper.go`)

Two new sentinels:

```go
errMemberStatusesLimitInvalid = errors.New("limit must be > 0 and <= room user count")
errMentionableLimitInvalid    = errors.New("limit must be > 0 and <= room user count + app count")
```

Both added to the `sanitizeError` pass-through switch alongside `errListLimitInvalid` and `errListOffsetInvalid`. No other error-handling changes — both handlers route through the existing `errNotRoomMember` for membership failures and through the existing `default` arm for unrecognised internal errors.

## 9. Testing strategy

Per the project's TDD rule, every test is written and seen failing before the corresponding implementation lands.

### 9.1 Unit tests (`room-service/handler_test.go`)

Mocked store; table-driven where the inputs vary along a single axis.

**`TestHandler_ListMemberStatuses_*`**
- Happy path with default limit (3)
- Explicit limit honoured
- Caller not a room member → `errNotRoomMember`
- `limit = 0` → `errMemberStatusesLimitInvalid`
- `limit < 0` → `errMemberStatusesLimitInvalid`
- `limit > room.UserCount` → `errMemberStatusesLimitInvalid`
- `GetRoom` returns error → wrapped error surfaced
- Store returns error → wrapped error surfaced
- Malformed JSON body → `invalid request`
- Subject failing `ParseUserRoomSubject` → `invalid member-statuses subject`

**`TestHandler_ListMentionableSubscriptions_*`**
- Happy path with default limit (3) and empty filter
- Explicit limit and filter honoured
- Filter containing regex metacharacters (`.`, `(`, `[`) is escaped before reaching the store
- Caller not a member → `errNotRoomMember`
- `limit = 0`, `< 0`, `> room.UserCount + room.AppCount` → `errMentionableLimitInvalid`
- `GetRoom` returns error
- Store returns error
- Malformed JSON body
- Invalid subject
- The `excludeAccount` argument passed to the store equals the requester's account from the subject

### 9.2 Integration tests (`room-service/integration_test.go`, `//go:build integration`)

Reuses the existing `testutil.MongoDB(t, "...")` per-test database from `pkg/testutil`. Seeds subscriptions, users, and apps fixtures, then calls the store directly.

**`TestMongoStore_ListMemberStatuses`**
- 5-field projection shape (`account`, `engName`, `chineseName`, `statusIsShow`, `statusText`)
- Limit enforcement (seed > limit; assert response length)
- Subscription whose user document was deleted is dropped (not returned half-populated)
- Legacy user document missing `statusIsShow`/`statusText` decodes as `false`/`""`

**`TestMongoStore_ListMentionableSubscriptions`**
- Classification: account ending in `.bot` → `optionType="app"`; non-bot → `"user"`
- Classification: account starting with `p_` → `optionType="app"` (matches the shared regex)
- Keyword built with `-` separators across all five source fields
- Case-insensitive substring matches (filter `"jo"` matches `"John"` and `"johnny"`)
- Filter with literal `.` does not act as a regex wildcard (Go-side `QuoteMeta` escaped it)
- Caller's own subscription is excluded
- App row response: no `hrInfo`, `siteId == ""`, `app.name` and `app.assistant.name` populated
- User row response: no `app`, `siteId` from the joined user document
- Orphan bot subscription with no app doc returns `app.name == ""` rather than null
- Limit enforcement

### 9.3 Cross-check tests

`TestBotPattern_GoAndMongoAgree` — for a fixture list of accounts (`alice`, `bob.bot`, `p_assistant`, `weird.botanist`, `p_`), the Go-side `isBot()` and the Mongo `$regexMatch` (verified through a small probe pipeline) classify the same set as bots.

### 9.4 Model round-trip

`pkg/model/model_test.go` — extend the existing `roundTrip` coverage with the new request/response types so JSON tags survive marshal/unmarshal.

### 9.5 Mocks

Run `make generate SERVICE=room-service` after store-interface changes.

## 10. Documentation (`docs/client-api.md`)

Add two new sections following the existing "List Members" template:

- **Get Member Statuses** — subject, request body (`limit`), success response (`members: MemberStatus[]`), error cases.
- **Get Mentionable Subscriptions** — subject, request body (`limit`, `filter`), success response (`subscriptions: MentionableSubscription[]` with discriminated `hrInfo`/`app`), error cases.

Both sections include a worked JSON example mirroring the existing style. Per CLAUDE.md's client-facing-handler rule, these doc updates ship in the same PR as the handler code.

## 11. Reuse summary

| Existing component | Reused for |
|---|---|
| `store.GetSubscription` | Membership check in both handlers |
| `store.GetRoom` | `UserCount` / `AppCount` for limit cap |
| `subject.ParseUserRoomSubject` | Account + roomID extraction for both subjects |
| `errNotRoomMember`, `sanitizeError` | Error mapping in both handlers |
| `botPattern` regex | App detection (Go and Mongo) |
| `natsutil.ReplyJSON` / `natsutil.ReplyError` | Reply plumbing |
| `wrappedCtx` | Request-ID propagation |
| `MongoStore.subscriptions` / `.users` / `.apps` | Collection handles |
| `testutil.MongoDB(t, ...)` | Per-test isolated Mongo DB in integration tests |

## 12. Out-of-band risks

- **`MemberStatusesLimit` default of 3** — intentionally tight; clients that want more must pass an explicit `limit`. Document this default clearly.
- **`MentionableSubscriptionsLimit` default of 3** — raised from the initial proposal of 1 so a no-limit autocomplete request is actually usable in the UI. When `Limit` is nil, the server returns `min(3, room.UserCount + room.AppCount)` rows; explicit values are bounded by the cap and validated.
- **No backfill** — legacy user documents will respond with `statusIsShow=false`, `statusText=""`. If a future migration backfills, no contract change is needed.
- **Single-pipeline cost on Feature 2** — two `$lookup` stages per row. The `$limit` runs after the filter `$match`, so worst-case work is O(roomMembers) before the limit. Acceptable for typical room sizes (`maxRoomSize` is already bounded by config).
