# Design: `GET /api/v1/drive.members` (media-service)

Date: 2026-07-17
Service: `media-service`
Branch: `claude/drive-members-endpoint-jj7z5p`

## 1. Purpose

Add an HTTP endpoint to `media-service` that, given a room and an account,
reports whether that account is a member of the room and returns the room's
name and type. It is a single-account membership probe: the response either
lists that one account (member) or an empty list (not a member), with a
hardcoded `count` of 1 or 0.

## 2. Endpoint

```
GET /api/v1/drive.members?roomId=<roomId>&accountName=<accountName>
```

- Unauthenticated, consistent with the other `media-service` HTTP routes
  (registered in `routes.go`).
- Both query params are required.

### 2.1 Query parameters

| Param | Type | Required | Notes |
|-------|------|----------|-------|
| `roomId` | string | yes | Room identifier. |
| `accountName` | string | yes | Account (login) name to probe. |

## 3. Behavior

1. If `roomId` is missing/blank → error `MISSING_PARAMETER` (400).
2. If `accountName` is missing/blank → error `MISSING_PARAMETER` (400).
3. Look up the room by `roomId` (any one of its local subscriptions). If no
   room exists → error `ROOM_NOT_FOUND` (404). Otherwise capture `roomName`
   and `roomType`.
4. Look up the account globally in the `users` collection. If no such user
   exists → error `ACCOUNT_NOT_FOUND` (404). Otherwise capture the user's
   `_id`, `account`, display name, and active flag.
5. Check whether a subscription exists for `(roomId, account)`:
   - **Member** → `members` = `[{ _id, username, name, active }]`, `count` = 1.
   - **Not a member** → `members` = `[]`, `count` = 0.
6. Any store/infra failure during steps 3–5 → error `INTERNAL_ERROR` (500).

"Account not in the room" (step 5, not a member) is a **success** with
`count: 0`. "Account does not exist at all" (step 4) is an **error**. These are
distinct: non-membership is judged from `subscriptions`, account existence from
`users`.

## 4. Responses

### 4.1 Success (`200`)

Wrapper: `{ success, data }`.

```json
{
  "success": true,
  "data": {
    "members": [
      { "_id": "u_123", "username": "alice", "name": "Alice Chan", "active": true }
    ],
    "count": 1,
    "roomName": "General",
    "roomType": "channel"
  }
}
```

Non-member:

```json
{
  "success": true,
  "data": {
    "members": [],
    "count": 0,
    "roomName": "General",
    "roomType": "channel"
  }
}
```

- `count` is hardcoded (0 or 1) — it is not `len(members)` computed generically;
  it mirrors the single probed account's membership.
- `members` is always initialized to an empty slice so it serializes as `[]`,
  never `null`.

### 4.2 Member object field mapping

| JSON field | Source (`model.User`) |
|------------|-----------------------|
| `_id` | `User.ID` |
| `username` | `User.Account` |
| `name` | `User.DisplayName()` (documented "for Drive ownership metadata") |
| `active` | `!User.Deactivated` |

### 4.3 Errors (`4xx` / `5xx`)

Bespoke envelope (NOT the house `errcode`/`errhttp` envelope, because this
endpoint follows a `{success,...}` contract end-to-end):

```json
{ "success": false, "error": "roomId is required", "errorType": "ROOM_NOT_FOUND" }
```

| Condition | HTTP | `errorType` | `error` (example) |
|-----------|------|-------------|-------------------|
| `roomId` missing/blank | 400 | `MISSING_PARAMETER` | `roomId is required` |
| `accountName` missing/blank | 400 | `MISSING_PARAMETER` | `accountName is required` |
| Room not found | 404 | `ROOM_NOT_FOUND` | `room not found` |
| Account not found | 404 | `ACCOUNT_NOT_FOUND` | `account not found` |
| Store/infra failure | 500 | `INTERNAL_ERROR` | `internal error` |

`error` carries a human-readable message; `errorType` is the machine-branchable
discriminator. Internal error messages never leak infra detail (the store's
wrapped error is logged server-side, not returned).

## 5. Implementation

### 5.1 New file: `media-service/drive.go`

- `HandleDriveMembers(c *gin.Context)` — the handler above.
- Response structs (handler-local; this is an HTTP-only payload, not a
  `pkg/model` NATS type):
  ```go
  type driveMember struct {
      ID       string `json:"_id"`
      Username string `json:"username"`
      Name     string `json:"name"`
      Active   bool   `json:"active"`
  }
  type driveMembersData struct {
      Members  []driveMember  `json:"members"`
      Count    int            `json:"count"`
      RoomName string         `json:"roomName"`
      RoomType model.RoomType `json:"roomType"`
  }
  type driveMembersResponse struct {
      Success bool             `json:"success"`
      Data    driveMembersData `json:"data"`
  }
  type driveErrorResponse struct {
      Success   bool   `json:"success"`
      Error     string `json:"error"`
      ErrorType string `json:"errorType"`
  }
  ```
- A small local helper writes the error envelope:
  ```go
  func writeDriveError(c *gin.Context, status int, errorType, msg string)
  ```
  Store failures are logged once server-side (`slog.ErrorContext`) with the
  wrapped error, then returned to the client as the generic `INTERNAL_ERROR`.

### 5.2 Route registration: `media-service/routes.go`

```go
r.GET("/api/v1/drive.members", h.HandleDriveMembers)
```

### 5.3 Store changes: `media-service/store.go` (+ `store_mongo.go`)

Reuse the existing `RoomSite(ctx, roomID) (siteID, roomType, name, found, err)`
for room existence + `roomName` + `roomType` (siteID is ignored here).

Add two methods to the `avatarStore` interface:

```go
// UserByAccount returns a user's identity fields for the drive.members probe.
// found=false when no user record exists for account.
UserByAccount(ctx context.Context, account string) (*model.User, bool, error)

// RoomMember reports whether account has a subscription to roomID.
RoomMember(ctx context.Context, roomID, account string) (bool, error)
```

`store_mongo.go` implementations:
- `UserByAccount` — `users.FindOne({account})` projecting
  `_id, account, engName, chineseName, deactivated`; `ErrNoDocuments` →
  `(nil, false, nil)`.
- `RoomMember` — `subscriptions.FindOne({roomId, "u.account": account})`
  projecting `_id` only (existence check); `ErrNoDocuments` →
  `(false, nil)`.

Both follow the existing store conventions (explicit projection, explicit
`ErrNoDocuments` handling, `fmt.Errorf("...: %w", err)` wrapping).

Regenerate the mock after the interface change: `make generate SERVICE=media-service`.

## 6. Testing (TDD)

New `media-service/drive_test.go`, table-driven, using the existing
`MockavatarStore` test harness (`newTestRouter`). Cases:

- missing `roomId` → 400 / `MISSING_PARAMETER`
- missing `accountName` → 400 / `MISSING_PARAMETER`
- room not found → 404 / `ROOM_NOT_FOUND`
- account not found → 404 / `ACCOUNT_NOT_FOUND`
- member → 200, `count: 1`, `members` has one object with correct
  `_id/username/name/active`, correct `roomName`/`roomType`
- non-member → 200, `count: 0`, `members: []`
- `RoomSite` store error → 500 / `INTERNAL_ERROR`
- `UserByAccount` store error → 500 / `INTERNAL_ERROR`
- `RoomMember` store error → 500 / `INTERNAL_ERROR`
- `active` reflects `Deactivated` (deactivated user → `active: false`)
- `name` falls back to account when name fields are absent (via `DisplayName()`)

Assertions verify the JSON body shape (including `members: []` not `null`) and
HTTP status. Red → Green → Refactor; commit when green.

## 7. Out of scope / notes

- No `docs/client-api.md` update: that rule covers `chat.user.*` NATS subjects
  and `auth-service` HTTP routes, not `media-service` HTTP routes.
- No cross-cluster redirect (unlike the avatar handlers): this is a metadata
  probe, not a blob fetch.
- No new dependencies.
