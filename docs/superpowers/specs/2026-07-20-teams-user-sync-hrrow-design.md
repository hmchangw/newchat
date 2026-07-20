# teams-user-sync: HR projection change (locationURL-derived siteID + engName/mail)

**Date:** 2026-07-20
**Service:** `teams-user-sync` (plus `pkg/model.TeamsUser`)
**Status:** Approved

## Goal

Change the HR lookup so the site assignment is derived from the HR row's
`locationURL` instead of read from a stored `siteID` column, and enrich
`teams_user` with the HR English name and mail. Users with no HR match are no
longer skipped — they are upserted with the HR-derived fields empty.

## Changes

### 1. Model — `pkg/model/teamsuser.go`

Add two fields to `TeamsUser`:

```go
// EngName is the HR system's English name for the account.
EngName string `json:"engName" bson:"engName"`
// Mail is the HR system's mail address for the account.
Mail string `json:"mail" bson:"mail"`
```

- `SiteID` stays, now derived from the HR `locationURL`.
- `LocationURL` is NOT persisted to `teams_user`.
- No `omitempty` on the new fields (consistent with `UPN`/`Account`/`SiteID`);
  HR-unmatched users store `""` for all three HR-derived fields.
- `TeamsUser` is a persisted collection document, not a client-facing
  request/reply or event struct — no `docs/client-api.md` update required.
- Update `pkg/model/model_test.go` round-trip tests (`TestTeamsUserJSON`,
  `TestTeamsUserBSON`, and the `_NoFrom` variants) to cover the new fields.

### 2. Store — `teams-user-sync/store.go`, `store_mongo.go`

- `hrRow` projection becomes:

```go
type hrRow struct {
    AccountName string `bson:"accountName"`
    LocationURL string `bson:"locationURL"`
    EngName     string `bson:"engName"`
    Mail        string `bson:"mail"`
}
```

  (`siteID` removed; Mongo projection updated to
  `{accountName: 1, locationURL: 1, engName: 1, mail: 1}`.)

- Consumer-side type in `store.go` (interface lives with its consumer):

```go
// hrUser is the raw HR data resolved for an account; siteID derivation
// happens in the handler.
type hrUser struct {
    LocationURL string
    EngName     string
    Mail        string
}
```

- Store method rename: `HRSiteIDs(ctx, accounts) (map[string]string, error)` →
  `HRUsers(ctx, accounts) (map[string]hrUser, error)`. Accounts without a
  match remain absent from the map.
- `make generate SERVICE=teams-user-sync` regenerates `mock_store_test.go`.

### 3. Extraction — `teams-user-sync/handler.go`

```go
// extractSiteIDFromLocationURL returns the substring after "://" and before
// ".mysite" (pattern https://{siteID}.mysite.com); "" when either marker is
// absent.
func extractSiteIDFromLocationURL(locationURL string) string {
    _, rest, ok := strings.Cut(locationURL, "://")
    if !ok {
        return ""
    }
    siteID, _, ok := strings.Cut(rest, ".mysite")
    if !ok {
        return ""
    }
    return siteID
}
```

### 4. syncPage + logging — `handler.go`, `main.go`

- `Syncer` gains a `logger *slog.Logger` field, injected via
  `NewSyncer(store, graph, pageSize, logger)`. `main.go` passes its existing
  `slog.With("requestId", idgen.GenerateRequestID())` logger so every line
  carries the request ID.
- Per page, after the HR lookup:
  `logger.Info("hr site ids lookup result", "requested", N, "matched", M, "unmatched", N-M)`
  (N = candidate accounts sent, M = accounts present in the returned map).
- Per candidate:
  - **No HR match:** `logger.Info("hr id not found", "account", c.Account, "userId", c.ID)`;
    `stats.HRUnmatched++` (stat kept — it now means "upserted without HR
    data", not "skipped"); the user IS appended to `merged` with empty
    `SiteID`/`EngName`/`Mail`.
  - **Matched:** set `c.EngName`, `c.Mail` from the hr row. Then:
    - `locationURL == ""` → `logger.Warn("hr locationURL is empty", "account", c.Account)`;
      `SiteID` stays `""`.
    - else `siteID := extractSiteIDFromLocationURL(locationURL)`; if `""` →
      `logger.Warn("extract siteID from locationURL returned empty", "account", c.Account, "locationURL", locationURL)`.
      `c.SiteID = siteID` either way.
  - The user is appended to `merged` in every branch — the
    `len(merged) == 0` early-return now only triggers on an empty candidate
    list.

### 5. Testing (TDD — red/green/refactor per task)

- **`extractSiteIDFromLocationURL`** table-driven tests: valid URL, URL with
  trailing path/port, no `://`, no `.mysite`, empty string,
  `https://.mysite.com` (empty siteID).
- **`syncPage`** table-driven tests (mocked store):
  - HR-unmatched user is upserted with empty HR fields and counted in
    `HRUnmatched`.
  - Matched user with valid locationURL gets derived `SiteID` + `EngName`/`Mail`.
  - Matched user with empty locationURL is upserted with empty `SiteID`.
  - Matched user with malformed locationURL is upserted with empty `SiteID`.
  - Existing tests updated for the renamed mock method and new upsert
    expectations.
- **Model round-trip tests** updated for the new fields (raw BSON key
  assertions include `engName`/`mail`).
- **Integration tests**: hr fixtures gain `locationURL`/`engName`/`mail` and
  drop `siteID`; end-to-end asserts the written `teams_user` doc has the
  derived `siteId` and new fields; store integration tests cover `HRUsers`.

## Downstream impact (reviewed, acceptable)

- `teams-chat-sync` scans all of `teams_user`; newly-upserted HR-unmatched
  users (empty `siteId`) will get chat-sync attention, but chats whose siteID
  vote is empty are already skipped there — safe, just extra work.
- `teams-chat-member-sync` resolves accounts by `_id` — unaffected.

## Out of scope

- **Backfill:** the sync only inserts users missing from `teams_user`
  (`ExistingIDs` diff). Existing docs keep their stored `siteId` and never
  gain `engName`/`mail`. A backfill, if wanted, is a separate change.
- No change to `RunStats` field names or the end-of-run log line shape.
