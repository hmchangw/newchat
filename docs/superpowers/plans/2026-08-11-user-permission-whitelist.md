# User Permission Whitelist Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** An append-only permission ledger recording whether a user may view images from outside the corporate network — admin-written over HTTP (API + console), self-queried over NATS, directly readable by Metabase.

**Architecture:** No new service. admin-service (Gin, `requireAdmin`) owns writes into a new `permission_grants` collection (transactional batch insert, latest-wins evaluation, half-open `[from, until)` validity windows anchored to a fixed UTC+8 rule) plus slim per-subject `admin_audit` entries; user-service answers `permission.get` over NATS request/reply where the subject's `{account}` token IS the authenticated identity; admin-frontend gains a Permissions console view. Evaluation is one pure function (`model.EvaluateGrant`) shared by every current and future entry point.

**Tech Stack:** Go 1.25, Gin, NATS (natsrouter), MongoDB driver v2 (no ORM), go.uber.org/mock, testify, testcontainers (via pkg/testutil), React 19 + Vitest (admin-frontend).

**Spec:** `docs/superpowers/specs/2026-08-10-user-permission-whitelist-design.md` — the design authority. When a step here seems to contradict it, the spec wins; stop and flag.

## Global Constraints

- All commands via the root Makefile: `make test SERVICE=<path>` (works for any package path, e.g. `SERVICE=pkg/model`), `make test-integration SERVICE=<name>`, `make generate SERVICE=<name>`, `make lint`, `make fmt`, `make sast`. Never raw `go` commands — sole exception: the coverage spot-check in Task 14 uses CLAUDE.md's documented `go test -coverprofile` procedure, and admin-frontend tests run `cd admin-frontend && npm test` (no make target exists).
- Strict TDD red-green-refactor: failing test first, run and confirm FAIL, minimal implementation, confirm PASS, lint, commit. Never skip the red phase.
- Coverage ≥80% per package; 90%+ on `EvaluateGrant` and handler validation.
- errcode Tier-1 in handlers (`errcode.BadRequest(msg, errcode.WithReason(...))` etc.); adapters `errhttp.Write` (admin-service) / natsrouter automatic (user-service); infra failures return `fmt.Errorf("...: %w", err)`. Never log AND return the same error. Every new reason registered in `allReasons` (`pkg/errcode/codes_test.go`).
- Mongo: explicit projection on every find; camelCase bson tags; `bson:"_id"` for IDs; no `$lookup`. `permission_grants` indexes are created by admin-service ONLY (user-service must not call EnsureIndexes for it).
- NATS subjects only via `pkg/subject` builders — never raw `fmt.Sprintf`.
- Transactions require a replica set: integration tests for transactional store methods use `testutil.MongoDBReplicaSet(t, prefix)`; plain `testutil.MongoDB(t, prefix)` is a STANDALONE Mongo where transactions fail. Read-only repo tests may use the standalone helper.
- Unit tests: same package, table-driven, named subtests, testify, mockgen mocks (regenerate via `make generate`, never hand-edit). Integration tests: `//go:build integration`, `pkg/testutil` containers, `TestMain` via `testutil.RunTests`/`RunTestsWithPrewarm`.
- Dates: the fixed interpretation rule is `time.FixedZone("UTC+8", 8*60*60)` — never `time.Local`, never `time.Parse` without a location (its UTC default is exactly the bug this design avoids). `+1 day` via civil arithmetic `time.Date(y, m, d+1, 0, 0, 0, 0, tzTaipei)`. No tzdata anywhere.
- Commit per task with the exact message given in the task's final step; conventional-commit style; NO AI-provenance trailers.
- One PR at the end; `docs/client-api.md` + `docs/client-api/request-reply.md` land in the same PR (Task 13). Delete any `docs/reviews/` files before creating the PR.

## Task overview (dependency order)

| # | Task | Package | Commit |
|---|---|---|---|
| 1 | PermissionGrant model + EvaluateGrant | pkg/model | `feat(model): permission grant ledger type + latest-wins evaluation` |
| 2 | Permission reason catalog | pkg/errcode | `feat(errcode): permission reason catalog` |
| 3 | permission.get subject builders | pkg/subject | `feat(subject): user permission.get subject builders` |
| 4 | Store: grants + audit batch | admin-service | `feat(admin-service): permission grants store — transactional batch insert, ledger reads, audit batch` |
| 5 | POST /v1/admin/permissions + body limit | admin-service | `feat(admin-service): POST /v1/admin/permissions with body limit` |
| 6 | GET /v1/admin/permissions | admin-service | `feat(admin-service): GET /v1/admin/permissions ledger + current decision` |
| 7 | PermissionRepo (primary reads) | user-service | `feat(user-service): permission grants repository (primary reads)` |
| 8 | permission.get NATS handler | user-service | `feat(user-service): permission.get NATS handler` |
| 9 | Permissions api client + reason copy | admin-frontend | `feat(admin-frontend): permissions api client` |
| 10 | PermissionsView form + nav | admin-frontend | `feat(admin-frontend): PermissionsView grant/revoke form + nav` |
| 11 | PermissionsView ledger lookup | admin-frontend | `feat(admin-frontend): PermissionsView ledger lookup` |
| 12 | docker-local RS + host URIs | docker-local, tools | `fix(docker-local): single-node mongo replica set + host directConnection URIs` |
| 13 | Client API docs | docs | `docs(client-api): permission endpoints + reason catalog` |
| 14 | Full verification pass | all | `chore: full verification pass` (only if fixes needed) |

---
## Phase 1: Shared packages (Tasks 1–3)

### Task 1: pkg/model — PermissionGrant + EvaluateGrant

**Files:**
- Create: `pkg/model/permission.go`
- Create: `pkg/model/permission_test.go`
- Modify: `pkg/model/model_test.go`
- Test: `pkg/model/permission_test.go`, `pkg/model/model_test.go`

**Interfaces:**
- Consumes: none from earlier tasks — this is the foundational task. Uses stdlib `time`, and the pre-existing test helper already defined in `pkg/model/model_test.go` (package `model_test`):
  ```go
  func roundTrip[T any](t *testing.T, src *T, dst *T)
  ```
  which marshals `src` to JSON, unmarshals into `dst`, and asserts `reflect.DeepEqual(*src, *dst)`.
- Produces: `model.PermissionKey` (type `string`); `model.PermissionExternalImageView PermissionKey = "external.image.view"`; `model.MaxReasonRunes = 1000`; `model.MaxSubjects = 200`; `model.KnownPermission(k model.PermissionKey) bool`; the `model.PermissionGrant` struct (fields `ID, SiteID, Permission, SubjectAccount, Granted, EffectiveFrom *time.Time, ExpiresAt *time.Time, ApplicantAccount, ApproverAccount, Reason, RecordedBy string/bool/time.Time` as below); `model.EvaluateGrant(latest *model.PermissionGrant, now time.Time) bool`. Later tasks (admin-service store/handlers, user-service repo/handler) import all of these directly.

- [ ] **Step 1: Write the failing test**

Create `pkg/model/permission_test.go`:

```go
package model_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/model"
)

func TestEvaluateGrant(t *testing.T) {
	now := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	from := now.Add(-24 * time.Hour)
	until := now.Add(24 * time.Hour)

	tests := []struct {
		name  string
		grant *model.PermissionGrant
		now   time.Time
		want  bool
	}{
		{
			name:  "nil ledger denies",
			grant: nil,
			now:   now,
			want:  false,
		},
		{
			name: "revoked row denies",
			grant: &model.PermissionGrant{
				Granted:       false,
				EffectiveFrom: &from,
				ExpiresAt:     &until,
			},
			now:  now,
			want: false,
		},
		{
			name: "nil effectiveFrom denies without panic",
			grant: &model.PermissionGrant{
				Granted:   true,
				ExpiresAt: &until,
			},
			now:  now,
			want: false,
		},
		{
			name: "nil expiresAt denies without panic",
			grant: &model.PermissionGrant{
				Granted:       true,
				EffectiveFrom: &from,
			},
			now:  now,
			want: false,
		},
		{
			name: "now equals effectiveFrom grants (closed start)",
			grant: &model.PermissionGrant{
				Granted:       true,
				EffectiveFrom: &from,
				ExpiresAt:     &until,
			},
			now:  from,
			want: true,
		},
		{
			name: "now equals expiresAt denies (open end)",
			grant: &model.PermissionGrant{
				Granted:       true,
				EffectiveFrom: &from,
				ExpiresAt:     &until,
			},
			now:  until,
			want: false,
		},
		{
			name: "now inside window grants",
			grant: &model.PermissionGrant{
				Granted:       true,
				EffectiveFrom: &from,
				ExpiresAt:     &until,
			},
			now:  now,
			want: true,
		},
		{
			name: "now before window denies",
			grant: &model.PermissionGrant{
				Granted:       true,
				EffectiveFrom: &from,
				ExpiresAt:     &until,
			},
			now:  from.Add(-time.Hour),
			want: false,
		},
		{
			name: "now after window denies",
			grant: &model.PermissionGrant{
				Granted:       true,
				EffectiveFrom: &from,
				ExpiresAt:     &until,
			},
			now:  until.Add(time.Hour),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, model.EvaluateGrant(tt.grant, tt.now))
		})
	}
}

func TestKnownPermission(t *testing.T) {
	tests := []struct {
		name string
		key  model.PermissionKey
		want bool
	}{
		{"known key is recognized", model.PermissionExternalImageView, true},
		{"unknown key is not recognized", model.PermissionKey("external.video.view"), false},
		{"empty key is not recognized", model.PermissionKey(""), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, model.KnownPermission(tt.key))
		})
	}
}
```

Then modify `pkg/model/model_test.go`: find this exact block (the end of the existing `TestUserJSON` function, immediately followed by `TestUserJSON_WithSectAndDept`):

```go
	roundTrip(t, &u, &model.User{})
}

func TestUserJSON_WithSectAndDept(t *testing.T) {
```

and insert two new test functions between them, so the block reads:

```go
	roundTrip(t, &u, &model.User{})
}

func TestPermissionGrantJSON(t *testing.T) {
	from := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	until := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	g := model.PermissionGrant{
		ID:               "0199f2c3a4b5701e8f2a4c6e9b1d3f00",
		SiteID:           "site-a",
		Permission:       model.PermissionExternalImageView,
		SubjectAccount:   "alice",
		Granted:          true,
		EffectiveFrom:    &from,
		ExpiresAt:        &until,
		ApplicantAccount: "carol",
		ApproverAccount:  "dave",
		Reason:           "On-call staff must review production line photos from outside the fab.",
		RecordedBy:       "p_admin_wang",
		RecordedAt:       time.Date(2026, 8, 11, 3, 0, 0, 0, time.UTC),
	}
	roundTrip(t, &g, &model.PermissionGrant{})
}

func TestPermissionGrantRevokeJSON(t *testing.T) {
	g := model.PermissionGrant{
		ID:               "0199f2c3a4b6802f9a3b5d7fac2e4011",
		SiteID:           "site-a",
		Permission:       model.PermissionExternalImageView,
		SubjectAccount:   "alice",
		Granted:          false,
		EffectiveFrom:    nil,
		ExpiresAt:        nil,
		ApplicantAccount: "carol",
		ApproverAccount:  "dave",
		Reason:           "Project ended.",
		RecordedBy:       "p_admin_wang",
		RecordedAt:       time.Date(2026, 8, 11, 3, 5, 0, 0, time.UTC),
	}
	roundTrip(t, &g, &model.PermissionGrant{})
}

func TestUserJSON_WithSectAndDept(t *testing.T) {
```

`model_test.go` already imports `"time"` and `"github.com/hmchangw/chat/pkg/model"` (see its existing header) — no import changes needed there.

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=pkg/model`

Expected: FAIL — a compile error, not a test-assertion failure. `permission_test.go` and the two new functions in `model_test.go` reference `model.PermissionGrant`, `model.EvaluateGrant`, `model.KnownPermission`, `model.PermissionKey`, and `model.PermissionExternalImageView`, none of which exist yet:

```
# github.com/hmchangw/chat/pkg/model_test [github.com/hmchangw/chat/pkg/model.test]
./permission_test.go:19:9: undefined: model.PermissionGrant
./permission_test.go:76:8: undefined: model.PermissionKey
./model_test.go:33:9: undefined: model.PermissionGrant
FAIL	github.com/hmchangw/chat/pkg/model [build failed]
```

(exact line numbers depend on where goimports settles the file; the load-bearing signal is repeated `undefined: model.*` lines ending in `FAIL ... [build failed]`.)

- [ ] **Step 3: Write minimal implementation**

Create `pkg/model/permission.go`:

```go
package model

import "time"

// PermissionKey identifies a whitelist permission.
type PermissionKey string

// PermissionExternalImageView gates viewing images from outside the corporate network.
const PermissionExternalImageView PermissionKey = "external.image.view"

const (
	MaxReasonRunes = 1000
	MaxSubjects    = 200
)

// knownPermissions is the closed set of recognized permission keys.
var knownPermissions = map[PermissionKey]bool{
	PermissionExternalImageView: true,
}

// KnownPermission reports whether k is a recognized permission key.
func KnownPermission(k PermissionKey) bool {
	return knownPermissions[k]
}

// PermissionGrant is one row in the permission_grants append-only ledger:
// insert-only, never updated or deleted. The current decision is not
// stored — EvaluateGrant computes it from the newest row for
// (SiteID, Permission, SubjectAccount).
type PermissionGrant struct {
	ID               string        `json:"id"                      bson:"_id"`
	SiteID           string        `json:"siteId"                  bson:"siteId"`
	Permission       PermissionKey `json:"permission"              bson:"permission"`
	SubjectAccount   string        `json:"subjectAccount"          bson:"subjectAccount"`
	Granted          bool          `json:"granted"                 bson:"granted"`
	EffectiveFrom    *time.Time    `json:"effectiveFrom,omitempty" bson:"effectiveFrom,omitempty"`
	ExpiresAt        *time.Time    `json:"expiresAt,omitempty"     bson:"expiresAt,omitempty"`
	ApplicantAccount string        `json:"applicantAccount"        bson:"applicantAccount"`
	ApproverAccount  string        `json:"approverAccount"         bson:"approverAccount"`
	Reason           string        `json:"reason"                  bson:"reason"`
	RecordedBy       string        `json:"recordedBy"              bson:"recordedBy"`
	RecordedAt       time.Time     `json:"recordedAt"              bson:"recordedAt"`
}

// EvaluateGrant reports whether latest currently grants the permission at
// instant now. This is the single "latest-wins" decision function every
// evaluation site shares, so two callers can never disagree.
func EvaluateGrant(latest *PermissionGrant, now time.Time) bool {
	if latest == nil || !latest.Granted {
		return false
	}
	// A grant row should always carry both bounds. If the data is malformed
	// (manual DB edit, an older bug), deny — never panic, never allow.
	if latest.EffectiveFrom == nil || latest.ExpiresAt == nil {
		return false
	}
	return !now.Before(*latest.EffectiveFrom) && now.Before(*latest.ExpiresAt)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=pkg/model`

Expected: PASS — every subtest (all 9 `TestEvaluateGrant` cases, all 3 `TestKnownPermission` cases, `TestPermissionGrantJSON`, `TestPermissionGrantRevokeJSON`, plus every pre-existing test in the package) prints `ok`:

```
ok  	github.com/hmchangw/chat/pkg/model	0.XXXs
```

- [ ] **Step 5: Run lint**

Run: `make lint`

- [ ] **Step 6: Commit**

```bash
git add pkg/model/permission.go pkg/model/permission_test.go pkg/model/model_test.go
git commit -m "feat(model): permission grant ledger type + latest-wins evaluation"
```

---

### Task 2: pkg/errcode — codes_permission.go

**Files:**
- Create: `pkg/errcode/codes_permission.go`
- Modify: `pkg/errcode/codes_test.go`
- Test: `pkg/errcode/codes_test.go`

**Interfaces:**
- Consumes: none from earlier tasks (independent of Task 1 and Task 3). Uses the pre-existing `type Reason string` (`pkg/errcode/reason.go`) and the pre-existing `var allReasons []Reason` plus `TestReasons_SnakeCase` / `TestReasons_Unique` (`pkg/errcode/codes_test.go`), which assert every entry in `allReasons` is flat snake_case and unique.
- Produces: `errcode.PermissionUnknownKey`, `errcode.PermissionInvalidSubjects`, `errcode.PermissionInvalidReason`, `errcode.PermissionMissingFields`, `errcode.PermissionInvalidWindow`, `errcode.PermissionUnexpectedWindow`, `errcode.PermissionInactiveSubject`, `errcode.PermissionUnknownAccounts` — all of type `errcode.Reason`. Later tasks (admin-service handlers, user-service handler) pair each with a Tier-1 constructor, e.g. `errcode.BadRequest(msg, errcode.WithReason(errcode.PermissionUnknownKey))` / `errcode.NotFound(msg, errcode.WithReason(errcode.PermissionUnknownAccounts))`.

Note on scope: the design spec's §9 code sample shows an inline `// 400` / `// 404` comment after each constant. Those HTTP statuses are not reproduced as comments here — in this codebase a `Reason` is a bare wire string with no HTTP semantics of its own; the status comes from which named constructor (`errcode.BadRequest` vs `errcode.NotFound`) the handler calls in Tasks 5/6/8. Per this task's brief, comment style instead follows `codes_user.go` verbatim: one descriptive header comment above the `const` block, no per-line annotations.

- [ ] **Step 1: Write the failing test**

Modify `pkg/errcode/codes_test.go` — replace the current `allReasons` declaration:

```go
var allReasons = []Reason{
	RoomMaxSizeReached, RoomNotMember, RoomNotOwner,
	RoomLastOwnerCannotLeave, RoomBotInChannel, RoomBotNotAvailable,
	RoomBotCannotBeOwner,
	RoomUserNotFound, RoomInvalidOrg,
	RoomSelfDM, RoomLastMemberCannotRemove, RoomTargetNotMember,
	RoomAlreadyOwner, RoomCannotDemoteLastOwner, RoomPromoteRequiresIndividual,
	RoomNonChannelOperation,
	MessageLargeRoomPostRestricted, MessageNotSubscribed, MessageOutsideAccessWindow,
	PinDisabled, PinLimitReached, PinRoomTooLarge,
	UserAppNotFound, UserAppDisabled, UserSubscriptionNotFound, UserSSOTokenNotFound,
	AuthTokenExpired, AuthInvalidToken, AuthInvalidRequest, AuthInvalidNKey, AuthMissingFields,
	PortalAccountNotReady,
	RequestIDRequired,
	EmojiShortcodeReserved,
	EmojiDeleteDisabled,
}
```

with:

```go
var allReasons = []Reason{
	RoomMaxSizeReached, RoomNotMember, RoomNotOwner,
	RoomLastOwnerCannotLeave, RoomBotInChannel, RoomBotNotAvailable,
	RoomBotCannotBeOwner,
	RoomUserNotFound, RoomInvalidOrg,
	RoomSelfDM, RoomLastMemberCannotRemove, RoomTargetNotMember,
	RoomAlreadyOwner, RoomCannotDemoteLastOwner, RoomPromoteRequiresIndividual,
	RoomNonChannelOperation,
	MessageLargeRoomPostRestricted, MessageNotSubscribed, MessageOutsideAccessWindow,
	PinDisabled, PinLimitReached, PinRoomTooLarge,
	UserAppNotFound, UserAppDisabled, UserSubscriptionNotFound, UserSSOTokenNotFound,
	AuthTokenExpired, AuthInvalidToken, AuthInvalidRequest, AuthInvalidNKey, AuthMissingFields,
	PortalAccountNotReady,
	RequestIDRequired,
	EmojiShortcodeReserved,
	EmojiDeleteDisabled,
	PermissionUnknownKey, PermissionInvalidSubjects, PermissionInvalidReason,
	PermissionMissingFields, PermissionInvalidWindow, PermissionUnexpectedWindow,
	PermissionInactiveSubject, PermissionUnknownAccounts,
}
```

(The rest of `codes_test.go` — `TestReasons_SnakeCase` and `TestReasons_Unique` — is unchanged; they iterate `allReasons`, so the new entries are covered automatically.)

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=pkg/errcode`

Expected: FAIL — a compile error. The eight new identifiers referenced in `allReasons` don't exist yet, so the package fails to build (this is the actual current file layout, so the line numbers below are exact):

```
# github.com/hmchangw/chat/pkg/errcode [github.com/hmchangw/chat/pkg/errcode.test]
./codes_test.go:24:2: undefined: PermissionUnknownKey
./codes_test.go:24:24: undefined: PermissionInvalidSubjects
./codes_test.go:24:51: undefined: PermissionInvalidReason
./codes_test.go:25:2: undefined: PermissionMissingFields
./codes_test.go:25:27: undefined: PermissionInvalidWindow
./codes_test.go:25:52: undefined: PermissionUnexpectedWindow
./codes_test.go:26:2: undefined: PermissionInactiveSubject
./codes_test.go:26:29: undefined: PermissionUnknownAccounts
FAIL	github.com/hmchangw/chat/pkg/errcode [build failed]
```

- [ ] **Step 3: Write minimal implementation**

Create `pkg/errcode/codes_permission.go`:

```go
package errcode

// Permission-domain reason constants; wire values are unprefixed (house style: RoomUserNotFound = "user_not_found").
const (
	PermissionUnknownKey       Reason = "unknown_permission"
	PermissionInvalidSubjects  Reason = "invalid_subject_count"
	PermissionInvalidReason    Reason = "invalid_reason"
	PermissionMissingFields    Reason = "missing_permission_fields"
	PermissionInvalidWindow    Reason = "invalid_permission_window"
	PermissionUnexpectedWindow Reason = "unexpected_permission_window"
	PermissionInactiveSubject  Reason = "inactive_subject"
	PermissionUnknownAccounts  Reason = "unknown_accounts"
)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=pkg/errcode`

Expected: PASS — `TestReasons_SnakeCase` and `TestReasons_Unique` (and every other test in the package) print `ok`:

```
ok  	github.com/hmchangw/chat/pkg/errcode	0.XXXs
```

- [ ] **Step 5: Run lint**

Run: `make lint`

- [ ] **Step 6: Commit**

```bash
git add pkg/errcode/codes_permission.go pkg/errcode/codes_test.go
git commit -m "feat(errcode): permission reason catalog"
```

---

### Task 3: pkg/subject — UserPermissionGet + UserPermissionGetPattern + ParseUserSubject whitelist

**Files:**
- Modify: `pkg/subject/subject.go`
- Modify: `pkg/subject/subject_test.go`
- Test: `pkg/subject/subject_test.go`

**Interfaces:**
- Consumes: none from earlier tasks (independent of Task 1 and Task 2). Uses the pre-existing unexported guard `func isValidAccountToken(token string) bool` (wraps the exported `func IsValidAccountToken(s string) bool`, both already in `pkg/subject/subject.go`) and the pre-existing:
  ```go
  func ParseUserSubject(subj string) (account, siteID, area, action string, ok bool)
  ```
- Produces: `subject.UserPermissionGet(account, siteID string) string` (panics on an invalid/wildcard account token, same as every other `User*Get` builder); `subject.UserPermissionGetPattern(siteID string) string`; `subject.ParseUserSubject` now additionally accepts area `"permission"`. Later tasks (user-service handler registration) call `natsrouter.Register(r, subject.UserPermissionGetPattern(s.siteID), s.GetPermission)` and build the concrete subject via `subject.UserPermissionGet(account, siteID)`.

Note on scope: the extract flags that `ParseUserSubject`'s doc comment (and its `switch`) already omit `"chatlist"` even though a `UserChatlistGet` builder exists — a pre-existing gap, not something this task introduces. This task adds `"permission"` only, to both the `switch` and the doc comment, exactly as spec §6 asks. It deliberately does **not** also add `"chatlist"` — the switch's behavior for chatlist subjects stays exactly as it is today (rejected), and claiming otherwise in the doc comment while I'm already touching that line would make the comment actively wrong instead of merely stale. Fixing the pre-existing chatlist gap is out of scope for this feature.

- [ ] **Step 1: Write the failing test**

Modify `pkg/subject/subject_test.go` in four places. (`TestUserServiceBuilders` tests the concrete builders; `TestUserServicePatternBuilders` — a separate table at subject_test.go:920 — tests every `*Pattern` sibling, including `UserSettingsGetPattern`/`UserSettingsSetPattern`. Both need a new row.)

**(a)** In `TestUserServiceBuilders`, find the last row of the table:

```go
		{"settings.get", subject.UserSettingsGet("alice", "s1"), "chat.user.alice.request.user.s1.settings.get"},
		{"settings.set", subject.UserSettingsSet("alice", "s1"), "chat.user.alice.request.user.s1.settings.set"},
	}
```

and add a new row immediately before the closing `}`:

```go
		{"settings.get", subject.UserSettingsGet("alice", "s1"), "chat.user.alice.request.user.s1.settings.get"},
		{"settings.set", subject.UserSettingsSet("alice", "s1"), "chat.user.alice.request.user.s1.settings.set"},
		{"permission.get", subject.UserPermissionGet("alice", "s1"), "chat.user.alice.request.user.s1.permission.get"},
	}
```

**(b)** In `TestParseUserSubject`, find:

```go
	t.Run("apps.list roundtrips", func(t *testing.T) {
		_, _, area, action, ok := subject.ParseUserSubject(subject.UserAppsList("alice", "s1"))
		assert.True(t, ok)
		assert.Equal(t, "apps", area)
		assert.Equal(t, "list", action)
	})

	t.Run("rejects malformed", func(t *testing.T) {
```

and insert a new subtest between them:

```go
	t.Run("apps.list roundtrips", func(t *testing.T) {
		_, _, area, action, ok := subject.ParseUserSubject(subject.UserAppsList("alice", "s1"))
		assert.True(t, ok)
		assert.Equal(t, "apps", area)
		assert.Equal(t, "list", action)
	})

	t.Run("permission.get roundtrips", func(t *testing.T) {
		_, _, area, action, ok := subject.ParseUserSubject(subject.UserPermissionGet("alice", "s1"))
		assert.True(t, ok)
		assert.Equal(t, "permission", area)
		assert.Equal(t, "get", action)
	})

	t.Run("rejects malformed", func(t *testing.T) {
```

**(c)** In `TestUserServicePatternBuilders`, find the last row of the table:

```go
		{"settings.get", subject.UserSettingsGetPattern("s1"), "chat.user.{account}.request.user.s1.settings.get"},
		{"settings.set", subject.UserSettingsSetPattern("s1"), "chat.user.{account}.request.user.s1.settings.set"},
	}
```

and add a new row immediately before the closing `}`:

```go
		{"settings.get", subject.UserSettingsGetPattern("s1"), "chat.user.{account}.request.user.s1.settings.get"},
		{"settings.set", subject.UserSettingsSetPattern("s1"), "chat.user.{account}.request.user.s1.settings.set"},
		{"permission.get", subject.UserPermissionGetPattern("s1"), "chat.user.{account}.request.user.s1.permission.get"},
	}
```

**(d)** In `TestUserServiceBuildersRejectWildcardAccounts`, find the last row of the table:

```go
		{"UserSettingsGet", func() { subject.UserSettingsGet("*", "s1") }},
		{"UserSettingsSet", func() { subject.UserSettingsSet(">", "s1") }},
	}
```

and add a new row immediately before the closing `}`:

```go
		{"UserSettingsGet", func() { subject.UserSettingsGet("*", "s1") }},
		{"UserSettingsSet", func() { subject.UserSettingsSet(">", "s1") }},
		{"UserPermissionGet", func() { subject.UserPermissionGet("*", "s1") }},
	}
```

No import changes needed — `subject_test.go` already imports `subject`, `assert`, `require`, `strings`, and `time`.

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=pkg/subject`

Expected: FAIL — a compile error. The four edits above reference `subject.UserPermissionGet` and `subject.UserPermissionGetPattern`, neither of which exists yet:

```
# github.com/hmchangw/chat/pkg/subject_test [github.com/hmchangw/chat/pkg/subject.test]
./subject_test.go:628:31: undefined: subject.UserPermissionGet
./subject_test.go:658:47: undefined: subject.UserPermissionGet
./subject_test.go:943:29: undefined: subject.UserPermissionGetPattern
./subject_test.go:822:47: undefined: subject.UserPermissionGet
FAIL	github.com/hmchangw/chat/pkg/subject [build failed]
```

(exact line numbers shift with each of the four insertions above; the load-bearing signal is `undefined: subject.UserPermissionGet` / `undefined: subject.UserPermissionGetPattern` repeated, ending in `FAIL ... [build failed]`.)

- [ ] **Step 3: Write minimal implementation**

Modify `pkg/subject/subject.go` in two places.

**(a)** Add the two new builders. Find:

```go
func UserSettingsSetPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.user.%s.settings.set", siteID)
}

// Chatlist section-definition registry subjects — get + five mutations. The
```

and insert the new functions between them:

```go
func UserSettingsSetPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.user.%s.settings.set", siteID)
}

// UserPermissionGet is the concrete subject for the self permission query.
func UserPermissionGet(account, siteID string) string {
	if !isValidAccountToken(account) {
		panic("invalid account token: contains NATS wildcard characters")
	}
	return fmt.Sprintf("chat.user.%s.request.user.%s.permission.get", account, siteID)
}

func UserPermissionGetPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.user.%s.permission.get", siteID)
}

// Chatlist section-definition registry subjects — get + five mutations. The
```

**(b)** Add `"permission"` to `ParseUserSubject`'s area whitelist and its doc comment. Find:

```go
// ParseUserSubject parses any 8-token subject of the form
//
//	chat.user.{account}.request.user.{siteID}.{area}.{action}
//
// where area is one of "status", "subscription", "profile", "apps", "settings".
// Does NOT match the room-scoped form — use ParseRoomSubject for those.
func ParseUserSubject(subj string) (account, siteID, area, action string, ok bool) {
	parts := strings.Split(subj, ".")
	if len(parts) != 8 {
		return "", "", "", "", false
	}
	if parts[0] != "chat" || parts[1] != "user" || parts[3] != "request" || parts[4] != "user" {
		return "", "", "", "", false
	}
	if !isValidAccountToken(parts[2]) {
		return "", "", "", "", false
	}
	switch parts[6] {
	case "status", "subscription", "profile", "apps", "settings":
	default:
		return "", "", "", "", false
	}
	return parts[2], parts[5], parts[6], parts[7], true
}
```

and replace with:

```go
// ParseUserSubject parses any 8-token subject of the form
//
//	chat.user.{account}.request.user.{siteID}.{area}.{action}
//
// where area is one of "status", "subscription", "profile", "apps", "settings", "permission".
// Does NOT match the room-scoped form — use ParseRoomSubject for those.
func ParseUserSubject(subj string) (account, siteID, area, action string, ok bool) {
	parts := strings.Split(subj, ".")
	if len(parts) != 8 {
		return "", "", "", "", false
	}
	if parts[0] != "chat" || parts[1] != "user" || parts[3] != "request" || parts[4] != "user" {
		return "", "", "", "", false
	}
	if !isValidAccountToken(parts[2]) {
		return "", "", "", "", false
	}
	switch parts[6] {
	case "status", "subscription", "profile", "apps", "settings", "permission":
	default:
		return "", "", "", "", false
	}
	return parts[2], parts[5], parts[6], parts[7], true
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=pkg/subject`

Expected: PASS — `TestUserServiceBuilders`, `TestUserServicePatternBuilders`, `TestParseUserSubject` (including the new `"permission.get roundtrips"` subtest), `TestUserServiceBuildersRejectWildcardAccounts`, and every other test in the package print `ok`:

```
ok  	github.com/hmchangw/chat/pkg/subject	0.XXXs
```

- [ ] **Step 5: Run lint**

Run: `make lint`

- [ ] **Step 6: Commit**

```bash
git add pkg/subject/subject.go pkg/subject/subject_test.go
git commit -m "feat(subject): user permission.get subject builders"
```
## Phase 2: admin-service (Tasks 4–6)

### Task 4: admin-service store — permission grants + audit batch

**Files:**
- `admin-service/store.go` (Modify — interface)
- `admin-service/store_mongo.go` (Modify — impl)
- `admin-service/mock_store_test.go` (Regenerate via `make generate` — never hand-edit)
- `admin-service/integration_test.go` (Modify — new tests)

**Interfaces:**

Consumes (Task 1, `pkg/model/permission.go` — locked, verbatim):
```go
package model

type PermissionKey string

const PermissionExternalImageView PermissionKey = "external.image.view"

const (
    MaxReasonRunes = 1000
    MaxSubjects    = 200
)

type PermissionGrant struct {
    ID               string        `json:"id"                      bson:"_id"`
    SiteID           string        `json:"siteId"                  bson:"siteId"`
    Permission       PermissionKey `json:"permission"              bson:"permission"`
    SubjectAccount   string        `json:"subjectAccount"          bson:"subjectAccount"`
    Granted          bool          `json:"granted"                 bson:"granted"`
    EffectiveFrom    *time.Time    `json:"effectiveFrom,omitempty" bson:"effectiveFrom,omitempty"`
    ExpiresAt        *time.Time    `json:"expiresAt,omitempty"     bson:"expiresAt,omitempty"`
    ApplicantAccount string        `json:"applicantAccount"        bson:"applicantAccount"`
    ApproverAccount  string        `json:"approverAccount"         bson:"approverAccount"`
    Reason           string        `json:"reason"                  bson:"reason"`
    RecordedBy       string        `json:"recordedBy"              bson:"recordedBy"`
    RecordedAt       time.Time     `json:"recordedAt"              bson:"recordedAt"`
}
```
Also consumes existing helpers: `(s *storeMongo) withTransaction(ctx, fn)` (store_mongo.go:213), the `AuditEntry` struct (store.go), `mongo.ErrNoDocuments`, `testutil.MongoDBReplicaSet(t, prefix)`.

Produces (for Task 5/6 and beyond): 5 new `AdminStore` interface methods, implemented on `storeMongo`, plus a regenerated `MockAdminStore` carrying matching mock methods — Task 5/6's handler unit tests mock these directly.

---

- [ ] **Step 1 — RED.** Append the following to `admin-service/integration_test.go` (after the existing `TestIntegration_EnsureIndexes_Idempotent` function, before `TestLoginAndChangePasswordEndToEnd`):

```go
// -------------------------------------------------------------------------
// Permission grants: InsertPermissionGrants (transactional) + ListPermissionGrants
// + GetLatestPermissionGrant (latest-wins) + FindAccountStates + AppendAuditMany
// -------------------------------------------------------------------------

// timePtr returns a pointer to t, for constructing PermissionGrant.EffectiveFrom/ExpiresAt test fixtures.
func timePtr(t time.Time) *time.Time { return &t }

func TestIntegration_InsertPermissionGrants_AllOrNothing(t *testing.T) {
	db := testutil.MongoDBReplicaSet(t, "adminsvc")
	st := newStoreMongo(db)
	require.NoError(t, st.EnsureIndexes(context.Background()))
	ctx := context.Background()

	preexisting := &model.PermissionGrant{
		ID: "dup-id", SiteID: "site-a", Permission: model.PermissionExternalImageView,
		SubjectAccount: "zoe", Granted: false, ApplicantAccount: "carol", ApproverAccount: "dave",
		Reason: "pre-existing row", RecordedBy: "p_admin", RecordedAt: time.Now().UTC(),
	}
	require.NoError(t, st.InsertPermissionGrants(ctx, []*model.PermissionGrant{preexisting}))

	now := time.Now().UTC()
	batch := []*model.PermissionGrant{
		{ID: idgen.GenerateUUIDv7(), SiteID: "site-a", Permission: model.PermissionExternalImageView, SubjectAccount: "alice", Granted: true, EffectiveFrom: timePtr(now), ExpiresAt: timePtr(now.AddDate(0, 1, 0)), ApplicantAccount: "carol", ApproverAccount: "dave", Reason: "batch row 1", RecordedBy: "p_admin", RecordedAt: now},
		{ID: idgen.GenerateUUIDv7(), SiteID: "site-a", Permission: model.PermissionExternalImageView, SubjectAccount: "bob", Granted: true, EffectiveFrom: timePtr(now), ExpiresAt: timePtr(now.AddDate(0, 1, 0)), ApplicantAccount: "carol", ApproverAccount: "dave", Reason: "batch row 2", RecordedBy: "p_admin", RecordedAt: now},
		{ID: "dup-id", SiteID: "site-a", Permission: model.PermissionExternalImageView, SubjectAccount: "carl", Granted: true, EffectiveFrom: timePtr(now), ExpiresAt: timePtr(now.AddDate(0, 1, 0)), ApplicantAccount: "carol", ApproverAccount: "dave", Reason: "batch row 3 — collides with preexisting _id", RecordedBy: "p_admin", RecordedAt: now},
	}

	err := st.InsertPermissionGrants(ctx, batch)
	require.Error(t, err, "a duplicate _id inside the batch must fail the whole transaction")

	count, err := st.permGrants.CountDocuments(ctx, bson.M{"subjectAccount": bson.M{"$in": []string{"alice", "bob"}}})
	require.NoError(t, err)
	assert.Equal(t, int64(0), count, "the transaction must roll back alice and bob too, not just the colliding row")

	total, err := st.permGrants.CountDocuments(ctx, bson.M{})
	require.NoError(t, err)
	assert.Equal(t, int64(1), total, "only the pre-existing row should remain")
}

func TestIntegration_GetLatestPermissionGrant(t *testing.T) {
	db := testutil.MongoDBReplicaSet(t, "adminsvc")
	st := newStoreMongo(db)
	require.NoError(t, st.EnsureIndexes(context.Background()))
	ctx := context.Background()

	t.Run("no rows returns (nil, nil)", func(t *testing.T) {
		got, err := st.GetLatestPermissionGrant(ctx, "site-a", model.PermissionExternalImageView, "nobody")
		require.NoError(t, err)
		assert.Nil(t, got)
	})

	t.Run("newest recordedAt wins", func(t *testing.T) {
		older := time.Now().UTC().Add(-time.Hour)
		newer := time.Now().UTC()
		grants := []*model.PermissionGrant{
			{ID: idgen.GenerateUUIDv7(), SiteID: "site-a", Permission: model.PermissionExternalImageView, SubjectAccount: "dana", Granted: true, EffectiveFrom: timePtr(older), ExpiresAt: timePtr(older.AddDate(0, 1, 0)), ApplicantAccount: "carol", ApproverAccount: "dave", Reason: "first grant", RecordedBy: "p_admin", RecordedAt: older},
			{ID: idgen.GenerateUUIDv7(), SiteID: "site-a", Permission: model.PermissionExternalImageView, SubjectAccount: "dana", Granted: false, ApplicantAccount: "carol", ApproverAccount: "dave", Reason: "revoked later", RecordedBy: "p_admin", RecordedAt: newer},
		}
		require.NoError(t, st.InsertPermissionGrants(ctx, grants))

		latest, err := st.GetLatestPermissionGrant(ctx, "site-a", model.PermissionExternalImageView, "dana")
		require.NoError(t, err)
		require.NotNil(t, latest)
		assert.False(t, latest.Granted, "the newer revoke row must win, not the older grant")
	})

	t.Run("same recordedAt ties break on _id desc", func(t *testing.T) {
		shared := time.Now().UTC()
		grants := []*model.PermissionGrant{
			{ID: "grant-a", SiteID: "site-a", Permission: model.PermissionExternalImageView, SubjectAccount: "erin", Granted: true, EffectiveFrom: timePtr(shared), ExpiresAt: timePtr(shared.AddDate(0, 1, 0)), ApplicantAccount: "carol", ApproverAccount: "dave", Reason: "batch row a", RecordedBy: "p_admin", RecordedAt: shared},
			{ID: "grant-b", SiteID: "site-a", Permission: model.PermissionExternalImageView, SubjectAccount: "erin", Granted: false, ApplicantAccount: "carol", ApproverAccount: "dave", Reason: "batch row b", RecordedBy: "p_admin", RecordedAt: shared},
		}
		require.NoError(t, st.InsertPermissionGrants(ctx, grants))

		latest, err := st.GetLatestPermissionGrant(ctx, "site-a", model.PermissionExternalImageView, "erin")
		require.NoError(t, err)
		require.NotNil(t, latest)
		assert.Equal(t, "grant-b", latest.ID, "identical recordedAt must tie-break on the larger _id")
	})
}

func TestIntegration_FindAccountStates(t *testing.T) {
	db := testutil.MongoDBReplicaSet(t, "adminsvc")
	st := newStoreMongo(db)
	require.NoError(t, st.EnsureIndexes(context.Background()))
	ctx := context.Background()

	activeFlag := false
	require.NoError(t, st.CreateUser(ctx, &model.User{ID: idgen.GenerateUUIDv7(), Account: "active-user", SiteID: "site-a"}))
	require.NoError(t, st.CreateUser(ctx, &model.User{ID: idgen.GenerateUUIDv7(), Account: "inactive-user", SiteID: "site-a", Active: &activeFlag}))

	states, err := st.FindAccountStates(ctx, "site-a", []string{"active-user", "inactive-user", "ghost-user"})
	require.NoError(t, err)

	assert.Equal(t, map[string]bool{"active-user": true, "inactive-user": false}, states, "ghost-user must be absent, not false")

	_, exists := states["ghost-user"]
	assert.False(t, exists, "an account that doesn't exist must not appear in the map at all")
}

func TestIntegration_ListPermissionGrants(t *testing.T) {
	db := testutil.MongoDBReplicaSet(t, "adminsvc")
	st := newStoreMongo(db)
	require.NoError(t, st.EnsureIndexes(context.Background()))
	ctx := context.Background()

	now := time.Now().UTC()
	otherPermission := model.PermissionKey("other.permission")
	grants := []*model.PermissionGrant{
		{ID: idgen.GenerateUUIDv7(), SiteID: "site-a", Permission: model.PermissionExternalImageView, SubjectAccount: "faye", Granted: true, EffectiveFrom: timePtr(now.Add(-3 * time.Hour)), ExpiresAt: timePtr(now.AddDate(0, 1, 0)), ApplicantAccount: "carol", ApproverAccount: "dave", Reason: "r1", RecordedBy: "p_admin", RecordedAt: now.Add(-3 * time.Hour)},
		{ID: idgen.GenerateUUIDv7(), SiteID: "site-a", Permission: model.PermissionExternalImageView, SubjectAccount: "faye", Granted: false, ApplicantAccount: "carol", ApproverAccount: "dave", Reason: "r2", RecordedBy: "p_admin", RecordedAt: now.Add(-2 * time.Hour)},
		{ID: idgen.GenerateUUIDv7(), SiteID: "site-a", Permission: otherPermission, SubjectAccount: "faye", Granted: true, EffectiveFrom: timePtr(now.Add(-1 * time.Hour)), ExpiresAt: timePtr(now.AddDate(0, 1, 0)), ApplicantAccount: "carol", ApproverAccount: "dave", Reason: "r3 — different permission", RecordedBy: "p_admin", RecordedAt: now.Add(-1 * time.Hour)},
		{ID: idgen.GenerateUUIDv7(), SiteID: "site-a", Permission: model.PermissionExternalImageView, SubjectAccount: "other-subject", Granted: true, EffectiveFrom: timePtr(now), ExpiresAt: timePtr(now.AddDate(0, 1, 0)), ApplicantAccount: "carol", ApproverAccount: "dave", Reason: "r4 — different subject", RecordedBy: "p_admin", RecordedAt: now},
	}
	require.NoError(t, st.InsertPermissionGrants(ctx, grants))

	t.Run("filters by siteId+subjectAccount+permission, newest first", func(t *testing.T) {
		results, total, err := st.ListPermissionGrants(ctx, "site-a", "faye", model.PermissionExternalImageView, 1, 10)
		require.NoError(t, err)
		assert.Equal(t, int64(2), total)
		require.Len(t, results, 2)
		assert.Equal(t, "r2", results[0].Reason, "revoke row (recorded later) must come first")
		assert.Equal(t, "r1", results[1].Reason)
	})

	t.Run("permission empty means all permissions for the subject", func(t *testing.T) {
		results, total, err := st.ListPermissionGrants(ctx, "site-a", "faye", "", 1, 10)
		require.NoError(t, err)
		assert.Equal(t, int64(3), total)
		assert.Len(t, results, 3)
	})

	t.Run("pagination – page 1 limit 1", func(t *testing.T) {
		results, total, err := st.ListPermissionGrants(ctx, "site-a", "faye", "", 1, 1)
		require.NoError(t, err)
		assert.Equal(t, int64(3), total)
		assert.Len(t, results, 1)
	})

	t.Run("pagination – page 2 limit 1", func(t *testing.T) {
		results, total, err := st.ListPermissionGrants(ctx, "site-a", "faye", "", 2, 1)
		require.NoError(t, err)
		assert.Equal(t, int64(3), total)
		assert.Len(t, results, 1)
	})

	t.Run("no match returns empty with total 0", func(t *testing.T) {
		results, total, err := st.ListPermissionGrants(ctx, "site-a", "no-such-subject", "", 1, 10)
		require.NoError(t, err)
		assert.Equal(t, int64(0), total)
		assert.Empty(t, results)
	})

	t.Run("every field round-trips through the explicit projection", func(t *testing.T) {
		results, _, err := st.ListPermissionGrants(ctx, "site-a", "other-subject", model.PermissionExternalImageView, 1, 10)
		require.NoError(t, err)
		require.Len(t, results, 1)
		g := results[0]
		assert.NotEmpty(t, g.ID)
		assert.Equal(t, "site-a", g.SiteID)
		assert.Equal(t, model.PermissionExternalImageView, g.Permission)
		assert.Equal(t, "other-subject", g.SubjectAccount)
		assert.True(t, g.Granted)
		require.NotNil(t, g.EffectiveFrom)
		require.NotNil(t, g.ExpiresAt)
		assert.Equal(t, "carol", g.ApplicantAccount)
		assert.Equal(t, "dave", g.ApproverAccount)
		assert.Equal(t, "r4 — different subject", g.Reason)
		assert.Equal(t, "p_admin", g.RecordedBy)
		assert.False(t, g.RecordedAt.IsZero())
	})
}

func TestIntegration_AppendAuditMany(t *testing.T) {
	db := testutil.MongoDBReplicaSet(t, "adminsvc")
	st := newStoreMongo(db)
	require.NoError(t, st.EnsureIndexes(context.Background()))
	ctx := context.Background()

	entries := []*AuditEntry{
		{ID: idgen.GenerateUUIDv7(), ActorUserID: "admin1", ActorAccount: "p_admin", Action: "permission.grant", TargetAccount: "alice", Details: map[string]string{"permission": "external.image.view"}, SiteID: "site-a", Timestamp: time.Now().UTC().UnixMilli()},
		{ID: idgen.GenerateUUIDv7(), ActorUserID: "admin1", ActorAccount: "p_admin", Action: "permission.grant", TargetAccount: "bob", Details: map[string]string{"permission": "external.image.view"}, SiteID: "site-a", Timestamp: time.Now().UTC().UnixMilli()},
	}
	require.NoError(t, st.AppendAuditMany(ctx, entries))

	results, total, err := st.ListAudit(ctx, "site-a", AuditFilter{Action: "permission.grant"}, 1, 10)
	require.NoError(t, err)
	assert.Equal(t, int64(2), total)
	assert.Len(t, results, 2)

	t.Run("empty batch is a no-op, not an error", func(t *testing.T) {
		require.NoError(t, st.AppendAuditMany(ctx, nil))
	})
}

// planStage captures one node of a MongoDB explain winningPlan tree. Decoded
// via bson.Raw so nesting depth doesn't have to be known up front.
type planStage struct {
	Stage      string   `bson:"stage"`
	IndexName  string   `bson:"indexName"`
	InputStage bson.Raw `bson:"inputStage"`
}

// collectStages walks a winningPlan tree and returns every stage name found, outermost first.
func collectStages(t *testing.T, raw bson.Raw) []string {
	t.Helper()
	if len(raw) == 0 {
		return nil
	}
	var s planStage
	require.NoError(t, bson.Unmarshal(raw, &s))
	return append([]string{s.Stage}, collectStages(t, s.InputStage)...)
}

func TestIntegration_ListPermissionGrants_UsesIndexNoInMemorySort(t *testing.T) {
	db := testutil.MongoDBReplicaSet(t, "adminsvc")
	st := newStoreMongo(db)
	require.NoError(t, st.EnsureIndexes(context.Background()))
	ctx := context.Background()

	now := time.Now().UTC()
	require.NoError(t, st.InsertPermissionGrants(ctx, []*model.PermissionGrant{
		{ID: idgen.GenerateUUIDv7(), SiteID: "site-a", Permission: model.PermissionExternalImageView, SubjectAccount: "gina", Granted: true, EffectiveFrom: timePtr(now), ExpiresAt: timePtr(now.AddDate(0, 1, 0)), ApplicantAccount: "carol", ApproverAccount: "dave", Reason: "r1", RecordedBy: "p_admin", RecordedAt: now},
	}))

	cmd := bson.D{
		{Key: "explain", Value: bson.D{
			{Key: "find", Value: "permission_grants"},
			{Key: "filter", Value: bson.D{
				{Key: "siteId", Value: "site-a"},
				{Key: "permission", Value: string(model.PermissionExternalImageView)},
				{Key: "subjectAccount", Value: "gina"},
			}},
			{Key: "sort", Value: bson.D{{Key: "recordedAt", Value: -1}, {Key: "_id", Value: -1}}},
		}},
		{Key: "verbosity", Value: "queryPlanner"},
	}

	var result struct {
		QueryPlanner struct {
			WinningPlan bson.Raw `bson:"winningPlan"`
		} `bson:"queryPlanner"`
	}
	require.NoError(t, db.RunCommand(ctx, cmd).Decode(&result))

	stages := collectStages(t, result.QueryPlanner.WinningPlan)
	assert.Contains(t, stages, "IXSCAN", "the hot query must use index 1, not a collection scan")
	assert.NotContains(t, stages, "SORT", "recordedAt desc,_id desc must come free from the index — no in-memory sort")
}
```

- [ ] **Step 2 — run FAIL.**

```bash
make test-integration SERVICE=admin-service
```

Expected (build failure — none of the 5 methods exist yet on `*storeMongo`, line numbers will vary):
```
# github.com/hmchangw/chat/admin-service [github.com/hmchangw/chat/admin-service.test]
./integration_test.go:XXX:X: st.InsertPermissionGrants undefined (type *storeMongo has no field or method InsertPermissionGrants)
./integration_test.go:XXX:X: st.GetLatestPermissionGrant undefined (type *storeMongo has no field or method GetLatestPermissionGrant)
./integration_test.go:XXX:X: st.FindAccountStates undefined (type *storeMongo has no field or method FindAccountStates)
./integration_test.go:XXX:X: st.ListPermissionGrants undefined (type *storeMongo has no field or method ListPermissionGrants)
./integration_test.go:XXX:X: st.AppendAuditMany undefined (type *storeMongo has no field or method AppendAuditMany)
./integration_test.go:XXX:X: st.permGrants undefined (type *storeMongo has no field or method permGrants)
FAIL	github.com/hmchangw/chat/admin-service [build failed]
```

- [ ] **Step 3 — interface.** In `admin-service/store.go`, insert the following between the `ListAudit` line and the `EnsureIndexes` line:

```go
	// InsertPermissionGrants appends the batch atomically (withTransaction + InsertMany).
	InsertPermissionGrants(ctx context.Context, grants []*model.PermissionGrant) error

	// ListPermissionGrants returns the ledger for one subject newest-first
	// (recordedAt desc, _id desc). permission == "" means all permissions.
	ListPermissionGrants(ctx context.Context, siteID, subjectAccount string, permission model.PermissionKey, page, limit int) ([]model.PermissionGrant, int64, error)

	// GetLatestPermissionGrant returns the newest grant row for the triple, or (nil, nil)
	// when none exists. Full document (no projection trimming needed on admin side).
	GetLatestPermissionGrant(ctx context.Context, siteID string, permission model.PermissionKey, subjectAccount string) (*model.PermissionGrant, error)

	// FindAccountStates returns account -> IsActive() for the accounts that exist at the
	// site; accounts not present in the map do not exist. One query, projection
	// {account:1, active:1}.
	FindAccountStates(ctx context.Context, siteID string, accounts []string) (map[string]bool, error)

	// AppendAuditMany inserts all entries in one InsertMany. Best-effort contract same
	// as AppendAudit (caller logs, never fails the request).
	AppendAuditMany(ctx context.Context, entries []*AuditEntry) error
```

So the interface block reads (full, for placement reference):
```go
	AppendAudit(ctx context.Context, e *AuditEntry) error
	ListAudit(ctx context.Context, siteID string, f AuditFilter, page, limit int) ([]AuditEntry, int64, error)

	// InsertPermissionGrants appends the batch atomically (withTransaction + InsertMany).
	InsertPermissionGrants(ctx context.Context, grants []*model.PermissionGrant) error

	// ListPermissionGrants returns the ledger for one subject newest-first
	// (recordedAt desc, _id desc). permission == "" means all permissions.
	ListPermissionGrants(ctx context.Context, siteID, subjectAccount string, permission model.PermissionKey, page, limit int) ([]model.PermissionGrant, int64, error)

	// GetLatestPermissionGrant returns the newest grant row for the triple, or (nil, nil)
	// when none exists. Full document (no projection trimming needed on admin side).
	GetLatestPermissionGrant(ctx context.Context, siteID string, permission model.PermissionKey, subjectAccount string) (*model.PermissionGrant, error)

	// FindAccountStates returns account -> IsActive() for the accounts that exist at the
	// site; accounts not present in the map do not exist. One query, projection
	// {account:1, active:1}.
	FindAccountStates(ctx context.Context, siteID string, accounts []string) (map[string]bool, error)

	// AppendAuditMany inserts all entries in one InsertMany. Best-effort contract same
	// as AppendAudit (caller logs, never fails the request).
	AppendAuditMany(ctx context.Context, entries []*AuditEntry) error

	EnsureIndexes(ctx context.Context) error
	Ping(ctx context.Context) error
}
```

- [ ] **Step 4 — regenerate mocks.**

```bash
make generate SERVICE=admin-service
```
No stdout on success. Verify:
```bash
grep -c "func (m \*MockAdminStore) InsertPermissionGrants" admin-service/mock_store_test.go
```
Expected: `1`.

- [ ] **Step 5 — implementation.** In `admin-service/store_mongo.go`:

Replace the struct + constructor:
```go
type storeMongo struct {
	users      *mongo.Collection
	adminAudit *mongo.Collection
	permGrants *mongo.Collection
}

func newStoreMongo(db *mongo.Database) *storeMongo {
	return &storeMongo{
		users:      db.Collection("users"),
		adminAudit: db.Collection("admin_audit"),
		permGrants: db.Collection("permission_grants"),
	}
}
```

In `EnsureIndexes`, insert the following immediately before the final `return nil` (after the `admin_audit siteId_targetAccount_timestamp` index block):
```go
	// Backs ListPermissionGrants/GetLatestPermissionGrant: equality prefix
	// (siteId, permission, subjectAccount) + sort suffix (recordedAt, _id) so
	// newest-first ordering comes free from the index (spec §3.6).
	_, err = s.permGrants.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "siteId", Value: 1},
			{Key: "permission", Value: 1},
			{Key: "subjectAccount", Value: 1},
			{Key: "recordedAt", Value: -1},
			{Key: "_id", Value: -1},
		},
	})
	if err != nil {
		return fmt.Errorf("create permission_grants siteId_permission_subjectAccount_recordedAt_id index: %w", err)
	}

	// Backs the audit/BI browse (no subjectAccount equality, so index 1 above doesn't apply).
	_, err = s.permGrants.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "siteId", Value: 1}, {Key: "recordedAt", Value: -1}},
	})
	if err != nil {
		return fmt.Errorf("create permission_grants siteId_recordedAt index: %w", err)
	}

	return nil
}
```

Insert the following new block immediately before `func (s *storeMongo) Ping`:
```go
// permissionGrantProjection returns every PermissionGrant field explicitly —
// house rule "always project precisely" (CLAUDE.md Coding Rules); the admin
// GET renders the full ledger row, so nothing is trimmed.
var permissionGrantProjection = bson.M{
	"_id":              1,
	"siteId":           1,
	"permission":       1,
	"subjectAccount":   1,
	"granted":          1,
	"effectiveFrom":    1,
	"expiresAt":        1,
	"applicantAccount": 1,
	"approverAccount":  1,
	"reason":           1,
	"recordedBy":       1,
	"recordedAt":       1,
}

// InsertPermissionGrants appends the batch atomically (withTransaction +
// InsertMany), so a resend after a partial failure cannot produce duplicate
// rows (spec §4.4 note on step 11).
func (s *storeMongo) InsertPermissionGrants(ctx context.Context, grants []*model.PermissionGrant) error {
	docs := make([]any, len(grants))
	for i, g := range grants {
		docs[i] = g
	}
	return s.withTransaction(ctx, func(ctx context.Context) error {
		if _, err := s.permGrants.InsertMany(ctx, docs); err != nil {
			return fmt.Errorf("insert permission grants: %w", err)
		}
		return nil
	})
}

// ListPermissionGrants returns the ledger for one subject newest-first
// (recordedAt desc, _id desc). permission == "" means all permissions for
// the subject (spec §4.6 — the equality prefix on index 1 breaks, so this
// path falls back to index 2 plus a residual filter; accepted per spec).
func (s *storeMongo) ListPermissionGrants(ctx context.Context, siteID, subjectAccount string, permission model.PermissionKey, page, limit int) ([]model.PermissionGrant, int64, error) {
	filter := bson.M{"siteId": siteID, "subjectAccount": subjectAccount}
	if permission != "" {
		filter["permission"] = permission
	}

	skip := int64((page - 1) * limit)

	total, err := s.permGrants.CountDocuments(ctx, filter)
	if err != nil {
		return nil, 0, fmt.Errorf("count permission grants: %w", err)
	}

	cur, err := s.permGrants.Find(ctx, filter,
		options.Find().
			SetProjection(permissionGrantProjection).
			SetSort(bson.D{{Key: "recordedAt", Value: -1}, {Key: "_id", Value: -1}}).
			SetSkip(skip).
			SetLimit(int64(limit)),
	)
	if err != nil {
		return nil, 0, fmt.Errorf("find permission grants: %w", err)
	}

	var grants []model.PermissionGrant
	if err := cur.All(ctx, &grants); err != nil {
		return nil, 0, fmt.Errorf("decode permission grants: %w", err)
	}
	if grants == nil {
		grants = []model.PermissionGrant{}
	}
	return grants, total, nil
}

// GetLatestPermissionGrant returns the newest grant row for the triple, or
// (nil, nil) when none exists — latest-wins evaluation reads this row
// through model.EvaluateGrant (spec §3.5).
func (s *storeMongo) GetLatestPermissionGrant(ctx context.Context, siteID string, permission model.PermissionKey, subjectAccount string) (*model.PermissionGrant, error) {
	var g model.PermissionGrant
	err := s.permGrants.FindOne(ctx,
		bson.M{"siteId": siteID, "permission": permission, "subjectAccount": subjectAccount},
		options.FindOne().
			SetProjection(permissionGrantProjection).
			SetSort(bson.D{{Key: "recordedAt", Value: -1}, {Key: "_id", Value: -1}}),
	).Decode(&g)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, fmt.Errorf("get latest permission grant: %w", err)
	}
	return &g, nil
}

// accountStateProjection contains only the fields FindAccountStates needs to
// derive IsActive() — never the rest of the user document.
var accountStateProjection = bson.M{"account": 1, "active": 1}

// FindAccountStates returns account -> IsActive() for the accounts that
// exist at the site; accounts not present in the map do not exist. One
// query for the whole batch (spec §4.4 step 10), rather than N lookups.
func (s *storeMongo) FindAccountStates(ctx context.Context, siteID string, accounts []string) (map[string]bool, error) {
	cur, err := s.users.Find(ctx,
		bson.M{"siteId": siteID, "account": bson.M{"$in": accounts}},
		options.Find().SetProjection(accountStateProjection),
	)
	if err != nil {
		return nil, fmt.Errorf("find account states: %w", err)
	}

	var rows []model.User
	if err := cur.All(ctx, &rows); err != nil {
		return nil, fmt.Errorf("decode account states: %w", err)
	}

	states := make(map[string]bool, len(rows))
	for i := range rows {
		states[rows[i].Account] = rows[i].IsActive()
	}
	return states, nil
}

// AppendAuditMany inserts all entries in one InsertMany — a 200-subject
// batch would otherwise cost 200 round trips through AppendAudit. Same
// best-effort contract: the caller logs a failure, never fails the request.
func (s *storeMongo) AppendAuditMany(ctx context.Context, entries []*AuditEntry) error {
	if len(entries) == 0 {
		return nil
	}
	docs := make([]any, len(entries))
	for i, e := range entries {
		docs[i] = e
	}
	if _, err := s.adminAudit.InsertMany(ctx, docs); err != nil {
		return fmt.Errorf("insert audit entries: %w", err)
	}
	return nil
}
```

No new imports are needed in either file — `store.go` already imports `context`, `errors`, `model`; `store_mongo.go` already imports `context`, `errors`, `fmt`, `bson`, `mongo`, `options`, `model`.

- [ ] **Step 6 — run PASS.**

```bash
make test-integration SERVICE=admin-service
```
Expected: `ok  	github.com/hmchangw/chat/admin-service	<time>` — all `TestIntegration_*` tests pass, including the 6 new functions above and the pre-existing ones.

- [ ] **Step 7 — lint.**

```bash
make lint
```
Expected: no output, exit code 0.

- [ ] **Step 8 — commit.**

```bash
git add admin-service/store.go admin-service/store_mongo.go admin-service/mock_store_test.go admin-service/integration_test.go
git commit -m "feat(admin-service): permission grants store — transactional batch insert, ledger reads, audit batch"
```

---

### Task 5: body limit + POST /v1/admin/permissions

**Files:**
- `admin-service/middleware.go` (Modify — `bodyLimit`)
- `admin-service/middleware_test.go` (Modify — `TestBodyLimit`)
- `admin-service/main.go` (Modify — wire the middleware globally)
- `admin-service/routes.go` (Modify — `POST /permissions`)
- `admin-service/permissions.go` (Create)
- `admin-service/permissions_test.go` (Create)

**Interfaces:**

Consumes:
- Task 4's `AdminStore.InsertPermissionGrants`, `.FindAccountStates`, `.AppendAuditMany`, and `MockAdminStore`.
- Task 1's `model.PermissionKey`, `model.PermissionGrant`, `model.KnownPermission`, `model.MaxSubjects`, `model.MaxReasonRunes`.
- Task 2's 8 reason constants (`pkg/errcode/codes_permission.go`): `errcode.PermissionUnknownKey`, `PermissionInvalidSubjects`, `PermissionInvalidReason`, `PermissionMissingFields`, `PermissionInvalidWindow`, `PermissionUnexpectedWindow`, `PermissionInactiveSubject`, `PermissionUnknownAccounts`.
- Existing helpers: `principalFrom(c) session.Session` (middleware.go), `h.cfg.SiteID`, `idgen.GenerateUUIDv7()`, `errhttp.Write`, `errcode.BadRequest`/`NotFound`/`WithReason`, `AuditEntry` (store.go).

Produces (consumed by Task 9's TS client + Task 13's docs — the wire shape of `POST /v1/admin/permissions`):
```json
// 201 response
{
  "created": 2,
  "duplicatesIgnored": [],
  "grants": [
    {"id": "0199f2c3a4b5...", "subjectAccount": "alice"},
    {"id": "0199f2c3a4b6...", "subjectAccount": "bob"}
  ]
}
```
Also produces `tzTaipei`, `parseWindow`, `displayDate`, `displayUntilDate`, `bodyLimit`, `maxPermissionBodyBytes` — consumed by Task 6 (same file) and, for the two pure date helpers, no other task.

---

- [ ] **Step 1 — RED.** Create `admin-service/permissions_test.go`:

```go
package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/session"
)

// setupPermissionsRouter wires h into a Gin engine with the same fake
// requireAdmin principal injection as setupRouter (handler_test.go), plus
// the real bodyLimit middleware, wired to only the permission routes.
func setupPermissionsRouter(h *Handler) *gin.Engine {
	r := gin.New()
	r.Use(bodyLimit(maxPermissionBodyBytes))
	r.Use(func(c *gin.Context) {
		c.Set(ctxPrincipal, session.Session{
			ID:      "sess-1",
			UserID:  "admin-user-id",
			Account: "p_admin",
			SiteID:  "site-A",
			Roles:   []string{"admin"},
		})
		c.Next()
	})
	r.POST("/permissions", h.createPermissions)
	r.GET("/permissions", h.listPermissions)
	return r
}

// strPtr returns a pointer to s, for constructing permissionRequest's
// optional *string date fields in test bodies.
func strPtr(s string) *string { return &s }

// manyAccounts returns n distinct account names, "acct-0".."acct-(n-1)".
func manyAccounts(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = "acct-" + strconv.Itoa(i)
	}
	return out
}

// -------------------------------------------------------------------------
// parseWindow / displayDate / displayUntilDate — pure-function unit tests
// -------------------------------------------------------------------------

func TestParseWindow(t *testing.T) {
	fixedNow := time.Date(2026, 8, 11, 10, 0, 0, 0, time.UTC)
	todayTaipei := fixedNow.In(tzTaipei)

	tests := []struct {
		name      string
		fromStr   *string
		untilStr  *string
		wantErr   bool
		wantFrom  time.Time
		wantUntil time.Time
	}{
		{
			name:      "explicit window, from before until",
			fromStr:   strPtr("2026-09-01"),
			untilStr:  strPtr("2026-12-31"),
			wantFrom:  time.Date(2026, 9, 1, 0, 0, 0, 0, tzTaipei),
			wantUntil: time.Date(2027, 1, 1, 0, 0, 0, 0, tzTaipei),
		},
		{
			name:      "nil effectiveFrom defaults to now",
			fromStr:   nil,
			untilStr:  strPtr("2026-12-31"),
			wantFrom:  fixedNow,
			wantUntil: time.Date(2027, 1, 1, 0, 0, 0, 0, tzTaipei),
		},
		{
			name:      "expiresAt == today (Taipei) is valid, until = tomorrow 00:00 Taipei",
			fromStr:   nil,
			untilStr:  strPtr(todayTaipei.Format("2006-01-02")),
			wantFrom:  fixedNow,
			wantUntil: time.Date(todayTaipei.Year(), todayTaipei.Month(), todayTaipei.Day()+1, 0, 0, 0, 0, tzTaipei),
		},
		{
			name:     "nil expiresAt is rejected — required on a grant",
			fromStr:  nil,
			untilStr: nil,
			wantErr:  true,
		},
		{
			name:     "malformed expiresAt",
			fromStr:  nil,
			untilStr: strPtr("31-12-2026"),
			wantErr:  true,
		},
		{
			name:     "malformed effectiveFrom",
			fromStr:  strPtr("not-a-date"),
			untilStr: strPtr("2026-12-31"),
			wantErr:  true,
		},
		{
			name:     "from after until is rejected",
			fromStr:  strPtr("2026-12-31"),
			untilStr: strPtr("2026-09-01"),
			wantErr:  true,
		},
		{
			name:     "expiresAt in the past is rejected",
			fromStr:  nil,
			untilStr: strPtr("2020-01-01"),
			wantErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			from, until, err := parseWindow(tc.fromStr, tc.untilStr, fixedNow)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.True(t, tc.wantFrom.Equal(from), "from: got %v want %v", from, tc.wantFrom)
			assert.True(t, tc.wantUntil.Equal(until), "until: got %v want %v", until, tc.wantUntil)
		})
	}
}

func TestDisplayDate(t *testing.T) {
	got := displayDate(time.Date(2026, 9, 1, 0, 0, 0, 0, tzTaipei))
	assert.Equal(t, "2026-09-01", got)
}

func TestDisplayUntilDate(t *testing.T) {
	tests := []struct {
		name  string
		input time.Time
		want  string
	}{
		{
			name:  "round-trip: 2026-12-31 request date -> stored until-instant -> displayed back as 2026-12-31",
			input: time.Date(2027, 1, 1, 0, 0, 0, 0, tzTaipei),
			want:  "2026-12-31",
		},
		{
			name:  "civil +1 day across a month boundary",
			input: time.Date(2026, 10, 1, 0, 0, 0, 0, tzTaipei),
			want:  "2026-09-30",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, displayUntilDate(tc.input))
		})
	}
}

// -------------------------------------------------------------------------
// createPermissions — one subtest per spec §4.4 validation rule + success paths
// -------------------------------------------------------------------------

func TestHandler_createPermissions(t *testing.T) {
	knownPermission := string(model.PermissionExternalImageView)

	tests := []struct {
		name       string
		body       map[string]any
		setupMock  func(m *MockAdminStore)
		wantStatus int
		wantReason string
		checkBody  func(t *testing.T, body map[string]any, raw []byte)
	}{
		{
			name: "unknown permission → 400 unknown_permission",
			body: map[string]any{
				"permission": "bogus.permission", "subjectAccounts": []string{"alice"}, "granted": true,
				"effectiveFrom": "2026-09-01", "expiresAt": "2026-12-31",
				"applicantAccount": "carol", "approverAccount": "dave", "reason": "reason text",
			},
			setupMock:  func(m *MockAdminStore) {},
			wantStatus: http.StatusBadRequest,
			wantReason: string(errcode.PermissionUnknownKey),
		},
		{
			name: "empty subjectAccounts → 400 invalid_subject_count",
			body: map[string]any{
				"permission": knownPermission, "subjectAccounts": []string{}, "granted": true,
				"effectiveFrom": "2026-09-01", "expiresAt": "2026-12-31",
				"applicantAccount": "carol", "approverAccount": "dave", "reason": "reason text",
			},
			setupMock:  func(m *MockAdminStore) {},
			wantStatus: http.StatusBadRequest,
			wantReason: string(errcode.PermissionInvalidSubjects),
		},
		{
			name: "201 subjectAccounts → 400 invalid_subject_count",
			body: map[string]any{
				"permission": knownPermission, "subjectAccounts": manyAccounts(201), "granted": true,
				"effectiveFrom": "2026-09-01", "expiresAt": "2026-12-31",
				"applicantAccount": "carol", "approverAccount": "dave", "reason": "reason text",
			},
			setupMock:  func(m *MockAdminStore) {},
			wantStatus: http.StatusBadRequest,
			wantReason: string(errcode.PermissionInvalidSubjects),
		},
		{
			name: "empty reason → 400 invalid_reason",
			body: map[string]any{
				"permission": knownPermission, "subjectAccounts": []string{"alice"}, "granted": true,
				"effectiveFrom": "2026-09-01", "expiresAt": "2026-12-31",
				"applicantAccount": "carol", "approverAccount": "dave", "reason": "",
			},
			setupMock:  func(m *MockAdminStore) {},
			wantStatus: http.StatusBadRequest,
			wantReason: string(errcode.PermissionInvalidReason),
		},
		{
			name: "reason over 1000 runes → 400 invalid_reason",
			body: map[string]any{
				"permission": knownPermission, "subjectAccounts": []string{"alice"}, "granted": true,
				"effectiveFrom": "2026-09-01", "expiresAt": "2026-12-31",
				"applicantAccount": "carol", "approverAccount": "dave", "reason": strings.Repeat("測", 1001),
			},
			setupMock:  func(m *MockAdminStore) {},
			wantStatus: http.StatusBadRequest,
			wantReason: string(errcode.PermissionInvalidReason),
		},
		{
			name: "missing applicantAccount → 400 missing_permission_fields",
			body: map[string]any{
				"permission": knownPermission, "subjectAccounts": []string{"alice"}, "granted": true,
				"effectiveFrom": "2026-09-01", "expiresAt": "2026-12-31",
				"applicantAccount": "", "approverAccount": "dave", "reason": "reason text",
			},
			setupMock:  func(m *MockAdminStore) {},
			wantStatus: http.StatusBadRequest,
			wantReason: string(errcode.PermissionMissingFields),
		},
		{
			name: "missing approverAccount → 400 missing_permission_fields",
			body: map[string]any{
				"permission": knownPermission, "subjectAccounts": []string{"alice"}, "granted": true,
				"effectiveFrom": "2026-09-01", "expiresAt": "2026-12-31",
				"applicantAccount": "carol", "approverAccount": "", "reason": "reason text",
			},
			setupMock:  func(m *MockAdminStore) {},
			wantStatus: http.StatusBadRequest,
			wantReason: string(errcode.PermissionMissingFields),
		},
		{
			name: "grant missing expiresAt → 400 invalid_permission_window",
			body: map[string]any{
				"permission": knownPermission, "subjectAccounts": []string{"alice"}, "granted": true,
				"applicantAccount": "carol", "approverAccount": "dave", "reason": "reason text",
			},
			setupMock:  func(m *MockAdminStore) {},
			wantStatus: http.StatusBadRequest,
			wantReason: string(errcode.PermissionInvalidWindow),
		},
		{
			name: "malformed expiresAt → 400 invalid_permission_window",
			body: map[string]any{
				"permission": knownPermission, "subjectAccounts": []string{"alice"}, "granted": true,
				"expiresAt": "31/12/2026", "applicantAccount": "carol", "approverAccount": "dave", "reason": "reason text",
			},
			setupMock:  func(m *MockAdminStore) {},
			wantStatus: http.StatusBadRequest,
			wantReason: string(errcode.PermissionInvalidWindow),
		},
		{
			name: "effectiveFrom after expiresAt → 400 invalid_permission_window",
			body: map[string]any{
				"permission": knownPermission, "subjectAccounts": []string{"alice"}, "granted": true,
				"effectiveFrom": "2026-12-31", "expiresAt": "2026-09-01",
				"applicantAccount": "carol", "approverAccount": "dave", "reason": "reason text",
			},
			setupMock:  func(m *MockAdminStore) {},
			wantStatus: http.StatusBadRequest,
			wantReason: string(errcode.PermissionInvalidWindow),
		},
		{
			name: "expiresAt in the past → 400 invalid_permission_window",
			body: map[string]any{
				"permission": knownPermission, "subjectAccounts": []string{"alice"}, "granted": true,
				"expiresAt": "2020-01-01", "applicantAccount": "carol", "approverAccount": "dave", "reason": "reason text",
			},
			setupMock:  func(m *MockAdminStore) {},
			wantStatus: http.StatusBadRequest,
			wantReason: string(errcode.PermissionInvalidWindow),
		},
		{
			name: "revoke with effectiveFrom present → 400 unexpected_permission_window",
			body: map[string]any{
				"permission": knownPermission, "subjectAccounts": []string{"alice"}, "granted": false,
				"effectiveFrom": "2026-09-01", "applicantAccount": "carol", "approverAccount": "dave", "reason": "reason text",
			},
			setupMock:  func(m *MockAdminStore) {},
			wantStatus: http.StatusBadRequest,
			wantReason: string(errcode.PermissionUnexpectedWindow),
		},
		{
			name: "revoke with expiresAt present → 400 unexpected_permission_window",
			body: map[string]any{
				"permission": knownPermission, "subjectAccounts": []string{"alice"}, "granted": false,
				"expiresAt": "2026-12-31", "applicantAccount": "carol", "approverAccount": "dave", "reason": "reason text",
			},
			setupMock:  func(m *MockAdminStore) {},
			wantStatus: http.StatusBadRequest,
			wantReason: string(errcode.PermissionUnexpectedWindow),
		},
		{
			name: "duplicate subjects deduped and reported, only unique accounts hit the store",
			body: map[string]any{
				"permission": knownPermission, "subjectAccounts": []string{"alice", "alice", "bob"}, "granted": true,
				"effectiveFrom": "2026-09-01", "expiresAt": "2026-12-31",
				"applicantAccount": "carol", "approverAccount": "dave", "reason": "reason text",
			},
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().FindAccountStates(gomock.Any(), "site-A", gomock.Any()).
					DoAndReturn(func(_ context.Context, _ string, accounts []string) (map[string]bool, error) {
						assert.Equal(t, []string{"alice", "bob"}, accounts, "dedup must run before the account-existence lookup")
						return map[string]bool{"alice": true, "bob": true}, nil
					})
				m.EXPECT().InsertPermissionGrants(gomock.Any(), gomock.Any()).Return(nil)
				m.EXPECT().AppendAuditMany(gomock.Any(), gomock.Any()).Return(nil)
			},
			wantStatus: http.StatusCreated,
			checkBody: func(t *testing.T, body map[string]any, raw []byte) {
				assert.Equal(t, float64(2), body["created"])
				assert.Equal(t, []any{"alice"}, body["duplicatesIgnored"])
			},
		},
		{
			name: "unknown accounts → 404 unknown_accounts, message names them",
			body: map[string]any{
				"permission": knownPermission, "subjectAccounts": []string{"alice", "ghost"}, "granted": true,
				"effectiveFrom": "2026-09-01", "expiresAt": "2026-12-31",
				"applicantAccount": "carol", "approverAccount": "dave", "reason": "reason text",
			},
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().FindAccountStates(gomock.Any(), "site-A", gomock.Any()).
					Return(map[string]bool{"alice": true}, nil)
			},
			wantStatus: http.StatusNotFound,
			wantReason: string(errcode.PermissionUnknownAccounts),
			checkBody: func(t *testing.T, body map[string]any, raw []byte) {
				assert.Contains(t, body["error"], "ghost")
				md, _ := body["metadata"].(map[string]any)
				assert.Equal(t, "ghost", md["accounts"]) // console renders metadata.accounts
			},
		},
		{
			name: "inactive subject → 400 inactive_subject, message names it",
			body: map[string]any{
				"permission": knownPermission, "subjectAccounts": []string{"alice"}, "granted": true,
				"effectiveFrom": "2026-09-01", "expiresAt": "2026-12-31",
				"applicantAccount": "carol", "approverAccount": "dave", "reason": "reason text",
			},
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().FindAccountStates(gomock.Any(), "site-A", gomock.Any()).
					Return(map[string]bool{"alice": false}, nil)
			},
			wantStatus: http.StatusBadRequest,
			wantReason: string(errcode.PermissionInactiveSubject),
			checkBody: func(t *testing.T, body map[string]any, raw []byte) {
				assert.Contains(t, body["error"], "alice")
				md, _ := body["metadata"].(map[string]any)
				assert.Equal(t, "alice", md["accounts"]) // console renders metadata.accounts
			},
		},
		{
			name: "store insert error → 500, no audit call",
			body: map[string]any{
				"permission": knownPermission, "subjectAccounts": []string{"alice"}, "granted": true,
				"effectiveFrom": "2026-09-01", "expiresAt": "2026-12-31",
				"applicantAccount": "carol", "approverAccount": "dave", "reason": "reason text",
			},
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().FindAccountStates(gomock.Any(), "site-A", gomock.Any()).
					Return(map[string]bool{"alice": true}, nil)
				m.EXPECT().InsertPermissionGrants(gomock.Any(), gomock.Any()).
					Return(fmt.Errorf("boom"))
				m.EXPECT().AppendAuditMany(gomock.Any(), gomock.Any()).Times(0)
			},
			wantStatus: http.StatusInternalServerError,
		},
		{
			name: "success grant: 201, store receives the derived instants, audit entries match",
			body: map[string]any{
				"permission": knownPermission, "subjectAccounts": []string{"alice", "bob"}, "granted": true,
				"effectiveFrom": "2026-09-01", "expiresAt": "2026-12-31",
				"applicantAccount": "carol", "approverAccount": "dave", "reason": "reason text",
			},
			setupMock: func(m *MockAdminStore) {
				wantFrom := time.Date(2026, 9, 1, 0, 0, 0, 0, tzTaipei)
				wantUntil := time.Date(2027, 1, 1, 0, 0, 0, 0, tzTaipei)

				m.EXPECT().FindAccountStates(gomock.Any(), "site-A", gomock.Any()).
					Return(map[string]bool{"alice": true, "bob": true}, nil)
				m.EXPECT().InsertPermissionGrants(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, grants []*model.PermissionGrant) error {
						require.Len(t, grants, 2)
						for _, g := range grants {
							assert.Equal(t, "site-A", g.SiteID)
							assert.Equal(t, model.PermissionExternalImageView, g.Permission)
							assert.True(t, g.Granted)
							require.NotNil(t, g.EffectiveFrom)
							require.NotNil(t, g.ExpiresAt)
							assert.True(t, wantFrom.Equal(*g.EffectiveFrom), "effectiveFrom: got %v want %v", *g.EffectiveFrom, wantFrom)
							assert.True(t, wantUntil.Equal(*g.ExpiresAt), "expiresAt: got %v want %v", *g.ExpiresAt, wantUntil)
							assert.Equal(t, "carol", g.ApplicantAccount)
							assert.Equal(t, "dave", g.ApproverAccount)
							assert.Equal(t, "p_admin", g.RecordedBy)
							assert.NotEmpty(t, g.ID)
						}
						return nil
					})
				m.EXPECT().AppendAuditMany(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, entries []*AuditEntry) error {
						require.Len(t, entries, 2)
						for _, e := range entries {
							assert.Equal(t, auditActionPermissionGrant, e.Action)
							assert.Equal(t, map[string]string{"permission": knownPermission}, e.Details)
							assert.Equal(t, "p_admin", e.ActorAccount)
						}
						return nil
					})
			},
			wantStatus: http.StatusCreated,
			checkBody: func(t *testing.T, body map[string]any, raw []byte) {
				assert.Equal(t, float64(2), body["created"])
				assert.Equal(t, []any{}, body["duplicatesIgnored"])
				grants, ok := body["grants"].([]any)
				require.True(t, ok)
				assert.Len(t, grants, 2)
			},
		},
		{
			name: "success revoke: stored rows carry nil window pointers",
			body: map[string]any{
				"permission": knownPermission, "subjectAccounts": []string{"alice"}, "granted": false,
				"applicantAccount": "carol", "approverAccount": "dave", "reason": "project ended",
			},
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().FindAccountStates(gomock.Any(), "site-A", gomock.Any()).
					Return(map[string]bool{"alice": true}, nil)
				m.EXPECT().InsertPermissionGrants(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, grants []*model.PermissionGrant) error {
						require.Len(t, grants, 1)
						assert.False(t, grants[0].Granted)
						assert.Nil(t, grants[0].EffectiveFrom)
						assert.Nil(t, grants[0].ExpiresAt)
						return nil
					})
				m.EXPECT().AppendAuditMany(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, entries []*AuditEntry) error {
						require.Len(t, entries, 1)
						assert.Equal(t, auditActionPermissionRevoke, entries[0].Action)
						return nil
					})
			},
			wantStatus: http.StatusCreated,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			m := NewMockAdminStore(ctrl)
			tc.setupMock(m)

			h := newHandler(m, emptySessionStore(), testCfg(), nil)
			r := setupPermissionsRouter(h)

			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/permissions", bodyBytes(t, tc.body))
			req.Header.Set("Content-Type", "application/json")
			r.ServeHTTP(w, req)

			assert.Equal(t, tc.wantStatus, w.Code)
			if tc.wantReason != "" {
				body := respBody(t, w)
				assert.Equal(t, tc.wantReason, body["reason"])
			}
			if tc.checkBody != nil {
				body := respBody(t, w)
				tc.checkBody(t, body, w.Body.Bytes())
			}
		})
	}
}
```

Also append to `admin-service/middleware_test.go` (`TestBodyLimit`). Replace the existing import block:
```go
import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/session"
)
```
with:
```go
import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
	"github.com/hmchangw/chat/pkg/session"
)
```

Then append this new test function at the end of the file:
```go
func TestBodyLimit(t *testing.T) {
	tests := []struct {
		name       string
		bodyBytes  int
		wantStatus int
	}{
		{"body within the limit passes through to the handler", 10, http.StatusOK},
		{"body over the limit fails JSON binding → 400", maxPermissionBodyBytes + 1024, http.StatusBadRequest},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := gin.New()
			r.Use(bodyLimit(maxPermissionBodyBytes))
			r.POST("/echo", func(c *gin.Context) {
				var body map[string]any
				if err := c.ShouldBindJSON(&body); err != nil {
					errhttp.Write(c.Request.Context(), c, errcode.BadRequest("invalid request body",
						errcode.WithReason(errcode.AuthMissingFields)))
					return
				}
				c.JSON(http.StatusOK, gin.H{"status": "ok"})
			})

			payload := []byte(`{"pad":"` + strings.Repeat("a", tc.bodyBytes) + `"}`)
			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/echo", bytes.NewReader(payload))
			req.Header.Set("Content-Type", "application/json")
			r.ServeHTTP(w, req)

			assert.Equal(t, tc.wantStatus, w.Code)
		})
	}
}
```

- [ ] **Step 2 — run FAIL.**

```bash
make test SERVICE=admin-service
```
Expected (build failure — none of these symbols exist yet; line numbers will vary):
```
# github.com/hmchangw/chat/admin-service [github.com/hmchangw/chat/admin-service.test]
./middleware_test.go:XX:X: undefined: bodyLimit
./permissions_test.go:XX:X: undefined: maxPermissionBodyBytes
./permissions_test.go:XX:X: h.createPermissions undefined (type *Handler has no field or method createPermissions)
./permissions_test.go:XX:X: h.listPermissions undefined (type *Handler has no field or method listPermissions)
./permissions_test.go:XX:X: undefined: tzTaipei
./permissions_test.go:XX:X: undefined: parseWindow
./permissions_test.go:XX:X: undefined: displayDate
./permissions_test.go:XX:X: undefined: displayUntilDate
./permissions_test.go:XX:X: undefined: auditActionPermissionGrant
./permissions_test.go:XX:X: undefined: auditActionPermissionRevoke
FAIL	github.com/hmchangw/chat/admin-service [build failed]
```
(`h.listPermissions` fails here too because `setupPermissionsRouter` references it — Task 6's handler is written in Step 4 below alongside Task 5's, as a stub is not worth a separate cycle; see Step 4 note.)

- [ ] **Step 3 — middleware.** In `admin-service/middleware.go`, add `"net/http"` to the import block:
```go
import (
	"net/http"
	"slices"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/session"
	"github.com/hmchangw/chat/pkg/sessiontoken"
)
```
Append at the end of the file (after `principalFrom`):
```go

// bodyLimit caps request bodies at max bytes; a caller that exceeds it gets a
// truncated read, which fails ShouldBindJSON downstream and surfaces as an
// ordinary 400 (spec §9 — no 413 in this service's closed Code set).
func bodyLimit(max int64) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, max)
		c.Next()
	}
}
```

- [ ] **Step 4 — implementation.** Create `admin-service/permissions.go`. This step also defines `listPermissions`'s SIGNATURE (empty body returning 501) purely so the package compiles for this step's PASS run — Task 6 fills in its real body in the next task's own red/green cycle:

```go
package main

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
)

// tzTaipei is the fixed date-interpretation rule (spec §8.1). Never time.Local.
var tzTaipei = time.FixedZone("UTC+8", 8*60*60)

const (
	maxPermissionBodyBytes = 1 << 20 // 1MB

	auditActionPermissionGrant  = "permission.grant"
	auditActionPermissionRevoke = "permission.revoke"
)

// permissionRequest is the request body for POST /v1/admin/permissions.
// EffectiveFrom/ExpiresAt are pointers so the handler can distinguish
// "omitted" (nil) from "empty string" — required for the revoke-must-omit
// rule (spec §4.4 step 9).
type permissionRequest struct {
	Permission       string   `json:"permission"`
	SubjectAccounts  []string `json:"subjectAccounts"`
	Granted          bool     `json:"granted"`
	EffectiveFrom    *string  `json:"effectiveFrom"` // "2026-09-01"; nil on grant = now
	ExpiresAt        *string  `json:"expiresAt"`     // "2026-12-31"; required on grant
	ApplicantAccount string   `json:"applicantAccount"`
	ApproverAccount  string   `json:"approverAccount"`
	Reason           string   `json:"reason"`
}

type grantCreated struct {
	ID             string `json:"id"`
	SubjectAccount string `json:"subjectAccount"`
}

type createPermissionsResponse struct {
	Created           int            `json:"created"`
	DuplicatesIgnored []string       `json:"duplicatesIgnored"` // [] not null
	Grants            []grantCreated `json:"grants"`
}

// dedupPreserveOrder returns subjectAccounts with duplicates removed,
// keeping each account's first-occurrence position, plus the accounts that
// were dropped, in the order they were dropped. Never returns a nil
// duplicates slice — the response field must marshal as [], not null.
func dedupPreserveOrder(accounts []string) (deduped, duplicates []string) {
	seen := make(map[string]bool, len(accounts))
	deduped = make([]string, 0, len(accounts))
	duplicates = []string{}
	for _, a := range accounts {
		if seen[a] {
			duplicates = append(duplicates, a)
			continue
		}
		seen[a] = true
		deduped = append(deduped, a)
	}
	return deduped, duplicates
}

// parseWindow validates and converts the request window. Call ONLY when
// granted=true. fromStr nil means effective immediately (now). until is the
// day-after-untilStr at midnight tzTaipei — the half-open interval's
// exclusive end (spec §3.4).
func parseWindow(fromStr, untilStr *string, now time.Time) (from, until time.Time, err error) {
	if untilStr == nil {
		return time.Time{}, time.Time{}, errors.New("expiresAt is required for a grant")
	}

	untilDay, err := time.ParseInLocation("2006-01-02", *untilStr, tzTaipei)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("expiresAt must be YYYY-MM-DD: %w", err)
	}
	until = time.Date(untilDay.Year(), untilDay.Month(), untilDay.Day()+1, 0, 0, 0, 0, tzTaipei)

	if fromStr == nil {
		from = now
	} else {
		from, err = time.ParseInLocation("2006-01-02", *fromStr, tzTaipei)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("effectiveFrom must be YYYY-MM-DD: %w", err)
		}
	}

	if from.After(until) {
		return time.Time{}, time.Time{}, errors.New("effectiveFrom must not be after expiresAt")
	}
	if !until.After(now) {
		return time.Time{}, time.Time{}, errors.New("expiresAt must not be in the past")
	}

	return from, until, nil
}

// displayDate renders the civil date of t in tzTaipei ("2006-01-02") — the
// admin GET's rendering of EffectiveFrom.
func displayDate(t time.Time) string {
	return t.In(tzTaipei).Format("2006-01-02")
}

// displayUntilDate renders the inclusive end date: the civil date of t in
// tzTaipei minus one day. t is the stored half-open ExpiresAt instant
// (already +1 day from what the admin typed); this reverses that shift so
// the admin GET never shows the exclusive boundary (spec §3.4).
func displayUntilDate(t time.Time) string {
	return t.In(tzTaipei).AddDate(0, 0, -1).Format("2006-01-02")
}

// createPermissions handles POST /v1/admin/permissions — grants or revokes
// a permission for one or more subject accounts, all-or-nothing, with one
// audit entry per created row. Validation order matches spec §4.4 exactly.
func (h *Handler) createPermissions(c *gin.Context) {
	ctx := c.Request.Context()

	var req permissionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		errhttp.Write(ctx, c, errcode.BadRequest("invalid request body",
			errcode.WithReason(errcode.AuthMissingFields)))
		return
	}

	if !model.KnownPermission(model.PermissionKey(req.Permission)) {
		errhttp.Write(ctx, c, errcode.BadRequest("unknown permission",
			errcode.WithReason(errcode.PermissionUnknownKey)))
		return
	}

	if len(req.SubjectAccounts) == 0 || len(req.SubjectAccounts) > model.MaxSubjects {
		errhttp.Write(ctx, c, errcode.BadRequest("subjectAccounts must contain 1 to 200 accounts",
			errcode.WithReason(errcode.PermissionInvalidSubjects)))
		return
	}

	subjects, duplicatesIgnored := dedupPreserveOrder(req.SubjectAccounts)

	if req.Reason == "" || utf8.RuneCountInString(req.Reason) > model.MaxReasonRunes {
		errhttp.Write(ctx, c, errcode.BadRequest("reason must be 1 to 1000 runes",
			errcode.WithReason(errcode.PermissionInvalidReason)))
		return
	}

	if req.ApplicantAccount == "" || req.ApproverAccount == "" {
		errhttp.Write(ctx, c, errcode.BadRequest("applicantAccount and approverAccount are required",
			errcode.WithReason(errcode.PermissionMissingFields)))
		return
	}

	now := time.Now().UTC()

	var from, until time.Time
	if req.Granted {
		var err error
		from, until, err = parseWindow(req.EffectiveFrom, req.ExpiresAt, now)
		if err != nil {
			errhttp.Write(ctx, c, errcode.BadRequest(err.Error(),
				errcode.WithReason(errcode.PermissionInvalidWindow)))
			return
		}
	} else if req.EffectiveFrom != nil || req.ExpiresAt != nil {
		errhttp.Write(ctx, c, errcode.BadRequest("effectiveFrom and expiresAt must be absent on a revoke",
			errcode.WithReason(errcode.PermissionUnexpectedWindow)))
		return
	}

	states, err := h.store.FindAccountStates(ctx, h.cfg.SiteID, subjects)
	if err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("find account states: %w", err))
		return
	}

	var unknown, inactive []string
	for _, acct := range subjects {
		active, exists := states[acct]
		if !exists {
			unknown = append(unknown, acct)
			continue
		}
		if !active {
			inactive = append(inactive, acct)
		}
	}
	// Metadata "accounts" is CLIENT-VISIBLE by design (errcode doc.go) — the console
	// renders it so the admin sees exactly which accounts were rejected; the message
	// carries the same list for curl callers.
	if len(unknown) > 0 {
		errhttp.Write(ctx, c, errcode.NotFound("unknown accounts: "+strings.Join(unknown, ", "),
			errcode.WithReason(errcode.PermissionUnknownAccounts),
			errcode.WithMetadata("accounts", strings.Join(unknown, ", "))))
		return
	}
	if len(inactive) > 0 {
		errhttp.Write(ctx, c, errcode.BadRequest("inactive accounts cannot be granted a permission: "+strings.Join(inactive, ", "),
			errcode.WithReason(errcode.PermissionInactiveSubject),
			errcode.WithMetadata("accounts", strings.Join(inactive, ", "))))
		return
	}

	principal := principalFrom(c)
	grants := make([]*model.PermissionGrant, len(subjects))
	for i, acct := range subjects {
		g := &model.PermissionGrant{
			ID:               idgen.GenerateUUIDv7(),
			SiteID:           h.cfg.SiteID,
			Permission:       model.PermissionKey(req.Permission),
			SubjectAccount:   acct,
			Granted:          req.Granted,
			ApplicantAccount: req.ApplicantAccount,
			ApproverAccount:  req.ApproverAccount,
			Reason:           req.Reason,
			RecordedBy:       principal.Account,
			RecordedAt:       now,
		}
		if req.Granted {
			g.EffectiveFrom = &from
			g.ExpiresAt = &until
		}
		grants[i] = g
	}

	if err := h.store.InsertPermissionGrants(ctx, grants); err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("insert permission grants: %w", err))
		return
	}

	action := auditActionPermissionRevoke
	if req.Granted {
		action = auditActionPermissionGrant
	}
	entries := make([]*AuditEntry, len(grants))
	for i, g := range grants {
		entries[i] = &AuditEntry{
			ID:            idgen.GenerateUUIDv7(),
			ActorUserID:   principal.UserID,
			ActorAccount:  principal.Account,
			Action:        action,
			TargetAccount: g.SubjectAccount,
			Details:       map[string]string{"permission": string(g.Permission)},
			SiteID:        h.cfg.SiteID,
			Timestamp:     now.UnixMilli(),
		}
	}
	if err := h.store.AppendAuditMany(ctx, entries); err != nil {
		slog.ErrorContext(ctx, "append permission audit entries failed", "action", action, "error", err)
	}

	resp := createPermissionsResponse{
		Created:           len(grants),
		DuplicatesIgnored: duplicatesIgnored,
		Grants:            make([]grantCreated, len(grants)),
	}
	for i, g := range grants {
		resp.Grants[i] = grantCreated{ID: g.ID, SubjectAccount: g.SubjectAccount}
	}
	c.JSON(http.StatusCreated, resp)
}

// listPermissions handles GET /v1/admin/permissions. Implemented in Task 6;
// this placeholder only exists so setupPermissionsRouter (permissions_test.go)
// compiles for this task's PASS run — Task 6 replaces this entire function.
func (h *Handler) listPermissions(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, gin.H{"status": "not implemented"})
}
```

- [ ] **Step 5 — wire the route and the middleware.** In `admin-service/routes.go`, add one line after `admin.GET("/audit", h.listAudit)`:
```go
	admin.POST("/permissions", h.createPermissions)
```
So the group reads:
```go
	admin := r.Group("/v1/admin", requireAdmin(sessions, siteID))
	admin.GET("/users", h.listUsers)
	admin.POST("/users", h.createUser)
	admin.GET("/users/:account", h.getUser)
	admin.PATCH("/users/:account", h.updateUser)
	admin.POST("/users/:account/password", h.setPassword)
	admin.POST("/rooms/:roomId/onduty", h.setRoomOnDuty)
	admin.GET("/sessions", h.listSessions)
	admin.DELETE("/sessions", h.revokeAllSessions)
	admin.DELETE("/sessions/:sessionId", h.revokeSession)
	admin.GET("/audit", h.listAudit)
	admin.POST("/permissions", h.createPermissions)
}
```

In `admin-service/main.go`, add one line after `r.Use(ginutil.AccessLog())` and before `registerRoutes(r, h, sessStore, cfg.SiteID)`:
```go
	r.Use(bodyLimit(maxPermissionBodyBytes))
```
So the middleware chain reads:
```go
	r.Use(ginutil.CORS())
	r.Use(o11ygin.Middleware("admin-service", sdk.TracerProvider(), sdk.MeterProvider(), sdk.Propagator, o11ygin.WithSkipPaths())...)
	r.Use(gin.Recovery())
	r.Use(ginutil.RequestID())
	r.Use(ginutil.AccessLog())
	r.Use(bodyLimit(maxPermissionBodyBytes))
	registerRoutes(r, h, sessStore, cfg.SiteID)
```

- [ ] **Step 6 — run PASS.**

```bash
make test SERVICE=admin-service
```
Expected: `ok  	github.com/hmchangw/chat/admin-service	<time>` — `TestHandler_createPermissions`, `TestParseWindow`, `TestDisplayDate`, `TestDisplayUntilDate`, `TestBodyLimit`, and the full pre-existing suite all pass.

- [ ] **Step 7 — lint.**

```bash
make lint
```
Expected: no output, exit code 0.

- [ ] **Step 8 — commit.**

```bash
git add admin-service/middleware.go admin-service/middleware_test.go admin-service/main.go admin-service/routes.go admin-service/permissions.go admin-service/permissions_test.go
git commit -m "feat(admin-service): POST /v1/admin/permissions with body limit"
```

---

### Task 6: GET /v1/admin/permissions

**Files:**
- `admin-service/permissions.go` (Modify — append `listPermissions` + view types, replace the Task 5 placeholder)
- `admin-service/permissions_test.go` (Modify — append `TestHandler_listPermissions`)
- `admin-service/routes.go` (Modify — `GET /permissions`)

**Interfaces:**

Consumes:
- Task 4's `AdminStore.ListPermissionGrants(ctx, siteID, subjectAccount string, permission model.PermissionKey, page, limit int) ([]model.PermissionGrant, int64, error)` and `.GetLatestPermissionGrant(ctx, siteID string, permission model.PermissionKey, subjectAccount string) (*model.PermissionGrant, error)` — **note the argument order differs between the two**: `ListPermissionGrants` takes `(siteID, subjectAccount, permission, ...)`, `GetLatestPermissionGrant` takes `(siteID, permission, subjectAccount)`. This is the locked skeleton's own signature order, not a typo — every call site below matches it exactly.
- Task 1's `model.EvaluateGrant(latest *model.PermissionGrant, now time.Time) bool` and `model.KnownPermission`.
- Task 5's `tzTaipei`, `displayDate`, `displayUntilDate` (same file, no new import).
- Existing `parsePaging(c, defaultPage, defaultLimit int) (page, limit int)` (handler.go).

Produces (consumed by Task 9's TS client, Task 10/11's lookup pane, Task 13's docs — the wire shape of `GET /v1/admin/permissions`):
```json
// 200 response, permission param supplied
{
  "currentlyGranted": true,
  "entries": [
    {
      "id": "0199f2c3a4b5...",
      "permission": "external.image.view",
      "subjectAccount": "alice",
      "granted": true,
      "effectiveFrom": "2026-09-01",
      "expiresAt": "2026-12-31",
      "expiresAtUTC": "2026-12-31T16:00:00Z",
      "applicantAccount": "carol",
      "approverAccount": "dave",
      "reason": "On-call staff must review production line photos from outside the fab.",
      "recordedBy": "p_admin_wang",
      "recordedAt": "2026-09-01T02:00:00Z"
    }
  ],
  "total": 1
}
```
When `permission` is omitted from the query, `currentlyGranted` is absent from the JSON entirely (not `null`). A revoke row's `entries[i]` carries no `effectiveFrom`/`expiresAt`/`expiresAtUTC` keys.

---

- [ ] **Step 1 — RED.** Append to `admin-service/permissions_test.go`:

```go
// -------------------------------------------------------------------------
// listPermissions
// -------------------------------------------------------------------------

// ptrTime returns a pointer to t, for constructing model.PermissionGrant
// fixtures returned by mocked store calls. Distinct name from
// integration_test.go's timePtr — that file compiles into the same package
// under -tags integration, so the two helpers must not collide.
func ptrTime(t time.Time) *time.Time { return &t }

func TestHandler_listPermissions(t *testing.T) {
	knownPermission := string(model.PermissionExternalImageView)

	tests := []struct {
		name       string
		query      string
		setupMock  func(m *MockAdminStore)
		wantStatus int
		wantReason string
		checkBody  func(t *testing.T, body map[string]any)
	}{
		{
			name:       "missing subjectAccount → 400 missing_permission_fields",
			query:      "?permission=" + knownPermission,
			setupMock:  func(m *MockAdminStore) {},
			wantStatus: http.StatusBadRequest,
			wantReason: string(errcode.PermissionMissingFields),
		},
		{
			name:       "unknown permission → 400 unknown_permission",
			query:      "?subjectAccount=alice&permission=bogus.permission",
			setupMock:  func(m *MockAdminStore) {},
			wantStatus: http.StatusBadRequest,
			wantReason: string(errcode.PermissionUnknownKey),
		},
		{
			name:  "store error → 500",
			query: "?subjectAccount=alice",
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().ListPermissionGrants(gomock.Any(), "site-A", "alice", model.PermissionKey(""), 1, 20).
					Return(nil, int64(0), fmt.Errorf("boom"))
			},
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:  "empty ledger without a permission param — no currentlyGranted key",
			query: "?subjectAccount=alice",
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().ListPermissionGrants(gomock.Any(), "site-A", "alice", model.PermissionKey(""), 1, 20).
					Return([]model.PermissionGrant{}, int64(0), nil)
				m.EXPECT().GetLatestPermissionGrant(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
			},
			wantStatus: http.StatusOK,
			checkBody: func(t *testing.T, body map[string]any) {
				assert.Equal(t, []any{}, body["entries"])
				assert.Equal(t, float64(0), body["total"])
				_, present := body["currentlyGranted"]
				assert.False(t, present, "permission omitted → no currentlyGranted key")
			},
		},
		{
			name:  "empty ledger WITH a permission param — currentlyGranted present, false",
			query: "?subjectAccount=alice&permission=" + knownPermission,
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().ListPermissionGrants(gomock.Any(), "site-A", "alice", model.PermissionExternalImageView, 1, 20).
					Return([]model.PermissionGrant{}, int64(0), nil)
				m.EXPECT().GetLatestPermissionGrant(gomock.Any(), "site-A", model.PermissionExternalImageView, "alice").
					Return(nil, nil)
			},
			wantStatus: http.StatusOK,
			checkBody: func(t *testing.T, body map[string]any) {
				assert.Equal(t, []any{}, body["entries"])
				val, ok := body["currentlyGranted"]
				require.True(t, ok, "permission given → currentlyGranted key must be present")
				assert.Equal(t, false, val)
			},
		},
		{
			name:  "currently granted true",
			query: "?subjectAccount=alice&permission=" + knownPermission,
			setupMock: func(m *MockAdminStore) {
				now := time.Now().UTC()
				latest := &model.PermissionGrant{
					ID: "g1", SiteID: "site-A", Permission: model.PermissionExternalImageView,
					SubjectAccount: "alice", Granted: true,
					EffectiveFrom: ptrTime(now.Add(-time.Hour)), ExpiresAt: ptrTime(now.Add(time.Hour)),
					ApplicantAccount: "carol", ApproverAccount: "dave", Reason: "r", RecordedBy: "p_admin", RecordedAt: now.Add(-time.Hour),
				}
				m.EXPECT().ListPermissionGrants(gomock.Any(), "site-A", "alice", model.PermissionExternalImageView, 1, 20).
					Return([]model.PermissionGrant{*latest}, int64(1), nil)
				m.EXPECT().GetLatestPermissionGrant(gomock.Any(), "site-A", model.PermissionExternalImageView, "alice").
					Return(latest, nil)
			},
			wantStatus: http.StatusOK,
			checkBody: func(t *testing.T, body map[string]any) {
				assert.Equal(t, true, body["currentlyGranted"])
			},
		},
		{
			name:  "ledger with a revoke row and a grant row: shapes differ",
			query: "?subjectAccount=alice",
			setupMock: func(m *MockAdminStore) {
				grantRow := model.PermissionGrant{
					ID: "g-grant", SiteID: "site-A", Permission: model.PermissionExternalImageView,
					SubjectAccount: "alice", Granted: true,
					EffectiveFrom:    ptrTime(time.Date(2026, 9, 1, 0, 0, 0, 0, tzTaipei)),
					ExpiresAt:        ptrTime(time.Date(2027, 1, 1, 0, 0, 0, 0, tzTaipei)),
					ApplicantAccount: "carol", ApproverAccount: "dave", Reason: "r1",
					RecordedBy: "p_admin", RecordedAt: time.Date(2026, 9, 1, 1, 0, 0, 0, time.UTC),
				}
				revokeRow := model.PermissionGrant{
					ID: "g-revoke", SiteID: "site-A", Permission: model.PermissionExternalImageView,
					SubjectAccount: "alice", Granted: false,
					ApplicantAccount: "carol", ApproverAccount: "dave", Reason: "r2",
					RecordedBy: "p_admin", RecordedAt: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC),
				}
				m.EXPECT().ListPermissionGrants(gomock.Any(), "site-A", "alice", model.PermissionKey(""), 1, 20).
					Return([]model.PermissionGrant{revokeRow, grantRow}, int64(2), nil)
				m.EXPECT().GetLatestPermissionGrant(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
			},
			wantStatus: http.StatusOK,
			checkBody: func(t *testing.T, body map[string]any) {
				entries, ok := body["entries"].([]any)
				require.True(t, ok)
				require.Len(t, entries, 2)

				revoke, ok := entries[0].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "g-revoke", revoke["id"])
				_, hasFrom := revoke["effectiveFrom"]
				_, hasUntil := revoke["expiresAt"]
				_, hasUntilUTC := revoke["expiresAtUTC"]
				assert.False(t, hasFrom, "revoke row must not carry an effectiveFrom key")
				assert.False(t, hasUntil, "revoke row must not carry an expiresAt key")
				assert.False(t, hasUntilUTC, "revoke row must not carry an expiresAtUTC key")

				grant, ok := entries[1].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "g-grant", grant["id"])
				assert.Equal(t, "2026-09-01", grant["effectiveFrom"])
				assert.Equal(t, "2026-12-31", grant["expiresAt"])
				assert.NotEmpty(t, grant["expiresAtUTC"])
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			m := NewMockAdminStore(ctrl)
			tc.setupMock(m)

			h := newHandler(m, emptySessionStore(), testCfg(), nil)
			r := setupPermissionsRouter(h)

			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/permissions"+tc.query, nil)
			r.ServeHTTP(w, req)

			assert.Equal(t, tc.wantStatus, w.Code)
			body := respBody(t, w)
			if tc.wantReason != "" {
				assert.Equal(t, tc.wantReason, body["reason"])
			}
			if tc.checkBody != nil {
				tc.checkBody(t, body)
			}
		})
	}
}
```

No new imports needed — `permissions_test.go` already imports `context`, `fmt`, `net/http`, `net/http/httptest`, `strconv`, `strings`, `testing`, `time`, `gin`, `assert`, `require`, `gomock`, `errcode`, `model`, `session` from Task 5.

- [ ] **Step 2 — run FAIL.**

```bash
make test SERVICE=admin-service
```
Expected (build failure — `listPermissions` is still the Task 5 placeholder returning 501, and `permissionGrantView`/`listPermissionsResponse` don't exist yet; line numbers will vary):
```
# github.com/hmchangw/chat/admin-service [github.com/hmchangw/chat/admin-service.test]
./permissions_test.go:XX:X: undefined: permissionGrantView
FAIL	github.com/hmchangw/chat/admin-service [build failed]
```
(If the table only referenced already-compiling symbols, this would instead show as a normal test FAIL on `TestHandler_listPermissions` — e.g. `store error → 500` failing because the placeholder returns 501, not 500 — rather than a build failure. Either failure mode confirms RED; run `make test SERVICE=admin-service` and confirm it is not a clean pass.)

- [ ] **Step 3 — implementation.** In `admin-service/permissions.go`, replace the Task 5 placeholder:
```go
// listPermissions handles GET /v1/admin/permissions. Implemented in Task 6;
// this placeholder only exists so setupPermissionsRouter (permissions_test.go)
// compiles for this task's PASS run — Task 6 replaces this entire function.
func (h *Handler) listPermissions(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, gin.H{"status": "not implemented"})
}
```
with:
```go
// permissionGrantView renders one ledger row for the admin GET: display
// dates (civil, tzTaipei), not raw instants — spec §3.4. Revoke rows have no
// window, so EffectiveFrom/ExpiresAt/ExpiresAtUTC stay "" and are omitted.
type permissionGrantView struct {
	ID               string    `json:"id"`
	Permission       string    `json:"permission"`
	SubjectAccount   string    `json:"subjectAccount"`
	Granted          bool      `json:"granted"`
	EffectiveFrom    string    `json:"effectiveFrom,omitempty"` // "2026-09-01"; empty on revoke rows
	ExpiresAt        string    `json:"expiresAt,omitempty"`     // "2026-12-31"; empty on revoke rows
	ExpiresAtUTC     string    `json:"expiresAtUTC,omitempty"`  // RFC3339 exact stored instant
	ApplicantAccount string    `json:"applicantAccount"`
	ApproverAccount  string    `json:"approverAccount"`
	Reason           string    `json:"reason"`
	RecordedBy       string    `json:"recordedBy"`
	RecordedAt       time.Time `json:"recordedAt"`
}

type listPermissionsResponse struct {
	// CurrentlyGranted is present ONLY when the permission query param was
	// supplied — there is no meaningful single decision across permissions.
	CurrentlyGranted *bool                 `json:"currentlyGranted,omitempty"`
	Entries          []permissionGrantView `json:"entries"`
	Total            int64                 `json:"total"`
}

// toPermissionGrantView converts a stored grant row to its display form.
func toPermissionGrantView(g *model.PermissionGrant) permissionGrantView {
	v := permissionGrantView{
		ID:               g.ID,
		Permission:       string(g.Permission),
		SubjectAccount:   g.SubjectAccount,
		Granted:          g.Granted,
		ApplicantAccount: g.ApplicantAccount,
		ApproverAccount:  g.ApproverAccount,
		Reason:           g.Reason,
		RecordedBy:       g.RecordedBy,
		RecordedAt:       g.RecordedAt,
	}
	if g.EffectiveFrom != nil {
		v.EffectiveFrom = displayDate(*g.EffectiveFrom)
	}
	if g.ExpiresAt != nil {
		v.ExpiresAt = displayUntilDate(*g.ExpiresAt)
		v.ExpiresAtUTC = g.ExpiresAt.UTC().Format(time.RFC3339)
	}
	return v
}

// listPermissions handles GET /v1/admin/permissions — returns the ledger for
// one subject, newest-first, plus the current computed decision when the
// permission query param narrows to a single key (spec §4.6).
func (h *Handler) listPermissions(c *gin.Context) {
	ctx := c.Request.Context()

	subjectAccount := c.Query("subjectAccount")
	if subjectAccount == "" {
		errhttp.Write(ctx, c, errcode.BadRequest("subjectAccount query parameter is required",
			errcode.WithReason(errcode.PermissionMissingFields)))
		return
	}

	permission := model.PermissionKey(c.Query("permission"))
	if permission != "" && !model.KnownPermission(permission) {
		errhttp.Write(ctx, c, errcode.BadRequest("unknown permission",
			errcode.WithReason(errcode.PermissionUnknownKey)))
		return
	}

	page, limit := parsePaging(c, 1, 20)

	grants, total, err := h.store.ListPermissionGrants(ctx, h.cfg.SiteID, subjectAccount, permission, page, limit)
	if err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("list permission grants: %w", err))
		return
	}

	entries := make([]permissionGrantView, len(grants))
	for i := range grants {
		entries[i] = toPermissionGrantView(&grants[i])
	}

	resp := listPermissionsResponse{
		Entries: entries,
		Total:   total,
	}

	if permission != "" {
		latest, err := h.store.GetLatestPermissionGrant(ctx, h.cfg.SiteID, permission, subjectAccount)
		if err != nil {
			errhttp.Write(ctx, c, fmt.Errorf("get latest permission grant: %w", err))
			return
		}
		decision := model.EvaluateGrant(latest, time.Now().UTC())
		resp.CurrentlyGranted = &decision
	}

	c.JSON(http.StatusOK, resp)
}
```
No import changes — `permissions.go` already imports `fmt`, `net/http`, `time`, `gin`, `errcode`, `errhttp`, `model` from Task 5.

- [ ] **Step 4 — wire the route.** In `admin-service/routes.go`, add one line after `admin.POST("/permissions", h.createPermissions)`:
```go
	admin.GET("/permissions", h.listPermissions)
```
So the group's final two lines read:
```go
	admin.POST("/permissions", h.createPermissions)
	admin.GET("/permissions", h.listPermissions)
}
```

- [ ] **Step 5 — run PASS.**

```bash
make test SERVICE=admin-service
```
Expected: `ok  	github.com/hmchangw/chat/admin-service	<time>` — `TestHandler_listPermissions` and the full suite (Tasks 4–6 plus every pre-existing test) pass.

- [ ] **Step 6 — lint.**

```bash
make lint
```
Expected: no output, exit code 0.

- [ ] **Step 7 — commit.**

```bash
git add admin-service/permissions.go admin-service/permissions_test.go admin-service/routes.go
git commit -m "feat(admin-service): GET /v1/admin/permissions ledger + current decision"
```
## Phase 3: user-service (Tasks 7–8)

> **Conflicts found & resolved (read before executing):**
> 1. **Compile-time assertion ordering.** The existing 4-repo pattern in `user-service/mongorepo/setup_test.go` pairs every repo with a `_ service.XRepository = (*XRepo)(nil)` line in one shared `var (...)` block. `service.PermissionRepository` does not exist until Task 8, so Task 7 cannot add that line without breaking the build. **Resolved:** Task 7 adds only the `newTestPermissionRepo` helper to `setup_test.go`; Task 8 adds the `_ service.PermissionRepository = (*PermissionRepo)(nil)` line to that same `var` block once the interface exists (alongside the existing `main.go` assertion the skeleton already calls for).
> 2. **`service.New` blast radius is larger than documented.** The extract flagged only `main.go` and `service_test.go`'s `newSvc` as callers needing updates when `New`'s signature grows. Direct inspection of `user-service/service/*_test.go` found **five more** direct-call sites that construct `UserService` via `New(...)` without going through `newSvc`: `sso_test.go` (`newSSOSvc`), `me_test.go` (`newMeSvc`), `threads_test.go` (`newThreadSvc`), `status_test.go` (inline in `TestPublishStatus_SkipsEmptyDest`), and `subscriptions_test.go` (**two** call sites: `newSvcRawHistory` and `newCountSvc`). Task 8 below updates all eight call sites (7 test files + `main.go`) in lockstep — verified by grepping every `\bNew(` occurrence under `user-service/service/`.
> 3. **`GetLatestGrant` error-wrap wording.** The task brief gives one literal wrap string (`"find latest permission grant: %w"`) for "other errors" without distinguishing the `FindOne` failure from the `Decode` failure. Implemented literally: both branches use the same message (rather than `users.go`'s separate `"decode X"` convention), matching the instruction as given.

---

### Task 7: user-service permission grants repository (primary reads)

Read-only Mongo repo answering "what is the newest `permission_grants` row for `(siteId, permission, subjectAccount)`?" — the query Task 8's handler needs to compute the current yes/no decision. `permission_grants` is written exclusively by admin-service (Task 4); this repo never writes.

**Files:**
- Create: `user-service/mongorepo/permissions.go`
- Create: `user-service/mongorepo/permissions_test.go` (integration, `//go:build integration`)
- Modify: `user-service/mongorepo/setup_test.go` (add `newTestPermissionRepo` helper only — see conflict note 1 above)

**Interfaces:**
- Consumes (Task 1, `pkg/model/permission.go` — assumed already merged):
  - `type PermissionKey string`
  - `const PermissionExternalImageView PermissionKey = "external.image.view"`
  - `type PermissionGrant struct { ID, SiteID, Permission, SubjectAccount string/PermissionKey; Granted bool; EffectiveFrom, ExpiresAt *time.Time; ApplicantAccount, ApproverAccount, Reason, RecordedBy string; RecordedAt time.Time }` (bson tags: `_id`, `siteId`, `permission`, `subjectAccount`, `granted`, `effectiveFrom`, `expiresAt`, `applicantAccount`, `approverAccount`, `reason`, `recordedBy`, `recordedAt`)
- Produces (consumed by Task 8):
  - `type PermissionRepo struct{ ... }`
  - `func NewPermissionRepo(db *mongo.Database) *PermissionRepo`
  - `func (r *PermissionRepo) GetLatestGrant(ctx context.Context, siteID string, permission model.PermissionKey, subjectAccount string) (*model.PermissionGrant, error)`

Docker must be running locally before the integration-test steps below (`testcontainers-go` starts a real Mongo container).

- [ ] **Step 1: Write the failing integration tests**

Create `user-service/mongorepo/permissions_test.go`:

```go
//go:build integration

package mongorepo

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

func TestPermissionRepo_GetLatestGrant_EmptyCollection(t *testing.T) {
	r, _ := newTestPermissionRepo(t)
	got, err := r.GetLatestGrant(context.Background(), "site-a", model.PermissionExternalImageView, "alice")
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestPermissionRepo_GetLatestGrant_SingleGrantRow_ProjectionTrimsOtherFields(t *testing.T) {
	r, db := newTestPermissionRepo(t)
	from := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	until := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	recordedAt := time.Date(2026, 8, 15, 10, 0, 0, 0, time.UTC)
	seed(t, db, "permission_grants", model.PermissionGrant{
		ID:               "id-1",
		SiteID:           "site-a",
		Permission:       model.PermissionExternalImageView,
		SubjectAccount:   "alice",
		Granted:          true,
		EffectiveFrom:    &from,
		ExpiresAt:        &until,
		ApplicantAccount: "carol",
		ApproverAccount:  "dave",
		Reason:           "On-call staff must review production line photos from outside the fab.",
		RecordedBy:       "p_admin_wang",
		RecordedAt:       recordedAt,
	})

	got, err := r.GetLatestGrant(context.Background(), "site-a", model.PermissionExternalImageView, "alice")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.True(t, got.Granted)
	require.NotNil(t, got.EffectiveFrom)
	assert.True(t, from.Equal(*got.EffectiveFrom))
	require.NotNil(t, got.ExpiresAt)
	assert.True(t, until.Equal(*got.ExpiresAt))
	// Only granted/effectiveFrom/expiresAt are projected — everything else must
	// come back zero-valued, proving the projection actually trims the document.
	// (Mongo still returns _id by default since the projection never excludes it
	// with "_id":0; that's fine, _id never reaches the wire — GetPermission's
	// response never echoes it.)
	assert.Empty(t, got.Reason, "projection must not return reason")
	assert.Empty(t, got.ApplicantAccount, "projection must not return applicantAccount")
	assert.Empty(t, got.ApproverAccount, "projection must not return approverAccount")
	assert.Empty(t, got.RecordedBy, "projection must not return recordedBy")
	assert.True(t, got.RecordedAt.IsZero(), "projection must not return recordedAt")
}

func TestPermissionRepo_GetLatestGrant_RevokeAfterGrant_ReturnsRevoke(t *testing.T) {
	r, db := newTestPermissionRepo(t)
	from := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	until := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	seed(t, db, "permission_grants",
		model.PermissionGrant{
			ID: "id-grant", SiteID: "site-a", Permission: model.PermissionExternalImageView, SubjectAccount: "alice",
			Granted: true, EffectiveFrom: &from, ExpiresAt: &until,
			ApplicantAccount: "carol", ApproverAccount: "dave", Reason: "initial grant", RecordedBy: "p_admin_wang",
			RecordedAt: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
		},
		model.PermissionGrant{
			ID: "id-revoke", SiteID: "site-a", Permission: model.PermissionExternalImageView, SubjectAccount: "alice",
			Granted:          false,
			ApplicantAccount: "carol", ApproverAccount: "dave", Reason: "project ended", RecordedBy: "p_admin_wang",
			RecordedAt: time.Date(2026, 8, 15, 0, 0, 0, 0, time.UTC), // later than the grant
		},
	)

	got, err := r.GetLatestGrant(context.Background(), "site-a", model.PermissionExternalImageView, "alice")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.False(t, got.Granted, "the later revoke row must win over the earlier grant")
	assert.Nil(t, got.EffectiveFrom)
	assert.Nil(t, got.ExpiresAt)
}

func TestPermissionRepo_GetLatestGrant_SameRecordedAt_HigherIDWins(t *testing.T) {
	r, db := newTestPermissionRepo(t)
	recordedAt := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC) // identical on both rows
	seed(t, db, "permission_grants",
		model.PermissionGrant{
			ID: "id-aaa", SiteID: "site-a", Permission: model.PermissionExternalImageView, SubjectAccount: "alice",
			Granted: false, ApplicantAccount: "carol", ApproverAccount: "dave", Reason: "batch row a", RecordedBy: "p_admin_wang",
			RecordedAt: recordedAt,
		},
		model.PermissionGrant{
			ID: "id-bbb", SiteID: "site-a", Permission: model.PermissionExternalImageView, SubjectAccount: "alice",
			Granted: true, ApplicantAccount: "carol", ApproverAccount: "dave", Reason: "batch row b", RecordedBy: "p_admin_wang",
			RecordedAt: recordedAt,
		},
	)

	got, err := r.GetLatestGrant(context.Background(), "site-a", model.PermissionExternalImageView, "alice")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "id-bbb", got.ID, "the higher _id must win the recordedAt tie-break")
	assert.True(t, got.Granted)
}

func TestPermissionRepo_GetLatestGrant_FilterIsolation(t *testing.T) {
	r, db := newTestPermissionRepo(t)
	target := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC) // the row that SHOULD be returned
	trap := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)  // later than target — would win if isolation failed
	seed(t, db, "permission_grants",
		// The row that must be returned.
		model.PermissionGrant{
			ID: "id-target", SiteID: "site-a", Permission: model.PermissionExternalImageView, SubjectAccount: "alice",
			Granted: true, RecordedAt: target,
		},
		// Same site+permission, different subject — must not leak into alice's answer.
		model.PermissionGrant{
			ID: "id-trap-subject", SiteID: "site-a", Permission: model.PermissionExternalImageView, SubjectAccount: "bob",
			Granted: false, RecordedAt: trap,
		},
		// Same permission+subject, different site.
		model.PermissionGrant{
			ID: "id-trap-site", SiteID: "site-b", Permission: model.PermissionExternalImageView, SubjectAccount: "alice",
			Granted: false, RecordedAt: trap,
		},
		// Same site+subject, different permission key.
		model.PermissionGrant{
			ID: "id-trap-permission", SiteID: "site-a", Permission: model.PermissionKey("other.permission"), SubjectAccount: "alice",
			Granted: false, RecordedAt: trap,
		},
	)

	got, err := r.GetLatestGrant(context.Background(), "site-a", model.PermissionExternalImageView, "alice")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "id-target", got.ID, "a row failing any one of siteId/permission/subjectAccount must never win, even with a newer recordedAt")
	assert.True(t, got.Granted)
}
```

Modify `user-service/mongorepo/setup_test.go` — add the `newTestPermissionRepo` helper (full updated file; only the new function at the end, before `seed`, is new):

```go
//go:build integration

package mongorepo

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/testutil"
	"github.com/hmchangw/chat/user-service/service"
)

// Compile-time assertions: `go vet -tags integration` fails if any repo drifts from its interface.
var (
	_ service.SubscriptionRepository = (*SubscriptionRepo)(nil)
	_ service.UserRepository         = (*UserRepo)(nil)
	_ service.AppRepository          = (*AppRepo)(nil)
	_ service.SSOTokenRepository     = (*SSOTokenRepo)(nil)
)

// newTestSubscriptionRepo builds a SubscriptionRepo with siteID "site-a"; seed cross-site rows with a different siteId to exercise the deleted-filter.
func newTestSubscriptionRepo(t *testing.T) (*SubscriptionRepo, *mongo.Database) {
	t.Helper()
	db := testutil.MongoDB(t, "user-service")
	r := NewSubscriptionRepo(db, "site-a")
	require.NoError(t, r.EnsureIndexes(context.Background()))
	return r, db
}

// newTestUserRepo builds a UserRepo over an isolated test database.
func newTestUserRepo(t *testing.T) (*UserRepo, *mongo.Database) {
	t.Helper()
	db := testutil.MongoDB(t, "user-service")
	r := NewUserRepo(db)
	require.NoError(t, r.EnsureIndexes(context.Background()))
	return r, db
}

// newTestAppRepo builds an AppRepo over an isolated test database.
func newTestAppRepo(t *testing.T) (*AppRepo, *mongo.Database) {
	t.Helper()
	db := testutil.MongoDB(t, "user-service")
	return NewAppRepo(db), db
}

// newTestSSOTokenRepo builds an SSOTokenRepo over an isolated test database.
func newTestSSOTokenRepo(t *testing.T) (*SSOTokenRepo, *mongo.Database) {
	t.Helper()
	db := testutil.MongoDB(t, "user-service")
	r := NewSSOTokenRepo(db)
	require.NoError(t, r.EnsureIndexes(context.Background()))
	return r, db
}

// newTestPermissionRepo builds a PermissionRepo over an isolated test
// database. No EnsureIndexes call: unlike every other repo in this file,
// PermissionRepo has no such method — admin-service alone creates the
// permission_grants indexes (spec §3.6).
func newTestPermissionRepo(t *testing.T) (*PermissionRepo, *mongo.Database) {
	t.Helper()
	db := testutil.MongoDB(t, "user-service")
	return NewPermissionRepo(db), db
}

// seed inserts raw docs into a collection on db.
func seed(t *testing.T, db *mongo.Database, coll string, docs ...any) {
	t.Helper()
	_, err := db.Collection(coll).InsertMany(context.Background(), docs)
	require.NoError(t, err)
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make test-integration SERVICE=user-service`

Expected: FAIL — compile error (`permissions.go` doesn't exist yet):

```
# github.com/hmchangw/chat/user-service/mongorepo [github.com/hmchangw/chat/user-service/mongorepo.test]
user-service/mongorepo/setup_test.go:68:10: undefined: NewPermissionRepo
FAIL	github.com/hmchangw/chat/user-service/mongorepo [build failed]
```

- [ ] **Step 3: Implement the repository**

Create `user-service/mongorepo/permissions.go`:

```go
package mongorepo

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
)

const permissionGrantsCollection = "permission_grants"

// PermissionRepo is the Mongo read model for service.PermissionRepository.
// permission_grants is an append-only ledger written only by admin-service.
//
// No WithReadPreference option: stays on the primary so a grant/revoke is
// visible immediately (spec §3.7). No EnsureIndexes: admin-service alone
// creates the indexes, to avoid a two-service IndexKeySpecsConflict (spec §3.6).
type PermissionRepo struct {
	grants *mongoutil.Collection[model.PermissionGrant]
}

// NewPermissionRepo builds a PermissionRepo over db.
func NewPermissionRepo(db *mongo.Database) *PermissionRepo {
	return &PermissionRepo{
		grants: mongoutil.NewCollection[model.PermissionGrant](db.Collection(permissionGrantsCollection)),
	}
}

// GetLatestGrant returns the newest row for (siteId, permission,
// subjectAccount) — recordedAt desc, _id desc tie-break — or (nil, nil).
// Uses the raw-driver escape hatch: mongoutil's typed FindOne has no sort
// (WithSort only wires into FindMany — pkg/mongoutil/options.go).
func (r *PermissionRepo) GetLatestGrant(ctx context.Context, siteID string, permission model.PermissionKey, subjectAccount string) (*model.PermissionGrant, error) {
	res := r.grants.Raw().FindOne(ctx,
		bson.M{"siteId": siteID, "permission": permission, "subjectAccount": subjectAccount},
		options.FindOne().
			SetSort(bson.D{{Key: "recordedAt", Value: -1}, {Key: "_id", Value: -1}}).
			SetProjection(bson.M{"granted": 1, "effectiveFrom": 1, "expiresAt": 1}),
	)
	if err := res.Err(); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, fmt.Errorf("find latest permission grant: %w", err)
	}
	var g model.PermissionGrant
	if err := res.Decode(&g); err != nil {
		return nil, fmt.Errorf("find latest permission grant: %w", err)
	}
	return &g, nil
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `make test-integration SERVICE=user-service`

Expected: PASS — all 5 new `TestPermissionRepo_GetLatestGrant_*` tests pass, plus every pre-existing `user-service` integration test still passes:

```
ok  	github.com/hmchangw/chat/user-service/mongorepo	(some seconds)
```

- [ ] **Step 5: Lint**

Run: `make lint`

Expected: exits 0, no findings.

- [ ] **Step 6: Commit**

```bash
git add user-service/mongorepo/permissions.go user-service/mongorepo/permissions_test.go user-service/mongorepo/setup_test.go
git commit -m "feat(user-service): permission grants repository (primary reads)"
```

---

### Task 8: user-service permission.get NATS handler

The read side of the whitelist: a user asks "do I currently hold `external.image.view`?" over NATS request/reply, identified only by the `{account}` token the NATS scoped signing key already put in the subject — never by anything in the request body.

**Files:**
- Create: `user-service/models/permission.go`
- Create: `user-service/models/permission_test.go`
- Modify: `user-service/service/service.go` (interface, struct field, `New` signature, mockgen directive, `RegisterHandlers`)
- Modify: `user-service/service/service_test.go` (`newSvc` — construct the new mock, pass it to `New`)
- Modify: `user-service/service/sso_test.go`, `user-service/service/me_test.go`, `user-service/service/threads_test.go`, `user-service/service/status_test.go`, `user-service/service/subscriptions_test.go` (every other direct `New(...)` call site — see conflict note 2 above)
- Regenerate: `user-service/service/mocks/mock_repository.go` (`make generate SERVICE=user-service` — never hand-edit)
- Create: `user-service/service/permission.go`
- Create: `user-service/service/permission_test.go`
- Modify: `user-service/mongorepo/setup_test.go` (add the deferred compile-time assertion — see conflict note 1 above)
- Modify: `user-service/main.go` (repo wiring + compile-time assertion)

**Interfaces:**
- Consumes:
  - Task 1 (`pkg/model/permission.go`): `model.PermissionKey`, `model.PermissionGrant`, `func KnownPermission(k PermissionKey) bool`, `func EvaluateGrant(latest *PermissionGrant, now time.Time) bool`
  - Task 2 (`pkg/errcode/codes_permission.go`): `PermissionUnknownKey Reason = "unknown_permission"`; plus existing `errcode.BadRequest(msg string, opts ...Option) *Error` and `errcode.WithReason(r Reason) Option`
  - Task 3 (`pkg/subject/subject.go`): `func UserPermissionGetPattern(siteID string) string` → `"chat.user.{account}.request.user.%s.permission.get"`
  - Task 7 (`user-service/mongorepo/permissions.go`): `func NewPermissionRepo(db *mongo.Database) *PermissionRepo`, `func (r *PermissionRepo) GetLatestGrant(ctx, siteID string, permission model.PermissionKey, subjectAccount string) (*model.PermissionGrant, error)`
- Produces (wire shapes Task 13 documents in `docs/client-api.md` §3.4):
  - NATS subject: `chat.user.{account}.request.user.{siteID}.permission.get`
  - Request `models.PermissionGetRequest`: `{"permission": "external.image.view"}`
  - Response `models.PermissionGetResponse`: `{"permission": "external.image.view", "granted": true}`
  - Error case: 400, reason `unknown_permission`, when `permission` is not a known key

#### Part A — models/permission.go

- [ ] **Step 1: Write the failing models test**

Create `user-service/models/permission_test.go`:

```go
package models

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPermissionGetRequest_RoundTrip(t *testing.T) {
	in := PermissionGetRequest{Permission: "external.image.view"}
	b, err := json.Marshal(in)
	require.NoError(t, err)
	var out PermissionGetRequest
	require.NoError(t, json.Unmarshal(b, &out))
	require.Equal(t, in, out)
}

func TestPermissionGetRequest_UnmarshalsFromWireBody(t *testing.T) {
	var req PermissionGetRequest
	require.NoError(t, json.Unmarshal([]byte(`{"permission":"external.image.view"}`), &req))
	assert.Equal(t, "external.image.view", req.Permission)
}

func TestPermissionGetResponse_RoundTrip(t *testing.T) {
	in := PermissionGetResponse{Permission: "external.image.view", Granted: true}
	b, err := json.Marshal(in)
	require.NoError(t, err)
	var out PermissionGetResponse
	require.NoError(t, json.Unmarshal(b, &out))
	require.Equal(t, in, out)
}

func TestPermissionGetResponse_WireShape(t *testing.T) {
	b, err := json.Marshal(PermissionGetResponse{Permission: "external.image.view", Granted: true})
	require.NoError(t, err)
	assert.JSONEq(t, `{"permission":"external.image.view","granted":true}`, string(b))
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `make test SERVICE=user-service`

Expected: FAIL — compile error:

```
# github.com/hmchangw/chat/user-service/models [github.com/hmchangw/chat/user-service/models.test]
user-service/models/permission_test.go:12:10: undefined: PermissionGetRequest
FAIL	github.com/hmchangw/chat/user-service/models [build failed]
```

- [ ] **Step 3: Implement the models**

Create `user-service/models/permission.go`:

```go
package models

// PermissionGetRequest is the body of permission.get.
type PermissionGetRequest struct {
	Permission string `json:"permission"`
}

// PermissionGetResponse echoes the requested key and whether the caller
// currently holds it. No dates: keeps user-service free of timezone handling
// (design §6) — chat-frontend is out of scope for this change.
type PermissionGetResponse struct {
	Permission string `json:"permission"`
	Granted    bool   `json:"granted"`
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `make test SERVICE=user-service`

Expected: PASS — the 4 new tests pass; every other `user-service` package (including Task 7's `mongorepo`) still passes.

#### Part B — wire PermissionRepository through service.go and every New() caller

This part has no test of its own — it is the plumbing the Part C handler test needs to compile against. `New`'s signature changes, which is a breaking change for every direct caller; conflict note 2 above lists all eight.

- [ ] **Step 5: Add the interface, struct field, constructor parameter, mockgen entry, and route registration**

Replace `user-service/service/service.go` in full:

```go
package service

import (
	"context"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/oidc"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/user-service/config"
	"github.com/hmchangw/chat/user-service/models"
)

//go:generate mockgen -destination=mocks/mock_repository.go -package=mocks . SubscriptionRepository,UserRepository,AppRepository,RoomClient,HistoryClient,PresenceClient,EventPublisher,ThreadSubscriptionRepository,SSOTokenRepository,TokenValidator,TokenRefresher,PermissionRepository

// SubscriptionRepository is the consumer-defined interface for subscription persistence (botDM app-subscription rows included).
type SubscriptionRepository interface {
	AggregateSubscriptions(ctx context.Context, account, listType string, favorite bool, withinDays *int, page mongoutil.OffsetPageRequest) (mongoutil.OffsetPageHasMore[model.EnrichedSubscription], error)
	FindChannelsByMembers(ctx context.Context, account string, members []string, page mongoutil.OffsetPageRequest) (mongoutil.OffsetPageHasMore[model.EnrichedSubscription], error)
	GetDMSubscription(ctx context.Context, account, target string) (*model.EnrichedDMSubscription, error)
	GetSubscriptionByRoomID(ctx context.Context, account, roomID string) (*model.EnrichedSubscription, error)
	CountActiveSubscriptions(ctx context.Context, account string) (int, error)
	GetActiveSubscriptions(ctx context.Context, account string, limit int) ([]model.EnrichedSubscription, error)
	GetAppSubscription(ctx context.Context, account, botName string) (*model.Subscription, error)
	SetAppSubscribed(ctx context.Context, account, botName string, subscribed, muted bool) error
}

// UserRepository is the consumer-defined interface for user status persistence.
type UserRepository interface {
	GetUserStatus(ctx context.Context, account string) (*model.User, error)
	SetUserStatus(ctx context.Context, account, text string, isShow *bool) (*model.User, error)
	GetHRInfoByAccounts(ctx context.Context, accounts []string) (map[string]*model.SubscriptionHRInfo, error)
	GetUserSettings(ctx context.Context, account string) (*model.User, error)
	UpdateUserSettings(ctx context.Context, account string, set *model.UserSettings) (*model.User, error)
	GetUserChatlist(ctx context.Context, account string) (*model.User, error)
	UpdateUserChatlist(ctx context.Context, account string, state *model.ChatlistState) (*model.User, error)
}

// AppRepository is the consumer-defined interface for app catalog reads.
type AppRepository interface {
	GetApp(ctx context.Context, appID string) (*model.App, error)
	ListApps(ctx context.Context, account string, page mongoutil.OffsetPageRequest) (mongoutil.OffsetPageHasMore[models.AppListItem], error)
	GetAppsByAssistants(ctx context.Context, botAccounts []string) (map[string]*model.App, error)
	ListAppCategories(ctx context.Context) ([]models.AppCategory, error)
}

// RoomClient is the consumer-defined interface for room-service / room-worker RPC calls.
type RoomClient interface {
	GetRoomsInfo(ctx context.Context, siteID string, roomIDs []string) ([]model.RoomInfo, error)
	CreateDMRoom(ctx context.Context, account, otherAccount string, roomType model.RoomType) (model.Subscription, error)
	GetThreadRoomInfoBatch(ctx context.Context, siteID string, threadRoomIDs []string) ([]model.ThreadRoomInfo, error)
	ClearAllThreadUnread(ctx context.Context, siteID, account string) error
}

// ThreadSubscriptionRepository reads the local thread_subscriptions replica for
// the thread-unread badge.
type ThreadSubscriptionRepository interface {
	ListByAccount(ctx context.Context, account string) ([]model.ThreadUnreadRow, error)
	ListByAccountInRooms(ctx context.Context, account string, roomIDs []string) ([]model.ThreadUnreadRow, error)
}

// HistoryClient is the consumer-defined interface for per-site history-service
// RPCs, fanned out across sites by the thread-inbox aggregator.
type HistoryClient interface {
	GetThreadList(ctx context.Context, siteID string, req model.ThreadSubscriptionListRequest) (model.ThreadSubscriptionListResponse, error)
	RoomsGet(ctx context.Context, siteID string, roomIDs []string, hints map[string]model.RoomTimeHint) (map[string]model.PreviewMessage, error)
}

// PresenceClient is the consumer-defined interface for user-presence-service RPC calls.
type PresenceClient interface {
	QueryPresence(ctx context.Context, siteID string, accounts []string) ([]model.PresenceState, error)
}

// EventPublisher is the consumer-defined interface for fire-and-forget
// federation publishing — a JetStream publish directly into the destination
// site's INBOX stream. Status is last-write-wins and idempotent, so no
// msgID/dedup is needed.
type EventPublisher interface {
	Publish(ctx context.Context, subject string, data []byte) error
}

// SSOTokenRepository is the consumer-defined interface for the SSO token vault (sso_tokens collection; legacy field names kept).
type SSOTokenRepository interface {
	GetByUsername(ctx context.Context, username string) (*model.SSOToken, error)
	Upsert(ctx context.Context, username, ssoToken string, ssoTokenExpMs int64, refreshToken string) error
}

// TokenValidator verifies an SSO token against the configured OIDC issuer; nil when the SSO feature is not configured (endpoints reply unavailable).
type TokenValidator interface {
	Validate(ctx context.Context, raw string) (oidc.Claims, error)
}

// TokenRefresher exchanges a refresh token at the issuer's token endpoint; nil when the SSO feature is not configured.
type TokenRefresher interface {
	Refresh(ctx context.Context, refreshToken string) (oidc.TokenSet, error)
}

// PermissionRepository is the consumer-defined interface for whitelist
// permission-grant reads (permission_grants; admin-service owns all writes).
type PermissionRepository interface {
	GetLatestGrant(ctx context.Context, siteID string, permission model.PermissionKey, subjectAccount string) (*model.PermissionGrant, error)
}

// UserService handles all user-related NATS request/reply endpoints.
type UserService struct {
	subs       SubscriptionRepository
	users      UserRepository
	apps       AppRepository
	threadSubs ThreadSubscriptionRepository
	rooms      RoomClient
	history    HistoryClient
	presence   PresenceClient
	pub        EventPublisher
	// clientPub fans out ephemeral client-facing events (settings.update) over
	// core NATS — same delivery pattern as room-worker's subscription.update.
	clientPub        EventPublisher
	ssoTokens        SSOTokenRepository
	tokenValidator   TokenValidator
	tokenRefresher   TokenRefresher
	permissions      PermissionRepository
	ssoRefreshWindow time.Duration
	siteID           string
	allSiteIDs       []string
	maxSubs          int
	defaultLimit     int
	maxApps          int
	defaultApps      int
	maxAccountNames  int
}

// New constructs a UserService with the given dependencies and configuration.
func New(subs SubscriptionRepository, users UserRepository, apps AppRepository, threadSubs ThreadSubscriptionRepository, rooms RoomClient, history HistoryClient, presence PresenceClient, pub, clientPub EventPublisher, ssoTokens SSOTokenRepository, tokenValidator TokenValidator, tokenRefresher TokenRefresher, permissions PermissionRepository, cfg *config.Config) *UserService {
	return &UserService{
		subs:             subs,
		users:            users,
		apps:             apps,
		threadSubs:       threadSubs,
		rooms:            rooms,
		history:          history,
		presence:         presence,
		pub:              pub,
		clientPub:        clientPub,
		ssoTokens:        ssoTokens,
		tokenValidator:   tokenValidator,
		tokenRefresher:   tokenRefresher,
		permissions:      permissions,
		ssoRefreshWindow: cfg.SSORefreshWindow,
		siteID:           cfg.SiteID,
		allSiteIDs:       cfg.AllSiteIDs,
		maxSubs:          cfg.MaxSubscriptionLimit,
		defaultLimit:     cfg.DefaultSubscriptionLimit,
		maxApps:          cfg.MaxAppsLimit,
		defaultApps:      cfg.DefaultAppsLimit,
		maxAccountNames:  cfg.MaxAccountNames,
	}
}

// RegisterHandlers wires all UserService endpoints onto the router.
// siteID is a literal token in each pattern — this instance only subscribes to its own siteID subjects.
func (s *UserService) RegisterHandlers(r *natsrouter.Router) {
	natsrouter.RegisterNoBody(r, subject.UserMePattern(s.siteID), s.Me)
	natsrouter.Register(r, subject.UserStatusGetByNamePattern(s.siteID), s.GetStatusByName)
	natsrouter.Register(r, subject.UserProfileGetByNamePattern(s.siteID), s.GetProfileByName)
	natsrouter.Register(r, subject.UserStatusSetPattern(s.siteID), s.SetStatus)
	natsrouter.RegisterNoBody(r, subject.UserSettingsGetPattern(s.siteID), s.GetSettings)
	natsrouter.Register(r, subject.UserSettingsSetPattern(s.siteID), s.SetSettings)
	natsrouter.RegisterNoBody(r, subject.UserChatlistGetPattern(s.siteID), s.GetChatlist)
	natsrouter.Register(r, subject.UserChatlistSectionCreatePattern(s.siteID), s.CreateChatlistSection)
	natsrouter.Register(r, subject.UserChatlistSectionDeletePattern(s.siteID), s.DeleteChatlistSection)
	natsrouter.Register(r, subject.UserChatlistSectionRenamePattern(s.siteID), s.RenameChatlistSection)
	natsrouter.Register(r, subject.UserChatlistSectionReorderPattern(s.siteID), s.ReorderChatlistSections)
	natsrouter.Register(r, subject.UserChatlistSectionSetSortModePattern(s.siteID), s.SetChatlistSectionSortMode)
	natsrouter.Register(r, subject.UserSubscriptionListPattern(s.siteID), s.ListSubscriptions)
	natsrouter.Register(r, subject.UserThreadListPattern(s.siteID), s.ListUserThreads)
	natsrouter.Register(r, subject.UserThreadUnreadSummaryPattern(s.siteID), s.GetThreadUnreadSummary)
	natsrouter.Register(r, subject.UserThreadReadAllPattern(s.siteID), s.ClearAllThreadUnread)
	natsrouter.Register(r, subject.UserSubscriptionGetChannelsPattern(s.siteID), s.GetChannels)
	natsrouter.Register(r, subject.UserSubscriptionGetDMPattern(s.siteID), s.GetDM)
	natsrouter.Register(r, subject.UserSubscriptionGetByRoomIDPattern(s.siteID), s.GetByRoomID)
	natsrouter.Register(r, subject.UserSubscriptionCountPattern(s.siteID), s.CountSubscriptions)
	natsrouter.Register(r, subject.UserSubscriptionSetAppSubscriptionPattern(s.siteID), s.SetAppSubscription)
	natsrouter.Register(r, subject.UserAppsListPattern(s.siteID), s.ListApps)
	natsrouter.RegisterNoBody(r, subject.UserAppsCategoriesPattern(s.siteID), s.ListAppCategories)
	natsrouter.Register(r, subject.UserSSOSetPattern(s.siteID), s.SSOSet)
	natsrouter.RegisterOptionalBody(r, subject.UserSSORefreshPattern(s.siteID), s.SSORefresh)
	natsrouter.Register(r, subject.UserPermissionGetPattern(s.siteID), s.GetPermission)
}
```

- [ ] **Step 6: Update every other `New(...)` call site in lockstep**

`New` now takes 14 positional args instead of 13 — every direct caller must add one argument (a `PermissionRepository`, real mock or `nil`) immediately before `cfg`. There are 7 such callers besides `service.go` itself; `main.go` is handled in Step 13.

Replace `user-service/service/service_test.go` in full:

```go
package service

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/user-service/config"
	"github.com/hmchangw/chat/user-service/service/mocks"
)

func newSvc(t *testing.T) (*UserService, *mocks.MockSubscriptionRepository, *mocks.MockUserRepository, *mocks.MockAppRepository, *mocks.MockRoomClient, *mocks.MockHistoryClient, *mocks.MockEventPublisher) {
	t.Helper()
	ctrl := gomock.NewController(t)
	subs := mocks.NewMockSubscriptionRepository(ctrl)
	users := mocks.NewMockUserRepository(ctrl)
	apps := mocks.NewMockAppRepository(ctrl)
	rooms := mocks.NewMockRoomClient(ctrl)
	history := mocks.NewMockHistoryClient(ctrl)
	presence := mocks.NewMockPresenceClient(ctrl)
	pub := mocks.NewMockEventPublisher(ctrl)
	cfg := &config.Config{SiteID: "site-a", AllSiteIDs: []string{"site-a", "site-b"}, MaxSubscriptionLimit: 1000, DefaultSubscriptionLimit: 40, MaxAppsLimit: 100, DefaultAppsLimit: 20, MaxAccountNames: 100, SSORefreshWindow: time.Hour}
	threadSubs := mocks.NewMockThreadSubscriptionRepository(ctrl)
	ssoTokens := mocks.NewMockSSOTokenRepository(ctrl)
	validator := mocks.NewMockTokenValidator(ctrl)
	refresher := mocks.NewMockTokenRefresher(ctrl)
	permissions := mocks.NewMockPermissionRepository(ctrl)
	// ListSubscriptions now enriches last-message via history.RoomsGet; default it to a
	// no-op so list tests that don't exercise last-message need no per-test stub.
	history.EXPECT().RoomsGet(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	// countUnread's thread phase reads pending rooms' thread-subs; default to none so
	// room-count tests that don't exercise threads need no per-test stub.
	threadSubs.EXPECT().ListByAccountInRooms(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	// The same mock backs both publishers (federation + client fanout) —
	// expectations are subject-scoped, so tests stay unambiguous.
	return New(subs, users, apps, threadSubs, rooms, history, presence, pub, pub, ssoTokens, validator, refresher, permissions, cfg), subs, users, apps, rooms, history, pub
}

// ctx builds a handler context. siteID is retained for readability but unused
// by handlers — site isolation is structural at the subject level.
func ctx(account, siteID string) *natsrouter.Context {
	return natsrouter.NewContext(map[string]string{"account": account, "siteID": siteID})
}

func requireCode(t *testing.T, err error, code errcode.Code) {
	t.Helper()
	require.Error(t, err)
	var ee *errcode.Error
	if errors.As(err, &ee) {
		assert.Equal(t, code, ee.Code)
		return
	}
	// Raw wrapped errors (no *errcode.Error in chain) classify to CodeInternal.
	assert.Equal(t, errcode.CodeInternal, code, "raw error %T classifies to CodeInternal, not %q", err, code)
}
```

`newSvc`'s return tuple is unchanged (still 7 values) — `permissions` is constructed but not returned, the same treatment `threadSubs`/`ssoTokens`/`validator`/`refresher` already get, because no test driven through `newSvc` needs to `.EXPECT()` on it.

In `user-service/service/sso_test.go`, replace the `newSSOSvc` function (lines 20-37) with:

```go
// newSSOSvc builds a UserService exposing the SSO-relevant mocks; other deps are mocked but unused by the sso handlers.
func newSSOSvc(t *testing.T) (*UserService, *mocks.MockUserRepository, *mocks.MockSSOTokenRepository, *mocks.MockTokenValidator, *mocks.MockTokenRefresher) {
	t.Helper()
	ctrl := gomock.NewController(t)
	users := mocks.NewMockUserRepository(ctrl)
	ssoTokens := mocks.NewMockSSOTokenRepository(ctrl)
	validator := mocks.NewMockTokenValidator(ctrl)
	refresher := mocks.NewMockTokenRefresher(ctrl)
	cfg := &config.Config{SiteID: "site-a", SSORefreshWindow: time.Hour}
	svc := New(
		mocks.NewMockSubscriptionRepository(ctrl), users, mocks.NewMockAppRepository(ctrl),
		mocks.NewMockThreadSubscriptionRepository(ctrl), mocks.NewMockRoomClient(ctrl),
		mocks.NewMockHistoryClient(ctrl), mocks.NewMockPresenceClient(ctrl),
		mocks.NewMockEventPublisher(ctrl), mocks.NewMockEventPublisher(ctrl),
		ssoTokens, validator, refresher, nil, cfg,
	)
	return svc, users, ssoTokens, validator, refresher
}
```

(only the last line of the `New(...)` call changed: `ssoTokens, validator, refresher, cfg,` → `ssoTokens, validator, refresher, nil, cfg,` — `permissions` is unused by every SSO handler.)

In `user-service/service/me_test.go`, replace the `newMeSvc` function (lines 16-37) with:

```go
// newMeSvc builds a UserService exposing the user + presence mocks /me drives.
func newMeSvc(t *testing.T) (*UserService, *mocks.MockUserRepository, *mocks.MockPresenceClient) {
	t.Helper()
	ctrl := gomock.NewController(t)
	users := mocks.NewMockUserRepository(ctrl)
	presence := mocks.NewMockPresenceClient(ctrl)
	cfg := &config.Config{SiteID: "site-a", AllSiteIDs: []string{"site-a", "site-b"}, MaxAccountNames: 100}
	svc := New(
		mocks.NewMockSubscriptionRepository(ctrl),
		users,
		mocks.NewMockAppRepository(ctrl),
		mocks.NewMockThreadSubscriptionRepository(ctrl),
		mocks.NewMockRoomClient(ctrl),
		mocks.NewMockHistoryClient(ctrl),
		presence,
		mocks.NewMockEventPublisher(ctrl),
		mocks.NewMockEventPublisher(ctrl),
		nil, nil, nil, nil,
		cfg,
	)
	return svc, users, presence
}
```

(`nil, nil, nil,` → `nil, nil, nil, nil,` — one more `nil` for `permissions`.)

In `user-service/service/threads_test.go`, replace the `newThreadSvc` function (lines 20-44) with:

```go
// newThreadSvc builds a UserService whose fan-out set is site-a (local) + site-b
// (cross), from ALL_SITE_IDS. The thread inbox only depends on the history
// client, so the other deps are fresh no-expectation mocks.
func newThreadSvc(t *testing.T) (*UserService, *mocks.MockHistoryClient, *mocks.MockUserRepository, *mocks.MockAppRepository) {
	t.Helper()
	ctrl := gomock.NewController(t)
	history := mocks.NewMockHistoryClient(ctrl)
	users := mocks.NewMockUserRepository(ctrl)
	apps := mocks.NewMockAppRepository(ctrl)
	cfg := &config.Config{SiteID: "site-a", AllSiteIDs: []string{"site-a", "site-b"}, MaxSubscriptionLimit: 1000, MaxAccountNames: 100}
	svc := New(
		mocks.NewMockSubscriptionRepository(ctrl),
		users,
		apps,
		mocks.NewMockThreadSubscriptionRepository(ctrl),
		mocks.NewMockRoomClient(ctrl),
		history,
		mocks.NewMockPresenceClient(ctrl),
		mocks.NewMockEventPublisher(ctrl),
		mocks.NewMockEventPublisher(ctrl),
		nil, nil, nil, nil,
		cfg,
	)
	return svc, history, users, apps
}
```

In `user-service/service/status_test.go`, replace the `TestPublishStatus_SkipsEmptyDest` function (lines 145-160) with:

```go
func TestPublishStatus_SkipsEmptyDest(t *testing.T) {
	ctrl := gomock.NewController(t)
	subs := mocks.NewMockSubscriptionRepository(ctrl)
	users := mocks.NewMockUserRepository(ctrl)
	apps := mocks.NewMockAppRepository(ctrl)
	rooms := mocks.NewMockRoomClient(ctrl)
	history := mocks.NewMockHistoryClient(ctrl)
	presence := mocks.NewMockPresenceClient(ctrl)
	pub := mocks.NewMockEventPublisher(ctrl)
	cfg := &config.Config{SiteID: "site-a", AllSiteIDs: []string{"site-a", "", "site-b"}, MaxSubscriptionLimit: 1000}
	threadSubs := mocks.NewMockThreadSubscriptionRepository(ctrl)
	svc := New(subs, users, apps, threadSubs, rooms, history, presence, pub, pub, nil, nil, nil, nil, cfg)
	// Only "site-b" must receive a publish; self "site-a" and the blank "" are skipped.
	pub.EXPECT().Publish(gomock.Any(), subject.InboxExternal("site-b", model.InboxUserStatusUpdated), gomock.Any()).Return(nil)
	svc.publishStatus(ctx("alice", "site-a"), "alice", "busy", nil)
}
```

(`presence := mocks.NewMockPresenceClient(ctrl)` note: `presence` is unused directly by this test — it already was before this change; leave it as-is.) The line that changed: `New(subs, users, apps, threadSubs, rooms, history, presence, pub, pub, nil, nil, nil, cfg)` → `New(subs, users, apps, threadSubs, rooms, history, presence, pub, pub, nil, nil, nil, nil, cfg)`.

In `user-service/service/subscriptions_test.go`, replace the `newSvcRawHistory` function (lines 22-38) with:

```go
// newSvcRawHistory builds a service exposing the history mock WITHOUT newSvc's
// permissive RoomsGet default, so last-message enrichment tests can set an exact
// RoomsGet expectation (result or error).
func newSvcRawHistory(t *testing.T) (*UserService, *mocks.MockSubscriptionRepository, *mocks.MockHistoryClient) {
	t.Helper()
	ctrl := gomock.NewController(t)
	subs := mocks.NewMockSubscriptionRepository(ctrl)
	users := mocks.NewMockUserRepository(ctrl)
	apps := mocks.NewMockAppRepository(ctrl)
	rooms := mocks.NewMockRoomClient(ctrl)
	history := mocks.NewMockHistoryClient(ctrl)
	presence := mocks.NewMockPresenceClient(ctrl)
	pub := mocks.NewMockEventPublisher(ctrl)
	threadSubs := mocks.NewMockThreadSubscriptionRepository(ctrl)
	cfg := &config.Config{SiteID: "site-a", AllSiteIDs: []string{"site-a", "site-b"}, MaxSubscriptionLimit: 1000, DefaultSubscriptionLimit: 40, MaxAppsLimit: 100, DefaultAppsLimit: 20, MaxAccountNames: 100}
	return New(subs, users, apps, threadSubs, rooms, history, presence, pub, pub, nil, nil, nil, nil, cfg), subs, history
}
```

and the `newCountSvc` function (lines 635-651) with:

```go
// newCountSvc builds a service exposing the subscription, room, and thread-sub
// mocks the thread-aware unread tests drive. maxSubs is large; per-test GetActiveSubscriptions
// stubs control the fetched page directly.
func newCountSvc(t *testing.T) (*UserService, *mocks.MockSubscriptionRepository, *mocks.MockRoomClient, *mocks.MockThreadSubscriptionRepository) {
	t.Helper()
	ctrl := gomock.NewController(t)
	subs := mocks.NewMockSubscriptionRepository(ctrl)
	users := mocks.NewMockUserRepository(ctrl)
	apps := mocks.NewMockAppRepository(ctrl)
	rooms := mocks.NewMockRoomClient(ctrl)
	history := mocks.NewMockHistoryClient(ctrl)
	presence := mocks.NewMockPresenceClient(ctrl)
	pub := mocks.NewMockEventPublisher(ctrl)
	threadSubs := mocks.NewMockThreadSubscriptionRepository(ctrl)
	cfg := &config.Config{SiteID: "site-a", AllSiteIDs: []string{"site-a", "site-b"}, MaxSubscriptionLimit: 1000, DefaultSubscriptionLimit: 40, MaxAppsLimit: 100, DefaultAppsLimit: 20, MaxAccountNames: 100}
	return New(subs, users, apps, threadSubs, rooms, history, presence, pub, pub, nil, nil, nil, nil, cfg), subs, rooms, threadSubs
}
```

(both changed identically: `nil, nil, nil, cfg)` → `nil, nil, nil, nil, cfg)`.)

- [ ] **Step 7: Regenerate mocks**

Run: `make generate SERVICE=user-service`

Expected: exits 0, no output. `user-service/service/mocks/mock_repository.go` is rewritten in place — its header comment and `//go:generate` command now list `PermissionRepository`, and it gains `MockPermissionRepository` / `MockPermissionRepositoryMockRecorder` / `NewMockPermissionRepository` / `(*MockPermissionRepository) GetLatestGrant` / `(*MockPermissionRepositoryMockRecorder) GetLatestGrant`, generated in the same shape as every other mock in that file. Do not hand-edit this file. Verify with:

```bash
grep -c MockPermissionRepository user-service/service/mocks/mock_repository.go
```

Expected: a non-zero count.

- [ ] **Step 8: Run the full user-service test suite as a wiring checkpoint**

Run: `make test SERVICE=user-service`

Expected: PASS — confirms the signature change compiled cleanly everywhere and broke nothing, before the handler itself exists.

#### Part C — the GetPermission handler

- [ ] **Step 9: Write the failing handler tests**

Create `user-service/service/permission_test.go`:

```go
package service

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/user-service/config"
	"github.com/hmchangw/chat/user-service/models"
	"github.com/hmchangw/chat/user-service/service/mocks"
)

// newPermissionSvc builds a UserService exposing the permissions mock; other
// deps are mocked but unused by GetPermission.
func newPermissionSvc(t *testing.T) (*UserService, *mocks.MockPermissionRepository) {
	t.Helper()
	ctrl := gomock.NewController(t)
	permissions := mocks.NewMockPermissionRepository(ctrl)
	cfg := &config.Config{SiteID: "site-a"}
	svc := New(
		mocks.NewMockSubscriptionRepository(ctrl), mocks.NewMockUserRepository(ctrl), mocks.NewMockAppRepository(ctrl),
		mocks.NewMockThreadSubscriptionRepository(ctrl), mocks.NewMockRoomClient(ctrl),
		mocks.NewMockHistoryClient(ctrl), mocks.NewMockPresenceClient(ctrl),
		mocks.NewMockEventPublisher(ctrl), mocks.NewMockEventPublisher(ctrl),
		mocks.NewMockSSOTokenRepository(ctrl), mocks.NewMockTokenValidator(ctrl), mocks.NewMockTokenRefresher(ctrl),
		permissions, cfg,
	)
	return svc, permissions
}

func TestGetPermission_GrantedActive(t *testing.T) {
	svc, permissions := newPermissionSvc(t)
	from := time.Now().Add(-1 * time.Hour)
	until := time.Now().Add(1 * time.Hour)
	permissions.EXPECT().GetLatestGrant(gomock.Any(), "site-a", model.PermissionExternalImageView, "alice").
		Return(&model.PermissionGrant{Granted: true, EffectiveFrom: &from, ExpiresAt: &until}, nil)

	resp, err := svc.GetPermission(ctx("alice", "site-a"), models.PermissionGetRequest{Permission: "external.image.view"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Granted)
	assert.Equal(t, "external.image.view", resp.Permission, "response must echo the requested key")
}

func TestGetPermission_NoRecord(t *testing.T) {
	svc, permissions := newPermissionSvc(t)
	permissions.EXPECT().GetLatestGrant(gomock.Any(), "site-a", model.PermissionExternalImageView, "alice").
		Return(nil, nil)

	resp, err := svc.GetPermission(ctx("alice", "site-a"), models.PermissionGetRequest{Permission: "external.image.view"})
	require.NoError(t, err)
	assert.False(t, resp.Granted)
	assert.Equal(t, "external.image.view", resp.Permission)
}

// TestGetPermission_NotCurrentlyGranted covers expired, not-yet-effective,
// revoked, and malformed (bounds absent — must fail closed, never panic) rows.
// The handler never has a fixed "now" to test against (it calls time.Now()
// directly), so fixtures use generous ±1h/±2h offsets from the real clock.
func TestGetPermission_NotCurrentlyGranted(t *testing.T) {
	past := time.Now().Add(-2 * time.Hour)
	justPast := time.Now().Add(-1 * time.Hour)
	future := time.Now().Add(1 * time.Hour)
	farFuture := time.Now().Add(2 * time.Hour)

	tests := map[string]*model.PermissionGrant{
		"expired":           {Granted: true, EffectiveFrom: &past, ExpiresAt: &justPast},
		"not yet effective": {Granted: true, EffectiveFrom: &future, ExpiresAt: &farFuture},
		"revoked":           {Granted: false},
		"effectiveFrom absent (malformed, fail closed)": {Granted: true, ExpiresAt: &future},
		"expiresAt absent (malformed, fail closed)":     {Granted: true, EffectiveFrom: &past},
	}
	for name, grant := range tests {
		t.Run(name, func(t *testing.T) {
			svc, permissions := newPermissionSvc(t)
			permissions.EXPECT().GetLatestGrant(gomock.Any(), "site-a", model.PermissionExternalImageView, "alice").
				Return(grant, nil)
			resp, err := svc.GetPermission(ctx("alice", "site-a"), models.PermissionGetRequest{Permission: "external.image.view"})
			require.NoError(t, err)
			assert.False(t, resp.Granted)
		})
	}
}

func TestGetPermission_UnknownPermissionKey(t *testing.T) {
	svc, _ := newPermissionSvc(t)
	// No GetLatestGrant EXPECT set: an unexpected call would fail the gomock
	// controller, proving validation runs before the repo is ever touched.
	_, err := svc.GetPermission(ctx("alice", "site-a"), models.PermissionGetRequest{Permission: "not.a.real.permission"})
	requireCode(t, err, errcode.CodeBadRequest)
	var ee *errcode.Error
	require.ErrorAs(t, err, &ee)
	assert.Equal(t, errcode.PermissionUnknownKey, ee.Reason)
}

func TestGetPermission_RepoError(t *testing.T) {
	svc, permissions := newPermissionSvc(t)
	permissions.EXPECT().GetLatestGrant(gomock.Any(), "site-a", model.PermissionExternalImageView, "alice").
		Return(nil, errors.New("mongo down"))

	_, err := svc.GetPermission(ctx("alice", "site-a"), models.PermissionGetRequest{Permission: "external.image.view"})
	// Raw wrapped error — classified to the generic boundary code by the router, not here.
	require.Error(t, err)
	var ee *errcode.Error
	assert.False(t, errors.As(err, &ee), "store errors must stay raw, not pre-classified")
}

func TestGetPermission_IgnoresAccountInRequestBody(t *testing.T) {
	svc, permissions := newPermissionSvc(t)
	// PermissionGetRequest (models/permission.go) carries no account field at all.
	// gomock's EXACT-argument match on subjectAccount=="alice" — the ctx()-derived
	// account, not anything from the body — IS the assertion that the handler
	// reads identity only from c.Param("account").
	permissions.EXPECT().GetLatestGrant(gomock.Any(), "site-a", model.PermissionExternalImageView, "alice").
		Return(&model.PermissionGrant{}, nil)

	resp, err := svc.GetPermission(ctx("alice", "site-a"), models.PermissionGetRequest{Permission: "external.image.view"})
	require.NoError(t, err)
	assert.False(t, resp.Granted)
}
```

This covers all eight spec §10 scenarios (Granted, absent×2, expired, not yet effective, revoked, no record, unknown key, store error) plus the explicit body-account test, across 6 test functions (10 executions counting subtests).

- [ ] **Step 10: Run the tests to verify they fail**

Run: `make test SERVICE=user-service`

Expected: FAIL — compile error:

```
# github.com/hmchangw/chat/user-service/service [github.com/hmchangw/chat/user-service/service.test]
user-service/service/permission_test.go:47:15: svc.GetPermission undefined (type *UserService has no field or method GetPermission)
FAIL	github.com/hmchangw/chat/user-service/service [build failed]
```

- [ ] **Step 11: Implement the handler**

Create `user-service/service/permission.go`:

```go
package service

import (
	"fmt"
	"time"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/user-service/models"
)

// GetPermission reports whether the caller currently holds a whitelist
// permission. Identity comes only from {account} — never the body (design §7).
func (s *UserService) GetPermission(c *natsrouter.Context, req models.PermissionGetRequest) (*models.PermissionGetResponse, error) {
	account := c.Param("account")
	c.WithLogValues("account", account)
	key := model.PermissionKey(req.Permission)
	if !model.KnownPermission(key) {
		return nil, errcode.BadRequest("unknown permission", errcode.WithReason(errcode.PermissionUnknownKey))
	}
	latest, err := s.permissions.GetLatestGrant(c, s.siteID, key, account)
	if err != nil {
		return nil, fmt.Errorf("get latest permission grant: %w", err)
	}
	granted := model.EvaluateGrant(latest, time.Now().UTC())
	return &models.PermissionGetResponse{Permission: req.Permission, Granted: granted}, nil
}
```

- [ ] **Step 12: Run the tests to verify they pass**

Run: `make test SERVICE=user-service`

Expected: PASS — all `TestGetPermission_*` tests pass; the whole `user-service` tree stays green.

#### Part D — production wiring

- [ ] **Step 13: Wire the repository into main.go and finish the compile-time assertion in setup_test.go**

Replace `user-service/main.go` in full:

```go
package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/obs"
	pkgoidc "github.com/hmchangw/chat/pkg/oidc"
	"github.com/hmchangw/chat/pkg/shutdown"
	"github.com/hmchangw/chat/user-service/config"
	"github.com/hmchangw/chat/user-service/historyclient"
	"github.com/hmchangw/chat/user-service/mongorepo"
	"github.com/hmchangw/chat/user-service/presenceclient"
	"github.com/hmchangw/chat/user-service/publisher"
	"github.com/hmchangw/chat/user-service/roomclient"
	"github.com/hmchangw/chat/user-service/service"
)

// Compile-time interface assertions — fail the build if implementations drift.
var (
	_ service.SubscriptionRepository       = (*mongorepo.SubscriptionRepo)(nil)
	_ service.UserRepository               = (*mongorepo.UserRepo)(nil)
	_ service.AppRepository                = (*mongorepo.AppRepo)(nil)
	_ service.ThreadSubscriptionRepository = (*mongorepo.ThreadSubscriptionRepo)(nil)
	_ service.RoomClient                   = (*roomclient.Client)(nil)
	_ service.HistoryClient                = (*historyclient.Client)(nil)
	_ service.PresenceClient               = (*presenceclient.Client)(nil)
	_ service.EventPublisher               = (*publisher.Publisher)(nil)
	_ service.EventPublisher               = (*publisher.CorePublisher)(nil)
	_ service.SSOTokenRepository           = (*mongorepo.SSOTokenRepo)(nil)
	_ service.TokenValidator               = (*pkgoidc.Validator)(nil)
	_ service.TokenRefresher               = (*pkgoidc.Validator)(nil)
	_ service.PermissionRepository         = (*mongorepo.PermissionRepo)(nil)
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("parse config", "error", err)
		os.Exit(1)
	}

	if err := model.SetPlatformAdminAccountPrefix(cfg.AdminAcctPrefix); err != nil {
		slog.Error("invalid ADMIN_ACCT_PREFIX", "error", err)
		os.Exit(1)
	}

	ctx := context.Background()

	sdk, obsShutdown, err := obs.Init(ctx)
	if err != nil {
		slog.Error("init observability failed", "error", err)
		os.Exit(1)
	}

	nc, err := natsutil.Connect(ctx, cfg.NATS.URL, cfg.NATS.CredsFile, sdk.TracerProvider(), sdk.Propagator, sdk.Toggles.Trace)
	if err != nil {
		slog.Error("nats connect failed", "error", err)
		os.Exit(1)
	}

	js, err := nc.JetStream()
	if err != nil {
		slog.Error("jetstream init failed", "error", err)
		os.Exit(1)
	}

	mongoClient, err := mongoutil.Connect(ctx, cfg.Mongo.URI, cfg.Mongo.Username, cfg.Mongo.Password, mongoutil.WithObservability(sdk))
	if err != nil {
		slog.Error("mongo connect failed", "error", err)
		os.Exit(1)
	}

	// Client stays on primary; each repo opts into secondary reads via
	// WithReadPreference (already validated in config).
	readPref, err := mongoutil.ParseReadPreference(cfg.Mongo.ReadPreference)
	if err != nil {
		slog.Error("invalid mongo read preference", "value", cfg.Mongo.ReadPreference, "error", err)
		os.Exit(1)
	}
	slog.Info("mongo secondary-read preference configured", "readPreference", readPref.Mode().String())
	readFromSecondary := mongorepo.WithReadPreference(readPref)

	db := mongoClient.Database(cfg.Mongo.DB)
	subRepo := mongorepo.NewSubscriptionRepo(db, cfg.SiteID, readFromSecondary)
	userRepo := mongorepo.NewUserRepo(db, readFromSecondary)
	appRepo := mongorepo.NewAppRepo(db, readFromSecondary)
	threadSubRepo := mongorepo.NewThreadSubscriptionRepo(db)
	ssoTokenRepo := mongorepo.NewSSOTokenRepo(db)
	permissionRepo := mongorepo.NewPermissionRepo(db)
	if err := subRepo.EnsureIndexes(ctx); err != nil {
		slog.Error("ensure indexes failed", "error", err)
		os.Exit(1)
	}
	if err := userRepo.EnsureIndexes(ctx); err != nil {
		slog.Error("ensure indexes failed", "error", err)
		os.Exit(1)
	}
	if err := appRepo.EnsureIndexes(ctx); err != nil {
		slog.Error("ensure indexes failed", "error", err)
		os.Exit(1)
	}
	if err := threadSubRepo.EnsureIndexes(ctx); err != nil {
		slog.Error("ensure indexes failed", "error", err)
		os.Exit(1)
	}
	if err := ssoTokenRepo.EnsureIndexes(ctx); err != nil {
		slog.Error("ensure indexes failed", "error", err)
		os.Exit(1)
	}
	// permissionRepo has no EnsureIndexes call: admin-service alone creates the
	// permission_grants indexes (spec §3.6) — calling it from both services risks
	// IndexKeySpecsConflict and a crash loop.

	tokenValidator, tokenRefresher, err := oidcValidator(ctx, &cfg)
	if err != nil {
		slog.Error("oidc validator init failed", "error", err)
		os.Exit(1)
	}

	svc := service.New(subRepo, userRepo, appRepo, threadSubRepo, roomclient.New(nc, cfg.SiteID), historyclient.New(nc), presenceclient.New(nc), publisher.New(js), publisher.NewCore(nc), ssoTokenRepo, tokenValidator, tokenRefresher, permissionRepo, &cfg)

	// Bound in-flight handlers so a burst is shed at the door (ErrUnavailable)
	// instead of piling unbounded work onto MongoDB. MAX_CONCURRENCY=0 disables.
	var routerOpts []natsrouter.Option
	if cfg.MaxConcurrency > 0 {
		routerOpts = append(routerOpts, natsrouter.WithMaxConcurrency(cfg.MaxConcurrency))
	}
	router := natsrouter.New(nc, "user-service", routerOpts...)
	router.Use(natsrouter.Recovery())
	// RequestID must precede any handler that reads request_id from ctx —
	// otherwise Classify's log line records an empty value.
	router.Use(natsrouter.RequestID())
	router.Use(natsrouter.Logging())
	// After Logging so the timeout wraps the handler chain; bounds the Mongo
	// aggregations from hanging past the configured deadline.
	router.Use(natsrouter.HandlerTimeout(cfg.HandlerTimeout))

	svc.RegisterHandlers(router)

	slog.Info("user-service running", "site", cfg.SiteID)

	shutdown.Wait(ctx, 25*time.Second,
		func(ctx context.Context) error { return router.Shutdown(ctx) },
		func(ctx context.Context) error { return nc.Drain() },
		func(ctx context.Context) error { mongoutil.Disconnect(ctx, mongoClient); return nil },
		func(ctx context.Context) error { return obsShutdown(ctx) },
	)
}
```

Replace `user-service/mongorepo/setup_test.go` in full — only the `var (...)` block gains one line versus Task 7's version:

```go
//go:build integration

package mongorepo

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/testutil"
	"github.com/hmchangw/chat/user-service/service"
)

// Compile-time assertions: `go vet -tags integration` fails if any repo drifts from its interface.
var (
	_ service.SubscriptionRepository = (*SubscriptionRepo)(nil)
	_ service.UserRepository         = (*UserRepo)(nil)
	_ service.AppRepository          = (*AppRepo)(nil)
	_ service.SSOTokenRepository     = (*SSOTokenRepo)(nil)
	_ service.PermissionRepository   = (*PermissionRepo)(nil)
)

// newTestSubscriptionRepo builds a SubscriptionRepo with siteID "site-a"; seed cross-site rows with a different siteId to exercise the deleted-filter.
func newTestSubscriptionRepo(t *testing.T) (*SubscriptionRepo, *mongo.Database) {
	t.Helper()
	db := testutil.MongoDB(t, "user-service")
	r := NewSubscriptionRepo(db, "site-a")
	require.NoError(t, r.EnsureIndexes(context.Background()))
	return r, db
}

// newTestUserRepo builds a UserRepo over an isolated test database.
func newTestUserRepo(t *testing.T) (*UserRepo, *mongo.Database) {
	t.Helper()
	db := testutil.MongoDB(t, "user-service")
	r := NewUserRepo(db)
	require.NoError(t, r.EnsureIndexes(context.Background()))
	return r, db
}

// newTestAppRepo builds an AppRepo over an isolated test database.
func newTestAppRepo(t *testing.T) (*AppRepo, *mongo.Database) {
	t.Helper()
	db := testutil.MongoDB(t, "user-service")
	return NewAppRepo(db), db
}

// newTestSSOTokenRepo builds an SSOTokenRepo over an isolated test database.
func newTestSSOTokenRepo(t *testing.T) (*SSOTokenRepo, *mongo.Database) {
	t.Helper()
	db := testutil.MongoDB(t, "user-service")
	r := NewSSOTokenRepo(db)
	require.NoError(t, r.EnsureIndexes(context.Background()))
	return r, db
}

// newTestPermissionRepo builds a PermissionRepo over an isolated test
// database. No EnsureIndexes call: unlike every other repo in this file,
// PermissionRepo has no such method — admin-service alone creates the
// permission_grants indexes (spec §3.6).
func newTestPermissionRepo(t *testing.T) (*PermissionRepo, *mongo.Database) {
	t.Helper()
	db := testutil.MongoDB(t, "user-service")
	return NewPermissionRepo(db), db
}

// seed inserts raw docs into a collection on db.
func seed(t *testing.T, db *mongo.Database, coll string, docs ...any) {
	t.Helper()
	_, err := db.Collection(coll).InsertMany(context.Background(), docs)
	require.NoError(t, err)
}
```

- [ ] **Step 14: Verify the full user-service tree, including the integration build tag**

Run: `make test SERVICE=user-service`

Expected: PASS.

Run: `make test-integration SERVICE=user-service` (requires Docker)

Expected: PASS — this is what actually compiles `setup_test.go` under `-tags integration` and exercises the new `_ service.PermissionRepository = (*PermissionRepo)(nil)` assertion plus every Task 7 test.

- [ ] **Step 15: Lint**

Run: `make lint`

Expected: exits 0, no findings.

- [ ] **Step 16: Commit**

```bash
git add user-service/models/permission.go user-service/models/permission_test.go \
  user-service/service/service.go user-service/service/service_test.go \
  user-service/service/sso_test.go user-service/service/me_test.go \
  user-service/service/threads_test.go user-service/service/status_test.go \
  user-service/service/subscriptions_test.go user-service/service/permission.go \
  user-service/service/permission_test.go user-service/service/mocks/mock_repository.go \
  user-service/mongorepo/setup_test.go user-service/main.go
git commit -m "feat(user-service): permission.get NATS handler"
```
## Phase 4: admin-frontend (Tasks 9–11)

### Conflicts / gaps found in the skeleton, and how they were resolved

All three tasks below were fully implemented and test-verified end-to-end against the
real repo during drafting (`npm ci`, red/green cycles run for real, `npm run typecheck`,
`npm run build`), then reverted so the worktree stays clean for the executing engineer.
Every "run FAIL" / "run PASS" block below is real captured output, not a prediction.

1. **`admin-frontend/src/api/index.ts` (the `@/api` barrel) is missing from the skeleton's
   Task 9 file map, but is required.** Every existing component imports api functions from
   the barrel (`import { AsyncJobError, listAudit } from '@/api'` in `AuditView.jsx`), never
   from `@/api/admin` directly, and Task 10/11's own test instructions say to
   `vi.mock('@/api', ...)` exactly like `AuditView.test.jsx` does. `PermissionsView.jsx`
   therefore must import `createPermissions`/`listPermissions` from `@/api`, which means the
   barrel must re-export them. Verified experimentally: without this, `npm test` still passes
   (tests mock `@/api` wholesale) but `npm run build` and any real browser session would break
   the first time either function is called (`createPermissions is not a function`). Added
   `admin-frontend/src/api/index.ts` as an explicit Task 9 file + step.
2. **`admin-frontend/src/api/_transport/httpEnvelope.ts` and its test file aren't in Task 9's
   file map**, despite the task prose explicitly requiring the `REASON_COPY` addition there.
   Added both as explicit Task 9 files.
3. **`style.css` is attributed only to Task 10 in the file map**, but Task 11's lookup pane
   (badge, table, search form) needs its own rules. Task 11 modifies `style.css` too.
4. **The exact `errcode.WithMetadata` key(s) for `unknown_accounts` / `inactive_subject` are
   not specified anywhere available to this drafting pass** (that lives in Tasks 5–6, a
   separate phase). Spec §4.4 only guarantees the offending accounts appear in the *message*
   string (aimed at a curl caller), not a specific metadata key. Resolved by rendering
   `err.metadata` generically (`Object.entries(...)`) in the result panel — this surfaces
   whatever key/value pairs the backend ends up sending without hard-coding a guess, and
   simply renders nothing extra if metadata is absent (the mapped `REASON_COPY` message alone
   still shows). This is a design decision, not a blocker.
5. **The plan-drafting "split into two steps if >250 lines" rule** is interpreted as applying
   to each step's own new/changed code, not the cumulative printed file size. Measured after
   building both for real: Task 10 introduces 234 lines (full component, form pane only),
   Task 11 adds 131 more lines (lookup pane) — both individually under 250 — so neither task
   is split into markup/handlers sub-steps. The full two-pane file is 365 lines when Task 11
   is done; that's a consequence of "print the whole file," not of either task being oversized.
6. `PermissionsView.jsx`/`PermissionsView.test.jsx` are listed "Create" for both "10,11" in the
   skeleton's file map — clarified here: **Task 10 creates them (form pane only); Task 11
   modifies them (adds the lookup pane to the same file/test file)**.

All code below is the exact, verified content — copy it as-is.

---

### Task 9: api client

**Files:**
- Modify: `admin-frontend/src/api/admin/index.ts`
- Modify: `admin-frontend/src/api/admin/admin.test.ts`
- Modify: `admin-frontend/src/api/_transport/httpEnvelope.ts` (gap #2 above)
- Modify: `admin-frontend/src/api/_transport/httpEnvelope.test.ts` (gap #2 above)
- Modify: `admin-frontend/src/api/index.ts` (gap #1 above)

**Interfaces:**
- Consumes: the wire JSON admin-service's `POST`/`GET /v1/admin/permissions` produce (spec
  §4.3, §4.6; Task 5+6 Go types `createPermissionsResponse`, `listPermissionsResponse`,
  `permissionGrantView`) and the 8 error `reason` strings from `pkg/errcode/codes_permission.go`
  (Task 2): `unknown_permission`, `invalid_subject_count`, `invalid_reason`,
  `missing_permission_fields`, `invalid_permission_window`, `unexpected_permission_window`,
  `inactive_subject`, `unknown_accounts`.
- Produces (verbatim from skeleton, all in `admin-frontend/src/api/admin/index.ts`):

```ts
export interface PermissionGrantView {
  id: string
  permission: string
  subjectAccount: string
  granted: boolean
  effectiveFrom?: string   // "2026-09-01"
  expiresAt?: string       // "2026-12-31"
  expiresAtUTC?: string    // RFC3339
  applicantAccount: string
  approverAccount: string
  reason: string
  recordedBy: string
  recordedAt: string
}

export interface CreatePermissionsRequest {
  permission: string
  subjectAccounts: string[]
  granted: boolean
  effectiveFrom?: string
  expiresAt?: string
  applicantAccount: string
  approverAccount: string
  reason: string
}

export interface CreatePermissionsResponse {
  created: number
  duplicatesIgnored: string[]
  grants: { id: string; subjectAccount: string }[]
}

export interface ListPermissionsResponse {
  currentlyGranted?: boolean
  entries: PermissionGrantView[]
  total: number
}

export async function createPermissions(authToken: string, body: CreatePermissionsRequest): Promise<CreatePermissionsResponse>
export async function listPermissions(authToken: string, params: { subjectAccount: string; permission?: string; page?: number; limit?: number }): Promise<ListPermissionsResponse>
```

Both functions, plus all four new types, are additionally re-exported from
`admin-frontend/src/api/index.ts` — this is what Tasks 10/11 actually import.

#### Steps

- [ ] **Step 1 — failing test.** Add to `admin-frontend/src/api/admin/admin.test.ts`: insert
  `createPermissions` and `listPermissions` into the existing alphabetical import list, and
  append two new `describe` blocks after the existing `listAudit` block (end of file). Full
  new content of the import block and the appended blocks:

  ```ts
  import { afterEach, describe, expect, it, vi } from 'vitest'
  import { AsyncJobError } from '@/api'
  import {
    createPermissions,
    createUser,
    getUser,
    listAudit,
    listPermissions,
    listSessions,
    listUsers,
    revokeAllSessions,
    revokeSession,
    setPassword,
    updateUser,
  } from './index'
  ```

  (the rest of the file — `mockResponse`, `stubFetch`, `USER`, and every existing `describe`
  block through `listAudit` — is unchanged; append the following immediately after the closing
  `})` of the `listAudit` describe block, i.e. at end of file):

  ```ts

  const GRANT_VIEW = {
    id: 'g-1',
    permission: 'external.image.view',
    subjectAccount: 'alice',
    granted: true,
    effectiveFrom: '2026-09-01',
    expiresAt: '2026-12-31',
    expiresAtUTC: '2026-12-31T16:00:00Z',
    applicantAccount: 'carol',
    approverAccount: 'dave',
    reason: 'On-call staff must review production line photos from outside the fab.',
    recordedBy: 'p_admin_wang',
    recordedAt: '2026-08-11T03:00:00Z',
  }

  describe('createPermissions', () => {
    afterEach(() => vi.unstubAllGlobals())

    it('POSTs /v1/admin/permissions with the request body and returns the created response', async () => {
      const response = {
        created: 2,
        duplicatesIgnored: [],
        grants: [
          { id: 'g-1', subjectAccount: 'alice' },
          { id: 'g-2', subjectAccount: 'bob' },
        ],
      }
      const fetchMock = stubFetch(201, response)

      const result = await createPermissions('tok', {
        permission: 'external.image.view',
        subjectAccounts: ['alice', 'bob'],
        granted: true,
        effectiveFrom: '2026-09-01',
        expiresAt: '2026-12-31',
        applicantAccount: 'carol',
        approverAccount: 'dave',
        reason: 'On-call staff must review production line photos from outside the fab.',
      })

      const [url, init] = fetchMock.mock.calls[0]
      expect(url).toBe('http://localhost:8082/v1/admin/permissions')
      expect(init.method).toBe('POST')
      expect(init.headers.Authorization).toBe('Bearer tok')
      expect(init.headers['Content-Type']).toBe('application/json')
      expect(JSON.parse(init.body)).toEqual({
        permission: 'external.image.view',
        subjectAccounts: ['alice', 'bob'],
        granted: true,
        effectiveFrom: '2026-09-01',
        expiresAt: '2026-12-31',
        applicantAccount: 'carol',
        approverAccount: 'dave',
        reason: 'On-call staff must review production line photos from outside the fab.',
      })
      expect(result).toEqual(response)
    })

    it('throws AsyncJobError with reason preserved on a non-2xx response', async () => {
      stubFetch(404, {
        error: 'unknown accounts: zzz',
        code: 'not_found',
        reason: 'unknown_accounts',
      })
      const body = {
        permission: 'external.image.view',
        subjectAccounts: ['zzz'],
        granted: true,
        expiresAt: '2026-12-31',
        applicantAccount: 'carol',
        approverAccount: 'dave',
        reason: 'test',
      }

      await expect(createPermissions('tok', body)).rejects.toBeInstanceOf(AsyncJobError)
      await expect(createPermissions('tok', body)).rejects.toMatchObject({
        reason: 'unknown_accounts',
      })
    })
  })

  describe('listPermissions', () => {
    afterEach(() => vi.unstubAllGlobals())

    it('GETs /v1/admin/permissions with subjectAccount plus optional params when all are provided', async () => {
      const fetchMock = stubFetch(200, { entries: [GRANT_VIEW], total: 1, currentlyGranted: true })

      const result = await listPermissions('tok', {
        subjectAccount: 'alice',
        permission: 'external.image.view',
        page: 2,
        limit: 10,
      })

      const [url, init] = fetchMock.mock.calls[0]
      const parsed = new URL(url)
      expect(parsed.pathname).toBe('/v1/admin/permissions')
      expect(parsed.searchParams.get('subjectAccount')).toBe('alice')
      expect(parsed.searchParams.get('permission')).toBe('external.image.view')
      expect(parsed.searchParams.get('page')).toBe('2')
      expect(parsed.searchParams.get('limit')).toBe('10')
      expect(init.method).toBe('GET')
      expect(result).toEqual({ entries: [GRANT_VIEW], total: 1, currentlyGranted: true })
    })

    it('omits permission/page/limit from the query string when not provided', async () => {
      const fetchMock = stubFetch(200, { entries: [], total: 0 })

      await listPermissions('tok', { subjectAccount: 'alice' })

      const [url] = fetchMock.mock.calls[0]
      expect(url).toBe('http://localhost:8082/v1/admin/permissions?subjectAccount=alice')
    })
  })
  ```

  Also add one test to `admin-frontend/src/api/_transport/httpEnvelope.test.ts`, in the
  existing `formatAsyncJobError` describe block, immediately after the `invalid_token` test:

  ```ts
    it('returns friendly copy for unknown_accounts', () => {
      const err = new AsyncJobError('unknown accounts: zzz', { reason: 'unknown_accounts' })
      const msg = formatAsyncJobError(err)
      expect(msg).not.toBe('')
      expect(msg).not.toBe('unknown accounts: zzz')
    })
  ```

- [ ] **Step 2 — run, confirm FAIL.**
  ```bash
  cd admin-frontend && npx vitest run src/api/admin/admin.test.ts src/api/_transport/httpEnvelope.test.ts
  ```
  Expected (captured real output, trimmed):
  ```
   ❯ src/api/_transport/httpEnvelope.test.ts (10 tests | 1 failed)
     × formatAsyncJobError > returns friendly copy for unknown_accounts
       → expected 'unknown accounts: zzz' not to be 'unknown accounts: zzz' // Object.is equality
   ❯ src/api/admin/admin.test.ts (20 tests | 4 failed)
     × createPermissions > POSTs /v1/admin/permissions with the request body and returns the created response
       → createPermissions is not a function
     × createPermissions > throws AsyncJobError with reason preserved on a non-2xx response
       → createPermissions is not a function
     × listPermissions > GETs /v1/admin/permissions with subjectAccount plus optional params when all are provided
       → listPermissions is not a function
     × listPermissions > omits permission/page/limit from the query string when not provided
       → listPermissions is not a function

   Test Files  2 failed (2)
        Tests  5 failed | 25 passed (30)
  ```

- [ ] **Step 3 — implementation.** In `admin-frontend/src/api/_transport/httpEnvelope.ts`,
  replace the `REASON_COPY` object:

  ```ts
  const REASON_COPY: Record<string, string> = {
    not_admin: 'You need admin access to do that.',
    invalid_token: 'That link has expired or is invalid — request a new one.',
    account_exists: 'An account with that email already exists.',
    unknown_permission: "That permission isn't recognized.",
    invalid_subject_count: 'Enter between 1 and 200 accounts.',
    invalid_reason: 'Enter a reason, up to 1000 characters.',
    missing_permission_fields: 'Applicant and approver are both required.',
    invalid_permission_window:
      "Check the dates — the start date must be on or before the expiry date, and the expiry date can't be in the past.",
    unexpected_permission_window:
      "Revoking a permission doesn't use the date fields — leave them blank.",
    inactive_subject: "Some accounts are deactivated and can't be granted this permission.",
    unknown_accounts: 'Some accounts do not exist on this site.',
  }
  ```

  In `admin-frontend/src/api/admin/index.ts`, insert the four new interfaces immediately after
  the existing `AuditFilter` interface:

  ```ts
  /** One row of the permission ledger (mirrors admin-service's `permissionGrantView`). */
  export interface PermissionGrantView {
    id: string
    permission: string
    subjectAccount: string
    granted: boolean
    effectiveFrom?: string // "2026-09-01"
    expiresAt?: string // "2026-12-31"
    expiresAtUTC?: string // RFC3339
    applicantAccount: string
    approverAccount: string
    reason: string
    recordedBy: string
    recordedAt: string
  }

  export interface CreatePermissionsRequest {
    permission: string
    subjectAccounts: string[]
    granted: boolean
    effectiveFrom?: string
    expiresAt?: string
    applicantAccount: string
    approverAccount: string
    reason: string
  }

  export interface CreatePermissionsResponse {
    created: number
    duplicatesIgnored: string[]
    grants: { id: string; subjectAccount: string }[]
  }

  export interface ListPermissionsResponse {
    currentlyGranted?: boolean
    entries: PermissionGrantView[]
    total: number
  }
  ```

  and append the two functions at the end of the file, immediately after `listAudit`:

  ```ts
  /** @throws {AsyncJobError} on a non-2xx response (e.g. `unknown_accounts`, `inactive_subject`). */
  export async function createPermissions(
    authToken: string,
    body: CreatePermissionsRequest,
  ): Promise<CreatePermissionsResponse> {
    return adminFetch<CreatePermissionsResponse>(authToken, 'POST', '/permissions', body)
  }

  /** @throws {AsyncJobError} on a non-2xx response. */
  export async function listPermissions(
    authToken: string,
    params: { subjectAccount: string; permission?: string; page?: number; limit?: number },
  ): Promise<ListPermissionsResponse> {
    const qs = buildQuery({
      subjectAccount: params.subjectAccount,
      permission: params.permission,
      page: params.page,
      limit: params.limit,
    })
    return adminFetch<ListPermissionsResponse>(authToken, 'GET', `/permissions${qs}`)
  }
  ```

  Finally, in `admin-frontend/src/api/index.ts`, replace the `export { ... } from './admin'` and
  `export type { ... } from './admin'` blocks (gap #1):

  ```ts
  export {
    createPermissions,
    createUser,
    getUser,
    listAudit,
    listPermissions,
    listSessions,
    listUsers,
    revokeAllSessions,
    revokeSession,
    setPassword,
    updateUser,
  } from './admin'
  export type {
    AdminSession,
    AdminUser,
    AuditEntry,
    AuditFilter,
    CreatePermissionsRequest,
    CreatePermissionsResponse,
    CreateUserInput,
    ListPermissionsResponse,
    ListUsersParams,
    PermissionGrantView,
    SetPasswordInput,
    UpdateUserPatch,
  } from './admin'
  ```

- [ ] **Step 4 — run, confirm PASS.**
  ```bash
  cd admin-frontend && npx vitest run src/api/admin/admin.test.ts src/api/_transport/httpEnvelope.test.ts
  ```
  Expected (captured real output):
  ```
   ✓ src/api/_transport/httpEnvelope.test.ts (10 tests)
   ✓ src/api/admin/admin.test.ts (20 tests)

   Test Files  2 passed (2)
        Tests  30 passed (30)
  ```
  Also run `cd admin-frontend && npm run typecheck` — confirmed clean (`tsc --noEmit`, no
  output, exit 0).

- [ ] **Step 5 — commit.**
  ```
  feat(admin-frontend): permissions api client
  ```

---

### Task 10: PermissionsView form pane + nav

**Files:**
- Create: `admin-frontend/src/components/PermissionsView/index.jsx`
- Create: `admin-frontend/src/components/PermissionsView/style.css` (form-pane rules; Task 11
  appends lookup-pane rules to this same file)
- Create: `admin-frontend/src/components/PermissionsView/PermissionsView.jsx` (form pane only;
  Task 11 adds the lookup pane to this same file)
- Create: `admin-frontend/src/components/PermissionsView/PermissionsView.test.jsx` (form-pane
  tests only; Task 11 appends lookup-pane tests to this same file)
- Modify: `admin-frontend/src/components/AppShell/AppShell.jsx`
- Modify: `admin-frontend/src/components/AppShell/AppShell.test.jsx`

**Interfaces:**
- Consumes: `createPermissions` + `CreatePermissionsRequest`/`CreatePermissionsResponse` (Task
  9, imported from `@/api`); existing `AsyncJobError`, `useAuth`, `useHandleAdminError`.
- Produces: `PermissionsView` default export — a self-contained component with no props (reads
  `useAuth().session.authToken` itself, exactly like `AuditView`/`UsersPage`); the
  `admin-frontend/src/components/PermissionsView/index.jsx` barrel; `AppShell`'s new
  `{ key: 'permissions', label: 'Permissions' }` nav entry and lazy import.

#### Steps

- [ ] **Step 1 — failing test.** Create `admin-frontend/src/components/PermissionsView/PermissionsView.test.jsx`:

  ```jsx
  import { describe, it, expect, vi, beforeEach } from 'vitest'
  import { act, render, screen, fireEvent, waitFor } from '@testing-library/react'

  vi.mock('@/context/AuthContext', () => ({ useAuth: vi.fn() }))
  vi.mock('@/api', async (importOriginal) => {
    const actual = await importOriginal()
    return { ...actual, createPermissions: vi.fn(), listPermissions: vi.fn() }
  })

  import PermissionsView from './PermissionsView'
  import { useAuth } from '@/context/AuthContext'
  import { createPermissions, listPermissions, AsyncJobError } from '@/api'

  function fillCommonFields() {
    fireEvent.change(screen.getByLabelText(/subject accounts/i), {
      target: { value: 'alice, bob' },
    })
    fireEvent.change(screen.getByLabelText(/applicant account/i), { target: { value: 'carol' } })
    fireEvent.change(screen.getByLabelText(/approver account/i), { target: { value: 'dave' } })
    fireEvent.change(screen.getByLabelText(/^reason/i), {
      target: { value: 'On-call staff must review photos.' },
    })
  }

  let logout

  beforeEach(() => {
    vi.clearAllMocks()
    logout = vi.fn()
    useAuth.mockReturnValue({
      session: { authToken: 'tok', account: 'root', siteId: 'site-1' },
      logout,
    })
    listPermissions.mockResolvedValue({ entries: [], total: 0 })
  })

  describe('PermissionsView — grant/revoke form', () => {
    it('renders the form fields with grant mode selected by default', () => {
      render(<PermissionsView />)
      expect(screen.getByRole('radio', { name: /^grant$/i })).toBeChecked()
      expect(screen.getByRole('radio', { name: /^revoke$/i })).not.toBeChecked()
      expect(screen.getByLabelText(/subject accounts/i)).toBeInTheDocument()
      expect(screen.getByLabelText(/effective from/i)).toBeInTheDocument()
      expect(screen.getByLabelText(/expires at/i)).toBeInTheDocument()
      expect(screen.getByLabelText(/applicant account/i)).toBeInTheDocument()
      expect(screen.getByLabelText(/approver account/i)).toBeInTheDocument()
      expect(screen.getByLabelText(/^reason/i)).toBeInTheDocument()
      expect(screen.getByRole('button', { name: /grant permission/i })).toBeInTheDocument()
    })

    it('updates the live subject count as the textarea changes, parsing on whitespace and commas', () => {
      render(<PermissionsView />)
      expect(screen.getByText(/0 accounts/i)).toBeInTheDocument()

      fireEvent.change(screen.getByLabelText(/subject accounts/i), {
        target: { value: 'alice, bob\ncharlie' },
      })

      expect(screen.getByText(/3 accounts/i)).toBeInTheDocument()
    })

    it('shows a client-side error and disables submit when more than 200 subjects are entered', () => {
      render(<PermissionsView />)
      // Fill every other required field validly so the disabled submit button can only be
      // attributed to the subject-count rule, not to the form being incomplete otherwise.
      fireEvent.change(screen.getByLabelText(/applicant account/i), { target: { value: 'carol' } })
      fireEvent.change(screen.getByLabelText(/approver account/i), { target: { value: 'dave' } })
      fireEvent.change(screen.getByLabelText(/^reason/i), { target: { value: 'Valid reason.' } })
      fireEvent.change(screen.getByLabelText(/expires at/i), { target: { value: '2026-12-31' } })
      const many = Array.from({ length: 201 }, (_, i) => `user${i}`).join(',')

      fireEvent.change(screen.getByLabelText(/subject accounts/i), { target: { value: many } })

      expect(screen.getByText(/enter at most 200 accounts/i)).toBeInTheDocument()
      expect(screen.getByRole('button', { name: /grant permission/i })).toBeDisabled()
    })

    it('hides the effectiveFrom/expiresAt date inputs in revoke mode', () => {
      render(<PermissionsView />)
      fireEvent.click(screen.getByRole('radio', { name: /^revoke$/i }))

      expect(screen.queryByLabelText(/effective from/i)).not.toBeInTheDocument()
      expect(screen.queryByLabelText(/expires at/i)).not.toBeInTheDocument()
      expect(screen.getByRole('button', { name: /revoke permission/i })).toBeInTheDocument()
    })

    it('submits a grant with the exact payload, omitting effectiveFrom when left blank', async () => {
      createPermissions.mockResolvedValue({ created: 2, duplicatesIgnored: [], grants: [] })
      render(<PermissionsView />)
      fillCommonFields()
      fireEvent.change(screen.getByLabelText(/expires at/i), { target: { value: '2026-12-31' } })

      fireEvent.click(screen.getByRole('button', { name: /grant permission/i }))

      await waitFor(() => expect(createPermissions).toHaveBeenCalledTimes(1))
      const [token, payload] = createPermissions.mock.calls[0]
      expect(token).toBe('tok')
      expect(payload).toEqual({
        permission: 'external.image.view',
        subjectAccounts: ['alice', 'bob'],
        granted: true,
        expiresAt: '2026-12-31',
        applicantAccount: 'carol',
        approverAccount: 'dave',
        reason: 'On-call staff must review photos.',
      })
      expect(payload).not.toHaveProperty('effectiveFrom')
    })

    it('includes effectiveFrom in the grant payload when filled in', async () => {
      createPermissions.mockResolvedValue({ created: 1, duplicatesIgnored: [], grants: [] })
      render(<PermissionsView />)
      fillCommonFields()
      fireEvent.change(screen.getByLabelText(/effective from/i), {
        target: { value: '2026-09-01' },
      })
      fireEvent.change(screen.getByLabelText(/expires at/i), { target: { value: '2026-12-31' } })

      fireEvent.click(screen.getByRole('button', { name: /grant permission/i }))

      await waitFor(() => expect(createPermissions).toHaveBeenCalledTimes(1))
      const [, payload] = createPermissions.mock.calls[0]
      expect(payload.effectiveFrom).toBe('2026-09-01')
    })

    it('submits a revoke with the window fields omitted entirely', async () => {
      createPermissions.mockResolvedValue({ created: 1, duplicatesIgnored: [], grants: [] })
      render(<PermissionsView />)
      fireEvent.click(screen.getByRole('radio', { name: /^revoke$/i }))
      fillCommonFields()

      fireEvent.click(screen.getByRole('button', { name: /revoke permission/i }))

      await waitFor(() => expect(createPermissions).toHaveBeenCalledTimes(1))
      const [, payload] = createPermissions.mock.calls[0]
      expect(payload).toEqual({
        permission: 'external.image.view',
        subjectAccounts: ['alice', 'bob'],
        granted: false,
        applicantAccount: 'carol',
        approverAccount: 'dave',
        reason: 'On-call staff must review photos.',
      })
      expect(payload).not.toHaveProperty('effectiveFrom')
      expect(payload).not.toHaveProperty('expiresAt')
    })

    it('disables the submit button while the create request is in flight', async () => {
      let resolveCreate
      createPermissions.mockReturnValue(
        new Promise((resolve) => {
          resolveCreate = resolve
        }),
      )
      render(<PermissionsView />)
      fillCommonFields()
      fireEvent.change(screen.getByLabelText(/expires at/i), { target: { value: '2026-12-31' } })

      fireEvent.click(screen.getByRole('button', { name: /grant permission/i }))

      expect(screen.getByRole('button', { name: /submitting/i })).toBeDisabled()

      await act(async () => {
        resolveCreate({ created: 2, duplicatesIgnored: [], grants: [] })
        await Promise.resolve()
      })

      expect(screen.getByRole('button', { name: /grant permission/i })).toBeEnabled()
    })

    it('renders created count and duplicatesIgnored on success', async () => {
      createPermissions.mockResolvedValue({
        created: 2,
        duplicatesIgnored: ['eve'],
        grants: [
          { id: 'g-1', subjectAccount: 'alice' },
          { id: 'g-2', subjectAccount: 'bob' },
        ],
      })
      render(<PermissionsView />)
      fillCommonFields()
      fireEvent.change(screen.getByLabelText(/expires at/i), { target: { value: '2026-12-31' } })

      fireEvent.click(screen.getByRole('button', { name: /grant permission/i }))

      expect(await screen.findByText(/created 2/i)).toBeInTheDocument()
      expect(screen.getByText(/eve/)).toBeInTheDocument()
    })

    it('renders the mapped copy and offending accounts when createPermissions rejects with a reason', async () => {
      createPermissions.mockRejectedValue(
        new AsyncJobError('unknown accounts: zzz', {
          code: 'not_found',
          reason: 'unknown_accounts',
          metadata: { accounts: 'zzz' },
        }),
      )
      render(<PermissionsView />)
      fillCommonFields()
      fireEvent.change(screen.getByLabelText(/expires at/i), { target: { value: '2026-12-31' } })

      fireEvent.click(screen.getByRole('button', { name: /grant permission/i }))

      expect(await screen.findByText(/do not exist on this site/i)).toBeInTheDocument()
      expect(screen.getByText(/zzz/)).toBeInTheDocument()
    })

    it('logs the admin out instead of showing a banner on invalid_token', async () => {
      createPermissions.mockRejectedValue(
        new AsyncJobError('expired', { code: 'unauthenticated', reason: 'invalid_token' }),
      )
      render(<PermissionsView />)
      fillCommonFields()
      fireEvent.change(screen.getByLabelText(/expires at/i), { target: { value: '2026-12-31' } })

      fireEvent.click(screen.getByRole('button', { name: /grant permission/i }))

      await waitFor(() => expect(logout).toHaveBeenCalledTimes(1))
      expect(screen.queryByText(/expired/i)).not.toBeInTheDocument()
    })
  })
  ```

  Also modify `admin-frontend/src/components/AppShell/AppShell.test.jsx`: extend the `@/api`
  mock factory and the `beforeEach`, and add one new `it`:

  ```jsx
  import { describe, it, expect, vi, beforeEach } from 'vitest'
  import { render, screen, fireEvent, waitFor } from '@testing-library/react'

  vi.mock('@/context/AuthContext', () => ({ useAuth: vi.fn() }))
  vi.mock('@/api', async (importOriginal) => {
    const actual = await importOriginal()
    return {
      ...actual,
      listUsers: vi.fn(),
      listAudit: vi.fn(),
      createPermissions: vi.fn(),
      listPermissions: vi.fn(),
    }
  })

  import AppShell from './AppShell'
  import { useAuth } from '@/context/AuthContext'
  import { listUsers, listAudit, listPermissions } from '@/api'

  beforeEach(() => {
    vi.clearAllMocks()
    useAuth.mockReturnValue({
      session: { authToken: 'tok', account: 'root', siteId: 'site-1' },
      logout: vi.fn(),
    })
    listUsers.mockResolvedValue({ users: [], total: 0 })
    listAudit.mockResolvedValue({ entries: [], total: 0 })
    listPermissions.mockResolvedValue({ entries: [], total: 0 })
  })

  describe('AppShell', () => {
    it('shows the signed-in account and mounts Users by default', async () => {
      render(<AppShell />)
      expect(screen.getByText(/root/i)).toBeInTheDocument()
      await waitFor(() => expect(listUsers).toHaveBeenCalledWith('tok', { page: 1, limit: 20 }))
    })

    it('calls logout when the Logout button is clicked', async () => {
      const logout = vi.fn()
      useAuth.mockReturnValue({
        session: { authToken: 'tok', account: 'root', siteId: 'site-1' },
        logout,
      })
      render(<AppShell />)
      await waitFor(() => expect(listUsers).toHaveBeenCalled())

      fireEvent.click(screen.getByRole('button', { name: /log out/i }))
      expect(logout).toHaveBeenCalledTimes(1)
    })

    it('switches from Users to Audit via nav and mounts AuditView', async () => {
      render(<AppShell />)
      await waitFor(() => expect(listUsers).toHaveBeenCalled())

      fireEvent.click(screen.getByRole('button', { name: /^audit$/i }))

      await waitFor(() => expect(listAudit).toHaveBeenCalledWith('tok', { page: 1, limit: 20 }))
    })

    it('switches back from Audit to Users via nav', async () => {
      render(<AppShell />)
      await waitFor(() => expect(listUsers).toHaveBeenCalled())

      fireEvent.click(screen.getByRole('button', { name: /^audit$/i }))
      await waitFor(() => expect(listAudit).toHaveBeenCalled())

      fireEvent.click(screen.getByRole('button', { name: /^users$/i }))
      await waitFor(() => expect(listUsers).toHaveBeenCalledTimes(2))
    })

    it('switches from Users to Permissions via nav and mounts PermissionsView', async () => {
      render(<AppShell />)
      await waitFor(() => expect(listUsers).toHaveBeenCalled())

      fireEvent.click(screen.getByRole('button', { name: /^permissions$/i }))

      expect(await screen.findByRole('heading', { name: /grant or revoke permission/i })).toBeInTheDocument()
    })
  })
  ```

- [ ] **Step 2 — run, confirm FAIL.**
  ```bash
  cd admin-frontend && npx vitest run src/components/PermissionsView/PermissionsView.test.jsx
  ```
  Expected (captured real output, trimmed):
  ```
   FAIL  src/components/PermissionsView/PermissionsView.test.jsx [ src/components/PermissionsView/PermissionsView.test.jsx ]
  Error: Failed to resolve import "./PermissionsView" from "src/components/PermissionsView/PermissionsView.test.jsx". Does the file exist?
    Plugin: vite:import-analysis

   Test Files  1 failed (1)
        Tests  no tests
  ```
  ```bash
  cd admin-frontend && npx vitest run src/components/AppShell/AppShell.test.jsx
  ```
  Expected (captured real output, trimmed):
  ```
  TestingLibraryElementError: Unable to find an accessible element with the role "button" and name `/^permissions$/i`
   ❯ src/components/AppShell/AppShell.test.jsx:75:28

   Test Files  1 failed (1)
        Tests  1 failed | 4 passed (5)
  ```

- [ ] **Step 3 — implementation.** Create `admin-frontend/src/components/PermissionsView/index.jsx`:

  ```jsx
  export { default } from './PermissionsView'
  ```

  Create `admin-frontend/src/components/PermissionsView/style.css` (form-pane rules; Task 11
  appends more below the last rule):

  ```css
  .permissions-view {
    display: flex;
    flex-direction: column;
    gap: var(--space-2xl);
    padding: var(--space-xl);
    height: 100%;
    overflow-y: auto;
  }

  .permissions-form,
  .permissions-lookup {
    display: flex;
    flex-direction: column;
    gap: var(--space-sm);
    max-width: 640px;
  }

  .permissions-form h2,
  .permissions-lookup h2 {
    font-size: var(--text-lg);
    font-weight: var(--font-semibold);
    color: var(--text-primary);
  }

  .permissions-form-subtitle {
    color: var(--text-muted);
    font-size: var(--text-sm);
  }

  .permissions-form form {
    display: flex;
    flex-direction: column;
    gap: var(--space-xs);
  }

  .permissions-form label {
    margin-top: var(--space-sm);
    font-weight: var(--font-medium);
    font-size: var(--text-sm);
    color: var(--text-secondary);
  }

  .permissions-form input:not([type='radio']),
  .permissions-form textarea {
    display: block;
    width: 100%;
    padding: var(--space-sm) var(--space-md);
    background: var(--bg-input);
    border: 1px solid transparent;
    border-radius: var(--radius-md);
    color: var(--text-primary);
    font: inherit;
    font-size: var(--text-base);
  }

  .permissions-form input:not([type='radio']):focus,
  .permissions-form textarea:focus {
    outline: none;
    background: var(--bg-surface);
    border-color: var(--accent);
    box-shadow: 0 0 0 3px var(--accent-soft);
  }

  .permissions-subjects-textarea,
  .permissions-reason-textarea {
    min-height: 72px;
    resize: vertical;
  }

  .permissions-mode-group {
    display: flex;
    gap: var(--space-lg);
  }

  .permissions-mode-option {
    display: flex;
    align-items: center;
    gap: var(--space-xs);
    margin-top: 0;
    font-weight: var(--font-medium);
    color: var(--text-primary);
    cursor: pointer;
  }

  .permissions-mode-option input {
    accent-color: var(--accent);
  }

  .permissions-subjects-count,
  .permissions-reason-count {
    color: var(--text-muted);
    font-size: var(--text-xs);
  }

  .permissions-subjects-count.is-over-limit {
    color: var(--status-error);
    font-weight: var(--font-medium);
  }

  .permissions-field-error {
    color: var(--status-error);
    font-size: var(--text-sm);
  }

  .permissions-date-row {
    display: flex;
    gap: var(--space-lg);
  }

  .permissions-date-field {
    flex: 1;
  }

  .permissions-date-field label {
    display: block;
  }

  .permissions-form button[type='submit'] {
    align-self: flex-start;
    margin-top: var(--space-sm);
  }

  .permissions-result {
    margin-top: var(--space-md);
    padding: var(--space-md);
    border-radius: var(--radius-md);
  }

  .permissions-duplicates {
    margin-top: var(--space-xs);
  }

  .permissions-metadata-list {
    margin-top: var(--space-xs);
    padding-left: var(--space-lg);
    font-size: var(--text-sm);
  }
  ```

  Create `admin-frontend/src/components/PermissionsView/PermissionsView.jsx` (form pane only —
  this is the version at the end of Task 10; Task 11 adds the lookup pane below it):

  ```jsx
  import { useState } from 'react'
  import { AsyncJobError, createPermissions } from '@/api'
  import { useAuth } from '@/context/AuthContext'
  import { useHandleAdminError } from '@/hooks/useHandleAdminError'
  import './style.css'

  // Sole known permission key today (design doc §1) — no picker needed until a second key ships.
  const PERMISSION_KEY = 'external.image.view'
  const MAX_SUBJECTS = 200
  const MAX_REASON_RUNES = 1000

  // Whitespace/comma separated, matching admin-service's own dedup-and-report contract.
  function parseSubjects(text) {
    return text.split(/[\s,]+/).filter(Boolean)
  }

  // Settings → Permissions console: grant/revoke form for the external.image.view whitelist.
  export default function PermissionsView() {
    const { session } = useAuth()
    const authToken = session?.authToken
    const handleAdminError = useHandleAdminError()

    const [mode, setMode] = useState('grant')
    const [subjectsText, setSubjectsText] = useState('')
    const [effectiveFrom, setEffectiveFrom] = useState('')
    const [expiresAt, setExpiresAt] = useState('')
    const [applicantAccount, setApplicantAccount] = useState('')
    const [approverAccount, setApproverAccount] = useState('')
    const [reason, setReason] = useState('')
    const [submitting, setSubmitting] = useState(false)
    const [formError, setFormError] = useState(null)
    const [formErrorMetadata, setFormErrorMetadata] = useState(null)
    const [result, setResult] = useState(null)

    const subjects = parseSubjects(subjectsText)
    const subjectCount = subjects.length
    const tooManySubjects = subjectCount > MAX_SUBJECTS
    const reasonRuneCount = [...reason].length
    const clientInvalid =
      subjectCount === 0 ||
      tooManySubjects ||
      reasonRuneCount === 0 ||
      reasonRuneCount > MAX_REASON_RUNES ||
      !applicantAccount.trim() ||
      !approverAccount.trim() ||
      (mode === 'grant' && !expiresAt)

    // Builds the wire payload field-by-field so an omitted window field is truly
    // absent from the object (and therefore from the JSON body) — never sent as "".
    const buildPayload = () => {
      const payload = {
        permission: PERMISSION_KEY,
        subjectAccounts: subjects,
        granted: mode === 'grant',
      }
      if (mode === 'grant') {
        if (effectiveFrom) payload.effectiveFrom = effectiveFrom
        payload.expiresAt = expiresAt
      }
      payload.applicantAccount = applicantAccount.trim()
      payload.approverAccount = approverAccount.trim()
      payload.reason = reason
      return payload
    }

    const handleSubmit = async (e) => {
      e.preventDefault()
      if (clientInvalid || submitting) return
      setSubmitting(true)
      setFormError(null)
      setFormErrorMetadata(null)
      setResult(null)
      try {
        const response = await createPermissions(authToken, buildPayload())
        setResult(response)
      } catch (err) {
        const message = handleAdminError(err)
        if (message !== null) {
          setFormError(message)
          if (err instanceof AsyncJobError && err.metadata) setFormErrorMetadata(err.metadata)
        }
      } finally {
        setSubmitting(false)
      }
    }

    return (
      <div className="permissions-view">
        <section className="permissions-form">
          <h2>Grant or revoke permission</h2>
          <p className="permissions-form-subtitle">Permission: {PERMISSION_KEY}</p>

          <form onSubmit={handleSubmit}>
            <div className="permissions-mode-group" role="radiogroup" aria-label="Grant or revoke">
              <label htmlFor="permissions-mode-grant" className="permissions-mode-option">
                <input
                  type="radio"
                  id="permissions-mode-grant"
                  name="permissions-mode"
                  value="grant"
                  checked={mode === 'grant'}
                  onChange={() => setMode('grant')}
                  disabled={submitting}
                />
                Grant
              </label>
              <label htmlFor="permissions-mode-revoke" className="permissions-mode-option">
                <input
                  type="radio"
                  id="permissions-mode-revoke"
                  name="permissions-mode"
                  value="revoke"
                  checked={mode === 'revoke'}
                  onChange={() => setMode('revoke')}
                  disabled={submitting}
                />
                Revoke
              </label>
            </div>

            <label htmlFor="permissions-subjects">Subject accounts</label>
            <textarea
              id="permissions-subjects"
              className="permissions-subjects-textarea"
              placeholder="alice, bob, charlie…"
              value={subjectsText}
              onChange={(e) => setSubjectsText(e.target.value)}
              disabled={submitting}
            />
            <span
              className={`permissions-subjects-count ${tooManySubjects ? 'is-over-limit' : ''}`}
            >
              {subjectCount} account{subjectCount === 1 ? '' : 's'} (max {MAX_SUBJECTS})
            </span>
            {tooManySubjects && (
              <div className="permissions-field-error">
                Enter at most {MAX_SUBJECTS} accounts — you have {subjectCount}.
              </div>
            )}

            {mode === 'grant' && (
              <div className="permissions-date-row">
                <div className="permissions-date-field">
                  <label htmlFor="permissions-effective-from">Effective from (optional)</label>
                  <input
                    type="date"
                    id="permissions-effective-from"
                    value={effectiveFrom}
                    onChange={(e) => setEffectiveFrom(e.target.value)}
                    disabled={submitting}
                  />
                </div>
                <div className="permissions-date-field">
                  <label htmlFor="permissions-expires-at">Expires at</label>
                  <input
                    type="date"
                    id="permissions-expires-at"
                    value={expiresAt}
                    onChange={(e) => setExpiresAt(e.target.value)}
                    disabled={submitting}
                  />
                </div>
              </div>
            )}

            <label htmlFor="permissions-applicant">Applicant account</label>
            <input
              id="permissions-applicant"
              value={applicantAccount}
              onChange={(e) => setApplicantAccount(e.target.value)}
              disabled={submitting}
            />

            <label htmlFor="permissions-approver">Approver account</label>
            <input
              id="permissions-approver"
              value={approverAccount}
              onChange={(e) => setApproverAccount(e.target.value)}
              disabled={submitting}
            />

            <label htmlFor="permissions-reason">Reason</label>
            <textarea
              id="permissions-reason"
              className="permissions-reason-textarea"
              value={reason}
              onChange={(e) => setReason(e.target.value)}
              disabled={submitting}
            />
            <span className="permissions-reason-count">
              {reasonRuneCount} / {MAX_REASON_RUNES}
            </span>

            <button type="submit" className="btn btn-primary" disabled={submitting || clientInvalid}>
              {submitting
                ? 'Submitting…'
                : mode === 'grant'
                  ? 'Grant permission'
                  : 'Revoke permission'}
            </button>
          </form>

          {result && (
            <div className="permissions-result dialog-success">
              <p>
                Created {result.created} grant{result.created === 1 ? '' : 's'}.
              </p>
              {result.duplicatesIgnored.length > 0 && (
                <p className="permissions-duplicates">
                  Duplicates ignored: {result.duplicatesIgnored.join(', ')}
                </p>
              )}
            </div>
          )}

          {formError && (
            <div className="permissions-result dialog-error">
              <p>{formError}</p>
              {formErrorMetadata && Object.keys(formErrorMetadata).length > 0 && (
                <ul className="permissions-metadata-list">
                  {Object.entries(formErrorMetadata).map(([key, value]) => (
                    <li key={key}>
                      {key}: {value}
                    </li>
                  ))}
                </ul>
              )}
            </div>
          )}
        </section>
      </div>
    )
  }
  ```

  Modify `admin-frontend/src/components/AppShell/AppShell.jsx`: add the lazy import and the
  nav entry —

  ```jsx
  const UsersPage = lazy(() => import('@/components/UsersConsole'))
  const AuditView = lazy(() => import('@/components/AuditView'))
  const PermissionsView = lazy(() => import('@/components/PermissionsView'))

  const SECTIONS = [
    { key: 'users', label: 'Users' },
    { key: 'audit', label: 'Audit' },
    { key: 'permissions', label: 'Permissions' },
  ]
  ```

  and the render branch —

  ```jsx
    const renderSection = () => {
      if (section === 'users') return <UsersPage />
      if (section === 'audit') return <AuditView />
      return <PermissionsView />
    }
  ```

  (everything else in `AppShell.jsx` — imports, `handleLogout`, the returned JSX — is
  unchanged).

- [ ] **Step 4 — run, confirm PASS.**
  ```bash
  cd admin-frontend && npx vitest run src/components/PermissionsView/PermissionsView.test.jsx src/components/AppShell/AppShell.test.jsx
  ```
  Expected (captured real output):
  ```
   ✓ src/components/PermissionsView/PermissionsView.test.jsx (11 tests)
   ✓ src/components/AppShell/AppShell.test.jsx (5 tests)
     ✓ AppShell > shows the signed-in account and mounts Users by default
     ✓ AppShell > switches from Users to Audit via nav and mounts AuditView
     ✓ AppShell > switches from Users to Permissions via nav and mounts PermissionsView

   Test Files  2 passed (2)
        Tests  16 passed (16)
  ```

- [ ] **Step 5 — commit.**
  ```
  feat(admin-frontend): PermissionsView grant/revoke form + nav
  ```

---

### Task 11: PermissionsView lookup pane

**Files:**
- Modify: `admin-frontend/src/components/PermissionsView/PermissionsView.jsx` (adds the lookup
  pane to the same component, same file, as its second `<section>`)
- Modify: `admin-frontend/src/components/PermissionsView/PermissionsView.test.jsx` (appends the
  lookup-pane test suite)
- Modify: `admin-frontend/src/components/PermissionsView/style.css` (appends lookup-pane rules)

**Interfaces:**
- Consumes: `listPermissions` + `ListPermissionsResponse`/`PermissionGrantView` (Task 9,
  imported from `@/api`); existing `useLatestRequest` hook and shared `Pager` component
  (`@/components/shared/Pager`, props `{page, limit, total, onPrev, onNext}` — unchanged).
- Produces: the completed two-pane `PermissionsView` (form pane from Task 10 unchanged; lookup
  pane added as the second `<section className="permissions-lookup">`).

#### Steps

- [ ] **Step 1 — failing test.** Append to the end of
  `admin-frontend/src/components/PermissionsView/PermissionsView.test.jsx` (after the closing
  `})` of the `'PermissionsView — grant/revoke form'` describe block from Task 10):

  ```jsx

  const GRANT_ROW = {
    id: 'g-1',
    permission: 'external.image.view',
    subjectAccount: 'alice',
    granted: true,
    effectiveFrom: '2026-09-01',
    expiresAt: '2026-12-31',
    expiresAtUTC: '2026-12-31T16:00:00Z',
    applicantAccount: 'carol',
    approverAccount: 'dave',
    reason: 'Business trip.',
    recordedBy: 'p_admin_wang',
    recordedAt: '2026-08-11T03:00:00Z',
  }

  const REVOKE_ROW = {
    id: 'g-2',
    permission: 'external.image.view',
    subjectAccount: 'alice',
    granted: false,
    applicantAccount: 'carol',
    approverAccount: 'dave',
    reason: 'Project ended.',
    recordedBy: 'p_admin_wang',
    recordedAt: '2026-08-12T03:00:00Z',
  }

  function searchFor(account) {
    fireEvent.change(screen.getByLabelText(/^subject account$/i), { target: { value: account } })
    fireEvent.click(screen.getByRole('button', { name: /^search$/i }))
  }

  describe('PermissionsView — subject lookup', () => {
    it('searches listPermissions with subjectAccount, the fixed permission key, and page 1', async () => {
      render(<PermissionsView />)
      searchFor('alice')

      await waitFor(() =>
        expect(listPermissions).toHaveBeenCalledWith('tok', {
          subjectAccount: 'alice',
          permission: 'external.image.view',
          page: 1,
          limit: 20,
        }),
      )
    })

    it('renders ledger rows, showing "—" for the absent dates on a revoke row', async () => {
      listPermissions.mockResolvedValue({
        entries: [GRANT_ROW, REVOKE_ROW],
        total: 2,
        currentlyGranted: false,
      })
      render(<PermissionsView />)
      searchFor('alice')

      const rows = await screen.findAllByRole('row')
      // header + 2 data rows, newest-first exactly as received (no client-side re-sort)
      expect(rows).toHaveLength(3)
      expect(rows[1]).toHaveTextContent('Grant')
      expect(rows[1]).toHaveTextContent('2026-09-01')
      expect(rows[1]).toHaveTextContent('2026-12-31')
      expect(rows[2]).toHaveTextContent('Revoke')
      expect(rows[2]).toHaveTextContent('—')
    })

    it('shows the "Currently granted" badge when currentlyGranted is true', async () => {
      listPermissions.mockResolvedValue({ entries: [GRANT_ROW], total: 1, currentlyGranted: true })
      render(<PermissionsView />)
      searchFor('alice')

      expect(await screen.findByText(/currently granted/i)).toBeInTheDocument()
    })

    it('shows the "Not granted" badge when currentlyGranted is false', async () => {
      listPermissions.mockResolvedValue({ entries: [REVOKE_ROW], total: 1, currentlyGranted: false })
      render(<PermissionsView />)
      searchFor('alice')

      expect(await screen.findByText(/^not granted$/i)).toBeInTheDocument()
    })

    it('shows no badge when currentlyGranted is absent from the response', async () => {
      listPermissions.mockResolvedValue({ entries: [], total: 0 })
      render(<PermissionsView />)
      searchFor('alice')

      await waitFor(() => expect(listPermissions).toHaveBeenCalled())
      expect(screen.queryByText(/currently granted/i)).not.toBeInTheDocument()
      expect(screen.queryByText(/^not granted$/i)).not.toBeInTheDocument()
    })

    it('requests page 2 on Next without changing the searched account', async () => {
      listPermissions.mockResolvedValue({ entries: [GRANT_ROW], total: 50, currentlyGranted: true })
      render(<PermissionsView />)
      searchFor('alice')
      await waitFor(() =>
        expect(listPermissions).toHaveBeenCalledWith('tok', expect.objectContaining({ page: 1 })),
      )

      fireEvent.click(screen.getByRole('button', { name: /next/i }))

      await waitFor(() =>
        expect(listPermissions).toHaveBeenLastCalledWith('tok', {
          subjectAccount: 'alice',
          permission: 'external.image.view',
          page: 2,
          limit: 20,
        }),
      )
    })

    it('renders a formatted error message when the search fails', async () => {
      listPermissions.mockRejectedValue(new AsyncJobError('boom', { code: 'internal' }))
      render(<PermissionsView />)
      searchFor('alice')

      expect(await screen.findByText(/boom/i)).toBeInTheDocument()
    })
  })
  ```

  (`listPermissions` and `AsyncJobError` are already imported at the top of the file from
  Task 10 — no import changes needed here. Label note: the lookup input's label is the
  *singular* "Subject account", deliberately distinct from the form textarea's *plural*
  "Subject accounts" — `searchFor` uses the anchored regex `/^subject account$/i` so it can't
  accidentally match the plural label too.)

- [ ] **Step 2 — run, confirm FAIL.**
  ```bash
  cd admin-frontend && npx vitest run src/components/PermissionsView/PermissionsView.test.jsx
  ```
  Expected (captured real output, trimmed — the 11 Task-10 tests still pass, the 7 new ones
  fail because the lookup pane doesn't exist yet):
  ```
     × PermissionsView — subject lookup > searches listPermissions with subjectAccount, the fixed permission key, and page 1
     × PermissionsView — subject lookup > renders ledger rows, showing "—" for the absent dates on a revoke row
     × PermissionsView — subject lookup > shows the "Currently granted" badge when currentlyGranted is true
     × PermissionsView — subject lookup > shows the "Not granted" badge when currentlyGranted is false
     × PermissionsView — subject lookup > shows no badge when currentlyGranted is absent from the response
     × PermissionsView — subject lookup > requests page 2 on Next without changing the searched account
     × PermissionsView — subject lookup > renders a formatted error message when the search fails
  ⎯⎯⎯⎯⎯⎯⎯ Failed Tests 7 ⎯⎯⎯⎯⎯⎯⎯
   Test Files  1 failed (1)
        Tests  7 failed | 11 passed (18)
  ```
  Root cause (from the actual thrown error): `TestingLibraryElementError: Unable to find a
  label with the text of: /^subject account$/i` inside `searchFor` — the lookup input doesn't
  exist yet.

- [ ] **Step 3 — implementation.** Replace the full content of
  `admin-frontend/src/components/PermissionsView/PermissionsView.jsx` with (Task 10's form pane
  is unchanged; this adds the imports, `PAGE_SIZE`, the lookup state/handlers, and the second
  `<section>`):

  ```jsx
  import { useState } from 'react'
  import { AsyncJobError, createPermissions, listPermissions } from '@/api'
  import { useAuth } from '@/context/AuthContext'
  import { useHandleAdminError } from '@/hooks/useHandleAdminError'
  import { useLatestRequest } from '@/hooks/useLatestRequest'
  import Pager from '@/components/shared/Pager'
  import './style.css'

  // Sole known permission key today (design doc §1) — no picker needed until a second key ships.
  const PERMISSION_KEY = 'external.image.view'
  const MAX_SUBJECTS = 200
  const MAX_REASON_RUNES = 1000
  // Matches admin-service's parsePaging default limit (handler.go).
  const PAGE_SIZE = 20

  // Whitespace/comma separated, matching admin-service's own dedup-and-report contract.
  function parseSubjects(text) {
    return text.split(/[\s,]+/).filter(Boolean)
  }

  // Settings → Permissions console: grant/revoke form for the external.image.view whitelist.
  export default function PermissionsView() {
    const { session } = useAuth()
    const authToken = session?.authToken
    const handleAdminError = useHandleAdminError()

    const [mode, setMode] = useState('grant')
    const [subjectsText, setSubjectsText] = useState('')
    const [effectiveFrom, setEffectiveFrom] = useState('')
    const [expiresAt, setExpiresAt] = useState('')
    const [applicantAccount, setApplicantAccount] = useState('')
    const [approverAccount, setApproverAccount] = useState('')
    const [reason, setReason] = useState('')
    const [submitting, setSubmitting] = useState(false)
    const [formError, setFormError] = useState(null)
    const [formErrorMetadata, setFormErrorMetadata] = useState(null)
    const [result, setResult] = useState(null)

    const subjects = parseSubjects(subjectsText)
    const subjectCount = subjects.length
    const tooManySubjects = subjectCount > MAX_SUBJECTS
    const reasonRuneCount = [...reason].length
    const clientInvalid =
      subjectCount === 0 ||
      tooManySubjects ||
      reasonRuneCount === 0 ||
      reasonRuneCount > MAX_REASON_RUNES ||
      !applicantAccount.trim() ||
      !approverAccount.trim() ||
      (mode === 'grant' && !expiresAt)

    // Builds the wire payload field-by-field so an omitted window field is truly
    // absent from the object (and therefore from the JSON body) — never sent as "".
    const buildPayload = () => {
      const payload = {
        permission: PERMISSION_KEY,
        subjectAccounts: subjects,
        granted: mode === 'grant',
      }
      if (mode === 'grant') {
        if (effectiveFrom) payload.effectiveFrom = effectiveFrom
        payload.expiresAt = expiresAt
      }
      payload.applicantAccount = applicantAccount.trim()
      payload.approverAccount = approverAccount.trim()
      payload.reason = reason
      return payload
    }

    const handleSubmit = async (e) => {
      e.preventDefault()
      if (clientInvalid || submitting) return
      setSubmitting(true)
      setFormError(null)
      setFormErrorMetadata(null)
      setResult(null)
      try {
        const response = await createPermissions(authToken, buildPayload())
        setResult(response)
      } catch (err) {
        const message = handleAdminError(err)
        if (message !== null) {
          setFormError(message)
          if (err instanceof AsyncJobError && err.metadata) setFormErrorMetadata(err.metadata)
        }
      } finally {
        setSubmitting(false)
      }
    }

    // --- lookup pane (per-subject ledger) ---
    const [lookupAccount, setLookupAccount] = useState('')
    const [searchedAccount, setSearchedAccount] = useState('')
    const [lookupPage, setLookupPage] = useState(1)
    const [entries, setEntries] = useState([])
    const [total, setTotal] = useState(0)
    const [currentlyGranted, setCurrentlyGranted] = useState(undefined)
    const [lookupLoading, setLookupLoading] = useState(false)
    const [lookupError, setLookupError] = useState(null)
    const { begin: beginLookup, isCurrent: isLookupCurrent } = useLatestRequest()

    const fetchLookup = async (account, pageArg) => {
      const token = beginLookup()
      setLookupLoading(true)
      setLookupError(null)
      try {
        const res = await listPermissions(authToken, {
          subjectAccount: account,
          permission: PERMISSION_KEY,
          page: pageArg,
          limit: PAGE_SIZE,
        })
        if (!isLookupCurrent(token)) return // superseded by a newer search
        setEntries(res.entries)
        setTotal(res.total)
        setCurrentlyGranted(res.currentlyGranted)
      } catch (err) {
        if (!isLookupCurrent(token)) return
        setEntries([])
        setTotal(0)
        setCurrentlyGranted(undefined)
        const message = handleAdminError(err)
        if (message !== null) setLookupError(message)
      } finally {
        if (isLookupCurrent(token)) setLookupLoading(false)
      }
    }

    const handleLookupSubmit = (e) => {
      e.preventDefault()
      const account = lookupAccount.trim()
      if (!account) return
      setSearchedAccount(account)
      setLookupPage(1)
      fetchLookup(account, 1)
    }

    const goToLookupPage = (nextPage) => {
      setLookupPage(nextPage)
      fetchLookup(searchedAccount, nextPage)
    }

    return (
      <div className="permissions-view">
        <section className="permissions-form">
          <h2>Grant or revoke permission</h2>
          <p className="permissions-form-subtitle">Permission: {PERMISSION_KEY}</p>

          <form onSubmit={handleSubmit}>
            <div className="permissions-mode-group" role="radiogroup" aria-label="Grant or revoke">
              <label htmlFor="permissions-mode-grant" className="permissions-mode-option">
                <input
                  type="radio"
                  id="permissions-mode-grant"
                  name="permissions-mode"
                  value="grant"
                  checked={mode === 'grant'}
                  onChange={() => setMode('grant')}
                  disabled={submitting}
                />
                Grant
              </label>
              <label htmlFor="permissions-mode-revoke" className="permissions-mode-option">
                <input
                  type="radio"
                  id="permissions-mode-revoke"
                  name="permissions-mode"
                  value="revoke"
                  checked={mode === 'revoke'}
                  onChange={() => setMode('revoke')}
                  disabled={submitting}
                />
                Revoke
              </label>
            </div>

            <label htmlFor="permissions-subjects">Subject accounts</label>
            <textarea
              id="permissions-subjects"
              className="permissions-subjects-textarea"
              placeholder="alice, bob, charlie…"
              value={subjectsText}
              onChange={(e) => setSubjectsText(e.target.value)}
              disabled={submitting}
            />
            <span
              className={`permissions-subjects-count ${tooManySubjects ? 'is-over-limit' : ''}`}
            >
              {subjectCount} account{subjectCount === 1 ? '' : 's'} (max {MAX_SUBJECTS})
            </span>
            {tooManySubjects && (
              <div className="permissions-field-error">
                Enter at most {MAX_SUBJECTS} accounts — you have {subjectCount}.
              </div>
            )}

            {mode === 'grant' && (
              <div className="permissions-date-row">
                <div className="permissions-date-field">
                  <label htmlFor="permissions-effective-from">Effective from (optional)</label>
                  <input
                    type="date"
                    id="permissions-effective-from"
                    value={effectiveFrom}
                    onChange={(e) => setEffectiveFrom(e.target.value)}
                    disabled={submitting}
                  />
                </div>
                <div className="permissions-date-field">
                  <label htmlFor="permissions-expires-at">Expires at</label>
                  <input
                    type="date"
                    id="permissions-expires-at"
                    value={expiresAt}
                    onChange={(e) => setExpiresAt(e.target.value)}
                    disabled={submitting}
                  />
                </div>
              </div>
            )}

            <label htmlFor="permissions-applicant">Applicant account</label>
            <input
              id="permissions-applicant"
              value={applicantAccount}
              onChange={(e) => setApplicantAccount(e.target.value)}
              disabled={submitting}
            />

            <label htmlFor="permissions-approver">Approver account</label>
            <input
              id="permissions-approver"
              value={approverAccount}
              onChange={(e) => setApproverAccount(e.target.value)}
              disabled={submitting}
            />

            <label htmlFor="permissions-reason">Reason</label>
            <textarea
              id="permissions-reason"
              className="permissions-reason-textarea"
              value={reason}
              onChange={(e) => setReason(e.target.value)}
              disabled={submitting}
            />
            <span className="permissions-reason-count">
              {reasonRuneCount} / {MAX_REASON_RUNES}
            </span>

            <button type="submit" className="btn btn-primary" disabled={submitting || clientInvalid}>
              {submitting
                ? 'Submitting…'
                : mode === 'grant'
                  ? 'Grant permission'
                  : 'Revoke permission'}
            </button>
          </form>

          {result && (
            <div className="permissions-result dialog-success">
              <p>
                Created {result.created} grant{result.created === 1 ? '' : 's'}.
              </p>
              {result.duplicatesIgnored.length > 0 && (
                <p className="permissions-duplicates">
                  Duplicates ignored: {result.duplicatesIgnored.join(', ')}
                </p>
              )}
            </div>
          )}

          {formError && (
            <div className="permissions-result dialog-error">
              <p>{formError}</p>
              {formErrorMetadata && Object.keys(formErrorMetadata).length > 0 && (
                <ul className="permissions-metadata-list">
                  {Object.entries(formErrorMetadata).map(([key, value]) => (
                    <li key={key}>
                      {key}: {value}
                    </li>
                  ))}
                </ul>
              )}
            </div>
          )}
        </section>

        <section className="permissions-lookup">
          <h2>Look up a subject</h2>
          <form className="permissions-lookup-form" onSubmit={handleLookupSubmit}>
            <label htmlFor="permissions-lookup-account">Subject account</label>
            <input
              id="permissions-lookup-account"
              className="permissions-lookup-input"
              value={lookupAccount}
              onChange={(e) => setLookupAccount(e.target.value)}
            />
            <button
              type="submit"
              className="btn btn-primary"
              disabled={lookupLoading || !lookupAccount.trim()}
            >
              Search
            </button>
          </form>

          {currentlyGranted === true && (
            <span className="permissions-badge is-granted">Currently granted</span>
          )}
          {currentlyGranted === false && (
            <span className="permissions-badge is-not-granted">Not granted</span>
          )}

          {lookupError && <div className="dialog-error">{lookupError}</div>}

          {!searchedAccount ? (
            <div className="permissions-table-status">
              Enter an account and search to view its permission history.
            </div>
          ) : lookupLoading ? (
            <div className="permissions-table-status">Loading…</div>
          ) : entries.length === 0 ? (
            <div className="permissions-table-status">No permission records found.</div>
          ) : (
            <table className="permissions-table">
              <thead>
                <tr>
                  <th>Change</th>
                  <th>Effective from</th>
                  <th>Expires at</th>
                  <th>Applicant</th>
                  <th>Approver</th>
                  <th>Reason</th>
                  <th>Recorded by</th>
                  <th>Recorded at</th>
                </tr>
              </thead>
              <tbody>
                {entries.map((entry) => (
                  <tr key={entry.id}>
                    <td>{entry.granted ? 'Grant' : 'Revoke'}</td>
                    <td>{entry.effectiveFrom ?? '—'}</td>
                    <td>{entry.expiresAt ?? '—'}</td>
                    <td>{entry.applicantAccount}</td>
                    <td>{entry.approverAccount}</td>
                    <td>{entry.reason}</td>
                    <td>{entry.recordedBy}</td>
                    <td>{new Date(entry.recordedAt).toLocaleString()}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}

          <Pager
            page={lookupPage}
            limit={PAGE_SIZE}
            total={total}
            onPrev={() => goToLookupPage(lookupPage - 1)}
            onNext={() => goToLookupPage(lookupPage + 1)}
          />
        </section>
      </div>
    )
  }
  ```

  Append to the end of `admin-frontend/src/components/PermissionsView/style.css` (after the
  last rule from Task 10, `.permissions-metadata-list`):

  ```css

  .permissions-lookup-form {
    display: flex;
    align-items: flex-end;
    gap: var(--space-md);
  }

  .permissions-lookup-form label {
    margin-top: 0;
    font-weight: var(--font-medium);
    font-size: var(--text-sm);
    color: var(--text-secondary);
  }

  .permissions-lookup-input {
    flex: 1;
    max-width: 280px;
    padding: var(--space-sm) var(--space-md);
    background: var(--bg-input);
    border: 1px solid transparent;
    border-radius: var(--radius-md);
    color: var(--text-primary);
    font: inherit;
  }

  .permissions-lookup-input:focus {
    outline: none;
    background: var(--bg-surface);
    border-color: var(--accent);
  }

  .permissions-badge {
    display: inline-flex;
    align-items: center;
    align-self: flex-start;
    padding: var(--space-2xs) var(--space-sm);
    border-radius: var(--radius-pill);
    font-size: var(--text-xs);
    font-weight: var(--font-medium);
  }

  .permissions-badge.is-granted {
    background: var(--status-success-bg);
    color: var(--status-success);
  }

  .permissions-badge.is-not-granted {
    background: var(--status-error-bg);
    color: var(--status-error);
  }

  .permissions-table {
    width: 100%;
    border-collapse: collapse;
  }

  .permissions-table th {
    text-align: left;
    padding: var(--space-sm) var(--space-md);
    color: var(--text-muted);
    font-size: var(--text-sm);
    font-weight: var(--font-medium);
    border-bottom: 1px solid var(--border-subtle);
  }

  .permissions-table td {
    padding: var(--space-sm) var(--space-md);
    border-bottom: 1px solid var(--border-subtle);
    font-size: var(--text-sm);
    color: var(--text-primary);
  }

  .permissions-table-status {
    color: var(--text-muted);
    font-size: var(--text-sm);
    padding: var(--space-lg) 0;
  }
  ```

- [ ] **Step 4 — run, confirm PASS.**
  ```bash
  cd admin-frontend && npx vitest run src/components/PermissionsView/PermissionsView.test.jsx
  ```
  Expected (captured real output):
  ```
   ✓ src/components/PermissionsView/PermissionsView.test.jsx (18 tests)

   Test Files  1 passed (1)
        Tests  18 passed (18)
  ```
  Then run the full suite and typecheck as a final sanity pass for this phase:
  ```bash
  cd admin-frontend && npm test && npm run typecheck
  ```
  Expected (captured real output, full suite including everything from Tasks 9–11):
  ```
   Test Files  20 passed (20)
        Tests  141 passed (141)

  > admin-frontend@0.0.1 typecheck
  > tsc --noEmit
  ```
  (`npm run build` also succeeds; it emits pre-existing Rollup circular-reexport warnings
  about the `@/api` barrel pattern — e.g. for `createPermissions`, `listPermissions`, and
  identically for every pre-existing function like `listAudit`/`createUser`/`updateUser`. This
  is not a regression: it is the same warning the barrel already produces for unrelated
  existing exports, unchanged by this work.)

- [ ] **Step 5 — commit.**
  ```
  feat(admin-frontend): PermissionsView ledger lookup
  ```
## Phase 5: local-dev, docs, verification (Tasks 12–14)

> **Deviations from the task brief, resolved by direct verification against the current
> repo (all files re-read at their cited line numbers before drafting; every number below
> matched the extract exactly):**
> 1. **request-reply.md admin compact blocks — dropped.** The brief asked for "two compact
>    blocks" for the new admin endpoints. Verified `docs/client-api/request-reply.md:232-257`
>    (`## HTTP — Admin Service`): all 12 existing entries (§9.1–§9.12) are table-rows-only,
>    zero per-endpoint `### ` blocks — full detail lives only in `client-api.md` §9, cited by
>    `(§9.x)`. Botplatform Service (`request-reply.md:214-230`) follows the same table-only
>    shape. Adding blocks for just these two new rows would be the only inconsistency in the
>    section. Task 13(d) below follows the file's real, 100%-consistent convention: table rows
>    only.
> 2. **§9.13/§9.14 "Triggered events" sections — omitted.** Verified: GET/POST admin entries
>    that emit nothing (9.1, 9.2, 9.9 — read in full) have no "Triggered events" section at
>    all; only 9.12 (which really does emit `room_restricted`) has one. §3.4 user-service
>    entries (`me`, `settings.get`, `sso.refresh`) DO always include one, even to say "None".
>    Followed each section's real convention: present in the new `permission.get` entry,
>    absent from 9.13/9.14 (request-reply.md's single blanket "**Emits:** None directly —
>    HTTP-only." line already covers them — that line needs no edit).
> 3. **curl count: 3, not 2.** The task brief's prose says "two curl examples for POST"; the
>    plan skeleton's own Task 13 bullet says "curl examples (grant, revoke, lookup)" — three.
>    Went with the skeleton (locked source of truth) and spec §14's "admin POST and GET, with
>    curl examples": grant + revoke curl for §9.13, one lookup curl for §9.14.
> 4. **ToC catch: `docs/client-api.md:64`.** Neither brief nor extract mentioned it, but
>    `docs/client-api.md`'s Table of Contents (unlike request-reply.md's, which only links
>    whole `##` sections) enumerates every §3.4 method by name on one line. Missing this would
>    silently orphan the new entry from the ToC. Added as its own step in Task 13(a). §9's ToC
>    line is a single bare `9. [Admin Service]` with no per-method breakdown, so §9.13/§9.14
>    need no ToC change.
> 5. **`GET .../permissions` missing `subjectAccount` → reason `missing_permission_fields`.**
>    RESOLVED during assembly: Task 6's handler uses `missing_permission_fields` for this
>    case, keeping the permission API's reasons self-consistent; the docs below match the
>    handler. (The generic `missing_fields` remains what JSON-parse failures return.)
> 6. **§3.4's stale "19 NATS request/reply endpoints" intro sentence — left untouched.** The
>    live RPC table actually has 25 rows today (counted directly), so this count already
>    drifted before this change, for reasons unrelated to it. Fixing a pre-existing,
>    ambiguous-to-correct count is out of scope per CLAUDE.md ("keep changes minimal and
>    focused"); noted here so it doesn't read as an oversight.
>
> Everything else below matched the extract exactly on direct re-verification (line numbers,
> table styles, `make test SERVICE=tools` mechanics — actually executed, see Task 12).

---

### Task 12: docker-local single-node RS + host directConnection URIs

**Files:**
- `docker-local/compose.deps.yaml` (modify — `mongodb` service block)
- `tools/seed-sample-data/main.go` (modify — one line)
- `tools/seed-sample-data/main_test.go` (modify — one line)
- `tools/jaeger/README.md` (modify — one line)

**Interfaces:** Consumes nothing from Tasks 1–11's Go code. What it *does* consume is a fact
established by Task 4: `InsertPermissionGrants` wraps `InsertMany` in `withTransaction`
(spec §4.4 rule 11), and MongoDB transactions require a replica set — a standalone `mongod`
rejects `startTransaction` outright. The current `docker-local` MongoDB is standalone, so the
permission-grants write path (and, as a side effect, admin-service's pre-existing
password-change/deactivate endpoints, which are already transactional) is broken in local
dev today. This task is infrastructure-only; it produces no Go interfaces for later tasks to
consume.

**Scope note:** this fixes `make deps-up` / `make up` / `make seed` / running a service binary
by hand against `docker-local`. It does **not** affect `make test-integration` — verified
`admin-service/integration_test.go:29-30` already calls
`testutil.RunTestsWithPrewarm(m, testutil.EnsureMongoReplicaSet, testutil.EnsureNATS)`, i.e.
integration tests already stand up their own throwaway replica-set container via
`pkg/testutil`, completely independent of `docker-local`. Nothing in Task 14's
`make test-integration` steps depends on this task landing first.

- [ ] **Step 1 — edit `docker-local/compose.deps.yaml`'s `mongodb` service block.**

  Current block, `docker-local/compose.deps.yaml:30-48`:

  ````yaml
    mongodb:
      # Patch-pinned to match pkg/testutil/testimages/testimages.go so local dev
      # tracks the same image testcontainers uses.
      image: mongo:8.2.9
      container_name: chat-local-mongodb
      ports:
        - "27017:27017"
      volumes:
        # No initdb.d hook — it only runs on first volume creation, so
        # chat.hr_employee went stale. `make seed` owns that collection now.
        - mongo-data:/data/db
      healthcheck:
        test: ["CMD-SHELL", "echo 'db.runCommand({ping:1}).ok' | mongosh --quiet | grep -q 1"]
        interval: 5s
        timeout: 5s
        retries: 10
        start_period: 10s
      networks:
        - chat-local
  ````

  Replace it with (adds `command:` + a new comment block explaining it; replaces the
  `healthcheck.test` one-liner with the `rs.status()`/`rs.initiate()` pattern, copied
  verbatim in style from `docker-local/compose.migration-demo.yaml:22-36`'s `source-mongo`
  block — same `test:` list shape, same `>-` folded scalar, same `interval`/`timeout`/
  `retries`, `member host = own service name`; dropped `start_period` since `source-mongo`
  has none and 15 retries at 5s already gives 75s of runway for the first election):

  ````yaml
    mongodb:
      # Patch-pinned to match pkg/testutil/testimages/testimages.go so local dev
      # tracks the same image testcontainers uses.
      image: mongo:8.2.9
      container_name: chat-local-mongodb
      # Single-node replica set — transactions (admin-service password-change,
      # deactivate, and the new permission-grant batch insert) require one; a
      # standalone mongod rejects them outright. The member host below is the
      # in-network service name "mongodb", so container-side clients need no
      # config change; host-side tools (make seed, tools/jaeger/README.md) add
      # ?directConnection=true instead of trying to resolve that name.
      command: ["--replSet", "rs0", "--bind_ip_all"]
      ports:
        - "27017:27017"
      volumes:
        # No initdb.d hook — it only runs on first volume creation, so
        # chat.hr_employee went stale. `make seed` owns that collection now.
        - mongo-data:/data/db
      healthcheck:
        test:
          - CMD-SHELL
          - >-
            mongosh --quiet --eval
            'try { rs.status().ok } catch (e) { rs.initiate({_id:"rs0",members:[{_id:0,host:"mongodb:27017"}]}) }'
        interval: 5s
        timeout: 10s
        retries: 15
      networks:
        - chat-local
  ````

  `ports`, `volumes`, and `networks` are unchanged — only `command` is added and
  `healthcheck` is replaced.

  **Verified this parses**: applied this exact edit to a scratch copy and ran
  `docker compose -f <scratch>.yaml config` — exit 0, and the rendered config shows the
  folded `test:` scalar collapsing to the identical single-line `mongosh --quiet --eval '...'`
  string used by `source-mongo`, confirming the YAML is both syntactically valid and
  semantically identical to the precedent it's copied from.

- [ ] **Step 2 (RED) — update the seed tool's test to expect the new default, confirm it fails.**

  `tools/seed-sample-data/main_test.go:13`, inside `TestParseConfig_Defaults`:

  ```go
  // before
  	assert.Equal(t, "mongodb://localhost:27017", cfg.MongoURI)

  // after
  	assert.Equal(t, "mongodb://localhost:27017/?directConnection=true", cfg.MongoURI)
  ```

  Run (only `main.go`'s `envDefault` hasn't changed yet, so this must fail):

  ```
  make test SERVICE=tools
  ```

  Expected: `FAIL github.com/hmchangw/chat/tools/seed-sample-data` —
  `TestParseConfig_Defaults` reports
  `Error: Not equal: expected: "mongodb://localhost:27017/?directConnection=true" actual: "mongodb://localhost:27017"`.

  Note on this command: `SERVICE` is a plain path substitution in the Makefile
  (`go test -race ./$(SERVICE)/...`, no allow-list), so `SERVICE=tools` legitimately scopes to
  every package under `tools/` — there is no dedicated `SERVICE=seed-sample-data` scope, and
  the brief's fallback ("if only global `make test` covers tools, use it") isn't needed here.
  **This was actually run against the current repo** (before this task's edits) to confirm
  the invocation works: `make test SERVICE=tools` → all 5 `tools/*` packages pass in ~155s,
  wall-clock dominated by `tools/loadgen` (150.8s) — `tools/seed-sample-data` itself is 2.5s.
  Don't mistake the ~2–3 minute runtime for a hang.

- [ ] **Step 3 (GREEN) — flip the seed tool's default, confirm the test passes.**

  `tools/seed-sample-data/main.go:29`, inside the `config` struct:

  ```go
  // before
  	MongoURI       string   `env:"MONGO_URI"       envDefault:"mongodb://localhost:27017"`

  // after
  	MongoURI       string   `env:"MONGO_URI"       envDefault:"mongodb://localhost:27017/?directConnection=true"`
  ```

  Run:

  ```
  make test SERVICE=tools
  ```

  Expected: `ok  	github.com/hmchangw/chat/tools/seed-sample-data` (and the other 4 `tools/*`
  packages, unaffected, also `ok`).

- [ ] **Step 4 — update the jaeger README's run example.**

  `tools/jaeger/README.md:20`:

  ```
  # before
     NATS_URL=nats://localhost:4222 MONGO_URI=mongodb://localhost:27017 SITE_ID=site-local go run ./broadcast-worker/

  # after
     NATS_URL=nats://localhost:4222 MONGO_URI=mongodb://localhost:27017/?directConnection=true SITE_ID=site-local go run ./broadcast-worker/
  ```

  No test to run — this is a doc line inside a fenced example, not executed by any suite.

- [ ] **Step 5 — verify the compose file itself.**

  ```
  docker compose -f docker-local/compose.deps.yaml config >/dev/null
  ```

  Expected: exit code 0, no output (this only validates YAML + compose-schema syntax, it does
  not start anything). **Confirmed working against the pre-edit file right now** (exit 0), so
  a non-zero exit after Step 1's edit means a YAML mistake in that edit — fix indentation
  (2-space, matches the rest of the file) and re-run.

- [ ] **Step 6 — manual runtime verification (not part of any automated suite; do this once,
  by hand, to prove the replica set actually comes up).**

  ```
  docker compose -f docker-local/compose.deps.yaml up -d mongodb
  ```

  Wait for the healthcheck to go green (first few attempts hit the `catch` branch and call
  `rs.initiate()`; the container only reports `healthy` once a primary is elected — normally
  within a few seconds, worst case the full 15×5s=75s retry budget):

  ```
  until [ "$(docker inspect -f '{{.State.Health.Status}}' chat-local-mongodb)" = healthy ]; do sleep 2; done
  docker exec chat-local-mongodb mongosh --quiet --eval 'rs.status().ok'
  ```

  Expected: `1`.

  Then prove the **host-side** fix works too — this is the part `docker compose config` and
  the container-internal check above cannot prove, since both stay inside the Docker network:

  ```
  make seed-dry-run
  ```

  Expected: prints the `seed dry-run summary` plan (`users 11`, `hr_employee 10`, …) and
  exits 0. Caveat, so this isn't mistaken for a full proof: `--dry-run` returns before
  `run()` ever calls `mongoutil.Connect` (`tools/seed-sample-data/main.go:85-88`), so this
  step only proves the binary still builds and runs — it never actually dials Mongo. For a
  real end-to-end check of the new `?directConnection=true` URI against the single-node RS:

  ```
  make seed
  ```

  Expected: `seed complete` log line with non-zero counts for `users`, `rooms`,
  `subscriptions`, etc., exit 0. This is the one command that actually opens a transaction
  (room-key provisioning) against the RS from the host — if `directConnection=true` were
  missing or the member host were wrong, this is where it would hang or fail with a
  topology/server-selection-timeout error, not at `config` or the in-container check.

  **Existing-volume note (no action needed, just don't be alarmed):** a dev who already has a
  `mongo-data` volume from before this change gets `rs.initiate()` run against their existing
  data on first start after pulling this change — that's safe. `rs.initiate()` only changes
  the server's replication metadata; it does not touch collections. No
  `docker compose down -v` / volume wipe is required or recommended.

- [ ] **Step 7 — commit.**

  ```
  git add docker-local/compose.deps.yaml tools/seed-sample-data/main.go tools/seed-sample-data/main_test.go tools/jaeger/README.md
  git commit -m "fix(docker-local): single-node mongo replica set + host directConnection URIs"
  ```

---

### Task 13: docs

**Files:**
- `docs/client-api.md` (modify — ToC, §3.4, §6, §9)
- `docs/client-api/request-reply.md` (modify — user-service table + block, admin table)

**Interfaces:** Consumes the wire shapes locked in the skeleton for Tasks 5, 6, and 8 —
`permissionRequest` / `grantCreated` / `createPermissionsResponse` / `permissionGrantView` /
`listPermissionsResponse` (`admin-service/permissions.go`, not yet written at doc-drafting
time — these are the *locked* JSON field names, taken as given), `models.PermissionGetRequest`
/ `models.PermissionGetResponse` (`user-service/models/permission.go`), and the subject
builders from Task 3 (`subject.UserPermissionGet` / `UserPermissionGetPattern`). This task
writes no Go code and has no unit tests — see the note at the end of this task for what
"testing" a docs change means here.

All four content blocks below (a–d) are complete, paste-ready markdown — not summaries of
what to write. Anchors cite exact current line numbers, independently re-verified against the
live files in this repo (not just the extract) immediately before drafting.

- [ ] **Step 1 — client-api.md ToC: add `permission.get` to the §3.4 method list.**

  **Anchor:** `docs/client-api.md:64` (the only ToC line enumerating §3.4 methods by name).

  ```
  # before
       - [`sso.set`](#ssoset) · [`sso.refresh`](#ssorefresh)

  # after
       - [`sso.set`](#ssoset) · [`sso.refresh`](#ssorefresh) · [`permission.get`](#permissionget)
  ```

- [ ] **Step 2 — client-api.md §3.4: add the RPC subject table row.**

  **Anchor:** insert after `docs/client-api.md:4521` (the `sso.refresh` row — currently the
  last row in the table), before the blank line at `4522` that precedes `#### me`.

  ```
  | `chat.user.{account}.request.user.{siteID}.permission.get` | [`permission.get`](#permissionget) |
  ```

  (Leave the section's intro sentence at `docs/client-api.md:4491`, "`user-service` exposes 19
  NATS request/reply endpoints…", untouched — see the deviations note at the top of this file:
  it's already stale against the actual 25-row table, for reasons predating this change.)

- [ ] **Step 3 — client-api.md §3.4: insert the full `#### permission.get` entry.**

  **Anchor:** insert after `docs/client-api.md:5766` (the blank line following `sso.refresh`'s
  trailing `---`), before `### 3.5 media-service` at `5767`.

  ````markdown
  #### permission.get

  **Subject:** `chat.user.{account}.request.user.{siteID}.permission.get`
  **Reply subject:** auto-generated `_INBOX.>` (NATS request/reply)

  Returns whether the **calling** user currently holds a given permission. Self-service — the `{account}` subject token is the caller's NATS-JWT-authenticated identity; there is no way to query another account's permission over this subject, and the request body carries no account field. The answer is computed from the newest matching row in the `permission_grants` ledger (latest-wins): never granted, expired, not yet effective, and revoked all read the same way — `granted: false` on an ordinary `200`, never an error. No dates are returned; this endpoint answers yes/no only.

  ##### Request body

  | Field | Type | Required | Notes |
  |---|---|---|---|
  | `permission` | string | yes | Permission key to check. The only key defined today is `external.image.view`. |

  ```json
  { "permission": "external.image.view" }
  ```

  ##### Success response

  | Field | Type | Notes |
  |---|---|---|
  | `permission` | string | Echoes the requested key. |
  | `granted` | boolean | Whether the caller currently holds the permission. `false` covers "never granted", "expired", "not yet effective", and "revoked" alike — all are ordinary `200` responses, not errors. |

  ```json
  { "permission": "external.image.view", "granted": true }
  ```

  ##### Error response

  | Condition | `code` | `reason` | Notes |
  |-----------|--------|----------|-------|
  | `permission` missing or not a recognized key | `bad_request` | `unknown_permission` | Only `external.image.view` exists today. |
  | Internal failure | `internal` | — | Collapses to the generic boundary error code — see [§6 Error envelope reference](#6-error-envelope-reference). |

  ##### Triggered events — success path

  `None — reply only.`

  ##### Triggered events — error path

  `None — error returned only via the reply subject.`

  ---
  ````

- [ ] **Step 4 — client-api.md §6: insert 8 reason-catalog rows.**

  **Anchor:** insert after `docs/client-api.md:6487` (the `old_password_mismatch` row),
  before `6488` (`emoji_shortcode_reserved`) — keeps the file's existing service-grouping
  order (admin-service rows stay contiguous).

  ```
  | `unknown_permission` | bad_request | admin-service `POST /v1/admin/permissions` (§9.13) (permission key not recognized); user-service `permission.get` (same) |
  | `invalid_subject_count` | bad_request | admin-service `POST /v1/admin/permissions` (§9.13) (`subjectAccounts` empty or over 200) |
  | `invalid_reason` | bad_request | admin-service `POST /v1/admin/permissions` (§9.13) (`reason` empty or over 1000 runes) |
  | `missing_permission_fields` | bad_request | admin-service `POST /v1/admin/permissions` (§9.13) (`applicantAccount` or `approverAccount` empty) |
  | `invalid_permission_window` | bad_request | admin-service `POST /v1/admin/permissions` (§9.13) (grant only: malformed date, `effectiveFrom` after `expiresAt`, or `expiresAt` not in the future) |
  | `unexpected_permission_window` | bad_request | admin-service `POST /v1/admin/permissions` (§9.13) (revoke only: `effectiveFrom`/`expiresAt` present) |
  | `inactive_subject` | bad_request | admin-service `POST /v1/admin/permissions` (§9.13) (a subject account is deactivated) |
  | `unknown_accounts` | not_found | admin-service `POST /v1/admin/permissions` (§9.13) (a subject, applicant, or approver account does not exist at this site) |
  ```

- [ ] **Step 5 — client-api.md §9: insert `### 9.13` and `### 9.14`.**

  **Anchor:** insert after `docs/client-api.md:7329` (blank line following 9.12's
  `#### Triggered events — error path` → `` `None.` ``), before `### UserView` at `7330`.

  ````markdown
  ### 9.13 Create / revoke permission grants

  **Endpoint:** `POST /v1/admin/permissions`
  **Auth:** `Authorization: Bearer <authToken>`, admin role + same-site required.

  Records a new row in the append-only `permission_grants` ledger for one or more subject accounts — either a grant (`granted: true`) or a revocation (`granted: false`) of the same permission key in one call. Unlike its siblings this is not a `/users/:account/...`-shaped endpoint, because `subjectAccounts` is a batch. The current state of a permission is never stored directly — it's always the newest row for `(permission, subjectAccount)`; revoking does not edit or delete any earlier row (see `currentlyGranted` in §9.14). One `admin_audit` entry (`action`: `permission.grant` or `permission.revoke`, `targetAccount`: the subject) is written per subject alongside the ledger rows.

  Dates are plain `YYYY-MM-DD` strings, interpreted under a fixed UTC+8 rule — never the caller's timezone, never the server's local timezone. There is no `timezone` field, and there never will be one for this endpoint.

  #### Request body

  | Field | Type | Required | Notes |
  |---|---|---|---|
  | `permission` | string | yes | Permission key. The only key defined today is `external.image.view`. |
  | `subjectAccounts` | string[] | yes | 1–200 accounts to grant or revoke for. Duplicates are silently deduplicated (first occurrence kept) and reported back in `duplicatesIgnored` — never rejected outright. |
  | `granted` | boolean | yes | `true` records a grant, `false` records a revocation. |
  | `effectiveFrom` | string | no | Grant only — `YYYY-MM-DD`. Omitted means "effective immediately" (the write instant). Backdating is allowed. **Must be entirely absent — not `null` — when `granted` is `false`**; a revocation has no validity window. |
  | `expiresAt` | string | when `granted` is `true` | Grant only — `YYYY-MM-DD`, the last valid day (inclusive). Stored internally as the exclusive-end instant, UTC+8 midnight the *following* day. A value equal to today is valid (the grant expires tonight); any earlier date is rejected. **Must be entirely absent when `granted` is `false`** — there are no permanent grants and no dated revocations. |
  | `applicantAccount` | string | yes | Account of the person who requested the change (offline approval). Must exist at this site; may be inactive. |
  | `approverAccount` | string | yes | Account of the person who approved the change (offline approval). Must exist at this site; may be inactive. |
  | `reason` | string | yes | Free text, 1–1000 runes (`utf8.RuneCountInString`). Retained permanently — this is an append-only ledger, not a mutable note. |

  Grant:

  ```json
  {
    "permission": "external.image.view",
    "subjectAccounts": ["alice", "bob"],
    "granted": true,
    "effectiveFrom": "2026-09-01",
    "expiresAt": "2026-12-31",
    "applicantAccount": "carol",
    "approverAccount": "dave",
    "reason": "On-call staff must review production line photos from outside the fab."
  }
  ```

  Revoke — `effectiveFrom`/`expiresAt` omitted entirely, not `null`:

  ```json
  {
    "permission": "external.image.view",
    "subjectAccounts": ["alice"],
    "granted": false,
    "applicantAccount": "carol",
    "approverAccount": "dave",
    "reason": "Project ended."
  }
  ```

  #### Success response

  `HTTP 201`

  | Field | Type | Notes |
  |---|---|---|
  | `created` | integer | Number of ledger rows written — the deduplicated subject count. |
  | `duplicatesIgnored` | string[] | Subject accounts dropped as duplicates of an earlier entry in the same request. `[]`, never `null`, when there were none. |
  | `grants` | object[] | One entry per row written, same order as the deduplicated subject list: `{ "id": string, "subjectAccount": string }`. |

  ```json
  {
    "created": 2,
    "duplicatesIgnored": [],
    "grants": [
      { "id": "0199f2c3a4b5c6d70199f2c3a4b5c6d7", "subjectAccount": "alice" },
      { "id": "0199f2c3a4b5c6d80199f2c3a4b5c6d8", "subjectAccount": "bob" }
    ]
  }
  ```

  #### Errors

  | Status | `code` | `reason` | Notes |
  |---|---|---|---|
  | 400 | `bad_request` | `missing_fields` | Body exceeds 1MB, or is not valid JSON. |
  | 400 | `bad_request` | `unknown_permission` | `permission` is not a recognized key. |
  | 400 | `bad_request` | `invalid_subject_count` | `subjectAccounts` is empty or exceeds 200 entries. |
  | 400 | `bad_request` | `invalid_reason` | `reason` is empty or exceeds 1000 runes. |
  | 400 | `bad_request` | `missing_permission_fields` | `applicantAccount` or `approverAccount` is empty. |
  | 400 | `bad_request` | `invalid_permission_window` | Grant only. `effectiveFrom`/`expiresAt` not `YYYY-MM-DD`, `effectiveFrom` after `expiresAt`, or `expiresAt`'s derived instant is not in the future (today is valid; yesterday or earlier is not). |
  | 400 | `bad_request` | `unexpected_permission_window` | Revoke only. `effectiveFrom` or `expiresAt` present. |
  | 404 | `not_found` | `unknown_accounts` | A subject, applicant, or approver account does not exist at this site. Message names the offending accounts; `metadata.accounts` carries the same comma-joined list for programmatic display. |
  | 400 | `bad_request` | `inactive_subject` | A subject account exists but is deactivated. `applicantAccount`/`approverAccount` are exempt — a departed staff member may still be recorded as applicant or approver. Message names the offending accounts; `metadata.accounts` carries the same comma-joined list. |
  | 401 | `unauthenticated` | `invalid_token` | Token missing, unknown, or session not found. |
  | 403 | `forbidden` | `not_admin` | Valid session, but caller lacks the `admin` role or the session `siteId` does not match. |
  | 500 | `internal` | — | The batch insert is transactional — on failure, no ledger row was written and no audit entry exists. |

  ```bash
  # Grant
  curl -X POST https://admin.example.com/v1/admin/permissions \
    -H "Authorization: Bearer $ADMIN_TOKEN" \
    -H "Content-Type: application/json" \
    -d '{
      "permission": "external.image.view",
      "subjectAccounts": ["alice", "bob"],
      "granted": true,
      "effectiveFrom": "2026-09-01",
      "expiresAt": "2026-12-31",
      "applicantAccount": "carol",
      "approverAccount": "dave",
      "reason": "On-call staff must review production line photos from outside the fab."
    }'
  ```

  ```bash
  # Revoke
  curl -X POST https://admin.example.com/v1/admin/permissions \
    -H "Authorization: Bearer $ADMIN_TOKEN" \
    -H "Content-Type: application/json" \
    -d '{
      "permission": "external.image.view",
      "subjectAccounts": ["alice"],
      "granted": false,
      "applicantAccount": "carol",
      "approverAccount": "dave",
      "reason": "Project ended."
    }'
  ```

  ### 9.14 List permission grants

  **Endpoint:** `GET /v1/admin/permissions`
  **Auth:** `Authorization: Bearer <authToken>`, admin role + same-site required.

  Returns one subject's permission ledger, newest-first, plus the current computed decision. Backs both the terminal workflow (confirm state before writing, confirm again after) and the console's lookup pane.

  #### Query parameters

  | Parameter | Type | Notes |
  |---|---|---|
  | `subjectAccount` | string | Required. The account whose ledger to list. |
  | `permission` | string | Optional. Filter to one permission key. Omitted lists every permission this subject has any row for — and in that case the response's top-level `currentlyGranted` is absent, since there is no single current decision across multiple permissions. |
  | `page` | integer | Page number, 1-based. Defaults to `1`. |
  | `limit` | integer | Page size. Defaults to `20`, max `100`. |

  #### Success response

  `HTTP 200`

  | Field | Type | Notes |
  |---|---|---|
  | `currentlyGranted` | boolean | Present only when `permission` was supplied. The current decision — latest-wins over the filtered ledger. Deliberately not named `granted`, which is a per-row field meaning "what this record did", not "current state". |
  | `entries` | [PermissionGrantView](#permissiongrantview)[] | The ledger, newest-first (`recordedAt` desc, `_id` desc tie-break). |
  | `total` | integer | Total matching rows across all pages. |

  ```json
  {
    "currentlyGranted": false,
    "entries": [
      {
        "id": "0199f2c3a4b5c6d90199f2c3a4b5c6d9",
        "permission": "external.image.view",
        "subjectAccount": "alice",
        "granted": false,
        "applicantAccount": "carol",
        "approverAccount": "dave",
        "reason": "Project ended.",
        "recordedBy": "p_admin_wang",
        "recordedAt": "2026-10-15T02:03:04Z"
      },
      {
        "id": "0199f2c3a4b5c6d70199f2c3a4b5c6d7",
        "permission": "external.image.view",
        "subjectAccount": "alice",
        "granted": true,
        "effectiveFrom": "2026-09-01",
        "expiresAt": "2026-12-31",
        "expiresAtUTC": "2026-12-31T16:00:00Z",
        "applicantAccount": "carol",
        "approverAccount": "dave",
        "reason": "On-call staff must review production line photos from outside the fab.",
        "recordedBy": "p_admin_wang",
        "recordedAt": "2026-09-01T01:00:00Z"
      }
    ],
    "total": 2
  }
  ```

  Note what this example demonstrates: the revoke row (newest, listed first) omits
  `effectiveFrom`/`expiresAt`/`expiresAtUTC` entirely, and `currentlyGranted` correctly
  reads `false` because the revoke is the newest row for `(permission, subjectAccount)`.

  #### Errors

  | Status | `code` | `reason` | Notes |
  |---|---|---|---|
  | 400 | `bad_request` | `missing_permission_fields` | `subjectAccount` query parameter absent. |
  | 401 | `unauthenticated` | `invalid_token` | Token missing, unknown, or session not found. |
  | 403 | `forbidden` | `not_admin` | Valid session, but caller lacks the `admin` role or the session `siteId` does not match. |
  | 500 | `internal` | — | Server-side fault; cause is logged server-side only. |

  ```bash
  # Lookup
  curl -G https://admin.example.com/v1/admin/permissions \
    -H "Authorization: Bearer $ADMIN_TOKEN" \
    --data-urlencode "subjectAccount=alice" \
    --data-urlencode "permission=external.image.view"
  ```
  ````

  Neither entry gets a "Triggered events" subsection — see deviation note 2 at the top of
  this file.

- [ ] **Step 6 — client-api.md: add the `PermissionGrantView` schema.**

  **Anchor:** insert after `docs/client-api.md:7379` (the blank line following `AuditEntry`'s
  last field row, `timestamp`), before the `---` at `7380` (which stays as-is — it already
  correctly precedes `## 10. Botplatform Service`). This mirrors how `UserView` → `SessionView`
  → `AuditEntry` are already back-to-back with no `---` between them, only after the last one.

  ```markdown
  ### PermissionGrantView

  | Field | Type | Notes |
  |---|---|---|
  | `id` | string | Ledger row ID (32-char UUIDv7 hex). |
  | `permission` | string | Permission key this row applies to. |
  | `subjectAccount` | string | The account this row grants or revokes the permission for. |
  | `granted` | boolean | What this row did — `true` for a grant, `false` for a revocation. Historical rows are never rewritten; this is not "does the subject currently hold the permission" (see `currentlyGranted`, §9.14). |
  | `effectiveFrom` | string | `YYYY-MM-DD`, decoded back from the stored instant at UTC+8. Omitted on revoke rows (no validity window). |
  | `expiresAt` | string | `YYYY-MM-DD`, the inclusive last valid day (decoded from the stored exclusive-end instant at UTC+8, minus one day). Omitted on revoke rows. |
  | `expiresAtUTC` | string | RFC 3339. The exact stored instant — the exclusive end of the half-open window, one day past `expiresAt`. Omitted on revoke rows. |
  | `applicantAccount` | string | Who requested the change. |
  | `approverAccount` | string | Who approved the change. |
  | `reason` | string | Free-text justification, as submitted. |
  | `recordedBy` | string | The admin account that recorded this row (from the session token). |
  | `recordedAt` | string | RFC 3339. Server clock at write time — also the moment the change took effect. |
  ```

- [ ] **Step 7 — request-reply.md: add the user-service RPC table row.**

  **Anchor:** insert after `docs/client-api/request-reply.md:1475` (the `sso.refresh` row —
  currently the last row), before the blank line at `1476`.

  ```
  | `chat.user.{account}.request.user.{siteID}.permission.get` | [permission.get](#permissionget) |
  ```

- [ ] **Step 8 — request-reply.md: add the compact `### permission.get` block.**

  **Anchor:** insert after `docs/client-api/request-reply.md:1974` (the blank line following
  `sso.refresh`'s trailing `---`), before `## media-service` at `1975`.

  ````markdown
  ### permission.get

  **Subject:** `chat.user.{account}.request.user.{siteID}.permission.get`

  Returns whether the calling user currently holds a given permission. Self-service —
  the `{account}` subject token is the caller's NATS-JWT-authenticated identity; there
  is no way to query another account's permission. Computed latest-wins over the
  `permission_grants` ledger; never granted, expired, not-yet-effective, and revoked
  all read as `granted: false` on a normal `200`, not an error.

  #### Request body

  `{ "permission": string }`

  #### Success response

  `{ "permission": string, "granted": boolean }`

  #### Errors

  `unknown_permission` (`bad_request`, not a recognized permission key), `internal`
  (local store failure).

  **Emits:** None.

  ---
  ````

- [ ] **Step 9 — request-reply.md: add the two admin HTTP table rows.**

  **Anchor:** insert after `docs/client-api/request-reply.md:253` (the
  `POST /v1/password/change` row — currently the last row), before the blank line at `254`.
  No compact blocks follow — see deviation note 1 at the top of this file. The existing
  blanket line right after the table, `**Emits:** None directly — HTTP-only. ...`
  (`request-reply.md:255`), already covers these two rows correctly and needs no edit.

  ```
  | `POST /v1/admin/permissions` | synchronous HTTP | Grant or revoke a permission for one or more subject accounts; appends to the permission ledger with one slim audit entry per subject (§9.13). |
  | `GET /v1/admin/permissions?subjectAccount=<account>` | synchronous HTTP | List an account's permission ledger newest-first, plus the current computed decision (§9.14). |
  ```

- [ ] **Step 10 — consistency check.** This is the only "test" for a docs-only task — there
  is no unit-test framework for markdown, so don't invent one. Grep for every anchor text
  used above and eyeball that each hit is exactly the row/heading intended, nothing stray:

  ```
  grep -n "permission.get" docs/client-api.md docs/client-api/request-reply.md
  ```

  Expected — **6 lines total**:
  - `docs/client-api.md`: 4 lines —
    1. the ToC bullet (Step 1),
    2. the RPC table row (Step 2 — this line has "permission.get" twice, once in the
       subject cell and once in the link text; `grep -n` still counts it as one matching
       line),
    3. the `#### permission.get` heading (Step 3),
    4. the `unknown_permission` reason-catalog row (Step 4, which cites `user-service
       permission.get`).

    If the count differs, diff against these four call sites — a 5th hit means stray text
    crept into the §9 admin entries, which must not mention user-service's `permission.get`
    at all.
  - `docs/client-api/request-reply.md`: 2 lines — the RPC table row (Step 7) and the
    `### permission.get` heading (Step 8).

  Then read back each of the nine insertion points from Steps 1–9 once in the rendered file
  to confirm table columns line up and no fence was left unclosed — an unclosed ` ``` ` 
  silently swallows every heading after it until the next fence, which `grep` alone would
  not catch.

- [ ] **Step 11 — commit.**

  ```
  git add docs/client-api.md docs/client-api/request-reply.md
  git commit -m "docs(client-api): permission endpoints + reason catalog"
  ```

---

### Task 14: final verification

**Files:** None (no source changes unless a step below finds something to fix).

**Interfaces:** Consumes everything produced by Tasks 1–13 — this task asserts the whole
branch compiles, generates cleanly, lints clean, passes its own tests, and meets the
coverage floor. It produces no new interfaces.

Run in order. Each step names its exact expected outcome and what to do if it doesn't match.
Where a step fails, fix forward (edit the offending file, re-run *that same step* — don't
skip ahead) and only commit at the very end, once, per the skeleton's Task 14 commit rule.

- [ ] **Step 1 — regenerate mocks, expect zero drift.**

  ```
  make generate
  git status --porcelain
  ```

  Expected: `make generate` exits 0; `git status --porcelain` prints **nothing** — every
  mock (`admin-service/mock_store_test.go`, `user-service/service/mocks/mock_repository.go`,
  and any other `//go:generate mockgen` output) was already regenerated and committed in
  Tasks 4 and 8, so a clean re-run produces byte-identical files.

  On failure (non-empty `git status --porcelain`): a task forgot to commit a regenerated
  mock, or the mockgen version drifted. Stage and commit the regenerated file(s) with a
  `chore(generate): ...` message before continuing, or fold the fix into this task's final
  commit (Step 13) if you're batching fixes.

- [ ] **Step 2 — format.**

  ```
  make fmt
  git status --porcelain
  ```

  Expected: no diff. On failure: `make fmt` already rewrote the files in place — review the
  diff (`git diff`), stage it, carry it into Step 13's commit.

- [ ] **Step 3 — lint.**

  ```
  make lint
  ```

  Expected: exits 0, no issues reported. On failure: fix the reported issue at its source —
  do not add a blanket `//nolint` without a specific linter name and reason (matches
  CLAUDE.md's SAST-suppression discipline, applied the same way here even though this is
  golangci-lint, not gosec).

- [ ] **Step 4 — admin-service unit tests.**

  ```
  make test SERVICE=admin-service
  ```

  Expected: `ok  	github.com/hmchangw/chat/admin-service` (and any subpackages), race
  detector clean. On failure: this is almost certainly a Task 4/5/6 regression — re-open
  that task's test file, not this one; Task 14 verifies, it doesn't redesign.

- [ ] **Step 5 — user-service unit tests.**

  ```
  make test SERVICE=user-service
  ```

  Expected: `ok` across `user-service/...` (including `user-service/service`,
  `user-service/mongorepo`, `user-service/models`). On failure: likely a Task 7/8
  regression, or a caller of `service.New(...)` that wasn't updated for the new
  `permRepo PermissionRepository` constructor parameter (skeleton Task 8 calls this out
  explicitly as a multi-call-site change).

- [ ] **Step 6 — full unit suite.**

  ```
  make test
  ```

  Expected: `ok` for every package in the repo, race detector clean. This is the one step
  that would catch a signature change rippling into an unrelated caller (e.g. if any other
  service imports `pkg/model` or `pkg/errcode` and a struct/const changed shape). On
  failure: identify the failing package from the output; if it's outside
  admin-service/user-service/pkg/model/pkg/errcode/pkg/subject, something in this feature
  broke a shared package's contract — treat as a stop-ship issue, not a quick patch.

- [ ] **Step 7 — admin-service integration tests.**

  ```
  make test-integration SERVICE=admin-service
  ```

  Requires Docker running. Expected: `ok`, including the new `InsertPermissionGrants` /
  `ListPermissionGrants` / `GetLatestPermissionGrant` / `FindAccountStates` /
  `AppendAuditMany` tests from Task 4 (transaction all-or-nothing, latest-wins incl. the
  same-millisecond `_id` tie-break, `explain` confirms `IXSCAN`). First run pulls the
  replica-set test image via `pkg/testutil` and may take noticeably longer than subsequent
  runs. On failure: check `docker ps` — if no containers came up at all, Docker itself isn't
  running; if containers came up but the test still failed, it's a real Task 4 regression.

- [ ] **Step 8 — user-service integration tests.**

  ```
  make test-integration SERVICE=user-service
  ```

  Expected: `ok`, including Task 7's `PermissionRepo.GetLatestGrant` tests against whatever
  harness `user-service/mongorepo/setup_test.go` already standardizes on. On failure: same
  triage as Step 7.

- [ ] **Step 9 — admin-frontend tests.**

  ```
  cd admin-frontend && npm test
  ```

  (`vitest run` — confirmed this is the only way to run these; there is no root Makefile
  target for any frontend, verified against the full 326-line Makefile.) Expected: all
  suites pass, including the new `PermissionsView.test.jsx` (Tasks 10–11) and the updated
  `AppShell.test.jsx` / `admin.test.ts`. On failure: fix in the frontend package itself;
  this command does not touch Go code.

- [ ] **Step 10 — coverage spot-check.**

  The root Makefile has no coverage target, and raw `go test` is otherwise off-limits per
  this repo's global constraints — but CLAUDE.md's own Coverage section documents
  `go test -coverprofile=... ` + `go tool cover -func=...` as *the* sanctioned mechanism
  for verifying the percentage itself ("Use `go test -coverprofile=coverage.out` and
  `go tool cover -func=coverage.out` to verify coverage percentages"). This step is that
  documented procedure, applied to the three packages this feature added the most
  validation/decision logic to. `coverage.out` is already covered by `.gitignore` (`coverage.*`,
  `.gitignore:20`), so nothing needs cleanup after.

  Core evaluation logic — target 90%+ (spec §10 calls this out specifically: the boundary
  tests for `EvaluateGrant` are the point of the whole test file):

  ```
  go test -race -coverprofile=coverage.out ./pkg/model/
  go tool cover -func=coverage.out | grep -E "permission|EvaluateGrant|^total:"
  ```

  Expected: every `permission.go` function line shows a percentage; `EvaluateGrant`
  ideally shows `100.0%` (5 branches, all exercised per spec §10's required boundary
  cases: `now == effectiveFrom` true, `now == expiresAt` false, nil ledger, nil bounds on a
  granted row, revoked row); `total:` ≥ `80.0%`.

  Admin-service handler validation — target 90%+ (12 validation rules, spec §4.4):

  ```
  go test -race -coverprofile=coverage.out ./admin-service/...
  go tool cover -func=coverage.out | grep -E "permissions\.go|^total:"
  ```

  Expected: `createPermissions`, `listPermissions`, `parseWindow`, `displayDate`,
  `displayUntilDate` all show high percentages (90%+ on the validation-order branches);
  `total:` ≥ `80.0%` for the whole `admin-service` package.

  user-service handler — target 90%+:

  ```
  go test -race -coverprofile=coverage.out ./user-service/service/
  go tool cover -func=coverage.out | grep -E "permission\.go|^total:"
  ```

  Expected: `GetPermission` at 90%+ (spec §10: granted, absent, expired, not-yet-effective,
  revoked, no record, unknown key, store error, plus the "never reads account from body"
  test); `total:` ≥ `80.0%` for `user-service/service`.

  On any figure below its floor: this is a real gap, not a rounding issue — open
  `go tool cover -html=coverage.out` (or just read the `-func` output line-by-line) to find
  the uncovered branch and add the missing table-driven subtest in that task's `_test.go`
  file. Do not lower the bar or mark it "acceptable" — CLAUDE.md's coverage floor is
  described as a merge blocker, not a target.

- [ ] **Step 11 — SAST.**

  ```
  make sast
  ```

  Expected: exits 0 — `gosec`, `govulncheck`, and `semgrep` all pass (the CI gate fails on
  medium+ severity). On failure: fix the flagged code. Do **not** suppress a finding in code
  that was just written in this feature — a `// #nosec` on brand-new validation/DB code is
  almost never a "genuine false positive" in the sense CLAUDE.md requires; it's much more
  likely the finding is correct (e.g. an unbounded query, an unescaped log field, a missing
  input-length check) and should be fixed in `permissions.go` / `permission.go` /
  `store_mongo.go` directly. If, after investigation, a specific finding genuinely is a false
  positive, suppress it with both mechanisms CLAUDE.md requires for gosec specifically: a
  `// #nosec <RULE> -- reason` comment directly above the flagged line (golangci-lint's
  `//nolint:gosec` alone does not suppress standalone `gosec`).

- [ ] **Step 12 — git log sanity check.**

  ```
  git log --oneline main..HEAD
  ```

  Expected: **16 lines** if no fixes were needed in Steps 1–11, **17** if Step 13 below
  commits a fix. Do not expect a bare "13" — that's only the count of *this feature's* task
  commits (Tasks 1–13); the branch already carried 3 commits ahead of `main` before any of
  Phase 1–5 started (`02ba8d24`, `b1d69bc1`, `0aed14d9` — the spec docs themselves, confirmed
  via `git log --oneline main..HEAD` on this exact branch before implementation began). So:
  3 pre-existing + 13 feature commits = 16 baseline. Check that the 13 feature commits'
  subject lines match the skeleton's list exactly, in order:

  ```
  feat(model): permission grant ledger type + latest-wins evaluation
  feat(errcode): permission reason catalog
  feat(subject): user permission.get subject builders
  feat(admin-service): permission grants store — transactional batch insert, ledger reads, audit batch
  feat(admin-service): POST /v1/admin/permissions with body limit
  feat(admin-service): GET /v1/admin/permissions ledger + current decision
  feat(user-service): permission grants repository (primary reads)
  feat(user-service): permission.get NATS handler
  feat(admin-frontend): permissions api client
  feat(admin-frontend): PermissionsView grant/revoke form + nav
  feat(admin-frontend): PermissionsView ledger lookup
  fix(docker-local): single-node mongo replica set + host directConnection URIs
  docs(client-api): permission endpoints + reason catalog
  ```

  On mismatch: this is a paperwork issue, not a code issue — don't rewrite history to fix a
  typo'd commit message on a branch that's about to be reviewed as a whole; note it in the
  PR description instead.

- [ ] **Step 13 — final commit, only if any step above required a fix.**

  If every step from 1–11 was clean on the first pass, **do not create an empty commit** —
  skip this step entirely.

  If any step needed a fix (a lint issue, a coverage gap that needed a new subtest, a gosec
  finding that needed a real code fix), stage exactly the files that changed — review
  `git status` first, since `git add -u` stages *all* modified tracked files and this task
  must never introduce a new untracked file:

  ```
  git status
  git add -u
  git commit -m "chore: full verification pass"
  ```
