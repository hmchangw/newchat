# Legacy `members_removed` Display-Name Resolution — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** When `history-service` returns a legacy system message of type `members_removed`, replace the raw account identifier baked into its `msg` text with the user's display name, resolving all accounts on a page with a single batched Mongo query.

**Architecture:** A new in-place post-processing pass over the message slice, wired into the six `messages.go` return paths alongside the existing `redactUnavailableQuotes` / `setDecodedAttachments` passes. The pass collects and dedupes accounts across the whole page, issues **one** `$in` query via the already-existing `userstore.FindUsersByAccounts`, then rewrites each matching row. Pages with no such rows return before touching Mongo. A `userstore.Cache` is wired in front of the store so repeated accounts across a scroll-back cost nothing.

**Tech Stack:** Go 1.25, MongoDB (`go.mongodb.org/mongo-driver/v2`), `go.uber.org/mock` (mockgen), `stretchr/testify`.

**Spec:** No separate design doc — this was a bounded change agreed in chat. The design is restated in full under "Design Decisions" below; executors read this file alone.

## Global Constraints

- Go 1.25. Single `go.mod` at repo root.
- Never run raw `go` commands — always the root `Makefile` targets (`make test SERVICE=history-service`, `make lint`, `make generate SERVICE=history-service`).
- TDD is mandatory: write the failing test, run it, confirm it fails, then implement. Never write implementation before its test.
- Minimum 80% coverage; target 90%+ for this service-layer logic.
- All tests use `-race` (the Makefile handles it).
- Errors always wrapped with context: `fmt.Errorf("short description: %w", err)`. Never bare `err`.
- Logging is `log/slog` with structured key-value fields only. Never `fmt.Println`. Never log tokens, passwords, or full message bodies.
- Generated mocks live in `history-service/internal/service/mocks/` and are NEVER hand-edited — regenerate with `make generate SERVICE=history-service`.
- Unit tests never connect to a real database, NATS, or any external service.
- Commit after each task's tests pass. Pre-commit hook runs lint + tests; fix failures before retrying.
- Branch: `claude/history-service-members-removed-yx6wx7`. Never push elsewhere.

---

## Design Decisions (agreed, do not re-litigate)

These were settled during brainstorming. They are requirements, not suggestions.

1. **Trigger condition.** A row qualifies only when BOTH hold:
   - `msg.Type == "members_removed"` (note: plural `members_`, distinct from the modern `member_removed` constant in `pkg/model/event.go:719` — do NOT reuse that constant, and do NOT add `members_removed` to `systemMessageTypes`).
   - `msg.Msg` ends with the exact literal `" has been removed from the channel."` — **including the trailing period**, over a non-empty prefix.

   Strictness is deliberate: a user could plausibly type the same sentence without the period, and the `Type` gate plus the required period make a false positive effectively impossible.

2. **Account location.** The account is the prefix of `Msg` before that suffix. There is nothing structured to read — these are migrated rows and carry no usable `sysMsgData`.

3. **Display name format.** `displayfmt.CombineWithFallback(u.EngName, u.ChineseName, u.Account)` — identical to `room-worker/sysmsg.go:displayName`, so a legacy row renders like a modern one. **Unquoted** — the legacy sentence keeps its own shape.

4. **Batching.** One `FindUsersByAccounts` call per response, regardless of how many qualifying rows the page holds. A page with zero qualifying rows must issue **zero** store calls.

5. **Degradation — a name lookup must never fail a history load.** Store error → log at WARN, return, leave every row untouched. Account absent from `users` → leave that row untouched (it reads as the raw account, exactly as today). No suffix match → skip the row.

6. **Scope.** `messages.go` only. `pin.go` and `threads.go` are explicitly OUT OF SCOPE — a `members_removed` row can never be pinned or become a thread parent, so those read paths never carry one. Do not wire the pass into them.

7. **Ordering at each call site.** After `redactUnavailableQuotes` / `setDecodedAttachments`, and BEFORE `fitPage` / `fitWindow`. Rewriting `Msg` changes the row's encoded size, so the budget trim must weigh final bytes.

---

## File Structure

| File | Action | Responsibility |
|---|---|---|
| `history-service/internal/service/sysmsgname.go` | Create | The matcher (`extractRemovedAccount`) and the batch-resolve pass (`resolveRemovedMemberNames`, `resolveRemovedMemberName`). Sole home for legacy system-message name rewriting. |
| `history-service/internal/service/sysmsgname_test.go` | Create | Unit tests for both, including the batching and degradation assertions. |
| `history-service/internal/service/service.go` | Modify (`:122-125`) | Add `FindUsersByAccounts` to the `UserStore` interface. |
| `history-service/internal/service/mocks/mock_repository.go` | Regenerate | Never hand-edit. |
| `history-service/internal/service/messages.go` | Modify (6 sites) | Wire the pass into each return path. |
| `history-service/internal/config/config.go` | Modify | Add `UserCacheSize` / `UserCacheTTL` + validation. |
| `history-service/internal/config/config_test.go` | Modify | Validation tests for the two new fields. |
| `history-service/cmd/main.go` | Modify (`:165`) | Wrap `userstore.NewMongoStore` in `userstore.Cache` when configured. |
| `docs/client-api.md` | Modify | Document the server-side rewrite on the `msg` field. |

---

### Task 1: The matcher

Pure, dependency-free string logic. Isolating it means the batching task can be tested without re-testing parsing, and vice versa.

**Files:**
- Create: `history-service/internal/service/sysmsgname.go`
- Test: `history-service/internal/service/sysmsgname_test.go`

**Interfaces:**
- Consumes: `models.Message` (alias of `cassandra.Message`; relevant fields `Type string`, `Msg string`).
- Produces:
  - `const legacyMembersRemovedType = "members_removed"`
  - `const removedFromChannelSuffix = " has been removed from the channel."`
  - `func extractRemovedAccount(m *models.Message) (string, bool)`

- [ ] **Step 1: Write the failing test**

Create `history-service/internal/service/sysmsgname_test.go`:

```go
package service

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/history-service/internal/models"
)

func TestExtractRemovedAccount(t *testing.T) {
	tests := []struct {
		name        string
		msgType     string
		msg         string
		wantAccount string
		wantOK      bool
	}{
		{
			name:        "legacy row yields the account prefix",
			msgType:     "members_removed",
			msg:         "bob has been removed from the channel.",
			wantAccount: "bob",
			wantOK:      true,
		},
		{
			name:        "account containing spaces is kept whole",
			msgType:     "members_removed",
			msg:         "bob smith has been removed from the channel.",
			wantAccount: "bob smith",
			wantOK:      true,
		},
		{
			name:    "modern member_removed type is not rewritten",
			msgType: "member_removed",
			msg:     "bob has been removed from the channel.",
			wantOK:  false,
		},
		{
			name:    "ordinary user message with no type is never touched",
			msgType: "",
			msg:     "bob has been removed from the channel.",
			wantOK:  false,
		},
		{
			name:    "missing trailing period does not match",
			msgType: "members_removed",
			msg:     "bob has been removed from the channel",
			wantOK:  false,
		},
		{
			name:    "suffix with no account prefix is not a rewrite candidate",
			msgType: "members_removed",
			msg:     " has been removed from the channel.",
			wantOK:  false,
		},
		{
			name:    "empty msg",
			msgType: "members_removed",
			msg:     "",
			wantOK:  false,
		},
		{
			name:    "unrelated text on the legacy type is left alone",
			msgType: "members_removed",
			msg:     "something else entirely",
			wantOK:  false,
		},
		{
			name:    "suffix in the middle rather than at the end",
			msgType: "members_removed",
			msg:     "bob has been removed from the channel. and then rejoined",
			wantOK:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := models.Message{Type: tc.msgType, Msg: tc.msg}
			account, ok := extractRemovedAccount(&m)
			assert.Equal(t, tc.wantOK, ok)
			assert.Equal(t, tc.wantAccount, account)
		})
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=history-service`

Expected: FAIL — compile error, `undefined: extractRemovedAccount`.

- [ ] **Step 3: Write minimal implementation**

Create `history-service/internal/service/sysmsgname.go`:

```go
package service

import (
	"strings"

	"github.com/hmchangw/chat/history-service/internal/models"
)

const (
	// legacyMembersRemovedType is the migrated system-message type (plural
	// "members_"), distinct from the modern model.MessageTypeMemberRemoved.
	// Rows of this type carry a raw account in their text, not in sysMsgData.
	legacyMembersRemovedType = "members_removed"
	// removedFromChannelSuffix must match exactly, trailing period included: a
	// user can type the same sentence without it, and the period plus the type
	// gate is what keeps an ordinary message from being rewritten.
	removedFromChannelSuffix = " has been removed from the channel."
)

// extractRemovedAccount returns the account baked into a legacy members_removed
// row's text. Reports false for every other row, leaving it untouched.
func extractRemovedAccount(m *models.Message) (string, bool) {
	if m == nil || m.Type != legacyMembersRemovedType {
		return "", false
	}
	account, found := strings.CutSuffix(m.Msg, removedFromChannelSuffix)
	if !found || account == "" {
		return "", false
	}
	return account, true
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=history-service`

Expected: PASS, all 9 subtests.

- [ ] **Step 5: Commit**

```bash
git add history-service/internal/service/sysmsgname.go history-service/internal/service/sysmsgname_test.go
git commit -m "feat(history-service): match legacy members_removed system messages"
```

---

### Task 2: Extend the UserStore interface with the batch read

`pkg/userstore` already implements `FindUsersByAccounts` on both the Mongo store and the cache. Only the consumer-side interface in `history-service` needs the method, per the "define interfaces in the consumer" rule.

**Files:**
- Modify: `history-service/internal/service/service.go:122-125`
- Regenerate: `history-service/internal/service/mocks/mock_repository.go`

**Interfaces:**
- Consumes: `pkg/userstore.UserStore` (already has `FindUsersByAccounts(ctx context.Context, accounts []string) ([]model.User, error)`); `pkg/userstore.Cache` satisfies the same shape.
- Produces: `mocks.MockUserStore.EXPECT().FindUsersByAccounts(ctx, accounts)` for Task 3.

- [ ] **Step 1: Extend the interface**

In `history-service/internal/service/service.go`, replace the `UserStore` block:

```go
// UserStore resolves the calling user's full profile for ReactorInfo and the Participant on the canonical event.
type UserStore interface {
	FindUserByAccount(ctx context.Context, account string) (*pkgmodel.User, error)
}
```

with:

```go
// UserStore resolves user profiles: one account for ReactorInfo and the Participant
// on the canonical event, and a batch for resolving a whole page of legacy
// system-message accounts in a single query.
type UserStore interface {
	FindUserByAccount(ctx context.Context, account string) (*pkgmodel.User, error)
	// FindUsersByAccounts resolves many accounts in one read. Accounts with no
	// matching user are simply absent from the result — not an error.
	FindUsersByAccounts(ctx context.Context, accounts []string) ([]pkgmodel.User, error)
}
```

- [ ] **Step 2: Regenerate the mocks**

Run: `make generate SERVICE=history-service`

Expected: `history-service/internal/service/mocks/mock_repository.go` gains `FindUsersByAccounts` on `MockUserStore` and its recorder. Do not hand-edit this file.

- [ ] **Step 3: Verify the whole service still compiles and passes**

Run: `make test SERVICE=history-service`

Expected: PASS. `userstore.NewMongoStore` already satisfies the widened interface, so `cmd/main.go` needs no change yet.

- [ ] **Step 4: Commit**

```bash
git add history-service/internal/service/service.go history-service/internal/service/mocks/mock_repository.go
git commit -m "feat(history-service): add batch account lookup to UserStore interface"
```

---

### Task 3: The batch-resolve pass

The load-bearing task. Two passes over the slice around exactly one store call.

**Files:**
- Modify: `history-service/internal/service/sysmsgname.go`
- Test: `history-service/internal/service/sysmsgname_test.go`

**Interfaces:**
- Consumes: `extractRemovedAccount` (Task 1); `UserStore.FindUsersByAccounts` (Task 2); `displayfmt.CombineWithFallback(first, second, fallback string) string` from `pkg/displayfmt`.
- Produces:
  - `func (s *HistoryService) resolveRemovedMemberNames(ctx context.Context, msgs []models.Message)`
  - `func (s *HistoryService) resolveRemovedMemberName(ctx context.Context, m *models.Message)`

- [ ] **Step 1: Write the failing tests**

Append to `history-service/internal/service/sysmsgname_test.go`. Also add these imports to the file's import block: `context`, `errors`, `go.uber.org/mock/gomock`, `github.com/hmchangw/chat/history-service/internal/config`, `github.com/hmchangw/chat/history-service/internal/service/mocks`, `github.com/hmchangw/chat/pkg/model`, and `github.com/stretchr/testify/require`.

```go
// newSysMsgNameService wires a service whose only live dependency is the user store.
func newSysMsgNameService(t *testing.T, users UserStore) *HistoryService {
	t.Helper()
	ctrl := gomock.NewController(t)
	return New(
		mocks.NewMockMessageRepository(ctrl),
		mocks.NewMockSubscriptionRepository(ctrl),
		mocks.NewMockRoomRepository(ctrl),
		mocks.NewMockEventPublisher(ctrl),
		mocks.NewMockThreadRoomRepository(ctrl),
		mocks.NewMockThreadSubscriptionRepository(ctrl),
		users,
		mocks.NewMockAppStore(ctrl),
		&config.Config{MessageHistoryFloorDays: 90, LargeRoomThreshold: 500, MaxPinnedPerRoom: 10},
	)
}

// legacyRemoved builds a legacy members_removed row for the given account.
func legacyRemoved(account string) models.Message {
	return models.Message{Type: "members_removed", Msg: account + " has been removed from the channel."}
}

// The whole point of the pass: many rows, ONE query, accounts deduped.
func TestResolveRemovedMemberNames_OneBatchedQueryForTheWholePage(t *testing.T) {
	ctrl := gomock.NewController(t)
	users := mocks.NewMockUserStore(ctrl)
	users.EXPECT().
		FindUsersByAccounts(gomock.Any(), gomock.Len(2)).
		DoAndReturn(func(_ context.Context, accounts []string) ([]model.User, error) {
			assert.ElementsMatch(t, []string{"bob", "carol"}, accounts)
			return []model.User{
				{Account: "bob", EngName: "Bob", ChineseName: "鮑勃"},
				{Account: "carol", EngName: "Carol"},
			}, nil
		}).
		Times(1)
	s := newSysMsgNameService(t, users)

	msgs := []models.Message{
		legacyRemoved("bob"),
		{Msg: "an ordinary message"},
		legacyRemoved("carol"),
		legacyRemoved("bob"),
	}
	s.resolveRemovedMemberNames(context.Background(), msgs)

	assert.Equal(t, "Bob 鮑勃 has been removed from the channel.", msgs[0].Msg)
	assert.Equal(t, "an ordinary message", msgs[1].Msg)
	assert.Equal(t, "Carol has been removed from the channel.", msgs[2].Msg)
	assert.Equal(t, "Bob 鮑勃 has been removed from the channel.", msgs[3].Msg)
}

// A page with no legacy rows is the overwhelmingly common case: it must not
// touch Mongo at all. gomock fails the test if the store is called.
func TestResolveRemovedMemberNames_NoQualifyingRowsIssuesNoQuery(t *testing.T) {
	ctrl := gomock.NewController(t)
	users := mocks.NewMockUserStore(ctrl)
	s := newSysMsgNameService(t, users)

	msgs := []models.Message{
		{Msg: "hello"},
		{Type: "member_removed", Msg: "bob has been removed from the channel."},
	}
	s.resolveRemovedMemberNames(context.Background(), msgs)

	assert.Equal(t, "hello", msgs[0].Msg)
	assert.Equal(t, "bob has been removed from the channel.", msgs[1].Msg)
}

func TestResolveRemovedMemberNames_EmptySliceIssuesNoQuery(t *testing.T) {
	ctrl := gomock.NewController(t)
	users := mocks.NewMockUserStore(ctrl)
	s := newSysMsgNameService(t, users)

	s.resolveRemovedMemberNames(context.Background(), nil)
	s.resolveRemovedMemberNames(context.Background(), []models.Message{})
}

// A name is a nicety; history is the product. A store failure leaves the rows
// exactly as they read today.
func TestResolveRemovedMemberNames_StoreErrorLeavesRowsUntouched(t *testing.T) {
	ctrl := gomock.NewController(t)
	users := mocks.NewMockUserStore(ctrl)
	users.EXPECT().
		FindUsersByAccounts(gomock.Any(), gomock.Any()).
		Return(nil, errors.New("mongo unavailable")).
		Times(1)
	s := newSysMsgNameService(t, users)

	msgs := []models.Message{legacyRemoved("bob")}
	s.resolveRemovedMemberNames(context.Background(), msgs)

	assert.Equal(t, "bob has been removed from the channel.", msgs[0].Msg)
}

// An account with no user document (deleted, or never migrated) keeps its raw form.
func TestResolveRemovedMemberNames_UnresolvedAccountKeepsRawText(t *testing.T) {
	ctrl := gomock.NewController(t)
	users := mocks.NewMockUserStore(ctrl)
	users.EXPECT().
		FindUsersByAccounts(gomock.Any(), gomock.Any()).
		Return([]model.User{{Account: "bob", EngName: "Bob"}}, nil).
		Times(1)
	s := newSysMsgNameService(t, users)

	msgs := []models.Message{legacyRemoved("bob"), legacyRemoved("ghost")}
	s.resolveRemovedMemberNames(context.Background(), msgs)

	assert.Equal(t, "Bob has been removed from the channel.", msgs[0].Msg)
	assert.Equal(t, "ghost has been removed from the channel.", msgs[1].Msg)
}

// A user document with no names at all must not blank the sentence.
func TestResolveRemovedMemberNames_UserWithNoNamesFallsBackToAccount(t *testing.T) {
	ctrl := gomock.NewController(t)
	users := mocks.NewMockUserStore(ctrl)
	users.EXPECT().
		FindUsersByAccounts(gomock.Any(), gomock.Any()).
		Return([]model.User{{Account: "bob"}}, nil).
		Times(1)
	s := newSysMsgNameService(t, users)

	msgs := []models.Message{legacyRemoved("bob")}
	s.resolveRemovedMemberNames(context.Background(), msgs)

	assert.Equal(t, "bob has been removed from the channel.", msgs[0].Msg)
}

// The single-message wrapper serves GetMessageByID and the spliced central row.
func TestResolveRemovedMemberName_SingleMessage(t *testing.T) {
	ctrl := gomock.NewController(t)
	users := mocks.NewMockUserStore(ctrl)
	users.EXPECT().
		FindUsersByAccounts(gomock.Any(), gomock.Len(1)).
		Return([]model.User{{Account: "bob", EngName: "Bob", ChineseName: "鮑勃"}}, nil).
		Times(1)
	s := newSysMsgNameService(t, users)

	m := legacyRemoved("bob")
	s.resolveRemovedMemberName(context.Background(), &m)

	assert.Equal(t, "Bob 鮑勃 has been removed from the channel.", m.Msg)
}

func TestResolveRemovedMemberName_NilAndNonQualifyingAreNoOps(t *testing.T) {
	ctrl := gomock.NewController(t)
	users := mocks.NewMockUserStore(ctrl)
	s := newSysMsgNameService(t, users)

	s.resolveRemovedMemberName(context.Background(), nil)

	m := models.Message{Msg: "hello"}
	s.resolveRemovedMemberName(context.Background(), &m)
	assert.Equal(t, "hello", m.Msg)
}

// A nil user store must degrade, not panic — New accepts one.
func TestResolveRemovedMemberNames_NilStoreDegrades(t *testing.T) {
	s := newSysMsgNameService(t, nil)

	msgs := []models.Message{legacyRemoved("bob")}
	require.NotPanics(t, func() { s.resolveRemovedMemberNames(context.Background(), msgs) })
	assert.Equal(t, "bob has been removed from the channel.", msgs[0].Msg)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=history-service`

Expected: FAIL — compile error, `s.resolveRemovedMemberNames undefined`.

- [ ] **Step 3: Write the implementation**

Append to `history-service/internal/service/sysmsgname.go`, and extend its import block to `context`, `log/slog`, `strings`, plus `github.com/hmchangw/chat/history-service/internal/models` and `github.com/hmchangw/chat/pkg/displayfmt`:

```go
// resolveRemovedMemberNames rewrites every legacy members_removed row in msgs,
// swapping the raw account in its text for the user's display name.
//
// One batched read serves the whole page however many rows qualify, and a page
// with none — the overwhelmingly common case — returns before touching Mongo.
//
// Best-effort throughout: a failed lookup or an account with no user document
// leaves the row reading exactly as it does today. A display name is never worth
// failing a history load over.
func (s *HistoryService) resolveRemovedMemberNames(ctx context.Context, msgs []models.Message) {
	if len(msgs) == 0 || s.users == nil {
		return
	}

	accounts := make([]string, 0, len(msgs))
	seen := make(map[string]struct{}, len(msgs))
	for i := range msgs {
		account, ok := extractRemovedAccount(&msgs[i])
		if !ok {
			continue
		}
		if _, dup := seen[account]; dup {
			continue
		}
		seen[account] = struct{}{}
		accounts = append(accounts, account)
	}
	if len(accounts) == 0 {
		return
	}

	users, err := s.users.FindUsersByAccounts(ctx, accounts)
	if err != nil {
		slog.WarnContext(ctx, "resolving removed-member display names, leaving raw accounts",
			"accounts", len(accounts), "error", err)
		return
	}

	names := make(map[string]string, len(users))
	for i := range users {
		u := &users[i]
		names[u.Account] = displayfmt.CombineWithFallback(u.EngName, u.ChineseName, u.Account)
	}

	for i := range msgs {
		account, ok := extractRemovedAccount(&msgs[i])
		if !ok {
			continue
		}
		if name, found := names[account]; found {
			msgs[i].Msg = name + removedFromChannelSuffix
		}
	}
}

// resolveRemovedMemberName is the one-message form, for the handlers that return
// a single row rather than a page.
func (s *HistoryService) resolveRemovedMemberName(ctx context.Context, m *models.Message) {
	if m == nil {
		return
	}
	one := []models.Message{*m}
	s.resolveRemovedMemberNames(ctx, one)
	*m = one[0]
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=history-service`

Expected: PASS, all subtests.

- [ ] **Step 5: Check coverage of the new file**

Run: `make test SERVICE=history-service` then, to inspect the numbers:

```bash
go test -race -coverprofile=/tmp/cov.out ./history-service/internal/service/... && go tool cover -func=/tmp/cov.out | grep sysmsgname
```

Expected: every function in `sysmsgname.go` at or above 90%. If `resolveRemovedMemberName`'s nil branch or the `names` miss branch is uncovered, the tests above already exercise them — investigate rather than adding filler tests.

- [ ] **Step 6: Lint**

Run: `make lint`

Expected: clean.

- [ ] **Step 7: Commit**

```bash
git add history-service/internal/service/sysmsgname.go history-service/internal/service/sysmsgname_test.go
git commit -m "feat(history-service): batch-resolve legacy members_removed display names"
```

---

### Task 4: Wire the pass into the six `messages.go` return paths

Each site already runs the redaction and attachment passes. The new call joins them, before any page-fitting.

**Files:**
- Modify: `history-service/internal/service/messages.go` at lines 95-96, 165-166, 231-232, 371-372, 436-437, 480-481
- Test: `history-service/internal/service/messages_test.go` (append)

**Note on test package.** `sysmsgname_test.go` from Tasks 1 and 3 is `package service` — it must be, to reach the unexported `extractRemovedAccount`. The handler test below goes in `messages_test.go` instead, which is `package service_test`, because that is where the shared handler fixtures (`newServiceWithRoomMock`, `testContext`, `makePage`, `joinTime`) already live. Both packages coexisting in one directory is the existing arrangement here (`appname_test.go` is internal, `messages_test.go` external) — follow it rather than duplicating helpers.

**Interfaces:**
- Consumes: `resolveRemovedMemberNames`, `resolveRemovedMemberName` (Task 3).
- Produces: nothing new — behavior change only.

**Reference — the six sites.** Line numbers shift as you edit; anchor on the surrounding `setDecodedAttachments` / `decodeMessageAttachments` call, which is unique per handler.

| Handler | Anchor | Shape |
|---|---|---|
| `LoadHistory` | `setDecodedAttachments(c, page.Data)` before `s.fitPage(c, page.Data, pageEnvelope)` | slice |
| `LoadNextMessages` | `setDecodedAttachments(c, page.Data)` before the `LoadNextMessagesResponse` literal | slice |
| `loadSurroundingByMessageID` | `decodeMessageAttachments(c, &only)` | single |
| `assembleSurrounding` | `setDecodedAttachments(c, messages)` before `s.fitWindow(...)` | slice |
| `GetMessageByID` | `decodeMessageAttachments(c, msg)` before `return msg, nil` | single |
| `GetMessagesByIDs` | `setDecodedAttachments(c, kept)` before the `GetMessagesByIDsResponse` literal | slice |

- [ ] **Step 1: Write the failing handler test**

Append to `history-service/internal/service/messages_test.go`, in the `// --- LoadHistory ---` section beside the other `TestHistoryService_LoadHistory_*` tests. This asserts the pass is actually reached through a real handler, not merely by a direct call.

All helpers below already exist in that file: `newServiceWithRoomMock` (returns the `*mocks.MockUserStore` as its 7th value), `testContext` (route params `account="u1"`, `roomID="r1"`), `makePage`, and `joinTime`. Every import it needs is already in the file's import block — add none.

```go
// A legacy members_removed row must come back name-resolved through the real
// LoadHistory path, with one batched lookup.
func TestHistoryService_LoadHistory_ResolvesRemovedMemberNames(t *testing.T) {
	svc, msgs, subs, rooms, _, _, users, _ := newServiceWithRoomMock(t)
	c := testContext()

	// newServiceWithRoomMock leaves the read-floor read to the caller.
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)

	messages := []models.Message{{
		MessageID: "m1", RoomID: "r1", CreatedAt: joinTime.Add(time.Minute),
		Type: "members_removed", Msg: "bob has been removed from the channel.",
	}}
	msgs.EXPECT().
		GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, gomock.Any(), gomock.Any()).
		Return(makePage(messages, false), nil)
	users.EXPECT().
		FindUsersByAccounts(gomock.Any(), []string{"bob"}).
		Return([]model.User{{Account: "bob", EngName: "Bob", ChineseName: "鮑勃"}}, nil).
		Times(1)

	resp, err := svc.LoadHistory(c, models.LoadHistoryRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Messages, 1)
	assert.Equal(t, "Bob 鮑勃 has been removed from the channel.", resp.Messages[0].Msg)
}

// The ordinary page must not acquire a Mongo round trip: gomock fails the test
// if FindUsersByAccounts is called without an expectation.
func TestHistoryService_LoadHistory_OrdinaryPageIssuesNoUserLookup(t *testing.T) {
	svc, msgs, subs, rooms, _, _, _, _ := newServiceWithRoomMock(t)
	c := testContext()

	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)

	messages := []models.Message{{
		MessageID: "m1", RoomID: "r1", CreatedAt: joinTime.Add(time.Minute), Msg: "hello",
	}}
	msgs.EXPECT().
		GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, gomock.Any(), gomock.Any()).
		Return(makePage(messages, false), nil)

	resp, err := svc.LoadHistory(c, models.LoadHistoryRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Messages, 1)
	assert.Equal(t, "hello", resp.Messages[0].Msg)
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `make test SERVICE=history-service`

Expected: `TestHistoryService_LoadHistory_ResolvesRemovedMemberNames` FAILS — the returned `Msg` is still `"bob has been removed from the channel."`, and gomock reports the `FindUsersByAccounts` expectation was never met. `TestHistoryService_LoadHistory_OrdinaryPageIssuesNoUserLookup` PASSES already; it is a regression guard, not a red test.

- [ ] **Step 3: Wire the four slice sites**

In `history-service/internal/service/messages.go`, add `s.resolveRemovedMemberNames(c, <slice>)` immediately after each `setDecodedAttachments` call:

```go
	redactUnavailableQuotes(page.Data, accessSince)
	setDecodedAttachments(c, page.Data)
	s.resolveRemovedMemberNames(c, page.Data)
```

Apply the same one-line addition at all four slice anchors from the reference table, matching the local slice variable each time — `page.Data` in `LoadHistory` and `LoadNextMessages`, `messages` in `assembleSurrounding`, `kept` in `GetMessagesByIDs`.

In `LoadHistory` and `assembleSurrounding` the new line must sit **above** the `s.fitPage(...)` / `s.fitWindow(...)` call, so the trim weighs the rewritten bytes.

- [ ] **Step 4: Wire the two single-message sites**

In `loadSurroundingByMessageID`:

```go
		only := *centralMsg
		redactUnavailableQuote(&only, accessSince)
		decodeMessageAttachments(c, &only)
		s.resolveRemovedMemberName(c, &only)
```

In `GetMessageByID`:

```go
	redactUnavailableQuote(msg, accessSince)
	decodeMessageAttachments(c, msg)
	s.resolveRemovedMemberName(c, msg)
	return msg, nil
```

- [ ] **Step 5: Confirm the out-of-scope files were not touched**

Run:

```bash
git diff --name-only
```

Expected: `history-service/internal/service/messages.go` and `history-service/internal/service/messages_test.go` only. `pin.go` and `threads.go` must NOT appear — a `members_removed` row never reaches those read paths.

- [ ] **Step 6: Run the full service suite**

Run: `make test SERVICE=history-service`

Expected: PASS, including every pre-existing `messages_test.go` test. The pass is a no-op on fixtures with no legacy rows, so no existing expectation should need loosening. **If an existing test now fails because a mock `UserStore` gets an unexpected `FindUsersByAccounts` call, that means a fixture contains a qualifying row — investigate the fixture rather than adding a blanket `AnyTimes()`.**

- [ ] **Step 7: Lint**

Run: `make lint`

Expected: clean.

- [ ] **Step 8: Commit**

```bash
git add history-service/internal/service/messages.go history-service/internal/service/messages_test.go
git commit -m "feat(history-service): resolve removed-member names on all message read paths"
```

---

### Task 5: Front the user store with an LRU+TTL cache

Without this, every page holding a legacy row costs a Mongo round trip, and a user scrolling back through a channel re-resolves the same handful of accounts repeatedly. `userstore.Cache.FindUsersByAccounts` already serves hits from its LRU and forwards only the misses in one call — this task is wiring, not new cache logic.

**Files:**
- Modify: `history-service/internal/config/config.go`
- Modify: `history-service/internal/config/config_test.go`
- Modify: `history-service/cmd/main.go:165`

**Interfaces:**
- Consumes: `userstore.NewCache(store UserStore, size int, ttl time.Duration, opts ...Option) (*Cache, error)`; `userstore.NewMongoStore(col *mongo.Collection) UserStore`.
- Produces: `cfg.UserCacheSize int`, `cfg.UserCacheTTL time.Duration`.

- [ ] **Step 1: Write the failing config test**

Read `history-service/internal/config/config_test.go` first and mirror its existing validation-test style exactly. Add cases asserting that a negative `UserCacheSize` and a negative `UserCacheTTL` are each rejected with the env-var name in the message, following the pattern the `HISTORY_SUB_CACHE_SIZE` / `HISTORY_SUB_CACHE_TTL` cases already use.

- [ ] **Step 2: Run to verify it fails**

Run: `make test SERVICE=history-service`

Expected: FAIL — `cfg.UserCacheSize` undefined.

- [ ] **Step 3: Add the config fields**

In `history-service/internal/config/config.go`, after the `PreviewCacheSize` / `PreviewCacheTTL` pair:

```go
	// User profile cache, fronting the batched account lookup that resolves
	// legacy members_removed display names. Names change rarely, so the TTL is
	// generous. Set size or ttl to 0 to disable.
	UserCacheSize int           `env:"HISTORY_USER_CACHE_SIZE" envDefault:"50000"`
	UserCacheTTL  time.Duration `env:"HISTORY_USER_CACHE_TTL"  envDefault:"10m"`
```

And in the same file's validation function, alongside the existing cache checks:

```go
	if cfg.UserCacheSize < 0 {
		return fmt.Errorf("HISTORY_USER_CACHE_SIZE must be >= 0, got %d", cfg.UserCacheSize)
	}
	if cfg.UserCacheTTL < 0 {
		return fmt.Errorf("HISTORY_USER_CACHE_TTL must be >= 0, got %s", cfg.UserCacheTTL)
	}
```

- [ ] **Step 4: Run to verify the config test passes**

Run: `make test SERVICE=history-service`

Expected: PASS.

- [ ] **Step 5: Wire the cache in main.go**

In `history-service/cmd/main.go`, the line

```go
	userStore := userstore.NewMongoStore(db.Collection("users"))
```

becomes:

```go
	var userSource service.UserStore = userstore.NewMongoStore(db.Collection("users"))
	if cfg.UserCacheSize > 0 && cfg.UserCacheTTL > 0 {
		uc, err := userstore.NewCache(
			userstore.NewMongoStore(db.Collection("users")), cfg.UserCacheSize, cfg.UserCacheTTL)
		if err != nil {
			slog.Error("init user cache failed", "error", err)
			os.Exit(1)
		}
		userSource = uc
		slog.Info("user cache enabled", "size", cfg.UserCacheSize, "ttl", cfg.UserCacheTTL)
	}
```

Then update the `service.New(...)` call to pass `userSource` where it currently passes `userStore`. This mirrors the `subSource` / `roomSource` pattern immediately below it, including fail-fast on a construction error.

- [ ] **Step 6: Verify the service builds and the suite passes**

Run: `make build SERVICE=history-service && make test SERVICE=history-service`

Expected: build succeeds, tests PASS.

- [ ] **Step 7: Lint**

Run: `make lint`

Expected: clean.

- [ ] **Step 8: Commit**

```bash
git add history-service/internal/config/config.go history-service/internal/config/config_test.go history-service/cmd/main.go
git commit -m "feat(history-service): cache user profile lookups behind an LRU+TTL"
```

---

### Task 6: Document the behavior and run the pre-push gates

Per CLAUDE.md, a change visible to clients on a `chat.user.` RPC updates `docs/client-api.md` in the same PR. No request or response struct changed, so the derived views (`docs/client-api/request-reply.md`, `docs/client-api/events.md`) do not need edits — verify that before concluding it.

**Files:**
- Modify: `docs/client-api.md`

- [ ] **Step 1: Locate the `msg` field row**

Run:

```bash
grep -n '^| `msg` |' docs/client-api.md
```

Read the surrounding Message field table (near line 2927) to match the file's existing wording and column style.

- [ ] **Step 2: Add the note**

In the shared Message field table, extend the `msg` field's description with one sentence, matching the table's terse house style:

> For legacy `members_removed` system messages the server substitutes the removed user's display name for the stored account identifier before returning the row; an account with no matching user is returned unchanged.

Keep it to that — minimal prose, no redundant explanation, per the documentation rules.

- [ ] **Step 3: Confirm the derived views need no change**

Run:

```bash
grep -rn "members_removed" docs/client-api/
```

Expected: no matches. No request/reply or event struct changed, so neither derived view drifts. If there ARE matches, update the matching view in this same commit.

- [ ] **Step 4: Run the full gate**

Run:

```bash
make lint && make test && make sast
```

Expected: all clean. `make test` is the whole repo, not just this service — the widened `UserStore` interface is internal to `history-service`, so nothing else should break, but confirm rather than assume.

- [ ] **Step 5: Commit and push**

```bash
git add docs/client-api.md
git commit -m "docs(client-api): note server-side name resolution on legacy members_removed rows"
git push -u origin claude/history-service-members-removed-yx6wx7
```

If the push fails on a network error, retry up to 4 times with exponential backoff (2s, 4s, 8s, 16s). Do NOT open a pull request unless explicitly asked.

---

## Verification Checklist

Confirm each by running the command and reading the output — never by assumption.

- [ ] `make test SERVICE=history-service` passes with `-race`.
- [ ] `make lint` is clean.
- [ ] `make sast` is clean (blocking CI gate).
- [ ] `go tool cover -func` shows `sysmsgname.go` at 90%+.
- [ ] A page with no legacy rows issues **zero** `FindUsersByAccounts` calls (asserted by `TestResolveRemovedMemberNames_NoQualifyingRowsIssuesNoQuery`).
- [ ] A page with N legacy rows over K distinct accounts issues **exactly one** call carrying K accounts (asserted by `TestResolveRemovedMemberNames_OneBatchedQueryForTheWholePage`).
- [ ] `git diff --stat origin/master...HEAD` touches no file outside the File Structure table — in particular, neither `pin.go` nor `threads.go`.
