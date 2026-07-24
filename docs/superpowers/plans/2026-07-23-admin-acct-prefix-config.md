# Configurable Admin-Account Prefix (Read-Floor & Receipts) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the hardcoded `p_` admin/platform prefix used by `room-service`'s read-floor and read-receipt queries operator-configurable via `ADMIN_ACCT_PREFIX` (default `p_chatadmin_`).

**Architecture:** A pure helper builds the two query-side exclusion regexes from the configured prefix (regex-escaped). `NewMongoStore` gains a functional option (`WithAdminAcctPrefix`) that overrides the built-in default; the store holds the precomputed regex strings and the four read queries reference them instead of package constants. `main.go` parses `ADMIN_ACCT_PREFIX` and passes the option.

**Tech Stack:** Go 1.25, `go.mongodb.org/mongo-driver/v2`, `caarlos0/env`, `stretchr/testify`, testcontainers-go (integration).

## Global Constraints

- Spec: `docs/superpowers/specs/2026-07-23-admin-acct-prefix-config-design.md`.
- Scope is **read-state only** — the 4 methods in `room-service/store_mongo.go`. Do **not** touch `helper.go`'s `platformAdminRegex`, `pkg/model.IsPlatformAdminAccount`, or any other `p_` site.
- The **bot filter is unchanged** (`u.isBot $ne true` / `.bot` suffix). Only the admin-prefix half becomes configurable.
- Empty prefix ⇒ admin exclusion **disabled** (bot filter still applies). Never emit a bare `^` regex.
- Regex-escape the configured prefix with `regexp.QuoteMeta` — a config value is never a trusted regex literal.
- Env var: `ADMIN_ACCT_PREFIX`, `envDefault:"p_chatadmin_"`, `SCREAMING_SNAKE_CASE`, parsed via `caarlos0/env` (never `os.Getenv`).
- Always use `make` targets. Unit: `make test SERVICE=room-service`. Integration: `make test-integration SERVICE=room-service`. Lint: `make lint`.
- Minimum 80% coverage; cover happy path, edge (empty prefix, regex metachars), and the narrowing behavior.
- No schema, no migration, no wire change ⇒ **no `docs/client-api.md` update** (request/response, errors, events all unchanged).

> **Refinement of spec §4.2:** the spec sketched `NewMongoStore(db, adminAcctPrefix)`. This plan uses a **functional option** (`NewMongoStore(db, opts ...Option)`) instead, because `NewMongoStore(db)` has ~130 call sites in `room-service/integration_test.go`; a required positional arg would force noisy edits to all of them. The variadic option keeps every existing call compiling (they get the default) and matches the repo's established options idiom (`pkg/roommetacache`, `pkg/mongoutil`). Task 4 syncs the spec text.

---

## File Structure

- `room-service/store_mongo.go` — Modify: add `adminAccountPatterns` helper, `Option`/`WithAdminAcctPrefix`, `defaultAdminAcctPrefix`, two `MongoStore` fields; rewrite the 4 read queries; remove `pseudoAccountRegex`/`botOrPseudoAccountRegex` constants.
- `room-service/store_mongo_test.go` — Create: unit test for `adminAccountPatterns` (no build tag; runs under `make test`).
- `room-service/integration_test.go` — Modify: update the admin fixtures in the two existing read tests; add prefix-config coverage (narrowing, custom prefix, empty prefix) for room and thread paths.
- `room-service/main.go` — Modify: add `AdminAcctPrefix` config field; pass `WithAdminAcctPrefix(cfg.AdminAcctPrefix)` at the `NewMongoStore` call.
- `room-service/deploy/docker-compose.yml` — Modify: document `ADMIN_ACCT_PREFIX` in the env block.
- `docs/superpowers/specs/2026-07-23-admin-acct-prefix-config-design.md` — Modify: sync §4.2 constructor signature to the functional-option form.

---

## Task 1: Pure regex-builder helper (`adminAccountPatterns`)

**Files:**
- Modify: `room-service/store_mongo.go`
- Test: `room-service/store_mongo_test.go` (create)

**Interfaces:**
- Produces: `func adminAccountPatterns(prefix string) (adminRegex, botOrAdminRegex string)` and `const defaultAdminAcctPrefix = "p_chatadmin_"`.
- Consumes: existing `const botAccountRegex = ` `` `\.bot$` `` (store_mongo.go:23) and the already-imported `regexp`.

- [ ] **Step 1: Write the failing unit test**

Create `room-service/store_mongo_test.go`:

```go
package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAdminAccountPatterns(t *testing.T) {
	tests := []struct {
		name            string
		prefix          string
		wantAdmin       string
		wantBotOrAdmin  string
	}{
		{"default admin prefix", "p_chatadmin_", "^p_chatadmin_", `(\.bot$|^p_chatadmin_)`},
		{"legacy broad prefix", "p_", "^p_", `(\.bot$|^p_)`},
		{"empty disables admin filter", "", "", `\.bot$`},
		{"regex metacharacters are escaped", "p.a(", `^p\.a\(`, `(\.bot$|^p\.a\()`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotAdmin, gotBotOrAdmin := adminAccountPatterns(tt.prefix)
			assert.Equal(t, tt.wantAdmin, gotAdmin, "adminRegex")
			assert.Equal(t, tt.wantBotOrAdmin, gotBotOrAdmin, "botOrAdminRegex")
		})
	}
}

func TestDefaultAdminAcctPrefix(t *testing.T) {
	assert.Equal(t, "p_chatadmin_", defaultAdminAcctPrefix)
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `make test SERVICE=room-service`
Expected: FAIL — compile error `undefined: adminAccountPatterns` / `undefined: defaultAdminAcctPrefix`.

- [ ] **Step 3: Add the helper and default constant**

In `room-service/store_mongo.go`, directly below the `botAccountPattern` var (after line 25), add:

```go
// defaultAdminAcctPrefix is the built-in ADMIN_ACCT_PREFIX default: admin
// accounts are named p_chatadmin_*. Overridden per-store via WithAdminAcctPrefix.
const defaultAdminAcctPrefix = "p_chatadmin_"

// adminAccountPatterns builds the query-side account-exclusion regexes from a
// configured admin-account prefix. It returns the admin-only regex (anchored,
// for the main-room u.account guard) and the combined bot+admin regex (for the
// flat thread_subscriptions queries). An empty prefix disables the admin-account
// exclusion (adminRegex == ""), leaving only the bot filter. The prefix is
// regex-escaped: a configured value must never be trusted to be a regex literal.
func adminAccountPatterns(prefix string) (adminRegex, botOrAdminRegex string) {
	if prefix == "" {
		return "", botAccountRegex
	}
	q := regexp.QuoteMeta(prefix)
	return "^" + q, `(\.bot$|^` + q + `)`
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `make test SERVICE=room-service`
Expected: PASS (`TestAdminAccountPatterns`, `TestDefaultAdminAcctPrefix`).

- [ ] **Step 5: Commit**

```bash
git add room-service/store_mongo.go room-service/store_mongo_test.go
git commit -m "feat(room-service): add adminAccountPatterns regex builder"
```

---

## Task 2: Store option, fields, and query rewrite

**Files:**
- Modify: `room-service/store_mongo.go`
- Test: `room-service/integration_test.go`

**Interfaces:**
- Consumes: `adminAccountPatterns`, `defaultAdminAcctPrefix` (Task 1).
- Produces:
  - `type Option func(*MongoStore)`
  - `func WithAdminAcctPrefix(prefix string) Option`
  - `func NewMongoStore(db *mongo.Database, opts ...Option) *MongoStore` (signature widened; existing zero-option calls unaffected)
  - `MongoStore` fields `adminAcctRegex string`, `botOrAdminRegex string`.

- [ ] **Step 1: Write the failing integration tests**

All edits are in `room-service/integration_test.go`.

(a) In `TestMongoStore_MinSubscriptionLastSeenByRoomID_Integration`, update the "padmin" fixture so the admin account matches the new default (currently `p_admin`, lines ~2145-2148):

```go
	mustInsertSub(t, db, &model.Subscription{
		ID: "s18", User: model.SubscriptionUser{ID: "u18", Account: "p_chatadmin_admin"},
		RoomID: "padmin", JoinedAt: earliest,
	})
```

(b) In the same test, immediately after the "padmin" assertion block (after line ~2152, before the "no subscriptions" block), add a narrowing case — a non-admin `p_` account is now a real participant:

```go
	// Room "pwebhook-counted": a non-admin p_ account (does NOT match the default
	// ADMIN_ACCT_PREFIX p_chatadmin_) is a real participant now. It has never read,
	// so the strict floor is nil — proving the narrowed prefix no longer excludes
	// every p_ account the way the old ^p_ guard did.
	mustInsertSub(t, db, &model.Subscription{
		ID: "s19", User: model.SubscriptionUser{ID: "u19", Account: "nina"},
		RoomID: "pwebhook-counted", JoinedAt: earliest, LastSeenAt: &mid,
	})
	mustInsertSub(t, db, &model.Subscription{
		ID: "s20", User: model.SubscriptionUser{ID: "u20", Account: "p_webhook"},
		RoomID: "pwebhook-counted", JoinedAt: earliest,
	})
	got, err = store.MinSubscriptionLastSeenByRoomID(ctx, "pwebhook-counted")
	require.NoError(t, err)
	assert.Nil(t, got)
```

(c) In `TestMongoStore_ListReadReceipts_Integration`, rename the admin fixture (users doc `uE` line ~2313 and sub `sE` line ~2327) from `p_ops` to `p_chatadmin_ops` so it stays excluded under the default:

```go
		bson.M{"_id": "uE", "account": "p_chatadmin_ops", "chineseName": "運維", "engName": "Ops"},
```
```go
		bson.M{"_id": "sE", "roomId": "r1", "u": bson.M{"_id": "uE", "account": "p_chatadmin_ops"}, "lastSeenAt": msgTime.Add(20 * time.Minute)},
```

(d) In `TestMongoStore_MinThreadSubscriptionLastSeenByThreadRoomID_Integration`, after the "bot-parent" block (after line ~2238), add admin-excluded and non-admin-counted thread cases:

```go
	// "admin-parent": a p_chatadmin_ admin thread subscriber that never read is
	// excluded by account, so the floor is the human's lastSeenAt.
	mustInsertThreadSub(t, db, &model.ThreadSubscription{ID: "ts8", ThreadRoomID: "admin-parent", UserAccount: "grace", LastSeenAt: &mid})
	mustInsertThreadSub(t, db, &model.ThreadSubscription{ID: "ts9", ThreadRoomID: "admin-parent", UserAccount: "p_chatadmin_ops"})
	got, err = store.MinThreadSubscriptionLastSeenByThreadRoomID(ctx, "admin-parent")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.WithinDuration(t, mid, *got, time.Second)

	// "pwebhook-thread": a non-admin p_ subscriber is counted now; never-read ⇒ nil.
	mustInsertThreadSub(t, db, &model.ThreadSubscription{ID: "ts10", ThreadRoomID: "pwebhook-thread", UserAccount: "harry", LastSeenAt: &mid})
	mustInsertThreadSub(t, db, &model.ThreadSubscription{ID: "ts11", ThreadRoomID: "pwebhook-thread", UserAccount: "p_webhook"})
	got, err = store.MinThreadSubscriptionLastSeenByThreadRoomID(ctx, "pwebhook-thread")
	require.NoError(t, err)
	assert.Nil(t, got)
```

(e) Add a dedicated test at the end of `integration_test.go` exercising the option (custom + empty prefix) on the room receipt path:

```go
func TestMongoStore_ListReadReceipts_AdminPrefixConfig_Integration(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	require.NoError(t, NewMongoStore(db).EnsureIndexes(ctx))

	_, err := db.Collection("users").InsertMany(ctx, []any{
		bson.M{"_id": "uB", "account": "bob", "chineseName": "鮑勃", "engName": "Bob"},
		bson.M{"_id": "uAdm", "account": "p_chatadmin_ops", "chineseName": "運維", "engName": "Ops"},
		bson.M{"_id": "uHook", "account": "p_webhook", "chineseName": "掛鉤", "engName": "Hook"},
	})
	require.NoError(t, err)

	msgTime := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	_, err = db.Collection("subscriptions").InsertMany(ctx, []any{
		bson.M{"_id": "sB", "roomId": "r1", "u": bson.M{"_id": "uB", "account": "bob"}, "lastSeenAt": msgTime.Add(time.Hour)},
		bson.M{"_id": "sAdm", "roomId": "r1", "u": bson.M{"_id": "uAdm", "account": "p_chatadmin_ops"}, "lastSeenAt": msgTime.Add(time.Hour)},
		bson.M{"_id": "sHook", "roomId": "r1", "u": bson.M{"_id": "uHook", "account": "p_webhook"}, "lastSeenAt": msgTime.Add(time.Hour)},
	})
	require.NoError(t, err)

	accountsOf := func(store *MongoStore) []string {
		rows, err := store.ListReadReceipts(ctx, "r1", msgTime, "someone-else", 100)
		require.NoError(t, err)
		got := make([]string, 0, len(rows))
		for _, r := range rows {
			got = append(got, r.Account)
		}
		return got
	}

	// Default prefix (p_chatadmin_): admin excluded, non-admin p_webhook counted.
	assert.ElementsMatch(t, []string{"bob", "p_webhook"}, accountsOf(NewMongoStore(db)))

	// Broad legacy prefix p_: both p_ accounts excluded.
	assert.ElementsMatch(t, []string{"bob"}, accountsOf(NewMongoStore(db, WithAdminAcctPrefix("p_"))))

	// Empty prefix: admin exclusion disabled, all three surface.
	assert.ElementsMatch(t, []string{"bob", "p_chatadmin_ops", "p_webhook"}, accountsOf(NewMongoStore(db, WithAdminAcctPrefix(""))))
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make test-integration SERVICE=room-service`
Expected: FAIL — compile error `undefined: WithAdminAcctPrefix` (option not yet defined). This is the RED for this task.

- [ ] **Step 3: Add the option, fields, and constructor**

In `room-service/store_mongo.go`, add two fields to the `MongoStore` struct (after `teamsMeetings`, line ~36):

```go
	// adminAcctRegex excludes admin accounts (accounts matching ADMIN_ACCT_PREFIX)
	// from read floors / receipts; "" disables the admin exclusion. botOrAdminRegex
	// is the combined bot+admin exclusion for the flat thread_subscriptions queries.
	adminAcctRegex  string
	botOrAdminRegex string
```

Replace `NewMongoStore` (lines ~39-51) with:

```go
// Option configures a MongoStore at construction.
type Option func(*MongoStore)

// WithAdminAcctPrefix overrides the account prefix used to exclude admin
// accounts from read-floor and read-receipt queries. An empty prefix disables
// the admin-account exclusion (bot filtering still applies).
func WithAdminAcctPrefix(prefix string) Option {
	return func(s *MongoStore) {
		s.adminAcctRegex, s.botOrAdminRegex = adminAccountPatterns(prefix)
	}
}

func NewMongoStore(db *mongo.Database, opts ...Option) *MongoStore {
	adminRegex, botOrAdminRegex := adminAccountPatterns(defaultAdminAcctPrefix)
	s := &MongoStore{
		rooms:               db.Collection("rooms"),
		subscriptions:       db.Collection("subscriptions"),
		threadSubscriptions: db.Collection("thread_subscriptions"),
		threadRooms:         db.Collection("thread_rooms"),
		roomMembers:         db.Collection("room_members"),
		users:               db.Collection("users"),
		apps:                db.Collection("apps"),
		botCmdMenus:         db.Collection("bot_cmd_menu"),
		teamsMeetings:       db.Collection("teams_meetings"),
		adminAcctRegex:      adminRegex,
		botOrAdminRegex:     botOrAdminRegex,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}
```

- [ ] **Step 4: Rewrite the four read queries to use the store fields**

In `room-service/store_mongo.go`:

Remove the now-unused constants (lines ~1118-1125): the `pseudoAccountRegex` / `botOrPseudoAccountRegex` `const (...)` block and its doc comment.

`MinSubscriptionLastSeenByRoomID` — replace the `FindOne` filter (the `bson.M{"roomId": roomID, ...}` literal at line ~1160) with a conditional build:

```go
	filter := bson.M{"roomId": roomID, "u.isBot": bson.M{"$ne": true}}
	if s.adminAcctRegex != "" {
		filter["u.account"] = bson.M{"$not": bson.Regex{Pattern: s.adminAcctRegex}}
	}
	err := s.subscriptions.FindOne(ctx,
		filter,
		options.FindOne().
			SetSort(bson.D{{Key: "lastSeenAt", Value: 1}}).
			SetProjection(bson.M{"lastSeenAt": 1, "_id": 0}),
	).Decode(&doc)
```

`ListReadReceipts` — replace the `$match` stage (lines ~1202-1209) with:

```go
	account := bson.M{"$ne": excludeAccount}
	if s.adminAcctRegex != "" {
		account["$not"] = bson.Regex{Pattern: s.adminAcctRegex}
	}
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.M{
			"roomId":     roomID,
			"lastSeenAt": bson.M{"$gte": since},
			"u.account":  account,
			"u.isBot":    bson.M{"$ne": true},
		}}},
```
(Leave the `$lookup`/`$unwind`/`$replaceWith`/`$limit` stages that follow unchanged.)

`MinThreadSubscriptionLastSeenByThreadRoomID` — replace the `FindOne` filter (line ~1789):

```go
		bson.M{"threadRoomId": threadRoomID, "userAccount": bson.M{"$not": bson.Regex{Pattern: s.botOrAdminRegex}}},
```

`ListThreadReadReceipts` — replace the `$match` stage (lines ~1258-1264) `userAccount` value:

```go
			"userAccount":  bson.M{"$ne": excludeAccount, "$not": bson.Regex{Pattern: s.botOrAdminRegex}},
```

- [ ] **Step 5: Run unit + integration tests to verify they pass**

Run: `make test SERVICE=room-service && make test-integration SERVICE=room-service`
Expected: PASS — Task 1 unit tests, the four updated read tests, and `TestMongoStore_ListReadReceipts_AdminPrefixConfig_Integration` all green.

- [ ] **Step 6: Lint**

Run: `make lint`
Expected: 0 issues (no dangling references to the removed constants).

- [ ] **Step 7: Commit**

```bash
git add room-service/store_mongo.go room-service/integration_test.go
git commit -m "feat(room-service): configurable admin prefix in read-floor/receipt queries"
```

---

## Task 3: Config wiring (`main.go` + docker-compose)

**Files:**
- Modify: `room-service/main.go`
- Modify: `room-service/deploy/docker-compose.yml`

**Interfaces:**
- Consumes: `WithAdminAcctPrefix` (Task 2).
- Produces: `config.AdminAcctPrefix` (env `ADMIN_ACCT_PREFIX`, default `p_chatadmin_`).

- [ ] **Step 1: Add the config field**

In `room-service/main.go`, in the `config` struct (after `RestrictedRoomMinMembers`, line ~43), add:

```go
	// ADMIN_ACCT_PREFIX scopes which accounts are treated as admins and excluded
	// from read floors / receipts. Empty disables the admin exclusion (bots still
	// excluded). See store_mongo.go adminAccountPatterns.
	AdminAcctPrefix string `env:"ADMIN_ACCT_PREFIX" envDefault:"p_chatadmin_"`
```

- [ ] **Step 2: Pass the option at store construction**

In `room-service/main.go`, replace line ~131:

```go
	store := NewMongoStore(db, WithAdminAcctPrefix(cfg.AdminAcctPrefix))
```

- [ ] **Step 3: Verify the service builds**

Run: `make build SERVICE=room-service`
Expected: builds cleanly.

- [ ] **Step 4: Document the env var in docker-compose**

In `room-service/deploy/docker-compose.yml`, in the `room-service.environment` list (after `MAX_BATCH_SIZE=500`, line ~22), add:

```yaml
      # Accounts matching this prefix are treated as admins and excluded from
      # read floors / receipts. Empty disables the admin exclusion (bots still
      # excluded). Default p_chatadmin_.
      - ADMIN_ACCT_PREFIX=p_chatadmin_
```

- [ ] **Step 5: Run the full room-service suite + lint**

Run: `make test SERVICE=room-service && make lint`
Expected: PASS, 0 lint issues.

- [ ] **Step 6: Commit**

```bash
git add room-service/main.go room-service/deploy/docker-compose.yml
git commit -m "feat(room-service): wire ADMIN_ACCT_PREFIX config"
```

---

## Task 4: Sync the spec, and verify no client-API drift

**Files:**
- Modify: `docs/superpowers/specs/2026-07-23-admin-acct-prefix-config-design.md`

**Interfaces:** none (docs only).

- [ ] **Step 1: Update spec §4.2 to the functional-option form**

In the spec, replace the §4.2 `NewMongoStore(db, adminAcctPrefix)` sketch with the shipped signature and note the rationale:

```markdown
### 4.2 Store construction

`NewMongoStore` gains a functional option rather than a positional arg, because
it has ~130 zero-arg call sites in tests; the variadic option keeps them
compiling and matches the repo's options idiom:

    func NewMongoStore(db *mongo.Database, opts ...Option) *MongoStore

Default prefix is `p_chatadmin_`; `main.go` overrides via
`NewMongoStore(db, WithAdminAcctPrefix(cfg.AdminAcctPrefix))`.
```

- [ ] **Step 2: Confirm no `docs/client-api.md` change is required**

Read the read-receipt RPC section of `docs/client-api.md` (§ "Read Message Receipts", around line 2063) and the `minUserLastSeenAt` field rows (lines ~912, ~3628). Confirm none document the `p_` exclusion rule as a wire contract — they describe sender-exclusion and floor semantics only. No request/response field, error case, or event changes, so no client-api edit is needed.

Run: `grep -n "p_\|ADMIN_ACCT_PREFIX" docs/client-api.md`
Expected: only unrelated `p_admin`/`p_` mentions (auth examples, mentionable, DM) — none in the read-receipt contract. If any read-receipt wire contract *did* mention the `p_` exclusion, update it; otherwise leave `docs/client-api.md` untouched.

- [ ] **Step 3: Commit**

```bash
git add docs/superpowers/specs/2026-07-23-admin-acct-prefix-config-design.md
git commit -m "docs: sync admin-prefix spec to functional-option constructor"
```

---

## Self-Review

**Spec coverage:**
- §2 goal (`ADMIN_ACCT_PREFIX`, default `p_chatadmin_`) → Task 3.
- §3 four affected queries → Task 2 Step 4.
- §4.1 config field → Task 3 Step 1. §4.2 store construction → Task 2 Step 3 (option). §4.3 regex builder → Task 1.
- §5.1/§5.2 conditional match building → Task 2 Step 4. §5.3 empty-prefix disables → Task 1 (helper) + Task 2 (conditional predicates) + Task 2 Step 1(e) empty-prefix test.
- §6 out-of-scope untouched → Global Constraints (no edits to helper.go / pkg/model).
- §7 testing: unit helper → Task 1; four integration tests extended → Task 2 Step 1(a-d); narrowing/custom/empty coverage → Task 2 Step 1(b,d,e); call-site updates → not needed (variadic option).
- §8 rollout (no schema/wire) → Task 4 Step 2. §8 docker-compose → Task 3 Step 4.

**Placeholder scan:** none — every code and test step shows full content.

**Type consistency:** `adminAccountPatterns(prefix string) (adminRegex, botOrAdminRegex string)`, `WithAdminAcctPrefix(prefix string) Option`, `Option func(*MongoStore)`, fields `adminAcctRegex`/`botOrAdminRegex`, const `defaultAdminAcctPrefix` — names/signatures identical across Tasks 1–3. Store fields set in `WithAdminAcctPrefix` and read in all four queries match.
