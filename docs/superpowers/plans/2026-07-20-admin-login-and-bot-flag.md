# Admin login via admin-service + bot-login feature flag — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** admin-frontend authenticates through admin-service directly (no portal-service, no HTTP hop to botplatform-service), and a `BOT_LOGIN_ENABLED` env var on portal-service disables bot-account logins so a future bot-devs client can take over that surface.

**Architecture:** Extract the shared `Session` type + Mongo helpers into `pkg/session` so admin-service and botplatform-service stop duplicating it. admin-service grows a `POST /v1/login` (local role + bcrypt check, session-token issue) and `POST /v1/password/change` (self-service, revokes sibling sessions). portal-service adds a `BOT_LOGIN_ENABLED` gate on bot-role logins and surfaces the flag through `/api/settings` so chat-frontend hides the `/dev-login` route when off.

**Tech Stack:** Go 1.25, MongoDB v2 driver, Gin, `pkg/pwhash` (bcrypt), `pkg/sessiontoken`, `pkg/errcode`, `github.com/caarlos0/env/v11`, React (Vite) admin-frontend + chat-frontend, vitest.

**Design spec:** `docs/superpowers/specs/2026-07-20-admin-login-and-bot-flag-design.md`
**Branch:** `claude/admin-login-bot-feature-flag-3x87ic`

---

## File map

**New files**
- `pkg/session/session.go` — Session struct, collection const, Store interface, MongoStore implementation.
- `pkg/session/session_test.go` — unit tests using a fake `Store`.
- `pkg/session/session_integration_test.go` — real Mongo integration tests (`//go:build integration`).
- `admin-service/login.go` — `HandleLogin` + `HandleChangePassword` handler methods and their request/response types.
- `admin-service/login_test.go` — table-driven unit tests for both handlers.

**Modified files**
- `pkg/model/user.go` — add `ContainsBotRole`.
- `pkg/model/user_test.go` — add tests for `ContainsBotRole`.
- `pkg/errcode/codes_admin.go` — add `AdminInvalidCredentials`, `AdminOldPasswordMismatch`.
- `pkg/errcode/codes_portal.go` — add `PortalBotLoginDisabled`.
- `botplatform-service/store.go` — drop local `session` type; `BotplatformStore` methods take `*session.Session`.
- `botplatform-service/store_mongo.go` — delegate session methods to a `session.Store` field.
- `botplatform-service/handler.go` — use `session.Session` type; wire the `session.Store` in constructor.
- `botplatform-service/handler_test.go` — update mock expectations to `session.Session`.
- `botplatform-service/integration_test.go` — construct `pkg/session.NewMongoStore` in test setup.
- `botplatform-service/main.go` — instantiate `session.NewMongoStore` and pass into store constructor.
- `admin-service/store.go` — drop `Session` struct + `FindSessionByHash`/`ListSessionsByAccount`/`DeleteSessionsByAccount`/`DeleteSession` from `AdminStore`. Handlers/middleware read them from a new `session.Store` field.
- `admin-service/store_mongo.go` — delete the migrated methods; delete the `sessions` collection field (now owned by `session.MongoStore`).
- `admin-service/store_mongo.go` — `UpdateUserPassword` no longer runs `sessions.DeleteMany` inline (login owner has moved; see Task 8 for the new revoke path).
- `admin-service/middleware.go` — swap `store.FindSessionByHash` → `sessions.FindByHash`, principal type from `Session` → `session.Session`.
- `admin-service/handler.go` — session-listing and revoke admin endpoints (`listSessions`/`revokeSession`/`revokeAllSessions`) use the new `session.Store`.
- `admin-service/handler_test.go` — update mocks to `session.Session`.
- `admin-service/routes.go` — add `POST /v1/login` and `POST /v1/password/change`.
- `admin-service/main.go` — construct `session.NewMongoStore`, pass into `Handler` and middleware.
- `admin-service/mock_store_test.go` — regenerate after `store.go` changes (`make generate SERVICE=admin-service`).
- `admin-service/integration_test.go` — end-to-end login + change-password test.
- `admin-service/deploy/docker-compose.yml` — add `SESSIONS_MAX_PER_ACCOUNT=100`.
- `portal-service/main.go` — add `BotLoginEnabled bool` to config struct.
- `portal-service/handler.go` — add flag gate in `HandleLogin`; extend `settingsResponse`.
- `portal-service/handler_test.go` — add cases for the flag.
- `admin-frontend/src/api/auth/botAuth.ts` — swap URL, trim response type.
- `admin-frontend/src/api/auth/botAuth.test.ts` — update fixture URL and shape.
- `admin-frontend/src/lib/runtimeConfig.ts` (or `.js`) — drop `PORTAL_URL` if unused elsewhere.
- `chat-frontend/src/lib/runtimeConfig.js` — surface `BOT_LOGIN_ENABLED` from `/api/settings`.
- `chat-frontend/src/App.jsx` — hide `/dev-login` when `BOT_LOGIN_ENABLED` is false.
- `chat-frontend/src/App.test.jsx` — coverage for both flag states.
- `docs/client-api.md` — new admin-service section for `POST /v1/login` + `POST /v1/password/change`; add `bot_login_disabled` reason.

---

## Task 0: Confirm branch + baseline green

- [ ] **Step 1: Verify branch**

Run:
```bash
git branch --show-current
```
Expected: `claude/admin-login-bot-feature-flag-3x87ic`

- [ ] **Step 2: Baseline lint + unit tests**

Run:
```bash
make lint
make test
```
Expected: both green. If not, stop and investigate before writing new code — a baseline red masks new regressions.

- [ ] **Step 3: Baseline integration for the services we'll touch**

Run:
```bash
make test-integration SERVICE=admin-service
make test-integration SERVICE=botplatform-service
make test-integration SERVICE=portal-service
```
Expected: all green.

---

## Task 1: `pkg/session` — extract shared Session type + Store interface

**Files:**
- Create: `pkg/session/session.go`
- Create: `pkg/session/session_test.go`

- [ ] **Step 1: Write failing tests**

Write `pkg/session/session_test.go`:

```go
package session_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/session"
)

func TestCollectionConstant(t *testing.T) {
	// Both admin-service and botplatform-service must target the same
	// collection; codify the name here so a rename can't silently drift.
	assert.Equal(t, "sessions", session.Collection)
}

func TestSession_ZeroValueIsValid(t *testing.T) {
	// Sanity: zero value must be usable (no required constructor).
	var s session.Session
	assert.Empty(t, s.ID)
	assert.Empty(t, s.Roles)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/session/...`
Expected: FAIL — `pkg/session` doesn't exist.

- [ ] **Step 3: Write minimal implementation**

Create `pkg/session/session.go`:

```go
// Package session owns the shared "one row per issued token" record used by
// both admin-service and botplatform-service. Both services write/read the
// same Mongo collection; the shape lives here so it can't drift.
package session

import "context"

// Collection is the Mongo collection name. Constant so a rename can't silently
// diverge between the two services that share the collection.
const Collection = "sessions"

// Session is the one-doc-per-token record. IDs are sessiontoken.Hash(rawToken);
// no plaintext token ever hits Mongo.
type Session struct {
	ID       string   `bson:"_id"`
	UserID   string   `bson:"userId"`
	Account  string   `bson:"account"`
	SiteID   string   `bson:"siteId"`
	Roles    []string `bson:"roles"`
	IssuedAt int64    `bson:"issuedAt"`
}

// Store is the narrow Mongo surface both services share.
type Store interface {
	Insert(ctx context.Context, s *Session) error
	FindByHash(ctx context.Context, hash string) (*Session, error)
	DeleteBeyondCap(ctx context.Context, userID string, max int) (int64, error)
	DeleteForAccountExcept(ctx context.Context, account, exceptID string) (int64, error)
	DeleteForAccount(ctx context.Context, siteID, account string) (int64, error)
	ListForAccount(ctx context.Context, siteID, account string) ([]Session, error)
	DeleteByID(ctx context.Context, siteID, account, id string) (int64, error)
	EnsureIndexes(ctx context.Context) error
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./pkg/session/...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/session/session.go pkg/session/session_test.go
git commit -m "pkg/session: extract shared Session type + Store interface

Both admin-service and botplatform-service write to the same sessions
collection but define their own local Session struct. Consolidate the
shape and collection name here so the two can't drift."
```

---

## Task 2: `pkg/session` — MongoStore implementation with integration coverage

**Files:**
- Modify: `pkg/session/session.go` (add `NewMongoStore` + implementation)
- Create: `pkg/session/session_integration_test.go`

- [ ] **Step 1: Write failing integration test**

Create `pkg/session/session_integration_test.go`:

```go
//go:build integration

package session_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/session"
	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

func newStore(t *testing.T) session.Store {
	db := testutil.MongoDB(t, "sess")
	s := session.NewMongoStore(db)
	require.NoError(t, s.EnsureIndexes(context.Background()))
	return s
}

func TestInsertAndFindByHash(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	sess := &session.Session{
		ID: "hash-a", UserID: "u1", Account: "alice", SiteID: "site-a",
		Roles: []string{"admin"}, IssuedAt: 100,
	}
	require.NoError(t, s.Insert(ctx, sess))

	got, err := s.FindByHash(ctx, "hash-a")
	require.NoError(t, err)
	assert.Equal(t, sess, got)
}

func TestFindByHash_NotFound(t *testing.T) {
	s := newStore(t)
	_, err := s.FindByHash(context.Background(), "missing")
	require.Error(t, err)
}

func TestDeleteBeyondCap_EvictsOldest(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	for i, ts := range []int64{100, 200, 300, 400, 500} {
		require.NoError(t, s.Insert(ctx, &session.Session{
			ID: string(rune('a' + i)), UserID: "u1", Account: "alice", SiteID: "site-a",
			Roles: []string{"admin"}, IssuedAt: ts,
		}))
	}

	deleted, err := s.DeleteBeyondCap(ctx, "u1", 2)
	require.NoError(t, err)
	assert.Equal(t, int64(3), deleted)

	// Only the two newest survive.
	for _, id := range []string{"a", "b", "c"} {
		_, err := s.FindByHash(ctx, id)
		require.Error(t, err, "expected %q evicted", id)
	}
	for _, id := range []string{"d", "e"} {
		_, err := s.FindByHash(ctx, id)
		require.NoError(t, err, "expected %q kept", id)
	}
}

func TestDeleteBeyondCap_NoOp_UnderCap(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	require.NoError(t, s.Insert(ctx, &session.Session{
		ID: "only", UserID: "u1", Account: "alice", SiteID: "site-a", IssuedAt: 1,
	}))
	deleted, err := s.DeleteBeyondCap(ctx, "u1", 5)
	require.NoError(t, err)
	assert.Zero(t, deleted)
}

func TestDeleteForAccountExcept(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	for _, id := range []string{"keep", "kill-1", "kill-2"} {
		require.NoError(t, s.Insert(ctx, &session.Session{
			ID: id, UserID: "u1", Account: "alice", SiteID: "site-a", IssuedAt: 1,
		}))
	}

	deleted, err := s.DeleteForAccountExcept(ctx, "alice", "keep")
	require.NoError(t, err)
	assert.Equal(t, int64(2), deleted)

	_, err = s.FindByHash(ctx, "keep")
	require.NoError(t, err)
	for _, id := range []string{"kill-1", "kill-2"} {
		_, err := s.FindByHash(ctx, id)
		require.Error(t, err)
	}
}

func TestEnsureIndexes_Idempotent(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	// second call must not error
	require.NoError(t, s.EnsureIndexes(ctx))
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test-integration SERVICE=session` (or `go test -tags=integration ./pkg/session/...` if the make target rejects the package name).
Expected: FAIL — `session.NewMongoStore` doesn't exist.

- [ ] **Step 3: Write the MongoStore implementation**

Append to `pkg/session/session.go`:

```go
import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

var ErrNotFound = errors.New("session not found")

// MongoStore implements Store against MongoDB. The DB is what the caller
// wants (production or test); collection selection is fixed at Collection.
type MongoStore struct {
	coll *mongo.Collection
}

func NewMongoStore(db *mongo.Database) *MongoStore {
	return &MongoStore{coll: db.Collection(Collection)}
}

var projection = bson.M{"_id": 1, "userId": 1, "account": 1, "siteId": 1, "roles": 1, "issuedAt": 1}

func (s *MongoStore) Insert(ctx context.Context, sess *Session) error {
	if _, err := s.coll.InsertOne(ctx, sess); err != nil {
		return fmt.Errorf("insert session: %w", err)
	}
	return nil
}

func (s *MongoStore) FindByHash(ctx context.Context, hash string) (*Session, error) {
	var out Session
	err := s.coll.FindOne(ctx, bson.M{"_id": hash},
		options.FindOne().SetProjection(projection)).Decode(&out)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("find session by hash: %w", err)
	}
	return &out, nil
}

// DeleteBeyondCap keeps the newest `max` sessions for account (by issuedAt) and
// deletes the rest. Sort orders by (issuedAt DESC, _id DESC) so ties within the
// same millisecond are broken deterministically — a concurrent login can't
// accidentally evict a session it just inserted because the tie-breaker is
// stable across the find + delete round-trips. Two round-trips only when the
// cap is exceeded.
func (s *MongoStore) DeleteBeyondCap(ctx context.Context, account string, max int) (int64, error) {
	cur, err := s.coll.Find(ctx, bson.M{"account": account},
		options.Find().
			SetProjection(bson.M{"_id": 1}).
			SetSort(bson.D{{Key: "issuedAt", Value: -1}, {Key: "_id", Value: -1}}).
			SetSkip(int64(max)),
	)
	if err != nil {
		return 0, fmt.Errorf("find over-cap sessions: %w", err)
	}
	var toDelete []struct {
		ID string `bson:"_id"`
	}
	if err := cur.All(ctx, &toDelete); err != nil {
		return 0, fmt.Errorf("decode over-cap sessions: %w", err)
	}
	if len(toDelete) == 0 {
		return 0, nil
	}
	ids := make([]string, len(toDelete))
	for i, d := range toDelete {
		ids[i] = d.ID
	}
	res, err := s.coll.DeleteMany(ctx, bson.M{"_id": bson.M{"$in": ids}})
	if err != nil {
		return 0, fmt.Errorf("delete over-cap sessions: %w", err)
	}
	return res.DeletedCount, nil
}

func (s *MongoStore) DeleteForAccountExcept(ctx context.Context, siteID, account, exceptID string) (int64, error) {
	res, err := s.coll.DeleteMany(ctx, bson.M{
		"siteId":  siteID,
		"account": account,
		"_id":     bson.M{"$ne": exceptID},
	})
	if err != nil {
		return 0, fmt.Errorf("delete sessions for account except: %w", err)
	}
	return res.DeletedCount, nil
}

func (s *MongoStore) DeleteForAccount(ctx context.Context, siteID, account string) (int64, error) {
	res, err := s.coll.DeleteMany(ctx, bson.M{"siteId": siteID, "account": account})
	if err != nil {
		return 0, fmt.Errorf("delete sessions for account: %w", err)
	}
	return res.DeletedCount, nil
}

func (s *MongoStore) ListForAccount(ctx context.Context, siteID, account string) ([]Session, error) {
	cur, err := s.coll.Find(ctx, bson.M{"siteId": siteID, "account": account},
		options.Find().
			SetProjection(projection).
			SetSort(bson.D{{Key: "issuedAt", Value: -1}}),
	)
	if err != nil {
		return nil, fmt.Errorf("list sessions for account: %w", err)
	}
	var out []Session
	if err := cur.All(ctx, &out); err != nil {
		return nil, fmt.Errorf("decode sessions: %w", err)
	}
	if out == nil {
		out = []Session{}
	}
	return out, nil
}

func (s *MongoStore) DeleteByID(ctx context.Context, siteID, account, id string) (int64, error) {
	res, err := s.coll.DeleteOne(ctx, bson.M{"_id": id, "siteId": siteID, "account": account})
	if err != nil {
		return 0, fmt.Errorf("delete session by id: %w", err)
	}
	return res.DeletedCount, nil
}

func (s *MongoStore) EnsureIndexes(ctx context.Context) error {
	// Matches botplatform's existing "userId_1_issuedAt_1" index.
	if _, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "userId", Value: 1}, {Key: "issuedAt", Value: 1}},
	}); err != nil {
		return fmt.Errorf("create sessions userId_issuedAt index: %w", err)
	}
	// Backs the ListForAccount / DeleteForAccount queries and the
	// DeleteForAccountExcept revocation.
	if _, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "siteId", Value: 1}, {Key: "account", Value: 1}},
	}); err != nil {
		return fmt.Errorf("create sessions siteId_account index: %w", err)
	}
	return nil
}
```

Move the top-of-file `package session` line so only one exists, and consolidate the imports at the top.

- [ ] **Step 4: Run integration test to verify it passes**

Run: `make test-integration SERVICE=session` (or `go test -tags=integration ./pkg/session/...`).
Expected: all cases PASS. Docker must be running.

- [ ] **Step 5: Compile check across the repo**

Run: `go build ./...`
Expected: clean. If botplatform-service now has a duplicate collection helper, that's fine — Tasks 3-4 tear it out next.

- [ ] **Step 6: Commit**

```bash
git add pkg/session/
git commit -m "pkg/session: MongoStore + integration tests

Implements Insert / FindByHash / DeleteBeyondCap / DeleteForAccountExcept /
DeleteForAccount / ListForAccount / DeleteByID / EnsureIndexes against the
shared sessions collection. Indexes match the ones both services currently
create in isolation so migrating them is a no-op at Mongo level."
```

---

## Task 3: Migrate `botplatform-service` to `pkg/session`

**Files:**
- Modify: `botplatform-service/store.go`
- Modify: `botplatform-service/store_mongo.go`
- Modify: `botplatform-service/handler.go`
- Modify: `botplatform-service/handler_test.go`
- Modify: `botplatform-service/integration_test.go`
- Modify: `botplatform-service/main.go`
- Regenerate: `botplatform-service/mock_store_test.go`

- [ ] **Step 1: Update `store.go` to drop the local `session` type**

Rewrite `botplatform-service/store.go`:

```go
package main

import (
	"context"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/session"
)

//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main

// BotplatformStore is the narrow Mongo surface botplatform-service needs.
// Session persistence lives behind session.Store, wired at construction.
type BotplatformStore interface {
	FindUserByAccount(ctx context.Context, account string) (*model.User, error)

	// Session operations are delegated to a pkg/session.Store composed in.
	InsertSession(ctx context.Context, s *session.Session) error
	FindSessionByHash(ctx context.Context, hash string) (*session.Session, error)
	DeleteSessionsBeyondCap(ctx context.Context, userID string, max int) (int64, error)

	Ping(ctx context.Context) error
}
```

- [ ] **Step 2: Update `store_mongo.go` to compose `session.Store`**

Modify `botplatform-service/store_mongo.go` (relevant excerpts — leave existing user + ping methods intact):

```go
type storeMongo struct {
	users    *mongo.Collection
	sessions session.Store
}

func newStoreMongo(db *mongo.Database) *storeMongo {
	return &storeMongo{
		users:    db.Collection("users"),
		sessions: session.NewMongoStore(db),
	}
}

func (s *storeMongo) InsertSession(ctx context.Context, sess *session.Session) error {
	return s.sessions.Insert(ctx, sess)
}

func (s *storeMongo) FindSessionByHash(ctx context.Context, hash string) (*session.Session, error) {
	return s.sessions.FindByHash(ctx, hash)
}

func (s *storeMongo) DeleteSessionsBeyondCap(ctx context.Context, userID string, max int) (int64, error) {
	return s.sessions.DeleteBeyondCap(ctx, userID, max)
}
```

Delete the previous manual `sessions` collection setup and any per-service session index code that duplicates `pkg/session.EnsureIndexes`. The `users` account-unique index and any user index remain untouched. If `EnsureIndexes(ctx)` on `storeMongo` currently sets up sessions indexes, replace those lines with `if err := s.sessions.EnsureIndexes(ctx); err != nil { return err }`.

- [ ] **Step 3: Update `handler.go` to use `session.Session`**

In `botplatform-service/handler.go` `HandleLogin`, change the local session construction to use the shared type:

```go
s := &session.Session{
	ID:       sessiontoken.Hash(raw),
	UserID:   u.ID,
	Account:  u.Account,
	SiteID:   u.SiteID,
	Roles:    roleStrs,
	IssuedAt: h.now(),
}
```

Add `"github.com/hmchangw/chat/pkg/session"` to imports; drop any reference to the deleted local `session` struct.

- [ ] **Step 4: Update `main.go` wiring**

`botplatform-service/main.go` should call `session.NewMongoStore(db).EnsureIndexes(ctx)` (or via `storeMongo.EnsureIndexes` if you consolidated it there) at startup after connecting Mongo — mirroring today's `EnsureIndexes` call site.

- [ ] **Step 5: Regenerate mocks**

Run: `make generate SERVICE=botplatform-service`
Expected: `mock_store_test.go` regenerated with `*session.Session` types.

- [ ] **Step 6: Update `handler_test.go`**

Replace every `*session` (local type) reference with `*session.Session`. Add `"github.com/hmchangw/chat/pkg/session"` to imports. Example:

```go
st.EXPECT().FindSessionByHash(gomock.Any(), hash).Return(&session.Session{
	ID: hash, UserID: "u1", Account: "alice", SiteID: "site-a",
	Roles: []string{"bot"}, IssuedAt: 100,
}, nil)
```

- [ ] **Step 7: Update `integration_test.go` if it constructs sessions inline**

Same pattern — swap `session{...}` → `session.Session{...}` with the new import.

- [ ] **Step 8: Run unit tests**

Run: `make test SERVICE=botplatform-service`
Expected: PASS with race detector. If tests reference deleted collection setup, remove those lines.

- [ ] **Step 9: Run integration tests**

Run: `make test-integration SERVICE=botplatform-service`
Expected: PASS.

- [ ] **Step 10: Repo-wide compile + lint**

Run:
```bash
go build ./...
make lint
```
Expected: both clean.

- [ ] **Step 11: Commit**

```bash
git add botplatform-service/
git commit -m "botplatform-service: migrate to pkg/session

Drops the local session struct and Mongo helpers; both are now in
pkg/session. Behavior unchanged — same collection, same indexes, same
FIFO cap semantics. Prepares the collection to be shared cleanly with
admin-service's new /v1/login route."
```

---

## Task 4: Migrate `admin-service` store to `pkg/session`

**Files:**
- Modify: `admin-service/store.go`
- Modify: `admin-service/store_mongo.go`
- Modify: `admin-service/handler.go`
- Modify: `admin-service/middleware.go`
- Modify: `admin-service/handler_test.go`
- Modify: `admin-service/middleware_test.go`
- Regenerate: `admin-service/mock_store_test.go`

- [ ] **Step 1: Update `store.go`**

Remove the local `Session` struct and the four session methods from `AdminStore`. Handlers that used them (session-listing / revoke, middleware) now depend on a `session.Store` field wired in `main.go`.

Final `AdminStore`:

```go
type AdminStore interface {
	SearchUsers(ctx context.Context, siteID, q string, page, limit int) ([]model.User, int64, error)
	GetUserByAccount(ctx context.Context, siteID, account string) (*model.User, error)
	CreateUser(ctx context.Context, u *model.User) error
	UpdateUser(ctx context.Context, siteID, account string, fields UserUpdate) error
	UpdateUserPassword(ctx context.Context, siteID, account, bcryptHash string, requireChange bool) error

	AppendAudit(ctx context.Context, e *AuditEntry) error
	ListAudit(ctx context.Context, siteID string, f AuditFilter, page, limit int) ([]AuditEntry, int64, error)

	EnsureIndexes(ctx context.Context) error
	Ping(ctx context.Context) error
}
```

Keep the file's `ErrUserNotFound` / `ErrAccountExists` sentinels. `UserUpdate` and `AuditEntry` unchanged.

- [ ] **Step 2: Update `store_mongo.go`**

Delete the following methods (now covered by `pkg/session`): `FindSessionByHash`, `ListSessionsByAccount`, `DeleteSessionsByAccount`, `DeleteSession`, and the `sessionProjection` var.

Delete the `sessions *mongo.Collection` field. In `newStoreMongo`, drop the sessions collection init.

Change `EnsureIndexes` to no longer create the sessions userId_issuedAt index (that ownership moved). Leave the users + admin_audit index calls.

Modify `UpdateUserPassword`: keep the transactional pattern, but replace `s.sessions.DeleteMany(...)` with `s.users.Database().Collection(session.Collection).DeleteMany(...)` — OR (cleaner) move revoke-on-password-reset out of the store entirely and let the handler do it via `session.Store.DeleteForAccount`. **Choose the handler-side variant** for consistency with the new change-password flow (Task 8). Update `UpdateUserPassword` to only touch users:

```go
func (s *storeMongo) UpdateUserPassword(ctx context.Context, siteID, account, bcryptHash string, requireChange bool) error {
	filter := bson.M{"account": account, "siteId": siteID}
	result, err := s.users.UpdateOne(ctx, filter,
		bson.M{"$set": bson.M{
			"services.password.bcrypt":  bcryptHash,
			"requirePasswordChange":     requireChange,
		}},
	)
	if err != nil {
		return fmt.Errorf("update user password: %w", err)
	}
	if result.MatchedCount == 0 {
		return ErrUserNotFound
	}
	return nil
}
```

Deactivation path in `UpdateUser` still needs to revoke sessions. Change that branch to also read through `session.Store` — but since the store doesn't hold that reference, the cleanest move is to lift the deactivate-revoke into the handler too. **Do that in Step 3 below.**

Delete `withTransaction` if no other caller uses it.

- [ ] **Step 3: Update `handler.go` deactivation path**

Find `updateUser` in `admin-service/handler.go` and adjust so that when the incoming patch sets `Deactivated=true`, the handler calls `sessions.DeleteForAccount(ctx, siteID, account)` after `store.UpdateUser` returns. Wire a `sessions session.Store` field into `Handler` (see Task 5 for the constructor change).

Session-management endpoints (`listSessions`, `revokeSession`, `revokeAllSessions`) also need to swap `h.store` calls for `h.sessions` — `ListForAccount`, `DeleteByID`, `DeleteForAccount` respectively.

- [ ] **Step 4: Update `middleware.go`**

Replace `store.FindSessionByHash` with `sessions.FindByHash`; swap the `Session` type for `session.Session`; add the import. `authenticate`'s signature changes:

```go
func authenticate(c *gin.Context, sessions session.Store, siteID string) (sess *session.Session, ok bool) {
	// ...
	sess, err := sessions.FindByHash(ctx, sessiontoken.Hash(tok))
	if err != nil {
		errhttp.Write(ctx, c, errcode.Unauthenticated("invalid session token",
			errcode.WithReason(errcode.AdminInvalidToken)))
		c.Abort()
		return nil, false
	}
	// site + role checks unchanged
}

func requireAdmin(sessions session.Store, siteID string) gin.HandlerFunc {
	return func(c *gin.Context) {
		sess, ok := authenticate(c, sessions, siteID)
		// ...
	}
}

func principalFrom(c *gin.Context) session.Session { ... }
```

- [ ] **Step 5: Regenerate mocks**

Run: `make generate SERVICE=admin-service`
Expected: `mock_store_test.go` no longer contains session methods.

- [ ] **Step 6: Update `handler_test.go` and `middleware_test.go`**

Tests that used `MockAdminStore.EXPECT().FindSessionByHash(...)` etc. need to construct a mock `session.Store` instead. Generate one:

Add to `pkg/session/session.go` a `//go:generate mockgen ...` line if you want a shared mock, OR (simpler for now) hand-roll a tiny fake in `admin-service` tests: a struct implementing `session.Store` with function-valued fields that record calls. Example fake (put in `admin-service/session_fake_test.go`):

```go
package main

import (
	"context"

	"github.com/hmchangw/chat/pkg/session"
)

type fakeSessionStore struct {
	InsertFn                func(ctx context.Context, s *session.Session) error
	FindByHashFn            func(ctx context.Context, hash string) (*session.Session, error)
	DeleteBeyondCapFn       func(ctx context.Context, userID string, max int) (int64, error)
	DeleteForAccountExceptFn func(ctx context.Context, account, exceptID string) (int64, error)
	DeleteForAccountFn      func(ctx context.Context, siteID, account string) (int64, error)
	ListForAccountFn        func(ctx context.Context, siteID, account string) ([]session.Session, error)
	DeleteByIDFn            func(ctx context.Context, siteID, account, id string) (int64, error)
	EnsureIndexesFn         func(ctx context.Context) error
}

func (f *fakeSessionStore) Insert(ctx context.Context, s *session.Session) error {
	return f.InsertFn(ctx, s)
}
func (f *fakeSessionStore) FindByHash(ctx context.Context, h string) (*session.Session, error) {
	return f.FindByHashFn(ctx, h)
}
func (f *fakeSessionStore) DeleteBeyondCap(ctx context.Context, u string, m int) (int64, error) {
	return f.DeleteBeyondCapFn(ctx, u, m)
}
func (f *fakeSessionStore) DeleteForAccountExcept(ctx context.Context, a, e string) (int64, error) {
	return f.DeleteForAccountExceptFn(ctx, a, e)
}
func (f *fakeSessionStore) DeleteForAccount(ctx context.Context, s, a string) (int64, error) {
	return f.DeleteForAccountFn(ctx, s, a)
}
func (f *fakeSessionStore) ListForAccount(ctx context.Context, s, a string) ([]session.Session, error) {
	return f.ListForAccountFn(ctx, s, a)
}
func (f *fakeSessionStore) DeleteByID(ctx context.Context, s, a, id string) (int64, error) {
	return f.DeleteByIDFn(ctx, s, a, id)
}
func (f *fakeSessionStore) EnsureIndexes(ctx context.Context) error {
	if f.EnsureIndexesFn == nil {
		return nil
	}
	return f.EnsureIndexesFn(ctx)
}
```

Rewrite every existing session-related test case in `handler_test.go` and `middleware_test.go` to construct a `*fakeSessionStore` with the specific `FindByHashFn` / `DeleteForAccountFn` / etc. it needs, and pass it into the router alongside the AdminStore mock.

- [ ] **Step 7: Run unit tests**

Run: `make test SERVICE=admin-service`
Expected: PASS with race detector.

- [ ] **Step 8: Commit**

```bash
git add admin-service/ pkg/session/
git commit -m "admin-service: migrate session persistence to pkg/session

Drops the local Session type and its four store methods; middleware and
session-management endpoints go through the shared session.Store now.
UpdateUserPassword and deactivate no longer revoke sessions inline —
that moves to the handler (Task 8) so login/change-password can share
one revocation call site."
```

---

## Task 5: Wire `session.Store` into `admin-service` `main.go` + `routes.go` + `Handler`

**Files:**
- Modify: `admin-service/main.go`
- Modify: `admin-service/handler.go` (Handler struct + `newHandler` signature)
- Modify: `admin-service/routes.go` (pass `session.Store` where needed)

- [ ] **Step 1: Extend `Handler`**

In `admin-service/handler.go`:

```go
type Handler struct {
	store    AdminStore
	sessions session.Store
	cfg      Config
}

func newHandler(store AdminStore, sessions session.Store, cfg Config) *Handler {
	return &Handler{store: store, sessions: sessions, cfg: cfg}
}
```

Add the `pkg/session` import.

- [ ] **Step 2: Update `main.go`**

At the site where the store is currently constructed, add:

```go
store := newStoreMongo(db)
sessionStore := session.NewMongoStore(db)

if err := store.EnsureIndexes(ctx); err != nil {
	return fmt.Errorf("ensure indexes: %w", err)
}
if err := sessionStore.EnsureIndexes(ctx); err != nil {
	return fmt.Errorf("ensure session indexes: %w", err)
}

h := newHandler(store, sessionStore, cfg)
```

Update `registerRoutes(r, h, store, sessionStore, cfg.SiteID)` to receive the new `session.Store`.

- [ ] **Step 3: Update `routes.go`**

```go
func registerRoutes(r *gin.Engine, h *Handler, sessions session.Store, siteID string) {
	r.GET("/healthz", h.healthz)
	r.GET("/readyz", h.readyz)

	admin := r.Group("/v1/admin", requireAdmin(sessions, siteID))
	admin.GET("/users", h.listUsers)
	admin.POST("/users", h.createUser)
	admin.GET("/users/:account", h.getUser)
	admin.PATCH("/users/:account", h.updateUser)
	admin.POST("/users/:account/password", h.setPassword)
	admin.GET("/sessions", h.listSessions)
	admin.DELETE("/sessions", h.revokeAllSessions)
	admin.DELETE("/sessions/:sessionId", h.revokeSession)
	admin.GET("/audit", h.listAudit)
}
```

- [ ] **Step 4: Adjust existing handler_test.go router setup**

Any test that today calls `registerRoutes(r, h, store, siteID)` becomes `registerRoutes(r, h, sessions, siteID)`. `newHandler` calls also gain the sessions arg.

- [ ] **Step 5: Run tests**

Run: `make test SERVICE=admin-service`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add admin-service/
git commit -m "admin-service: thread session.Store through Handler + routes

Prepares for the new /v1/login and /v1/password/change routes, which
need the store to insert and revoke sessions."
```

---

## Task 6: `pkg/model.ContainsBotRole`

**Files:**
- Modify: `pkg/model/user.go`
- Modify: `pkg/model/user_test.go`

- [ ] **Step 1: Write failing test**

In `pkg/model/user_test.go`:

```go
func TestContainsBotRole(t *testing.T) {
	tests := []struct {
		name  string
		roles []UserRole
		want  bool
	}{
		{"nil", nil, false},
		{"empty", []UserRole{}, false},
		{"user only", []UserRole{UserRoleUser}, false},
		{"admin only", []UserRole{UserRoleAdmin}, false},
		{"bot only", []UserRole{UserRoleBot}, true},
		{"user + bot", []UserRole{UserRoleUser, UserRoleBot}, true},
		{"admin + bot", []UserRole{UserRoleAdmin, UserRoleBot}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, ContainsBotRole(tc.roles))
		})
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/model/... -run ContainsBotRole -v`
Expected: FAIL — undefined `ContainsBotRole`.

- [ ] **Step 3: Implement**

Add to `pkg/model/user.go` beneath `HasLoginRole`:

```go
// ContainsBotRole reports whether the role slice contains the bot role.
// Used by portal-service's bot-login feature-flag gate.
func ContainsBotRole(roles []UserRole) bool {
	for _, r := range roles {
		if r == UserRoleBot {
			return true
		}
	}
	return false
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./pkg/model/... -run ContainsBotRole -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/model/user.go pkg/model/user_test.go
git commit -m "pkg/model: add ContainsBotRole helper

Used by portal-service's BOT_LOGIN_ENABLED gate."
```

---

## Task 7: New errcode reasons

**Files:**
- Modify: `pkg/errcode/codes_admin.go`
- Modify: `pkg/errcode/codes_portal.go`

- [ ] **Step 1: Extend admin reasons**

`pkg/errcode/codes_admin.go` becomes:

```go
package errcode

// Admin-service reasons. Emitted by admin-service handlers/middleware.
const (
	AdminNotAuthorized       Reason = "not_admin"              // 403: valid session, role != admin
	AdminInvalidToken        Reason = "invalid_token"          // 401: missing/unknown session token
	AdminUserNotFound        Reason = "user_not_found"         // 404
	AdminAccountExists       Reason = "account_exists"         // 409: duplicate account on create
	AdminInvalidCredentials  Reason = "invalid_credentials"    // 401: /v1/login denied (unknown / wrong password / not admin / deactivated)
	AdminOldPasswordMismatch Reason = "old_password_mismatch"  // 401: /v1/password/change oldPassword wrong
)
```

- [ ] **Step 2: Extend portal reasons**

`pkg/errcode/codes_portal.go`:

```go
package errcode

const (
	PortalAccountNotReady   Reason = "account_not_ready"
	PortalBotLoginDisabled  Reason = "bot_login_disabled"
)
```

- [ ] **Step 3: Compile + reason-test sanity**

Run: `go build ./... && go test ./pkg/errcode/...`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add pkg/errcode/
git commit -m "pkg/errcode: add admin login + portal bot-flag reasons

New reasons emitted by admin-service /v1/login,
admin-service /v1/password/change, and portal-service's
BOT_LOGIN_ENABLED gate."
```

---

## Task 8: admin-service `POST /v1/login` (handler + tests)

**Files:**
- Create: `admin-service/login.go`
- Create: `admin-service/login_test.go`
- Modify: `admin-service/config.go` — add `SessionsMaxPerAccount`.
- Modify: `admin-service/routes.go` — register the route.

- [ ] **Step 1: Extend `Config`**

`admin-service/config.go`:

```go
type Config struct {
	Port                  string `env:"PORT" envDefault:"8082"`
	SiteID                string `env:"SITE_ID,required"`
	MongoURI              string `env:"MONGO_URI,required"`
	MongoDB               string `env:"MONGO_DB" envDefault:"chat"`
	MongoUsername         string `env:"MONGO_USERNAME"`
	MongoPassword         string `env:"MONGO_PASSWORD"`
	BcryptCost            int    `env:"BCRYPT_COST" envDefault:"10"`
	SessionsMaxPerAccount int    `env:"SESSIONS_MAX_PER_ACCOUNT" envDefault:"100"`
}
```

- [ ] **Step 2: Register the route**

`admin-service/routes.go` gains one unauthenticated route:

```go
func registerRoutes(r *gin.Engine, h *Handler, sessions session.Store, siteID string) {
	r.GET("/healthz", h.healthz)
	r.GET("/readyz", h.readyz)

	r.POST("/v1/login", h.handleLogin)

	admin := r.Group("/v1/admin", requireAdmin(sessions, siteID))
	// … existing /v1/admin/* routes unchanged …
}
```

(The `/v1/password/change` route is added in Task 9.)

- [ ] **Step 3: Write failing tests**

Create `admin-service/login_test.go`:

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/pwhash"
	"github.com/hmchangw/chat/pkg/session"
)

func loginRouter(t *testing.T, adminStore AdminStore, sessions session.Store, cfg Config) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h := newHandler(adminStore, sessions, cfg)
	r.POST("/v1/login", h.handleLogin)
	return r
}

func mustHash(t *testing.T, pw string) string {
	t.Helper()
	h, err := pwhash.Hash(pw, 4) // low cost for tests
	require.NoError(t, err)
	return h
}

func postJSON(t *testing.T, r *gin.Engine, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	buf, err := json.Marshal(body)
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(buf))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func TestHandleLogin_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	st := NewMockAdminStore(ctrl)

	user := &model.User{
		ID:      "u1",
		Account: "p_alice",
		SiteID:  "site-a",
		Roles:   []model.UserRole{model.UserRoleAdmin},
		Services: model.Services{Password: model.PasswordCredentials{
			Bcrypt: mustHash(t, "correct-horse"),
		}},
	}
	st.EXPECT().GetUserByAccount(gomock.Any(), "site-a", "p_alice").Return(user, nil)

	inserted := false
	sessions := &fakeSessionStore{
		InsertFn: func(_ context.Context, s *session.Session) error {
			inserted = true
			assert.Equal(t, "u1", s.UserID)
			assert.Equal(t, "p_alice", s.Account)
			assert.Contains(t, s.Roles, string(model.UserRoleAdmin))
			return nil
		},
		DeleteBeyondCapFn: func(_ context.Context, u string, _ int) (int64, error) {
			assert.Equal(t, "u1", u)
			return 0, nil
		},
	}

	r := loginRouter(t, st, sessions, Config{SiteID: "site-a", SessionsMaxPerAccount: 100})
	w := postJSON(t, r, "/v1/login", map[string]string{"username": "p_alice", "password": "correct-horse"})

	require.Equal(t, http.StatusOK, w.Code)
	assert.True(t, inserted)
	var body struct {
		AuthToken             string `json:"authToken"`
		Account               string `json:"account"`
		SiteID                string `json:"siteId"`
		RequirePasswordChange bool   `json:"requirePasswordChange"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.NotEmpty(t, body.AuthToken)
	assert.Equal(t, "p_alice", body.Account)
	assert.Equal(t, "site-a", body.SiteID)
	assert.False(t, body.RequirePasswordChange)
}

func TestHandleLogin_InvalidCredentials_Cases(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*testing.T) (AdminStore, session.Store)
		body  map[string]string
	}{
		{
			name: "user not found",
			setup: func(t *testing.T) (AdminStore, session.Store) {
				ctrl := gomock.NewController(t)
				st := NewMockAdminStore(ctrl)
				st.EXPECT().GetUserByAccount(gomock.Any(), "site-a", "ghost").Return(nil, ErrUserNotFound)
				return st, &fakeSessionStore{}
			},
			body: map[string]string{"username": "ghost", "password": "x"},
		},
		{
			name: "not admin (bot with correct password)",
			setup: func(t *testing.T) (AdminStore, session.Store) {
				ctrl := gomock.NewController(t)
				st := NewMockAdminStore(ctrl)
				st.EXPECT().GetUserByAccount(gomock.Any(), "site-a", "bob.bot").Return(&model.User{
					ID: "u2", Account: "bob.bot", SiteID: "site-a",
					Roles: []model.UserRole{model.UserRoleBot},
					Services: model.Services{Password: model.PasswordCredentials{Bcrypt: mustHash(t, "hunter2")}},
				}, nil)
				return st, &fakeSessionStore{}
			},
			body: map[string]string{"username": "bob.bot", "password": "hunter2"},
		},
		{
			name: "wrong password",
			setup: func(t *testing.T) (AdminStore, session.Store) {
				ctrl := gomock.NewController(t)
				st := NewMockAdminStore(ctrl)
				st.EXPECT().GetUserByAccount(gomock.Any(), "site-a", "p_alice").Return(&model.User{
					ID: "u1", Account: "p_alice", SiteID: "site-a",
					Roles: []model.UserRole{model.UserRoleAdmin},
					Services: model.Services{Password: model.PasswordCredentials{Bcrypt: mustHash(t, "right")}},
				}, nil)
				return st, &fakeSessionStore{}
			},
			body: map[string]string{"username": "p_alice", "password": "wrong"},
		},
		{
			name: "deactivated admin",
			setup: func(t *testing.T) (AdminStore, session.Store) {
				ctrl := gomock.NewController(t)
				st := NewMockAdminStore(ctrl)
				st.EXPECT().GetUserByAccount(gomock.Any(), "site-a", "p_alice").Return(&model.User{
					ID: "u1", Account: "p_alice", SiteID: "site-a",
					Roles:       []model.UserRole{model.UserRoleAdmin},
					Deactivated: true,
					Services:    model.Services{Password: model.PasswordCredentials{Bcrypt: mustHash(t, "right")}},
				}, nil)
				return st, &fakeSessionStore{}
			},
			body: map[string]string{"username": "p_alice", "password": "right"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			st, sess := tc.setup(t)
			r := loginRouter(t, st, sess, Config{SiteID: "site-a", SessionsMaxPerAccount: 100})
			w := postJSON(t, r, "/v1/login", tc.body)
			require.Equal(t, http.StatusUnauthorized, w.Code)
			var env struct {
				Code   string `json:"code"`
				Reason string `json:"reason"`
			}
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &env))
			assert.Equal(t, string(errcode.CodeUnauthenticated), env.Code)
			assert.Equal(t, string(errcode.AdminInvalidCredentials), env.Reason)
		})
	}
}

func TestHandleLogin_BadRequest(t *testing.T) {
	ctrl := gomock.NewController(t)
	st := NewMockAdminStore(ctrl)
	r := loginRouter(t, st, &fakeSessionStore{}, Config{SiteID: "site-a", SessionsMaxPerAccount: 100})

	w := postJSON(t, r, "/v1/login", map[string]string{"username": ""})
	require.Equal(t, http.StatusBadRequest, w.Code)
}
```

- [ ] **Step 4: Run test to verify it fails**

Run: `make test SERVICE=admin-service`
Expected: FAIL — `handleLogin` undefined.

- [ ] **Step 5: Implement the handler**

Create `admin-service/login.go`:

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/pwhash"
	"github.com/hmchangw/chat/pkg/session"
	"github.com/hmchangw/chat/pkg/sessiontoken"
)

type loginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type loginResponse struct {
	AuthToken             string `json:"authToken"`
	Account               string `json:"account"`
	SiteID                string `json:"siteId"`
	RequirePasswordChange bool   `json:"requirePasswordChange"`
}

func (h *Handler) handleLogin(c *gin.Context) {
	ctx := errcode.WithLogValues(c.Request.Context(), "request_id", c.GetString("request_id"))

	var req loginRequest
	if err := c.ShouldBindJSON(&req); err != nil || req.Username == "" || req.Password == "" {
		errhttp.Write(ctx, c, errcode.BadRequest("username and password are required",
			errcode.WithReason(errcode.AuthMissingFields)))
		return
	}
	ctx = errcode.WithLogValues(ctx, "account", req.Username)

	u, err := h.store.GetUserByAccount(ctx, h.cfg.SiteID, req.Username)
	switch {
	case errors.Is(err, ErrUserNotFound):
		h.loginDenied(c, ctx, req.Username, "invalid_credentials")
		return
	case err != nil:
		errhttp.Write(ctx, c, fmt.Errorf("get user by account: %w", err))
		return
	}

	if !model.IsPlatformAdmin(u) {
		h.loginDenied(c, ctx, req.Username, "invalid_credentials")
		return
	}

	if !pwhash.Verify(u.Services.Password.Bcrypt, req.Password) {
		h.loginDenied(c, ctx, req.Username, "invalid_credentials")
		return
	}

	// Deactivated check after password verify — keeps timing indistinguishable
	// from wrong-password, so bots can't probe accounts.
	if u.Deactivated {
		h.loginDenied(c, ctx, req.Username, "invalid_credentials")
		return
	}

	raw, err := sessiontoken.New()
	if err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("generate token: %w", err))
		return
	}
	roles := make([]string, len(u.Roles))
	for i, r := range u.Roles {
		roles[i] = string(r)
	}
	s := &session.Session{
		ID:       sessiontoken.Hash(raw),
		UserID:   u.ID,
		Account:  u.Account,
		SiteID:   u.SiteID,
		Roles:    roles,
		IssuedAt: nowMillis(),
	}
	if err := h.sessions.Insert(ctx, s); err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("insert session: %w", err))
		return
	}

	if h.cfg.SessionsMaxPerAccount > 0 {
		if _, err := h.sessions.DeleteBeyondCap(ctx, u.ID, h.cfg.SessionsMaxPerAccount); err != nil {
			// Eviction is best-effort — log but don't fail the login.
			slog.WarnContext(ctx, "evict sessions failed", "error", err)
		}
	}

	c.Set("login_outcome", "ok")
	slog.InfoContext(ctx, "admin login ok", "account", req.Username, "userId", u.ID)
	c.JSON(http.StatusOK, loginResponse{
		AuthToken:             raw,
		Account:               u.Account,
		SiteID:                u.SiteID,
		RequirePasswordChange: u.RequirePasswordChange,
	})
}

func (h *Handler) loginDenied(c *gin.Context, ctx context.Context, account, outcome string) {
	c.Set("login_outcome", outcome)
	slog.WarnContext(ctx, "admin login denied", "account", account, "reason", outcome)
	errhttp.Write(ctx, c, errcode.Unauthenticated("invalid credentials",
		errcode.WithReason(errcode.AdminInvalidCredentials)))
}
```

Add a `nowMillis` helper if `handler.go` doesn't already have one:

```go
// nowMillis returns the current UTC time in unix milliseconds. Injected as a
// package-level indirection so tests can stub without a shim on Handler.
var nowMillis = func() int64 { return time.Now().UTC().UnixMilli() }
```

(Import `"time"`; consolidate into `handler.go` if a helper already exists there.)

- [ ] **Step 6: Run test to verify it passes**

Run: `make test SERVICE=admin-service`
Expected: PASS. If a not-admin test case's `DeleteBeyondCapFn` fires unexpectedly, that's a bug — the denied path must never insert a session.

- [ ] **Step 7: Lint**

Run: `make lint`
Expected: clean.

- [ ] **Step 8: Commit**

```bash
git add admin-service/
git commit -m "admin-service: add POST /v1/login (admin-only)

Local password verify against users, session issued into the shared
pkg/session collection, FIFO cap eviction, uniform invalid_credentials
response for user-not-found / not-admin / wrong-password / deactivated
(timing-safe). Zero HTTP calls to botplatform-service."
```

---

## Task 9: admin-service `POST /v1/password/change`

**Files:**
- Modify: `admin-service/login.go`
- Modify: `admin-service/login_test.go`
- Modify: `admin-service/routes.go`
- Modify: `admin-service/middleware.go` (context helpers)

- [ ] **Step 1: Extend the middleware context**

`middleware.go` currently stores the whole `Session` under `ctxPrincipal`. `principalFrom` already returns it. Confirm the session's `ID` field is what the change-password handler will read for the "except this ID" call — that's the token hash, which is exactly what we need.

- [ ] **Step 2: Register the route**

`admin-service/routes.go` add inside the existing `/v1/admin`-adjacent block:

```go
r.POST("/v1/password/change", requireAdmin(sessions, siteID), h.handleChangePassword)
```

(Outside the `/v1/admin` group intentionally — the URL path is `/v1/password/change`, not `/v1/admin/password/change` — matches what admin-frontend already calls.)

- [ ] **Step 3: Write failing tests**

Append to `admin-service/login_test.go`:

```go
func changePasswordRouter(t *testing.T, adminStore AdminStore, sessions session.Store, cfg Config) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h := newHandler(adminStore, sessions, cfg)
	r.POST("/v1/password/change", requireAdmin(sessions, cfg.SiteID), h.handleChangePassword)
	return r
}

// authFor returns a Bearer header for a caller-session already installed in
// `sessions` via FindByHashFn.
func authFor(sess *session.Session, sessions *fakeSessionStore) (string, string) {
	raw := "raw-token"
	hash := sessiontoken.Hash(raw)
	sess.ID = hash
	sessions.FindByHashFn = func(_ context.Context, h string) (*session.Session, error) {
		if h != hash {
			return nil, session.ErrNotFound
		}
		return sess, nil
	}
	return "Bearer " + raw, hash
}

func TestHandleChangePassword_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	st := NewMockAdminStore(ctrl)

	user := &model.User{
		ID: "u1", Account: "p_alice", SiteID: "site-a",
		Roles: []model.UserRole{model.UserRoleAdmin},
		Services: model.Services{Password: model.PasswordCredentials{
			Bcrypt: mustHash(t, "old-pw"),
		}},
	}
	st.EXPECT().GetUserByAccount(gomock.Any(), "site-a", "p_alice").Return(user, nil)
	st.EXPECT().UpdateUserPassword(gomock.Any(), "site-a", "p_alice", gomock.Any(), false).Return(nil)
	st.EXPECT().AppendAudit(gomock.Any(), gomock.Any()).Return(nil)

	sessions := &fakeSessionStore{}
	caller := &session.Session{Account: "p_alice", SiteID: "site-a", UserID: "u1",
		Roles: []string{string(model.UserRoleAdmin)}}
	authHeader, callerID := authFor(caller, sessions)

	revokedExcept := ""
	sessions.DeleteForAccountExceptFn = func(_ context.Context, acct, except string) (int64, error) {
		revokedExcept = except
		assert.Equal(t, "p_alice", acct)
		return 3, nil
	}

	r := changePasswordRouter(t, st, sessions, Config{SiteID: "site-a", SessionsMaxPerAccount: 100, BcryptCost: 4})
	body := map[string]string{"oldPassword": "old-pw", "newPassword": "new-pw"}
	buf, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/v1/password/change", bytes.NewReader(buf))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", authHeader)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusNoContent, w.Code)
	assert.Equal(t, callerID, revokedExcept, "caller's own session must be preserved")
}

func TestHandleChangePassword_OldPasswordMismatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	st := NewMockAdminStore(ctrl)
	st.EXPECT().GetUserByAccount(gomock.Any(), "site-a", "p_alice").Return(&model.User{
		ID: "u1", Account: "p_alice", SiteID: "site-a",
		Roles: []model.UserRole{model.UserRoleAdmin},
		Services: model.Services{Password: model.PasswordCredentials{Bcrypt: mustHash(t, "old-pw")}},
	}, nil)
	// no UpdateUserPassword expectation — must not fire

	sessions := &fakeSessionStore{}
	caller := &session.Session{Account: "p_alice", SiteID: "site-a", UserID: "u1",
		Roles: []string{string(model.UserRoleAdmin)}}
	authHeader, _ := authFor(caller, sessions)

	r := changePasswordRouter(t, st, sessions, Config{SiteID: "site-a", BcryptCost: 4})
	body := map[string]string{"oldPassword": "WRONG", "newPassword": "new-pw"}
	buf, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/v1/password/change", bytes.NewReader(buf))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", authHeader)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusUnauthorized, w.Code)
	var env struct{ Reason string `json:"reason"` }
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &env))
	assert.Equal(t, string(errcode.AdminOldPasswordMismatch), env.Reason)
}

func TestHandleChangePassword_MissingBearer(t *testing.T) {
	ctrl := gomock.NewController(t)
	st := NewMockAdminStore(ctrl)
	r := changePasswordRouter(t, st, &fakeSessionStore{}, Config{SiteID: "site-a", BcryptCost: 4})
	body := map[string]string{"oldPassword": "x", "newPassword": "y"}
	buf, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/v1/password/change", bytes.NewReader(buf))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusUnauthorized, w.Code)
}
```

- [ ] **Step 4: Run test to verify it fails**

Run: `make test SERVICE=admin-service`
Expected: FAIL — `handleChangePassword` undefined.

- [ ] **Step 5: Implement the handler**

Append to `admin-service/login.go`:

```go
type changePasswordRequest struct {
	OldPassword string `json:"oldPassword"`
	NewPassword string `json:"newPassword"`
}

func (h *Handler) handleChangePassword(c *gin.Context) {
	ctx := errcode.WithLogValues(c.Request.Context(), "request_id", c.GetString("request_id"))

	var req changePasswordRequest
	if err := c.ShouldBindJSON(&req); err != nil || req.OldPassword == "" || req.NewPassword == "" {
		errhttp.Write(ctx, c, errcode.BadRequest("oldPassword and newPassword are required",
			errcode.WithReason(errcode.AuthMissingFields)))
		return
	}

	caller := principalFrom(c)
	ctx = errcode.WithLogValues(ctx, "account", caller.Account)

	u, err := h.store.GetUserByAccount(ctx, caller.SiteID, caller.Account)
	if err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("get user for change-password: %w", err))
		return
	}

	if !pwhash.Verify(u.Services.Password.Bcrypt, req.OldPassword) {
		errhttp.Write(ctx, c, errcode.Unauthenticated("old password does not match",
			errcode.WithReason(errcode.AdminOldPasswordMismatch)))
		return
	}

	newHash, err := pwhash.Hash(req.NewPassword, h.cfg.BcryptCost)
	if err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("hash password: %w", err))
		return
	}

	// requireChange=false: caller has just satisfied the change-password
	// prompt, don't re-prompt them on next login.
	if err := h.store.UpdateUserPassword(ctx, caller.SiteID, caller.Account, newHash, false); err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("update user password: %w", err))
		return
	}

	// Revoke every other session for this account (admin-console AND chat-frontend,
	// since both live in the shared collection). Keep the caller's session alive.
	if _, err := h.sessions.DeleteForAccountExcept(ctx, caller.Account, caller.ID); err != nil {
		slog.WarnContext(ctx, "revoke sibling sessions failed", "error", err)
		// Best-effort — the password itself is already changed. Log and move on.
	}

	if err := h.store.AppendAudit(ctx, &AuditEntry{
		ID:            idgen.GenerateID(),
		ActorUserID:   caller.UserID,
		ActorAccount:  caller.Account,
		Action:        "password_change_self",
		TargetUserID:  caller.UserID,
		TargetAccount: caller.Account,
		SiteID:        caller.SiteID,
		Timestamp:     nowMillis(),
	}); err != nil {
		slog.WarnContext(ctx, "append audit failed", "error", err)
	}

	c.Status(http.StatusNoContent)
}
```

Add `"github.com/hmchangw/chat/pkg/idgen"` to imports.

- [ ] **Step 6: Run tests**

Run: `make test SERVICE=admin-service`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add admin-service/
git commit -m "admin-service: add POST /v1/password/change

Self-service password change for the logged-in admin. Verifies the old
password locally, updates the bcrypt hash + clears requirePasswordChange,
revokes every OTHER session for the account (both admin-console and
chat-frontend surfaces, one shared collection), and audits the change.
The caller's own session survives so they stay logged in."
```

---

## Task 10: admin-service end-to-end integration test

**Files:**
- Modify: `admin-service/integration_test.go`

- [ ] **Step 1: Write failing integration test**

Append to `admin-service/integration_test.go` (inside the existing `//go:build integration` file):

```go
func TestLoginAndChangePasswordEndToEnd(t *testing.T) {
	db := testutil.MongoDB(t, "adminlogin")
	sessions := session.NewMongoStore(db)
	require.NoError(t, sessions.EnsureIndexes(context.Background()))
	store := newStoreMongo(db)
	require.NoError(t, store.EnsureIndexes(context.Background()))

	cfg := Config{SiteID: "site-a", BcryptCost: 4, SessionsMaxPerAccount: 100}
	h := newHandler(store, sessions, cfg)

	r := gin.New()
	registerRoutes(r, h, sessions, cfg.SiteID)

	// Seed one admin
	hash, err := pwhash.Hash("s3cret", cfg.BcryptCost)
	require.NoError(t, err)
	require.NoError(t, store.CreateUser(context.Background(), &model.User{
		ID: "u-alice", Account: "p_alice", SiteID: "site-a",
		Roles: []model.UserRole{model.UserRoleAdmin},
		Services: model.Services{Password: model.PasswordCredentials{Bcrypt: hash}},
	}))

	// 1. Login
	w := postJSON(t, r, "/v1/login", map[string]string{"username": "p_alice", "password": "s3cret"})
	require.Equal(t, http.StatusOK, w.Code)
	var login loginResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &login))
	require.NotEmpty(t, login.AuthToken)

	// Sanity: session row exists
	_, err = sessions.FindByHash(context.Background(), sessiontoken.Hash(login.AuthToken))
	require.NoError(t, err)

	// 2. Seed a second session for the same account (represents a chat-frontend session)
	other := &session.Session{
		ID: "other-session-hash", UserID: "u-alice", Account: "p_alice", SiteID: "site-a",
		Roles: []string{string(model.UserRoleAdmin)}, IssuedAt: 1,
	}
	require.NoError(t, sessions.Insert(context.Background(), other))

	// 3. Change password using the login session
	body := map[string]string{"oldPassword": "s3cret", "newPassword": "newp@ss"}
	buf, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/v1/password/change", bytes.NewReader(buf))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+login.AuthToken)
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusNoContent, w.Code)

	// Caller session still valid
	_, err = sessions.FindByHash(context.Background(), sessiontoken.Hash(login.AuthToken))
	require.NoError(t, err)
	// Sibling session gone
	_, err = sessions.FindByHash(context.Background(), "other-session-hash")
	require.Error(t, err)

	// Old password no longer works
	w = postJSON(t, r, "/v1/login", map[string]string{"username": "p_alice", "password": "s3cret"})
	require.Equal(t, http.StatusUnauthorized, w.Code)

	// New password does
	w = postJSON(t, r, "/v1/login", map[string]string{"username": "p_alice", "password": "newp@ss"})
	require.Equal(t, http.StatusOK, w.Code)
}
```

Ensure `TestMain` in this file already calls `testutil.RunTests(m)` — the existing integration file should. Add any missing imports (`bytes`, `context`, `encoding/json`, `net/http`, `net/http/httptest`, `pkg/pwhash`, `pkg/session`, `pkg/sessiontoken`, `pkg/testutil`, `stretchr/testify/require`).

- [ ] **Step 2: Run test to verify it fails**

Run: `make test-integration SERVICE=admin-service`
Expected: FAIL if any wiring is off — otherwise PASS immediately, in which case the tests above weren't actually exercising the new paths. Double-check.

- [ ] **Step 3: Fix any wiring issues surfaced**

- [ ] **Step 4: Verify PASS**

Run: `make test-integration SERVICE=admin-service`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add admin-service/integration_test.go
git commit -m "admin-service: integration test for /v1/login + /v1/password/change

End-to-end against a real Mongo: login, seed a sibling session, rotate
password with the login session's token, assert the sibling is revoked
and the caller survives, old password rejected, new password accepted."
```

---

## Task 11: admin-service `deploy/docker-compose.yml`

**Files:**
- Modify: `admin-service/deploy/docker-compose.yml`

- [ ] **Step 1: Add the env var**

Add `- SESSIONS_MAX_PER_ACCOUNT=100` to the service `environment:` block alongside the existing entries.

- [ ] **Step 2: Verify compose parses**

Run: `docker compose -f admin-service/deploy/docker-compose.yml config > /dev/null`
Expected: no output (parse ok).

- [ ] **Step 3: Commit**

```bash
git add admin-service/deploy/docker-compose.yml
git commit -m "admin-service: local compose sets SESSIONS_MAX_PER_ACCOUNT=100

Matches botplatform-service's default so local dev sees the same FIFO
cap on both surfaces sharing the sessions collection."
```

---

## Task 12: portal-service `BOT_LOGIN_ENABLED` flag + settings surface

**Files:**
- Modify: `portal-service/main.go` (config struct)
- Modify: `portal-service/handler.go` (login gate + settingsResponse)
- Modify: `portal-service/handler_test.go` (new cases)

- [ ] **Step 1: Extend `config`**

`portal-service/main.go`:

```go
type config struct {
	// … existing fields …

	// BotLoginEnabled gates portal's bot-role password login. Flip to false
	// once the dedicated bot-devs client (which talks to botplatform directly)
	// ships — then bot accounts can no longer log in via chat-frontend.
	BotLoginEnabled bool `env:"BOT_LOGIN_ENABLED" envDefault:"true"`
}
```

At the site that constructs the `PortalHandler`, pass the flag through — extend `settings` (or add a new constructor arg) so `HandleLogin` and `HandleSettings` both see it.

Simplest: add a field to `settingsResponse` (which is already stored on the handler and returned by `HandleSettings`), and use `settings.BotLoginEnabled` in `HandleLogin`.

Update `settingsResponse` in `portal-service/handler.go`:

```go
type settingsResponse struct {
	APIVersion      string `json:"apiVersion"`
	OTELBaseURL     string `json:"otelBaseUrl"`
	BotLoginEnabled bool   `json:"botLoginEnabled"`
}
```

Populate `BotLoginEnabled: cfg.BotLoginEnabled` where `settings` is constructed in `main.go`.

- [ ] **Step 2: Write failing tests**

In `portal-service/handler_test.go`, add:

```go
func TestHandleLogin_BotLoginDisabled_RejectsBot(t *testing.T) {
	// ... existing test helpers assumed. Seed cache with a bot account, set
	// settings.BotLoginEnabled=false on the handler, POST /api/v1/login,
	// assert 403 with reason=bot_login_disabled AND upstream is never called.
}

func TestHandleLogin_BotLoginDisabled_AllowsAdmin(t *testing.T) {
	// Seed cache with an admin account, settings.BotLoginEnabled=false, POST
	// /api/v1/login, assert the request still reaches upstream (200 from a
	// stub) — proves the flag doesn't over-block.
}

func TestHandleSettings_ExposesBotLoginFlag(t *testing.T) {
	// Construct handler with settings.BotLoginEnabled=false, GET /api/settings,
	// assert response body contains "botLoginEnabled":false.
}
```

Adapt to the file's existing test helpers (`newTestPortalHandler` or similar — grep the file first).

- [ ] **Step 3: Run test to verify they fail**

Run: `make test SERVICE=portal-service`
Expected: FAIL.

- [ ] **Step 4: Implement the gate**

In `HandleLogin`, immediately after `!model.HasLoginRole(e.Roles)` reject:

```go
if !h.settings.BotLoginEnabled && model.ContainsBotRole(e.Roles) {
	slog.WarnContext(ctx, "bot login denied by feature flag", "account", req.Username)
	errhttp.Write(ctx, c, errcode.Forbidden("bot accounts cannot log in through this client",
		errcode.WithReason(errcode.PortalBotLoginDisabled)))
	return
}
```

(If `PortalHandler` doesn't already have a `settings` field carrying `settingsResponse`, wire one in — it's already returned by `HandleSettings`, so the field exists. Verify by grepping `h.settings`.)

- [ ] **Step 5: Run tests to verify PASS**

Run: `make test SERVICE=portal-service`
Expected: PASS.

- [ ] **Step 6: Repo-wide lint + build**

Run: `go build ./... && make lint`
Expected: clean.

- [ ] **Step 7: Commit**

```bash
git add portal-service/ pkg/errcode/codes_portal.go
git commit -m "portal-service: BOT_LOGIN_ENABLED flag on /api/v1/login

Default true. When false, portal rejects bot-role accounts with
reason=bot_login_disabled before forwarding to botplatform, so bots can
no longer log in via chat-frontend. Admins are unaffected. The flag is
also exposed on /api/settings so chat-frontend can hide the surface."
```

---

## Task 13: admin-frontend — swap `botLogin` to admin-service

**Files:**
- Modify: `admin-frontend/src/api/auth/botAuth.ts`
- Modify: `admin-frontend/src/api/auth/botAuth.test.ts`
- Modify: `admin-frontend/src/lib/runtimeConfig` (only if `PORTAL_URL` becomes unused)

- [ ] **Step 1: Update `botAuth.ts`**

```ts
import { ADMIN_SERVICE_URL } from '@/lib/runtimeConfig'
import { parseHttpEnvelopeError } from '../_transport/httpEnvelope'

export interface Bundle {
  authToken: string
  account: string
  siteId: string
  requirePasswordChange: boolean
}

/** Admin password login via admin-service. @throws {AsyncJobError} on non-2xx. */
export async function botLogin({
  username,
  password,
}: {
  username: string
  password: string
}): Promise<Bundle> {
  const resp = await fetch(`${ADMIN_SERVICE_URL}/v1/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ username, password }),
  })
  if (!resp.ok) await parseHttpEnvelopeError(resp, 'Login failed')
  return (await resp.json()) as Bundle
}

export async function changePassword({
  authToken,
  oldPassword,
  newPassword,
}: {
  authToken: string
  oldPassword: string
  newPassword: string
}): Promise<void> {
  const resp = await fetch(`${ADMIN_SERVICE_URL}/v1/password/change`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${authToken}` },
    body: JSON.stringify({ oldPassword, newPassword }),
  })
  if (!resp.ok) await parseHttpEnvelopeError(resp, 'Password change failed')
}
```

Drop the `PORTAL_URL` import if no longer used.

- [ ] **Step 2: Update `botAuth.test.ts`**

Change the login fetch URL assertion to `http://localhost:8082/v1/login` (or whatever the test `ADMIN_SERVICE_URL` mock resolves to — grep the existing test setup). Trim the response fixture to `{authToken, account, siteId, requirePasswordChange}` and assert the returned object matches. Existing `changePassword` test unchanged.

- [ ] **Step 3: Check for other `PORTAL_URL` consumers**

Run: `grep -rn "PORTAL_URL" admin-frontend/src`
Expected: no other hits after step 1. If any remain, keep the env var; otherwise remove it from `runtimeConfig` and the runtime schema.

- [ ] **Step 4: Typecheck + tests**

Run:
```bash
cd admin-frontend && npm run typecheck && npm test
```
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add admin-frontend/
git commit -m "admin-frontend: log in via admin-service, no portal hop

Swaps botLogin's URL to ADMIN_SERVICE_URL/v1/login and trims the bundle
to what the console uses (authToken, account, siteId,
requirePasswordChange). Portal is out of the admin console's login path."
```

---

## Task 14: chat-frontend — consume `botLoginEnabled` from portal `/api/settings`

**Files:**
- Modify: `chat-frontend/src/lib/runtimeConfig.js` (or `.ts` — grep first)
- Modify: `chat-frontend/src/App.jsx`
- Modify: `chat-frontend/src/App.test.jsx`

- [ ] **Step 1: Extend `runtimeConfig`**

Grep for how existing settings (`APIVersion`, `OTELBaseURL`) are read from portal `/api/settings`. Add `BOT_LOGIN_ENABLED` alongside them, defaulting to `true` when absent (missing-flag stance: assume enabled to avoid a silent lockout during rollout).

Example (adapt to existing shape):

```js
export const BOT_LOGIN_ENABLED = settings?.botLoginEnabled ?? true
```

- [ ] **Step 2: Gate the route in `App.jsx`**

Replace the existing `/dev-login` branch:

```jsx
import { BOT_LOGIN_ENABLED } from '@/lib/runtimeConfig'
// …
if (!connected && pathname === '/dev-login') {
  if (!BOT_LOGIN_ENABLED) {
    window.history.replaceState({}, '', '/')
    return <LoginPage />
  }
  return <BotLoginPage />
}
```

- [ ] **Step 3: Write failing tests**

Add two cases in `chat-frontend/src/App.test.jsx`:

```jsx
import { vi } from 'vitest'

vi.mock('@/lib/runtimeConfig', async () => {
  const actual = await vi.importActual('@/lib/runtimeConfig')
  return { ...actual, BOT_LOGIN_ENABLED: false } // per-test override — see below
})

// Use vi.doMock in each test to flip the flag per case, since the module value
// is snapshotted at import. See existing App.test.jsx for the pattern used.

test('renders LoginPage when BOT_LOGIN_ENABLED=false and path is /dev-login', () => {
  // window.location.pathname = '/dev-login'
  // render(<App />)
  // expect(screen.getByRole('heading', { name: /sign in with SSO/i })).toBeInTheDocument()
  // expect(screen.queryByRole('heading', { name: /bot/i })).not.toBeInTheDocument()
})

test('renders BotLoginPage when BOT_LOGIN_ENABLED=true and path is /dev-login', () => {
  // window.location.pathname = '/dev-login'
  // render(<App />)
  // expect(screen.getByRole('heading', { name: /bot/i })).toBeInTheDocument()
})
```

Adapt the mock strategy to how the existing test file handles NatsProvider and runtimeConfig — grep the current file for patterns.

- [ ] **Step 4: Run tests to verify they fail**

Run: `cd chat-frontend && npm test -- App.test.jsx`
Expected: FAIL on the new cases.

- [ ] **Step 5: Verify the App.jsx change makes them pass**

Run: `cd chat-frontend && npm test -- App.test.jsx`
Expected: PASS.

- [ ] **Step 6: Full frontend gates**

Run:
```bash
cd chat-frontend && npm run typecheck && npm test
```
Expected: clean.

- [ ] **Step 7: Commit**

```bash
git add chat-frontend/
git commit -m "chat-frontend: hide /dev-login when portal reports botLoginEnabled=false

Reads the new botLoginEnabled from /api/settings; defaults to true when
absent (rollout-safe). Backend enforces regardless — this is UI-only so
the app doesn't advertise a disabled surface."
```

---

## Task 15: `docs/client-api.md` update

**Files:**
- Modify: `docs/client-api.md`

- [ ] **Step 1: Add admin-service section**

Find where the client-facing HTTP endpoints are documented today (portal, auth-service). Add a new subsection for admin-service:

- `POST /v1/login` — request `{username, password}`, response `{authToken, account, siteId, requirePasswordChange}`, error surface `401 invalid_credentials`, `400 missing_fields`, `500 internal`.
- `POST /v1/password/change` — request `{oldPassword, newPassword}` with `Authorization: Bearer`, response `204 No Content`, error surface `401 old_password_mismatch`, `401 invalid_token`, `403 not_admin`, `400 missing_fields`.

Both include a JSON example on success.

- [ ] **Step 2: Add `bot_login_disabled` to §6 error reasons**

Alongside the existing portal reason `account_not_ready`, add `bot_login_disabled` — "Portal is configured to reject bot-account logins. Direct the user to the bot developer console."

- [ ] **Step 3: Sanity render**

`grep -c "PortalBotLoginDisabled\|bot_login_disabled\|/v1/login\|/v1/password/change" docs/client-api.md` should return ≥ 4.

- [ ] **Step 4: Commit**

```bash
git add docs/client-api.md
git commit -m "docs: document admin-service /v1/login + /v1/password/change

Plus new reason bot_login_disabled emitted by portal's flag gate."
```

---

## Task 16: Final gates + push

- [ ] **Step 1: Run everything**

```bash
make generate
make lint
make test
make test-integration
make sast
```
Expected: all green. `make sast` runs `gosec`, `govulncheck`, and `semgrep` per CLAUDE.md §5.

- [ ] **Step 2: Delete transient review notes if any**

```bash
ls docs/reviews/ 2>/dev/null && rm docs/reviews/*.md
```
CLAUDE.md §5 requires review reports be deleted before opening the PR.

- [ ] **Step 3: Push to the feature branch**

```bash
git push -u origin claude/admin-login-bot-feature-flag-3x87ic
```
Retry per project git rules on network failure.

- [ ] **Step 4: Stop here**

Do NOT create a PR unless explicitly asked (project-wide rule at the top of the sandbox instructions).

---

## Notes on ordering

- Tasks 1-4 land the shared package cleanly before any handler consumer moves. `botplatform-service` behavior is unchanged after Task 3 — same collection, same indexes, same FIFO cap semantics — so it can ship on its own if desired.
- Tasks 5-11 add the admin login/change-password routes. admin-frontend continues to hit portal until Task 13 flips its URL; production admins keep working throughout.
- Tasks 12 + 14 are the bot-login flag pair (backend gate + UI surface). Default `true`, so shipping both together is a no-op behavior change until an operator flips the env var later.
- Task 13 is the only step that changes admin-frontend routing. Before deploying it to production, confirm the admin-service deploy carrying Tasks 5-11 is already live at the admin-frontend's `ADMIN_SERVICE_URL`.
