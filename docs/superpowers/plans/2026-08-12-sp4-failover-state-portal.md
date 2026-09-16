# SP4-core: Operator-Driven Failover State (portal-service) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an operator-driven per-site failover state machine + ops-token-gated control surface to `portal-service`, producing the `servingTarget` (home|backup) signal SP3 will consume.

**Architecture:** A Mongo-backed `FailoverState` document per site (portal is sole writer, CAS-guarded on a `version` field). A pure state machine validates operator transitions (`healthy → failed_over → failing_back → healthy`). The control surface is three HTTP routes on a **separate internal-only listener**, gated by a dedicated ops bearer token — kept off portal's public browser-facing server. Auth is a Gin middleware seam so a later per-operator login (option 3) swaps in without touching handlers.

**Tech Stack:** Go 1.25, Gin, MongoDB (`mongo-driver/v2`), `caarlos0/env`, `pkg/errcode`, `go.uber.org/mock`, `stretchr/testify`, `testcontainers` via `pkg/testutil`.

**Spec:** `docs/superpowers/specs/2026-08-11-sp4-failover-trigger-health-detection.md`

## Global Constraints

- Go 1.25; single root `go.mod`; `portal-service` is a flat `package main` at repo root.
- Use `make` targets, never raw `go`: `make test SERVICE=portal-service`, `make test-integration SERVICE=portal-service`, `make generate SERVICE=portal-service`, `make lint`, `make build SERVICE=portal-service`.
- Errors: wrap infra failures `fmt.Errorf("doing X: %w", err)`; client-facing errors via `pkg/errcode` constructors + `errhttp.Write`; compare with `errors.Is`. Never log the ops token, never log secrets.
- Logging: `log/slog` JSON, structured key-value fields, via `slog.InfoContext`/`WarnContext` with the request context.
- All model structs get `json` **and** `bson` tags, `camelCase` except `bson:"_id"`.
- Every NATS/event-shaped struct carries `timestamp int64` set via `time.Now().UTC().UnixMilli()`. (Here the failover state carries `timestamp` + `since` the same way.)
- TDD: Red → Green → Refactor → Commit. Tests in `package main` (`_test.go`) to reach unexported types. Integration tests tagged `//go:build integration`, reuse the existing `TestMain` in `portal-service/integration_test.go`.
- Coverage ≥80% floor; ≥90% on the state machine + handler logic.
- **No `docs/client-api.md` change:** the control surface is an internal ops HTTP API, not a `chat.user.*` RPC or an auth-service route, so the client-API doc rule does not apply. (Recorded so a reviewer does not flag its absence.)

## File Structure

- **Create** `portal-service/failover.go` — domain: `FailoverStatus`, `ServingTarget`, `FailoverState` (+ `ServingTarget()` method), `FailoverAction`, `nextStatus`, `applyAction`, sentinel errors. Pure, no I/O.
- **Create** `portal-service/failover_mongo.go` — `mongoFailoverStore` implementing `FailoverStore` (Get / List / Transition with CAS).
- **Create** `portal-service/failover_middleware.go` — `bearer` + `requireOps` Gin middleware (the swappable auth seam).
- **Create** `portal-service/failover_handler.go` — `FailoverHandler` (List/Get/Post) + `registerFailoverRoutes`.
- **Create** `portal-service/failover_test.go` — unit tests for the domain state machine.
- **Create** `portal-service/failover_middleware_test.go` — unit tests for `requireOps`.
- **Create** `portal-service/failover_handler_test.go` — unit tests for the handlers (mocked `FailoverStore`).
- **Create** `portal-service/failover_integration_test.go` — Mongo CAS integration tests (`//go:build integration`).
- **Modify** `portal-service/store.go` — add the `FailoverStore` interface (picked up by the existing mockgen directive).
- **Modify** `portal-service/mock_store_test.go` — regenerated (never hand-edit).
- **Modify** `pkg/errcode/codes_portal.go` — add three failover reasons.
- **Modify** `portal-service/main.go` — config fields + start the internal control server (only when the token is set) + graceful shutdown.

**Scope note — the TTL-cached `FailoverReader` moves to SP3.** The spec's read-path cache has exactly one consumer: SP3's `resolve()`. Building it here would leave an unused function that `staticcheck` (via `make lint`) flags. So SP4 builds the *derivation* (`FailoverState.ServingTarget()`, fully tested) and the store; SP3 builds the TTL cache over the store where it is actually consumed. The control surface reads the store **directly** (operators need fresh state, not a cached value).

---

### Task 1: Domain state machine (`failover.go`)

**Files:**
- Create: `portal-service/failover.go`
- Test: `portal-service/failover_test.go`

**Interfaces:**
- Produces:
  - `type FailoverStatus string` with consts `StatusHealthy="healthy"`, `StatusFailedOver="failed_over"`, `StatusFailingBack="failing_back"`.
  - `type ServingTarget string` with consts `ServingHome="home"`, `ServingBackup="backup"`.
  - `type FailoverState struct` (fields below) with method `ServingTarget() ServingTarget`.
  - `type FailoverAction string` with consts `ActionFailover="failover"`, `ActionFailback="failback"`, `ActionComplete="complete"`, `ActionResume="resume"`.
  - `func isKnownAction(a FailoverAction) bool`
  - `func applyAction(cur FailoverState, action FailoverAction, operator, reason string, nowMs int64) (FailoverState, error)` — returns the next state or `errIllegalTransition`.
  - `var errIllegalTransition error`, `var errFailoverVersionConflict error`.

- [ ] **Step 1: Write the failing tests**

Create `portal-service/failover_test.go`:

```go
package main

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFailoverState_ServingTarget(t *testing.T) {
	tests := []struct {
		status FailoverStatus
		want   ServingTarget
	}{
		{StatusHealthy, ServingHome},
		{StatusFailedOver, ServingBackup},
		{StatusFailingBack, ServingBackup},
		{FailoverStatus("garbage"), ServingHome}, // unknown => fail-safe home
	}
	for _, tc := range tests {
		t.Run(string(tc.status), func(t *testing.T) {
			s := FailoverState{Status: tc.status}
			assert.Equal(t, tc.want, s.ServingTarget())
		})
	}
}

func TestApplyAction_LegalTransitions(t *testing.T) {
	tests := []struct {
		name   string
		from   FailoverStatus
		action FailoverAction
		want   FailoverStatus
	}{
		{"failover", StatusHealthy, ActionFailover, StatusFailedOver},
		{"failback", StatusFailedOver, ActionFailback, StatusFailingBack},
		{"complete", StatusFailingBack, ActionComplete, StatusHealthy},
		{"resume", StatusFailedOver, ActionResume, StatusHealthy},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cur := FailoverState{SiteID: "site-a", Status: tc.from, Version: 3}
			next, err := applyAction(cur, tc.action, "jane", "because", 1700)
			require.NoError(t, err)
			assert.Equal(t, tc.want, next.Status)
			assert.Equal(t, "site-a", next.SiteID)
			assert.Equal(t, int64(4), next.Version, "version increments by 1")
			assert.Equal(t, "jane", next.Operator)
			assert.Equal(t, "because", next.Reason)
			assert.Equal(t, int64(1700), next.Since)
			assert.Equal(t, int64(1700), next.Timestamp)
		})
	}
}

func TestApplyAction_IllegalTransitions(t *testing.T) {
	tests := []struct {
		name   string
		from   FailoverStatus
		action FailoverAction
	}{
		{"double failover", StatusFailedOver, ActionFailover},
		{"failback from healthy", StatusHealthy, ActionFailback},
		{"complete from healthy", StatusHealthy, ActionComplete},
		{"resume from failing_back", StatusFailingBack, ActionResume},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cur := FailoverState{SiteID: "site-a", Status: tc.from, Version: 1}
			_, err := applyAction(cur, tc.action, "jane", "because", 1700)
			assert.ErrorIs(t, err, errIllegalTransition)
		})
	}
}

func TestIsKnownAction(t *testing.T) {
	for _, a := range []FailoverAction{ActionFailover, ActionFailback, ActionComplete, ActionResume} {
		assert.True(t, isKnownAction(a))
	}
	assert.False(t, isKnownAction(FailoverAction("nope")))
	assert.False(t, isKnownAction(FailoverAction("")))
}

func TestSentinelsDistinct(t *testing.T) {
	assert.False(t, errors.Is(errIllegalTransition, errFailoverVersionConflict))
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=portal-service`
Expected: FAIL — undefined `FailoverStatus`, `applyAction`, etc.

- [ ] **Step 3: Write minimal implementation**

Create `portal-service/failover.go`:

```go
package main

import "errors"

// FailoverStatus is a site's operator-driven failover lifecycle state.
type FailoverStatus string

const (
	StatusHealthy     FailoverStatus = "healthy"
	StatusFailedOver  FailoverStatus = "failed_over"
	StatusFailingBack FailoverStatus = "failing_back"
)

// ServingTarget says where a site's users are served from right now. It is the
// entire contract SP3's routing override consumes.
type ServingTarget string

const (
	ServingHome   ServingTarget = "home"
	ServingBackup ServingTarget = "backup"
)

// FailoverAction is an operator-requested transition.
type FailoverAction string

const (
	ActionFailover FailoverAction = "failover" // healthy      -> failed_over
	ActionFailback FailoverAction = "failback" // failed_over  -> failing_back
	ActionComplete FailoverAction = "complete" // failing_back -> healthy
	ActionResume   FailoverAction = "resume"   // failed_over  -> healthy (false alarm)
)

// errIllegalTransition is returned when an action is not valid from the current
// status. errFailoverVersionConflict is returned by the store when an optimistic
// CAS loses a race. Both are matched with errors.Is at the HTTP boundary.
var (
	errIllegalTransition       = errors.New("illegal failover transition")
	errFailoverVersionConflict = errors.New("failover state version conflict")
)

// FailoverState is the sole authoritative record of a site's serving target.
// One document per site, _id = siteID. version is the optimistic-concurrency
// guard for the sole-writer CAS.
type FailoverState struct {
	SiteID    string         `json:"siteId"    bson:"_id"`
	Status    FailoverStatus `json:"status"    bson:"status"`
	Reason    string         `json:"reason"    bson:"reason"`
	Operator  string         `json:"operator"  bson:"operator"`
	Since     int64          `json:"since"     bson:"since"`
	Version   int64          `json:"version"   bson:"version"`
	Timestamp int64          `json:"timestamp" bson:"timestamp"`
}

// ServingTarget derives where this site is served from. Any status other than
// failed_over/failing_back (including an unknown value) is home — the fail-safe.
func (s FailoverState) ServingTarget() ServingTarget {
	switch s.Status {
	case StatusFailedOver, StatusFailingBack:
		return ServingBackup
	default:
		return ServingHome
	}
}

// isKnownAction reports whether a is one of the four defined actions.
func isKnownAction(a FailoverAction) bool {
	switch a {
	case ActionFailover, ActionFailback, ActionComplete, ActionResume:
		return true
	default:
		return false
	}
}

// nextStatus returns the status resulting from applying action to cur, or
// errIllegalTransition if the action is not valid from cur.
func nextStatus(cur FailoverStatus, action FailoverAction) (FailoverStatus, error) {
	switch {
	case cur == StatusHealthy && action == ActionFailover:
		return StatusFailedOver, nil
	case cur == StatusFailedOver && action == ActionFailback:
		return StatusFailingBack, nil
	case cur == StatusFailedOver && action == ActionResume:
		return StatusHealthy, nil
	case cur == StatusFailingBack && action == ActionComplete:
		return StatusHealthy, nil
	default:
		return "", errIllegalTransition
	}
}

// applyAction computes the next FailoverState for an operator action. It bumps
// version by 1 and stamps operator/reason/since/timestamp. It does not persist —
// the caller CAS-writes the result via FailoverStore.Transition.
func applyAction(cur FailoverState, action FailoverAction, operator, reason string, nowMs int64) (FailoverState, error) {
	ns, err := nextStatus(cur.Status, action)
	if err != nil {
		return FailoverState{}, err
	}
	return FailoverState{
		SiteID:    cur.SiteID,
		Status:    ns,
		Reason:    reason,
		Operator:  operator,
		Since:     nowMs,
		Version:   cur.Version + 1,
		Timestamp: nowMs,
	}, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=portal-service`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add portal-service/failover.go portal-service/failover_test.go
git commit -m "portal-service: failover state machine (domain)"
```

---

### Task 2: FailoverStore interface + Mongo CAS store

**Files:**
- Modify: `portal-service/store.go` (add `FailoverStore` interface)
- Create: `portal-service/failover_mongo.go`
- Modify: `portal-service/mock_store_test.go` (regenerated)
- Create: `portal-service/failover_integration_test.go`

**Interfaces:**
- Consumes: `FailoverState`, `errFailoverVersionConflict` (Task 1).
- Produces:
  - `type FailoverStore interface { Get(ctx, siteID) (FailoverState, error); List(ctx) ([]FailoverState, error); Transition(ctx, next FailoverState) error }`
  - `func newMongoFailoverStore(db *mongo.Database) *mongoFailoverStore`
  - `Get` returns a synthesized `{SiteID, Status: healthy, Version: 0}` when no document exists.
  - `Transition` inserts when `next.Version == 1`, else CAS-updates on `version == next.Version-1`; returns `errFailoverVersionConflict` on a lost race.

- [ ] **Step 1: Add the interface to `store.go`**

Append to `portal-service/store.go` (below the existing `DirectoryStore`; the file's existing `//go:generate mockgen -source=store.go ...` directive will regenerate the mock to include it):

```go
// FailoverStore is the sole-writer, CAS-guarded persistence for per-site
// FailoverState (portal is the split-brain fence). Get synthesizes a healthy,
// version-0 state for a site with no document, so callers treat "no doc" as
// healthy. Transition inserts the first state (version 1) or CAS-updates a
// later one, returning errFailoverVersionConflict when the stored version does
// not match next.Version-1.
type FailoverStore interface {
	Get(ctx context.Context, siteID string) (FailoverState, error)
	List(ctx context.Context) ([]FailoverState, error)
	Transition(ctx context.Context, next FailoverState) error
}
```

- [ ] **Step 2: Regenerate the mock**

Run: `make generate SERVICE=portal-service`
Expected: `portal-service/mock_store_test.go` now contains `MockFailoverStore`. Do not hand-edit it.

- [ ] **Step 3: Write the failing integration test**

Create `portal-service/failover_integration_test.go`:

```go
//go:build integration

package main

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMongoFailoverStore_GetDefaultsHealthy(t *testing.T) {
	db := testutil.MongoDB(t, "portal")
	store := newMongoFailoverStore(db)
	ctx := context.Background()

	got, err := store.Get(ctx, "site-unknown")
	require.NoError(t, err)
	assert.Equal(t, StatusHealthy, got.Status)
	assert.Equal(t, int64(0), got.Version)
	assert.Equal(t, "site-unknown", got.SiteID)
}

func TestMongoFailoverStore_TransitionInsertThenUpdate(t *testing.T) {
	db := testutil.MongoDB(t, "portal")
	store := newMongoFailoverStore(db)
	ctx := context.Background()

	// First transition inserts version 1.
	v1 := FailoverState{SiteID: "site-a", Status: StatusFailedOver, Operator: "jane", Reason: "down", Since: 100, Version: 1, Timestamp: 100}
	require.NoError(t, store.Transition(ctx, v1))

	got, err := store.Get(ctx, "site-a")
	require.NoError(t, err)
	assert.Equal(t, StatusFailedOver, got.Status)
	assert.Equal(t, int64(1), got.Version)

	// Second transition CAS-updates to version 2.
	v2 := FailoverState{SiteID: "site-a", Status: StatusFailingBack, Operator: "jane", Reason: "draining", Since: 200, Version: 2, Timestamp: 200}
	require.NoError(t, store.Transition(ctx, v2))
	got, err = store.Get(ctx, "site-a")
	require.NoError(t, err)
	assert.Equal(t, StatusFailingBack, got.Status)
	assert.Equal(t, int64(2), got.Version)
}

func TestMongoFailoverStore_TransitionStaleVersionConflicts(t *testing.T) {
	db := testutil.MongoDB(t, "portal")
	store := newMongoFailoverStore(db)
	ctx := context.Background()

	require.NoError(t, store.Transition(ctx, FailoverState{SiteID: "site-b", Status: StatusFailedOver, Version: 1, Since: 1, Timestamp: 1}))
	// Now at version 1. A second "version 1" insert (stale) must conflict.
	err := store.Transition(ctx, FailoverState{SiteID: "site-b", Status: StatusFailedOver, Version: 1, Since: 2, Timestamp: 2})
	assert.ErrorIs(t, err, errFailoverVersionConflict)

	// A version-3 update (skipping 2) finds no version-2 doc -> conflict.
	err = store.Transition(ctx, FailoverState{SiteID: "site-b", Status: StatusHealthy, Version: 3, Since: 3, Timestamp: 3})
	assert.ErrorIs(t, err, errFailoverVersionConflict)
}

func TestMongoFailoverStore_ConcurrentCASOneWinner(t *testing.T) {
	db := testutil.MongoDB(t, "portal")
	store := newMongoFailoverStore(db)
	ctx := context.Background()

	// Seed version 1.
	require.NoError(t, store.Transition(ctx, FailoverState{SiteID: "site-c", Status: StatusFailedOver, Version: 1, Since: 1, Timestamp: 1}))

	// Two goroutines both try to move version 1 -> 2. Exactly one wins.
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = store.Transition(ctx, FailoverState{SiteID: "site-c", Status: StatusHealthy, Version: 2, Since: 2, Timestamp: 2})
		}(i)
	}
	wg.Wait()

	conflicts := 0
	oks := 0
	for _, e := range errs {
		switch {
		case e == nil:
			oks++
		case assert.ErrorIs(t, e, errFailoverVersionConflict):
			conflicts++
		}
	}
	assert.Equal(t, 1, oks, "exactly one writer wins")
	assert.Equal(t, 1, conflicts, "exactly one writer conflicts")
}

func TestMongoFailoverStore_List(t *testing.T) {
	db := testutil.MongoDB(t, "portal")
	store := newMongoFailoverStore(db)
	ctx := context.Background()

	require.NoError(t, store.Transition(ctx, FailoverState{SiteID: "site-x", Status: StatusFailedOver, Version: 1, Since: 1, Timestamp: 1}))
	require.NoError(t, store.Transition(ctx, FailoverState{SiteID: "site-y", Status: StatusHealthy, Version: 1, Since: 1, Timestamp: 1}))

	all, err := store.List(ctx)
	require.NoError(t, err)
	ids := map[string]bool{}
	for _, s := range all {
		ids[s.SiteID] = true
	}
	assert.True(t, ids["site-x"])
	assert.True(t, ids["site-y"])
}
```

- [ ] **Step 4: Run integration tests to verify they fail**

Run: `make test-integration SERVICE=portal-service`
Expected: FAIL — `newMongoFailoverStore` undefined.

- [ ] **Step 5: Write the Mongo store**

Create `portal-service/failover_mongo.go`:

```go
package main

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// mongoFailoverStore persists FailoverState in the failover_states collection,
// keyed by _id = siteID, with optimistic-concurrency CAS on the version field.
type mongoFailoverStore struct {
	coll *mongo.Collection
}

func newMongoFailoverStore(db *mongo.Database) *mongoFailoverStore {
	return &mongoFailoverStore{coll: db.Collection("failover_states")}
}

// Get returns the stored state for siteID, or a synthesized healthy version-0
// state when no document exists (an unfailed site has no row).
func (s *mongoFailoverStore) Get(ctx context.Context, siteID string) (FailoverState, error) {
	var st FailoverState
	err := s.coll.FindOne(ctx, bson.D{{Key: "_id", Value: siteID}}).Decode(&st)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return FailoverState{SiteID: siteID, Status: StatusHealthy, Version: 0}, nil
	}
	if err != nil {
		return FailoverState{}, fmt.Errorf("get failover state %q: %w", siteID, err)
	}
	return st, nil
}

// List returns every stored failover state.
func (s *mongoFailoverStore) List(ctx context.Context) ([]FailoverState, error) {
	cur, err := s.coll.Find(ctx, bson.D{})
	if err != nil {
		return nil, fmt.Errorf("list failover states: %w", err)
	}
	var out []FailoverState
	if err := cur.All(ctx, &out); err != nil {
		return nil, fmt.Errorf("decode failover states: %w", err)
	}
	return out, nil
}

// Transition persists next. When next.Version == 1 it inserts the first state
// (a concurrent insert -> duplicate key -> conflict). Otherwise it CAS-updates
// the document whose stored version == next.Version-1; a non-match means a
// racing writer moved first -> conflict.
func (s *mongoFailoverStore) Transition(ctx context.Context, next FailoverState) error {
	if next.Version < 1 {
		return fmt.Errorf("transition: next version must be >= 1, got %d", next.Version)
	}
	if next.Version == 1 {
		if _, err := s.coll.InsertOne(ctx, next); err != nil {
			if mongo.IsDuplicateKeyError(err) {
				return errFailoverVersionConflict
			}
			return fmt.Errorf("insert failover state: %w", err)
		}
		return nil
	}
	res, err := s.coll.UpdateOne(ctx,
		bson.D{{Key: "_id", Value: next.SiteID}, {Key: "version", Value: next.Version - 1}},
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "status", Value: next.Status},
			{Key: "reason", Value: next.Reason},
			{Key: "operator", Value: next.Operator},
			{Key: "since", Value: next.Since},
			{Key: "version", Value: next.Version},
			{Key: "timestamp", Value: next.Timestamp},
		}}},
	)
	if err != nil {
		return fmt.Errorf("update failover state: %w", err)
	}
	if res.MatchedCount == 0 {
		return errFailoverVersionConflict
	}
	return nil
}
```

- [ ] **Step 6: Run integration tests to verify they pass**

Run: `make test-integration SERVICE=portal-service`
Expected: PASS. Also run `make test SERVICE=portal-service` to confirm the regenerated mock compiles.

- [ ] **Step 7: Commit**

```bash
git add portal-service/store.go portal-service/failover_mongo.go portal-service/mock_store_test.go portal-service/failover_integration_test.go
git commit -m "portal-service: Mongo CAS store for failover state"
```

---

### Task 3: Ops-token auth middleware (`failover_middleware.go`)

**Files:**
- Create: `portal-service/failover_middleware.go`
- Modify: `pkg/errcode/codes_portal.go` (add the unauthorized reason)
- Create: `portal-service/failover_middleware_test.go`

**Interfaces:**
- Produces: `func bearer(c *gin.Context) string`; `func requireOps(token string) gin.HandlerFunc`.
- Consumes: `errcode.PortalFailoverUnauthorized` (added this task).

- [ ] **Step 1: Add the reason to `pkg/errcode/codes_portal.go`**

Add inside the existing `const (...)` block in `pkg/errcode/codes_portal.go`:

```go
	// PortalFailoverUnauthorized: the failover control surface rejected a request whose ops bearer token was missing or wrong.
	PortalFailoverUnauthorized Reason = "failover_unauthorized"
```

- [ ] **Step 2: Write the failing test**

Create `portal-service/failover_middleware_test.go`:

```go
package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
)

func opsTestRouter(token string) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	grp := r.Group("/internal", requireOps(token))
	grp.GET("/ping", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"ok": true}) })
	return r
}

func TestRequireOps(t *testing.T) {
	tests := []struct {
		name       string
		authHeader string
		wantStatus int
	}{
		{"no header", "", http.StatusUnauthorized},
		{"non-bearer scheme", "Basic abc", http.StatusUnauthorized},
		{"wrong token", "Bearer wrong", http.StatusForbidden},
		{"correct token", "Bearer s3cr3t", http.StatusOK},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := opsTestRouter("s3cr3t")
			req := httptest.NewRequest(http.MethodGet, "/internal/ping", nil)
			if tc.authHeader != "" {
				req.Header.Set("Authorization", tc.authHeader)
			}
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)
			assert.Equal(t, tc.wantStatus, w.Code)
		})
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `make test SERVICE=portal-service`
Expected: FAIL — `requireOps` undefined.

- [ ] **Step 4: Write the middleware**

Create `portal-service/failover_middleware.go`:

```go
package main

import (
	"crypto/subtle"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
)

// bearer extracts the token from an "Authorization: Bearer <token>" header, or
// "" when absent or a different scheme.
func bearer(c *gin.Context) string {
	if after, ok := strings.CutPrefix(c.GetHeader("Authorization"), "Bearer "); ok {
		return strings.TrimSpace(after)
	}
	return ""
}

// requireOps gates the failover control surface on a shared ops bearer token.
// It is the auth seam: a later per-operator login (option 3) replaces this
// middleware without touching the handlers. Compares in constant time so a
// wrong token cannot be timing-probed. The token is never logged.
func requireOps(token string) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := c.Request.Context()
		got := bearer(c)
		if got == "" {
			errhttp.Write(ctx, c, errcode.Unauthenticated("missing ops token",
				errcode.WithReason(errcode.PortalFailoverUnauthorized)))
			c.Abort()
			return
		}
		if subtle.ConstantTimeCompare([]byte(got), []byte(token)) != 1 {
			errhttp.Write(ctx, c, errcode.Forbidden("invalid ops token",
				errcode.WithReason(errcode.PortalFailoverUnauthorized)))
			c.Abort()
			return
		}
		c.Next()
	}
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `make test SERVICE=portal-service`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add portal-service/failover_middleware.go portal-service/failover_middleware_test.go pkg/errcode/codes_portal.go
git commit -m "portal-service: ops-token auth middleware for failover control surface"
```

---

### Task 4: Control-surface handlers + routes (`failover_handler.go`)

**Files:**
- Create: `portal-service/failover_handler.go`
- Modify: `pkg/errcode/codes_portal.go` (add two transition reasons)
- Create: `portal-service/failover_handler_test.go`

**Interfaces:**
- Consumes: `FailoverStore` + `MockFailoverStore` (Task 2), `applyAction`/`isKnownAction`/`errFailoverVersionConflict` (Task 1), `requireOps` (Task 3).
- Produces:
  - `type FailoverHandler struct` with `func NewFailoverHandler(store FailoverStore) *FailoverHandler`.
  - Methods `List`, `Get`, `Post` (gin handlers).
  - `func registerFailoverRoutes(r *gin.Engine, h *FailoverHandler, opsToken string)` mounting `GET /internal/v1/failover`, `GET /internal/v1/failover/:siteId`, `POST /internal/v1/failover/:siteId` behind `requireOps`.
  - Handler exposes an overridable `now func() time.Time` field (default `time.Now`) for deterministic tests.

- [ ] **Step 1: Add the two transition reasons to `pkg/errcode/codes_portal.go`**

Add inside the same `const (...)` block:

```go
	// PortalFailoverIllegalTransition: the requested failover action is not valid from the site's current status.
	PortalFailoverIllegalTransition Reason = "failover_illegal_transition"
	// PortalFailoverVersionConflict: the failover state changed concurrently (optimistic-concurrency CAS lost); retry.
	PortalFailoverVersionConflict Reason = "failover_version_conflict"
```

- [ ] **Step 2: Write the failing tests**

Create `portal-service/failover_handler_test.go`:

```go
package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

const testOpsToken = "s3cr3t"

// newFailoverTestServer wires the handler + routes with a mocked store and a
// fixed clock, behind the real requireOps middleware.
func newFailoverTestServer(t *testing.T) (*gin.Engine, *MockFailoverStore) {
	t.Helper()
	ctrl := gomock.NewController(t)
	store := NewMockFailoverStore(ctrl)
	h := NewFailoverHandler(store)
	h.now = func() time.Time { return time.UnixMilli(1700).UTC() }
	gin.SetMode(gin.TestMode)
	r := gin.New()
	registerFailoverRoutes(r, h, testOpsToken)
	return r, store
}

func do(t *testing.T, r *gin.Engine, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var reader *strings.Reader
	if body == "" {
		reader = strings.NewReader("")
	} else {
		reader = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, reader)
	req.Header.Set("Authorization", "Bearer "+testOpsToken)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func TestFailoverHandler_List(t *testing.T) {
	r, store := newFailoverTestServer(t)
	store.EXPECT().List(gomock.Any()).Return([]FailoverState{
		{SiteID: "site-a", Status: StatusFailedOver, Version: 1},
	}, nil)

	w := do(t, r, http.MethodGet, "/internal/v1/failover", "")
	require.Equal(t, http.StatusOK, w.Code)

	var got []failoverStateResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	require.Len(t, got, 1)
	assert.Equal(t, "site-a", got[0].SiteID)
	assert.Equal(t, ServingBackup, got[0].ServingTarget)
}

func TestFailoverHandler_Get(t *testing.T) {
	r, store := newFailoverTestServer(t)
	store.EXPECT().Get(gomock.Any(), "site-a").Return(
		FailoverState{SiteID: "site-a", Status: StatusHealthy, Version: 0}, nil)

	w := do(t, r, http.MethodGet, "/internal/v1/failover/site-a", "")
	require.Equal(t, http.StatusOK, w.Code)

	var got failoverStateResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.Equal(t, StatusHealthy, got.Status)
	assert.Equal(t, ServingHome, got.ServingTarget)
}

func TestFailoverHandler_PostFailoverHappyPath(t *testing.T) {
	r, store := newFailoverTestServer(t)
	store.EXPECT().Get(gomock.Any(), "site-a").Return(
		FailoverState{SiteID: "site-a", Status: StatusHealthy, Version: 0}, nil)
	store.EXPECT().Transition(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ any, next FailoverState) error {
			assert.Equal(t, StatusFailedOver, next.Status)
			assert.Equal(t, int64(1), next.Version)
			assert.Equal(t, "jane", next.Operator)
			assert.Equal(t, "nats down", next.Reason)
			assert.Equal(t, int64(1700), next.Timestamp)
			return nil
		})

	w := do(t, r, http.MethodPost, "/internal/v1/failover/site-a",
		`{"action":"failover","operator":"jane","reason":"nats down"}`)
	require.Equal(t, http.StatusOK, w.Code)

	var got failoverStateResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.Equal(t, StatusFailedOver, got.Status)
	assert.Equal(t, ServingBackup, got.ServingTarget)
}

func TestFailoverHandler_PostIllegalTransition(t *testing.T) {
	r, store := newFailoverTestServer(t)
	store.EXPECT().Get(gomock.Any(), "site-a").Return(
		FailoverState{SiteID: "site-a", Status: StatusFailedOver, Version: 1}, nil)
	// No Transition call expected.

	w := do(t, r, http.MethodPost, "/internal/v1/failover/site-a",
		`{"action":"failover","operator":"jane","reason":"again"}`)
	assert.Equal(t, http.StatusConflict, w.Code)
	assert.Contains(t, w.Body.String(), "failover_illegal_transition")
}

func TestFailoverHandler_PostVersionConflict(t *testing.T) {
	r, store := newFailoverTestServer(t)
	store.EXPECT().Get(gomock.Any(), "site-a").Return(
		FailoverState{SiteID: "site-a", Status: StatusHealthy, Version: 0}, nil)
	store.EXPECT().Transition(gomock.Any(), gomock.Any()).Return(errFailoverVersionConflict)

	w := do(t, r, http.MethodPost, "/internal/v1/failover/site-a",
		`{"action":"failover","operator":"jane","reason":"race"}`)
	assert.Equal(t, http.StatusConflict, w.Code)
	assert.Contains(t, w.Body.String(), "failover_version_conflict")
}

func TestFailoverHandler_PostValidation(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{"missing operator", `{"action":"failover","reason":"x"}`},
		{"missing reason", `{"action":"failover","operator":"jane"}`},
		{"unknown action", `{"action":"nope","operator":"jane","reason":"x"}`},
		{"malformed json", `{`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r, _ := newFailoverTestServer(t) // no store calls expected
			w := do(t, r, http.MethodPost, "/internal/v1/failover/site-a", tc.body)
			assert.Equal(t, http.StatusBadRequest, w.Code)
		})
	}
}

func TestFailoverHandler_PostGetStoreError(t *testing.T) {
	r, store := newFailoverTestServer(t)
	store.EXPECT().Get(gomock.Any(), "site-a").Return(FailoverState{}, errors.New("mongo down"))

	w := do(t, r, http.MethodPost, "/internal/v1/failover/site-a",
		`{"action":"failover","operator":"jane","reason":"x"}`)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestFailoverHandler_Unauthorized(t *testing.T) {
	r, _ := newFailoverTestServer(t)
	// No auth header -> requireOps rejects before any handler runs.
	req := httptest.NewRequest(http.MethodGet, "/internal/v1/failover", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `make test SERVICE=portal-service`
Expected: FAIL — `NewFailoverHandler`, `failoverStateResponse`, `registerFailoverRoutes` undefined.

- [ ] **Step 4: Write the handlers**

Create `portal-service/failover_handler.go`:

```go
package main

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
)

// FailoverHandler serves the operator control surface over the internal
// listener. It reads/writes FailoverState directly (operators need fresh state,
// not a cached view). now is overridable in tests.
type FailoverHandler struct {
	store FailoverStore
	now   func() time.Time
}

func NewFailoverHandler(store FailoverStore) *FailoverHandler {
	return &FailoverHandler{store: store, now: time.Now}
}

// failoverActionRequest is the POST body: the operator-requested transition,
// who requested it, and why (both required for the audit trail).
type failoverActionRequest struct {
	Action   FailoverAction `json:"action"`
	Operator string         `json:"operator"`
	Reason   string         `json:"reason"`
}

// failoverStateResponse is the wire shape, with the derived servingTarget added.
type failoverStateResponse struct {
	SiteID        string         `json:"siteId"`
	Status        FailoverStatus `json:"status"`
	ServingTarget ServingTarget  `json:"servingTarget"`
	Reason        string         `json:"reason"`
	Operator      string         `json:"operator"`
	Since         int64          `json:"since"`
	Version       int64          `json:"version"`
	Timestamp     int64          `json:"timestamp"`
}

func toResponse(s FailoverState) failoverStateResponse {
	return failoverStateResponse{
		SiteID:        s.SiteID,
		Status:        s.Status,
		ServingTarget: s.ServingTarget(),
		Reason:        s.Reason,
		Operator:      s.Operator,
		Since:         s.Since,
		Version:       s.Version,
		Timestamp:     s.Timestamp,
	}
}

// List returns every site's failover state.
func (h *FailoverHandler) List(c *gin.Context) {
	ctx := errcode.WithLogValues(c.Request.Context(), "request_id", c.GetString("request_id"))
	states, err := h.store.List(ctx)
	if err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("list failover states: %w", err))
		return
	}
	out := make([]failoverStateResponse, 0, len(states))
	for _, s := range states {
		out = append(out, toResponse(s))
	}
	c.JSON(http.StatusOK, out)
}

// Get returns one site's failover state (a healthy default for an unknown site).
func (h *FailoverHandler) Get(c *gin.Context) {
	ctx := errcode.WithLogValues(c.Request.Context(), "request_id", c.GetString("request_id"))
	siteID := c.Param("siteId")
	st, err := h.store.Get(ctx, siteID)
	if err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("get failover state: %w", err))
		return
	}
	c.JSON(http.StatusOK, toResponse(st))
}

// Post applies an operator transition to a site's failover state.
func (h *FailoverHandler) Post(c *gin.Context) {
	ctx := errcode.WithLogValues(c.Request.Context(), "request_id", c.GetString("request_id"))
	siteID := c.Param("siteId")

	var req failoverActionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		errhttp.Write(ctx, c, errcode.BadRequest("invalid request body",
			errcode.WithReason(errcode.AuthMissingFields)))
		return
	}
	if req.Operator == "" || req.Reason == "" {
		errhttp.Write(ctx, c, errcode.BadRequest("operator and reason are required",
			errcode.WithReason(errcode.AuthMissingFields)))
		return
	}
	if !isKnownAction(req.Action) {
		errhttp.Write(ctx, c, errcode.BadRequest(fmt.Sprintf("unknown action %q", req.Action)))
		return
	}

	cur, err := h.store.Get(ctx, siteID)
	if err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("get failover state: %w", err))
		return
	}

	nowMs := h.now().UTC().UnixMilli()
	next, err := applyAction(cur, req.Action, req.Operator, req.Reason, nowMs)
	if err != nil {
		errhttp.Write(ctx, c, errcode.Conflict(
			fmt.Sprintf("action %q not allowed from status %q", req.Action, cur.Status),
			errcode.WithReason(errcode.PortalFailoverIllegalTransition)))
		return
	}

	if err := h.store.Transition(ctx, next); err != nil {
		if errors.Is(err, errFailoverVersionConflict) {
			errhttp.Write(ctx, c, errcode.Conflict("failover state changed concurrently, retry",
				errcode.WithReason(errcode.PortalFailoverVersionConflict)))
			return
		}
		errhttp.Write(ctx, c, fmt.Errorf("transition failover state: %w", err))
		return
	}

	slog.InfoContext(ctx, "failover transition",
		"siteId", siteID, "from", cur.Status, "to", next.Status,
		"operator", req.Operator, "reason", req.Reason, "version", next.Version)
	c.JSON(http.StatusOK, toResponse(next))
}

// registerFailoverRoutes mounts the control surface behind the ops-token gate.
func registerFailoverRoutes(r *gin.Engine, h *FailoverHandler, opsToken string) {
	grp := r.Group("/internal/v1/failover", requireOps(opsToken))
	grp.GET("", h.List)
	grp.GET("/:siteId", h.Get)
	grp.POST("/:siteId", h.Post)
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `make test SERVICE=portal-service`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add portal-service/failover_handler.go portal-service/failover_handler_test.go pkg/errcode/codes_portal.go
git commit -m "portal-service: failover control-surface handlers + routes"
```

---

### Task 5: Wire the internal control server into `main.go`

**Files:**
- Modify: `portal-service/main.go`

**Interfaces:**
- Consumes: `newMongoFailoverStore` (Task 2), `NewFailoverHandler` + `registerFailoverRoutes` (Task 4).
- Produces: no new exported symbols; wires config + a second `http.Server`.

- [ ] **Step 1: Add config fields**

In `portal-service/main.go`, add to the `config` struct (after the Mongo fields):

```go
	// FailoverOpsToken gates the operator failover control surface. Empty
	// disables the internal control server entirely (no control surface).
	FailoverOpsToken string `env:"FAILOVER_OPS_TOKEN" envDefault:""`
	// FailoverInternalAddr is the listen address for the internal-only control
	// surface — kept off the public browser-facing server.
	FailoverInternalAddr string `env:"FAILOVER_INTERNAL_ADDR" envDefault:":8090"`
```

- [ ] **Step 2: Start the internal server (fail-fast bind) when the token is set**

In `run()`, after `registerRoutes(r, handler)` and before building the public `srv`, add:

```go
	// Optional internal-only failover control surface. Bound on a separate
	// listener so no privileged write shares the public discovery server.
	var internalSrv *http.Server
	if cfg.FailoverOpsToken != "" {
		failoverStore := newMongoFailoverStore(mongoClient.Database(cfg.MongoDB))
		failoverHandler := NewFailoverHandler(failoverStore)

		ir := gin.New()
		ir.Use(gin.Recovery())
		ir.Use(ginutil.RequestID())
		ir.Use(ginutil.AccessLog())
		registerFailoverRoutes(ir, failoverHandler, cfg.FailoverOpsToken)

		// net.Listen up front so a bad bind fails startup, not silently later.
		ln, err := net.Listen("tcp", cfg.FailoverInternalAddr)
		if err != nil {
			return fmt.Errorf("bind failover control surface %q: %w", cfg.FailoverInternalAddr, err)
		}
		internalSrv = &http.Server{
			Handler:      ir,
			ReadTimeout:  10 * time.Second,
			WriteTimeout: 10 * time.Second,
		}
		go func() {
			slog.Info("failover control surface starting", "addr", cfg.FailoverInternalAddr)
			if serveErr := internalSrv.Serve(ln); serveErr != nil && serveErr != http.ErrServerClosed {
				slog.Error("failover control surface stopped", "error", serveErr)
			}
		}()
	} else {
		slog.Info("failover control surface disabled (FAILOVER_OPS_TOKEN unset)")
	}
```

Add `"net"` to the import block.

- [ ] **Step 3: Shut the internal server down gracefully**

In the shutdown closure (the first function passed to `shutdown.Wait`), add `internalSrv.Shutdown` before the existing `srv.Shutdown` result is returned. Replace the closure body:

```go
			func(ctx context.Context) error {
				slog.Info("shutting down portal service")
				if internalSrv != nil {
					if err := internalSrv.Shutdown(ctx); err != nil {
						slog.Error("shutdown failover control surface", "error", err)
					}
				}
				err := srv.Shutdown(ctx)
				refreshCancel()
				refreshWG.Wait()
				mongoutil.Disconnect(ctx, mongoClient)
				return err
			},
```

- [ ] **Step 4: Verify build, lint, and full test suite**

Run: `make build SERVICE=portal-service`
Expected: builds clean.

Run: `make lint`
Expected: no findings (staticcheck sees every new function used; the ops token is never logged).

Run: `make test SERVICE=portal-service && make test-integration SERVICE=portal-service`
Expected: PASS.

- [ ] **Step 5: Add local-dev env so the control surface runs in docker-compose**

In `portal-service/deploy/docker-compose.yml`, add to the portal service's `environment:` block (a dev-only token — never a real secret):

```yaml
      FAILOVER_OPS_TOKEN: "dev-failover-token"
      FAILOVER_INTERNAL_ADDR: ":8090"
```

Verify the YAML parses: `make build SERVICE=portal-service` (compose is not built here, but confirm the file is well-formed by eye — 6-space indent matching the sibling env keys).

- [ ] **Step 6: Commit**

```bash
git add portal-service/main.go portal-service/deploy/docker-compose.yml
git commit -m "portal-service: wire internal failover control server + config"
```

---

## Self-Review

**1. Spec coverage:**
- Spec §3 FailoverState fields → Task 1 (`FailoverState`) + Task 2 (persistence). ✓ (`servingTarget` derived, not stored — spec says derived.)
- Spec §4 state machine (`healthy→failed_over→failing_back→healthy`, `resume`, `complete`) → Task 1 `nextStatus`/`applyAction` + tests. ✓
- Spec §5 control surface (3 routes, internal listener, ops token, `operator`/`reason` audit, 409 on conflict, no token logged) → Tasks 3–5. ✓
- Spec §6 read path / TTL reader → **deferred to SP3** (documented in File Structure scope note; the derivation `ServingTarget()` is built + tested here). ✓ (intentional decomposition, not a gap)
- Spec §7 split-brain fence (sole writer, CAS, multi-replica) → Task 2 CAS + the concurrent-writer integration test. ✓
- Spec §8 forward-compat (middleware seam, stable contract, `operator` field) → Task 3 `requireOps` seam + Task 4 body. ✓
- Spec §9 config (`FAILOVER_OPS_TOKEN`, `FAILOVER_INTERNAL_ADDR`) → Task 5. (`FAILOVER_STATE_TTL` belongs to the deferred reader → SP3.) ✓
- Spec §10 testing (state machine, control surface, integration CAS) → Tasks 1–4 tests. ✓

**2. Placeholder scan:** No TBD/TODO; every step has runnable code or an exact command. ✓

**3. Type consistency:** `FailoverState`, `FailoverStore` (Get/List/Transition), `FailoverStatus`/`ServingTarget`/`FailoverAction` consts, `applyAction`, `errFailoverVersionConflict`, `requireOps`, `NewFailoverHandler`, `registerFailoverRoutes`, `failoverStateResponse` — names and signatures match across Tasks 1→5. The mock `MockFailoverStore` is generated from the `store.go` interface (Task 2) and consumed in Task 4. ✓

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-08-12-sp4-failover-state-portal.md`.
