# Room-Members Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate `company_room_members` CDC events into the new-stack `room_members` collection via a fifth handler in `oplog-collections-transformer` that writes target Mongo directly.

**Architecture:** Per the approved spec (`docs/superpowers/specs/2026-07-13-room-members-migration-design.md`): adopt the source `_id` as the target `_id` (deletes route by `_id`), map `org`/`individual` member types only (catch-all error-log+skip for anything else), resolve individual `member.id` to the new-stack user id via the existing `FindUserID` (Nak until seeded), copy `ts`, insert+hard-delete source semantics with defensive update handling. No connector, stream, or subject changes.

**Tech Stack:** Go 1.25, existing `oplog-collections-transformer` machinery (`pkg/migration`, `pkg/subject`, `caarlos0/env`, testify, testutil Mongo for integration).

## Global Constraints

- Worktree: `/home/user/chat-oplog-connector`, branch `claude/oplog-room-members`. All commands run there.
- Make targets only: `make fmt`, `make lint`, `make test SERVICE=data-migration/oplog-collections-transformer`, `make test-integration SERVICE=data-migration/oplog-collections-transformer` (integration needs Docker — if unavailable in the sandbox, state so; CI runs `-tags=integration`).
- TDD (CLAUDE.md §4): failing test first, watch it fail, implement, green, commit.
- Never log document bodies (log only ids/collection/op/type fields). Structured slog only.
- Spec mapping is binding: `_id` adopted from source; `member.type` gate is a **catch-all** (`org`/`individual` map; anything else → `slog.Error` + Ack + `onSkipped(ctx, "room_member_type_unmapped")`).
- Commit as the already-configured identity; end commit messages with:
  `Co-Authored-By: Claude <noreply@anthropic.com>`
- Push to `origin claude/oplog-room-members` after each task's commit (branch is the shared review surface). Do NOT open a PR.

---

### Task 1: Config — `RoomMembersCollection`

**Files:**
- Modify: `data-migration/oplog-collections-transformer/config.go` (fields ~line 36-40, trim ~line 70-75, required-map ~line 76-87)
- Test: `data-migration/oplog-collections-transformer/config_test.go`

**Interfaces:**
- Produces: `cfg.RoomMembersCollection string` (env `ROOM_MEMBERS_COLLECTION`, default `company_room_members`) — Tasks 3 and 4 reference it.

- [ ] **Step 1: Write the failing test**

Append to `data-migration/oplog-collections-transformer/config_test.go` (it already has a `setRequiredEnv(t)` helper — reuse it exactly like the neighboring tests do):

```go
func TestParseConfig_RoomMembersCollectionDefault(t *testing.T) {
	setRequiredEnv(t)
	cfg, err := parseConfig()
	require.NoError(t, err)
	assert.Equal(t, "company_room_members", cfg.RoomMembersCollection)
}

func TestParseConfig_RoomMembersCollectionBlankFails(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("ROOM_MEMBERS_COLLECTION", "   ")
	_, err := parseConfig()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ROOM_MEMBERS_COLLECTION")
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cd /home/user/chat-oplog-connector && make test SERVICE=data-migration/oplog-collections-transformer`
Expected: FAIL — compile error `cfg.RoomMembersCollection undefined`.

- [ ] **Step 3: Implement**

In `config.go`, add after the `UsersCollection` field:

```go
	RoomMembersCollection string `env:"ROOM_MEMBERS_COLLECTION" envDefault:"company_room_members"`
```

Add to the trim block (after `cfg.UsersCollection = strings.TrimSpace(cfg.UsersCollection)`):

```go
	cfg.RoomMembersCollection = strings.TrimSpace(cfg.RoomMembersCollection)
```

Add to the required-map literal:

```go
		"ROOM_MEMBERS_COLLECTION":  cfg.RoomMembersCollection,
```

- [ ] **Step 4: Run to verify pass**

Run: `make test SERVICE=data-migration/oplog-collections-transformer`
Expected: PASS.

- [ ] **Step 5: Lint, commit, push**

```bash
cd /home/user/chat-oplog-connector
make fmt && make lint
git add data-migration/oplog-collections-transformer/config.go data-migration/oplog-collections-transformer/config_test.go
git commit -m "feat(oplog-collections-transformer): ROOM_MEMBERS_COLLECTION config

Co-Authored-By: Claude <noreply@anthropic.com>"
git push origin claude/oplog-room-members
```

---

### Task 2: targetStore — `UpsertRoomMember` / `DeleteRoomMember`

**Files:**
- Modify: `data-migration/oplog-collections-transformer/targetstore.go` (constructor ~line 26, methods appended)
- Modify: `data-migration/oplog-collections-transformer/handler.go` (targetStore interface ~line 38-46)
- Modify: `data-migration/oplog-collections-transformer/handler_test.go` (`fakeTarget` ~line 42)
- Test: `data-migration/oplog-collections-transformer/integration_test.go` (append; file already has `//go:build integration`)

**Interfaces:**
- Consumes: `model.RoomMember` (`pkg/model/member.go`): `{ID string "bson _id", RoomID string "bson rid", Ts time.Time "bson ts", Member RoomMemberEntry "bson member"}`; `RoomMemberEntry{ID, Type (model.RoomMemberIndividual|model.RoomMemberOrg), Account}`.
- Produces (Task 3 depends on these exact signatures, added to the `targetStore` interface in handler.go):
  - `UpsertRoomMember(ctx context.Context, rm model.RoomMember) error`
  - `DeleteRoomMember(ctx context.Context, id string) error`

- [ ] **Step 1: Extend the interface + fake (red state)**

In `handler.go`, add to the `targetStore` interface after `FindUserID`:

```go
	// UpsertRoomMember replaces-or-inserts a migrated room-member doc keyed by its (source-adopted)
	// _id — idempotent under redelivery. DeleteRoomMember removes by _id; missing row is a no-op.
	UpsertRoomMember(ctx context.Context, rm model.RoomMember) error
	DeleteRoomMember(ctx context.Context, id string) error
```

In `handler_test.go`, extend `fakeTarget` (struct at ~line 42) with recording fields and methods:

```go
	// appended inside type fakeTarget struct { ... }:
	roomMemberUpserts []model.RoomMember
	roomMemberDeletes []string
	roomMemberErr     error
```

```go
func (f *fakeTarget) UpsertRoomMember(_ context.Context, rm model.RoomMember) error {
	if f.roomMemberErr != nil {
		return f.roomMemberErr
	}
	f.roomMemberUpserts = append(f.roomMemberUpserts, rm)
	return nil
}

func (f *fakeTarget) DeleteRoomMember(_ context.Context, id string) error {
	if f.roomMemberErr != nil {
		return f.roomMemberErr
	}
	f.roomMemberDeletes = append(f.roomMemberDeletes, id)
	return nil
}
```

- [ ] **Step 2: Run to verify failure**

Run: `make test SERVICE=data-migration/oplog-collections-transformer`
Expected: FAIL — `*mongoTargetStore` does not implement `targetStore` (missing `UpsertRoomMember`).

- [ ] **Step 3: Implement the store methods**

In `targetstore.go`: add a `roomMembers *mongo.Collection` field to `mongoTargetStore`, bind it in `NewMongoTargetStore` with `db.Collection("room_members")`, and append:

```go
// UpsertRoomMember replaces-or-inserts the migrated doc by its source-adopted _id.
// Idempotent under JetStream redelivery. room_members indexes are room-worker-owned —
// none are created here (same principle as thread_rooms above).
func (s *mongoTargetStore) UpsertRoomMember(ctx context.Context, rm model.RoomMember) error {
	_, err := s.roomMembers.ReplaceOne(ctx,
		bson.D{{Key: "_id", Value: rm.ID}}, rm, options.Replace().SetUpsert(true))
	if err != nil {
		return fmt.Errorf("upsert room member %q: %w", rm.ID, err)
	}
	return nil
}

// DeleteRoomMember removes the migrated doc by _id. A missing row (e.g. a delete for an
// entry whose type was never mapped) deletes nothing and is not an error.
func (s *mongoTargetStore) DeleteRoomMember(ctx context.Context, id string) error {
	if _, err := s.roomMembers.DeleteOne(ctx, bson.D{{Key: "_id", Value: id}}); err != nil {
		return fmt.Errorf("delete room member %q: %w", id, err)
	}
	return nil
}
```

- [ ] **Step 4: Integration test (build-verify locally; executes in CI)**

Append to `integration_test.go`:

```go
func TestMongoTargetStore_RoomMemberUpsertAndDelete(t *testing.T) {
	db := testutil.MongoDB(t, "collxform-rm")
	store := NewMongoTargetStore(db)
	ctx := context.Background()

	rm := model.RoomMember{
		ID: "legacyRandomId17ch", RoomID: "GENERAL",
		Ts:     time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
		Member: model.RoomMemberEntry{ID: "org-123", Type: model.RoomMemberOrg},
	}
	require.NoError(t, store.UpsertRoomMember(ctx, rm))
	var got model.RoomMember
	require.NoError(t, db.Collection("room_members").FindOne(ctx, bson.M{"_id": rm.ID}).Decode(&got))
	assert.Equal(t, "GENERAL", got.RoomID)
	assert.Equal(t, model.RoomMemberOrg, got.Member.Type)

	// Redelivery-idempotent: same _id replaced, still one doc.
	require.NoError(t, store.UpsertRoomMember(ctx, rm))
	n, err := db.Collection("room_members").CountDocuments(ctx, bson.M{"_id": rm.ID})
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)

	require.NoError(t, store.DeleteRoomMember(ctx, rm.ID))
	n, err = db.Collection("room_members").CountDocuments(ctx, bson.M{"_id": rm.ID})
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)

	// Delete of a never-migrated id is a no-op, not an error.
	require.NoError(t, store.DeleteRoomMember(ctx, "ghost"))
}
```

Run: `make test SERVICE=data-migration/oplog-collections-transformer && make lint`, plus
`make test-integration SERVICE=data-migration/oplog-collections-transformer` if Docker is available
(otherwise note it and rely on CI).
Expected: unit PASS, lint 0 issues, integration compiles.

- [ ] **Step 5: Commit, push**

```bash
git add data-migration/oplog-collections-transformer/targetstore.go data-migration/oplog-collections-transformer/handler.go data-migration/oplog-collections-transformer/handler_test.go data-migration/oplog-collections-transformer/integration_test.go
git commit -m "feat(oplog-collections-transformer): room_members upsert/delete on the target store

Co-Authored-By: Claude <noreply@anthropic.com>"
git push origin claude/oplog-room-members
```

---

### Task 3: The room-members handler (`roommembers.go`)

**Files:**
- Create: `data-migration/oplog-collections-transformer/roommembers.go`
- Modify: `data-migration/oplog-collections-transformer/handler.go` (handler struct ~line 48-62: add `roomMembersColl string`; `handle` switch ~line 77-92: add the case)
- Test: `data-migration/oplog-collections-transformer/roommembers_test.go` (new)

**Interfaces:**
- Consumes: Task 1's `cfg.RoomMembersCollection` (wired in Task 4); Task 2's `UpsertRoomMember`/`DeleteRoomMember`; existing `documentKeyID(ev.DocumentKey)`, `h.resolveDoc(ctx, ev)`, `h.target.FindUserID(ctx, account)`, `h.metrics.onSkipped(ctx, reason)`, `migration.ErrSkipped`/`ErrPoison`.
- Produces: `h.handleRoomMember(ctx, ev)` routed from `handle()` for `ev.Collection == h.roomMembersColl`.

- [ ] **Step 1: Write the failing tests**

Create `roommembers_test.go`:

```go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/migration"
	"github.com/hmchangw/chat/pkg/model"
)

const rmColl = "company_room_members"

// newRMHandler builds a handler wired for room-member tests. seededUsers maps account -> new-stack
// user id for FindUserID resolution (fakeTarget must expose this map — see step notes).
func newRMHandler(tgt *fakeTarget, lk migration.SourceLookup) *handler {
	lookups := map[string]migration.SourceLookup{}
	if lk != nil {
		lookups[rmColl] = lk
	}
	return &handler{
		siteID:          "site1",
		roomMembersColl: rmColl,
		target:          tgt,
		lookups:         lookups,
	}
}

func rmInsertEvent(doc string) oplogEvent {
	return oplogEvent{
		Op: "insert", Collection: rmColl,
		DocumentKey:  json.RawMessage(`{"_id":"src1"}`),
		FullDocument: json.RawMessage(doc),
	}
}

func TestHandleRoomMember_OrgInsert_Upserts(t *testing.T) {
	tgt := &fakeTarget{}
	h := newRMHandler(tgt, nil)
	ev := rmInsertEvent(`{"_id":"src1","rid":"GENERAL","member":{"type":"org","id":"org-9"},"ts":{"$date":"2026-07-01T00:00:00Z"}}`)
	require.NoError(t, h.handle(context.Background(), ev))
	require.Len(t, tgt.roomMemberUpserts, 1)
	rm := tgt.roomMemberUpserts[0]
	assert.Equal(t, "src1", rm.ID) // source _id adopted
	assert.Equal(t, "GENERAL", rm.RoomID)
	assert.Equal(t, model.RoomMemberOrg, rm.Member.Type)
	assert.Equal(t, "org-9", rm.Member.ID)
	assert.Empty(t, rm.Member.Account)
	assert.Equal(t, 2026, rm.Ts.Year())
}

func TestHandleRoomMember_IndividualInsert_ResolvesUser(t *testing.T) {
	tgt := &fakeTarget{userIDs: map[string]string{"jdoe": "newUser42"}}
	h := newRMHandler(tgt, nil)
	ev := rmInsertEvent(`{"_id":"src2","rid":"GENERAL","member":{"type":"individual","id":"legacyU1","username":"jdoe"},"ts":{"$date":"2026-07-02T00:00:00Z"}}`)
	require.NoError(t, h.handle(context.Background(), ev))
	require.Len(t, tgt.roomMemberUpserts, 1)
	rm := tgt.roomMemberUpserts[0]
	assert.Equal(t, model.RoomMemberIndividual, rm.Member.Type)
	assert.Equal(t, "newUser42", rm.Member.ID) // NEW-stack id, not legacyU1
	assert.Equal(t, "jdoe", rm.Member.Account)
}

func TestHandleRoomMember_IndividualUnseededUser_Naks(t *testing.T) {
	tgt := &fakeTarget{userIDs: map[string]string{}} // jdoe not seeded yet
	h := newRMHandler(tgt, nil)
	ev := rmInsertEvent(`{"_id":"src3","rid":"GENERAL","member":{"type":"individual","id":"legacyU1","username":"jdoe"},"ts":{"$date":"2026-07-02T00:00:00Z"}}`)
	err := h.handle(context.Background(), ev)
	require.Error(t, err) // plain error => Nak-retry until seeded
	assert.NotErrorIs(t, err, migration.ErrSkipped)
	assert.NotErrorIs(t, err, migration.ErrPoison)
	assert.Empty(t, tgt.roomMemberUpserts)
}

func TestHandleRoomMember_UnmappedTypes_SkipLoudly(t *testing.T) {
	for _, typ := range []string{"app", "user", "something_new"} {
		t.Run(typ, func(t *testing.T) {
			tgt := &fakeTarget{}
			h := newRMHandler(tgt, nil)
			ev := rmInsertEvent(`{"_id":"src4","rid":"GENERAL","member":{"type":"` + typ + `","id":"x"},"ts":{"$date":"2026-07-02T00:00:00Z"}}`)
			err := h.handle(context.Background(), ev)
			assert.ErrorIs(t, err, migration.ErrSkipped)
			assert.Empty(t, tgt.roomMemberUpserts)
		})
	}
}

func TestHandleRoomMember_Delete_DeletesByID(t *testing.T) {
	tgt := &fakeTarget{}
	h := newRMHandler(tgt, nil)
	ev := oplogEvent{Op: "delete", Collection: rmColl, DocumentKey: json.RawMessage(`{"_id":"src1"}`)}
	require.NoError(t, h.handle(context.Background(), ev))
	assert.Equal(t, []string{"src1"}, tgt.roomMemberDeletes)
}

func TestHandleRoomMember_Update_ReReadsAndUpserts(t *testing.T) {
	// Contract violation per SOURCE_DATA §7 (legacy never updates) — handled defensively.
	tgt := &fakeTarget{}
	lk := &fakeLookup{doc: []byte(`{"_id":"src5","rid":"GENERAL","member":{"type":"org","id":"org-7"},"ts":{"$date":"2026-07-03T00:00:00Z"}}`)}
	h := newRMHandler(tgt, lk)
	ev := oplogEvent{Op: "update", Collection: rmColl, DocumentKey: json.RawMessage(`{"_id":"src5"}`)}
	require.NoError(t, h.handle(context.Background(), ev))
	require.Len(t, tgt.roomMemberUpserts, 1)
	assert.Equal(t, "org-7", tgt.roomMemberUpserts[0].Member.ID)
}

func TestHandleRoomMember_TargetError_Naks(t *testing.T) {
	tgt := &fakeTarget{roomMemberErr: errors.New("mongo down")}
	h := newRMHandler(tgt, nil)
	ev := oplogEvent{Op: "delete", Collection: rmColl, DocumentKey: json.RawMessage(`{"_id":"src1"}`)}
	err := h.handle(context.Background(), ev)
	require.Error(t, err)
	assert.NotErrorIs(t, err, migration.ErrSkipped)
	assert.NotErrorIs(t, err, migration.ErrPoison)
}

func TestHandleRoomMember_MalformedDoc_Poisons(t *testing.T) {
	tgt := &fakeTarget{}
	h := newRMHandler(tgt, nil)
	ev := rmInsertEvent(`{not json`)
	err := h.handle(context.Background(), ev)
	assert.ErrorIs(t, err, migration.ErrPoison)
}
```

Notes for the implementer:
- `fakeLookup` already exists in this package's tests (grep for it; if its zero value differs, adapt the literal, not the assertions).
- `fakeTarget` needs a `userIDs map[string]string` field driving `FindUserID` if it doesn't already have one; check its existing `FindUserID` implementation first and reuse whatever seam it has — only add `userIDs` if no seam exists. The assertions above are the contract; the fake plumbing may be adapted.

- [ ] **Step 2: Run to verify failure**

Run: `make test SERVICE=data-migration/oplog-collections-transformer`
Expected: FAIL — `h.roomMembersColl undefined` / `h.handleRoomMember undefined`.

- [ ] **Step 3: Implement**

In `handler.go`: add `roomMembersColl string` to the handler struct (after `usersColl`), and in the `handle` switch add before `default:`:

```go
	case h.roomMembersColl:
		return h.handleRoomMember(ctx, ev)
```

Guard: the new case must not match when `roomMembersColl` is "" (unit tests build partial handlers). Place it AFTER the existing cases and add `if h.roomMembersColl == ""`? No — empty `ev.Collection` never occurs; a "" case arm only matches `ev.Collection == ""` which real events never carry. No extra guard needed (same as the four existing fields).

Create `roommembers.go`:

```go
package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/migration"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
)

// sourceRoomMember mirrors one legacy company_room_members doc (SOURCE_DATA §7): one doc per
// (room, member) pair, insert + hard delete only.
type sourceRoomMember struct {
	ID     string `bson:"_id"`
	RoomID string `bson:"rid"`
	Member struct {
		Type     string `bson:"type"` // org | individual | app | user (only first two are mapped)
		ID       string `bson:"id"`   // HR org id (org) or legacy user _id (individual)
		Username string `bson:"username"`
	} `bson:"member"`
	Ts time.Time `bson:"ts"`
	// federation.origin exists on source docs but is informational only — the collections lane
	// migrates every doc in this site's source DB (spec §3); nothing to stamp on the target.
}

// handleRoomMember migrates one company_room_members event into the target room_members collection
// (direct write, spec 2026-07-13). Target _id = source _id, so deletes route without a lookup and
// deletes of never-mapped (skipped-type) entries no-op naturally.
//
//nolint:gocritic // ev passed by value to mirror handle's signature; off the hot path.
func (h *handler) handleRoomMember(ctx context.Context, ev oplogEvent) error {
	if ev.Op == "delete" {
		id, err := documentKeyID(ev.DocumentKey)
		if err != nil {
			return err // poison: unaddressable delete
		}
		if derr := h.target.DeleteRoomMember(ctx, id); derr != nil {
			return fmt.Errorf("delete room member: %w", derr)
		}
		return nil
	}

	doc, skip, err := h.resolveDoc(ctx, ev)
	if err != nil {
		return err
	}
	if skip { // update whose source doc vanished before the re-read, or unknown op
		if ev.Op != "insert" && ev.Op != "replace" && ev.Op != "update" {
			h.metrics.onSkipped(ctx, "unknown_op")
		} else {
			h.metrics.onSkipped(ctx, "room_member_gone")
		}
		return migration.ErrSkipped
	}
	if ev.Op == "update" {
		// Contract violation per SOURCE_DATA §7 (legacy is insert+hard-delete only) — the
		// re-read+upsert above still converges, but say so loudly.
		slog.Warn("unexpected room-member update — legacy contract says insert/delete only; applied defensively",
			"eventId", ev.EventID, "request_id", natsutil.RequestIDFromContext(ctx))
	}

	var srm sourceRoomMember
	if uerr := bson.UnmarshalExtJSON(doc, false, &srm); uerr != nil {
		return fmt.Errorf("%w: decode room member: %v", migration.ErrPoison, uerr) //nolint:errorlint // single-%w sentinel wrap; decode err is informational
	}

	rm, mapped, merr := h.mapRoomMember(ctx, &srm)
	if merr != nil {
		return merr
	}
	if !mapped { // unmapped member.type — catch-all skip, decision in SOURCE_DATA §7 finding
		slog.Error("unmapped room-member type — skipping (revisit once app/user semantics are decided)",
			"member_type", srm.Member.Type, "rid", srm.RoomID, "eventId", ev.EventID,
			"request_id", natsutil.RequestIDFromContext(ctx))
		h.metrics.onSkipped(ctx, "room_member_type_unmapped")
		return migration.ErrSkipped
	}
	if uerr := h.target.UpsertRoomMember(ctx, rm); uerr != nil {
		return fmt.Errorf("upsert room member: %w", uerr)
	}
	return nil
}

// mapRoomMember maps a source doc to the target model. mapped=false when member.type is not one
// of the two confirmed types (org/individual) — the caller skips loudly. Individual member ids are
// resolved to the NEW-stack user id via account; unresolved ⇒ plain error (Nak until seeded),
// the thread-subs precedent.
func (h *handler) mapRoomMember(ctx context.Context, srm *sourceRoomMember) (model.RoomMember, bool, error) {
	rm := model.RoomMember{ID: srm.ID, RoomID: srm.RoomID, Ts: srm.Ts}
	switch srm.Member.Type {
	case "org":
		rm.Member = model.RoomMemberEntry{ID: srm.Member.ID, Type: model.RoomMemberOrg}
	case "individual":
		userID, found, err := h.target.FindUserID(ctx, srm.Member.Username)
		if err != nil {
			return model.RoomMember{}, false, fmt.Errorf("resolve room-member user %q: %w", srm.Member.Username, err)
		}
		if !found {
			return model.RoomMember{}, false, fmt.Errorf("room-member user %q not seeded yet — retrying", srm.Member.Username)
		}
		rm.Member = model.RoomMemberEntry{ID: userID, Type: model.RoomMemberIndividual, Account: srm.Member.Username}
	default:
		return model.RoomMember{}, false, nil
	}
	return rm, true, nil
}
```

IMPORTANT implementation check: `resolveDoc`'s `delete` arm returns skip — room-member deletes are
intercepted BEFORE `resolveDoc` (first lines above), so that arm is never hit for this collection.
Do not modify `resolveDoc`.

- [ ] **Step 4: Run to verify pass**

Run: `make test SERVICE=data-migration/oplog-collections-transformer && make lint`
Expected: all PASS, 0 issues.

- [ ] **Step 5: Commit, push**

```bash
git add data-migration/oplog-collections-transformer/roommembers.go data-migration/oplog-collections-transformer/roommembers_test.go data-migration/oplog-collections-transformer/handler.go data-migration/oplog-collections-transformer/handler_test.go
git commit -m "feat(oplog-collections-transformer): migrate company_room_members to room_members (direct write)

org/individual mapped (individual resolved to the new-stack user id via
account, Nak until seeded); any other member.type error-logged and skipped
per SOURCE_DATA §7 finding. Target _id adopts the source _id so hard
deletes route directly and skipped-type deletes no-op.

Co-Authored-By: Claude <noreply@anthropic.com>"
git push origin claude/oplog-room-members
```

---

### Task 4: Wiring — main.go, compose, end-to-end integration test

**Files:**
- Modify: `data-migration/oplog-collections-transformer/main.go` (lookups map ~line 93-98; handler literal ~line 140-152; FilterSubjects ~line 160-168)
- Modify: `data-migration/oplog-collections-transformer/deploy/docker-compose.yml` (transformer service env block)
- Test: `data-migration/oplog-collections-transformer/integration_test.go` (append)

**Interfaces:**
- Consumes: `cfg.RoomMembersCollection` (Task 1), `h.handleRoomMember` routing via `roomMembersColl` (Task 3).

- [ ] **Step 1: Wire main.go**

In the `lookups` map literal, add:

```go
		cfg.RoomMembersCollection: migration.NewMongoSourceLookup(sourceDB.Collection(cfg.RoomMembersCollection, options.Collection().SetReadPreference(rp))),
```

In the `&handler{...}` literal, add after `usersColl`:

```go
		roomMembersColl: cfg.RoomMembersCollection,
```

In the consumer `FilterSubjects` slice, add:

```go
			subject.MigrationOplog(cfg.SiteID, cfg.RoomMembersCollection, "*"),
```

- [ ] **Step 2: Compose env**

In `deploy/docker-compose.yml`, add to the transformer service's `environment:` list (defaults suffice; listed for discoverability, mirroring the other collection envs):

```yaml
      - ROOM_MEMBERS_COLLECTION=company_room_members
```

- [ ] **Step 3: End-to-end integration test**

Append to `integration_test.go` a test following the file's existing end-to-end pattern (real NATS + Mongo; reuse the file's existing helpers for stream/publish setup — read the neighboring end-to-end test first and mirror its scaffolding exactly):

```go
func TestRoomMembers_EndToEnd_InsertThenDelete(t *testing.T) {
	// Scaffold: target Mongo via testutil.MongoDB(t, "collxform-rm-e2e"); build the handler
	// directly (as other handler-level integration tests here do) with a real
	// NewMongoTargetStore and a seeded user for the individual case.
	db := testutil.MongoDB(t, "collxform-rm-e2e")
	store := NewMongoTargetStore(db)
	ctx := context.Background()

	// Seed the user the individual entry resolves against.
	_, err := db.Collection("users").InsertOne(ctx, bson.M{"_id": "newU1", "account": "jdoe"})
	require.NoError(t, err)

	h := &handler{
		siteID:          "site1",
		roomMembersColl: "company_room_members",
		target:          store,
		lookups:         map[string]migration.SourceLookup{},
	}

	ins := oplogEvent{Op: "insert", Collection: "company_room_members",
		DocumentKey:  json.RawMessage(`{"_id":"srcE2E"}`),
		FullDocument: json.RawMessage(`{"_id":"srcE2E","rid":"GENERAL","member":{"type":"individual","id":"legacyU9","username":"jdoe"},"ts":{"$date":"2026-07-04T00:00:00Z"}}`)}
	require.NoError(t, h.handle(ctx, ins))

	var got model.RoomMember
	require.NoError(t, db.Collection("room_members").FindOne(ctx, bson.M{"_id": "srcE2E"}).Decode(&got))
	assert.Equal(t, "newU1", got.Member.ID)
	assert.Equal(t, "jdoe", got.Member.Account)

	del := oplogEvent{Op: "delete", Collection: "company_room_members",
		DocumentKey: json.RawMessage(`{"_id":"srcE2E"}`)}
	require.NoError(t, h.handle(ctx, del))
	n, err := db.Collection("room_members").CountDocuments(ctx, bson.M{"_id": "srcE2E"})
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)
}
```

(Adjust the users-collection seed shape if `FindUserID` projects different field names — read `targetstore.go`'s `FindUserID` filter first and match it exactly.)

- [ ] **Step 4: Verify**

Run: `make test SERVICE=data-migration/oplog-collections-transformer && make lint`, plus
`make test-integration SERVICE=data-migration/oplog-collections-transformer` if Docker is available; otherwise verify compile via `go vet -tags=integration ./data-migration/oplog-collections-transformer/` and note CI covers execution.
Expected: PASS / 0 issues.

- [ ] **Step 5: Commit, push**

```bash
git add data-migration/oplog-collections-transformer/main.go data-migration/oplog-collections-transformer/deploy/docker-compose.yml data-migration/oplog-collections-transformer/integration_test.go
git commit -m "feat(oplog-collections-transformer): consume company_room_members (5th filter subject)

Co-Authored-By: Claude <noreply@anthropic.com>"
git push origin claude/oplog-room-members
```

---

### Task 5: Docs + full gate

**Files:**
- Modify: `data-migration/CDC_COVERAGE.md` (append rows to the coverage table + events list, matching the file's existing table format — read it first)

**Interfaces:** none.

- [ ] **Step 1: CDC_COVERAGE rows**

Append a `company_room_members` block (match the file's style; content requirements):
- insert → ✅ direct upsert into `room_members` (org/individual; `_id` adopted; individual resolved to new-stack user id, Nak until seeded)
- delete → ✅ direct delete by `_id` (hard deletes; skipped-type deletes no-op)
- update → n/a per source contract (insert+hard-delete only); handled defensively (re-read + upsert + Warn) if ever seen
- unmapped `member.type` (`app`/`user`/other) → ⚠️ error-logged + skipped (`room_member_type_unmapped`), deferred per SOURCE_DATA §7 finding

- [ ] **Step 2: Full gate**

```bash
cd /home/user/chat-oplog-connector
make fmt && make lint && make test SERVICE=data-migration/oplog-collections-transformer
make sast-gosec   # gosec must pass; govulncheck/semgrep may be proxy-blocked -> CI
```
Expected: 0 issues / PASS.

- [ ] **Step 3: Commit, push**

```bash
git add data-migration/CDC_COVERAGE.md
git commit -m "docs: CDC coverage for company_room_members migration

Co-Authored-By: Claude <noreply@anthropic.com>"
git push origin claude/oplog-room-members
```

Do NOT open a PR — the user decides when.
