//go:build integration

package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

// TestMongoDirectoryStore_ListEmployees_UsersPrimary is the regression test
// for the directory-exclusion bug: pre-fix, ListEmployees started from
// hr_employee and inner-joined users, so any account present only in users
// (every bot/admin account, since they're never HR employees) was silently
// dropped from the directory — which broke their login. The fixed pipeline
// starts from users (primary) and left-joins hr_employee (enrichment), so
// every provisioned account surfaces; only humans get the hr_employee fields.
func TestMongoDirectoryStore_ListEmployees_UsersPrimary(t *testing.T) {
	db := testutil.MongoDB(t, "portal")
	store := newMongoDirectoryStore(db)
	ctx := context.Background()

	// hr_employee: alice/bob are humans with a matching users row on the same
	// site and get enriched; carol has an hr_employee row but NO matching
	// users row at all (hr_employee-only) and must NOT surface now that users
	// drives the directory; eve's hr_employee row is on a different site than
	// her users row, so the left-join must not match it.
	for _, doc := range []bson.M{
		{"account": "alice", "employeeId": "E001", "siteId": "site-a"},
		{"account": "bob", "employeeId": "E002", "siteId": "site-b"},
		{"account": "carol", "employeeId": "E003", "siteId": "site-a"},
		{"account": "eve", "employeeId": "E004", "siteId": "site-a"},
	} {
		_, err := db.Collection("hr_employee").InsertOne(ctx, doc)
		require.NoError(t, err)
	}
	// users: alice/bob are humans provisioned on the matching site (get
	// enriched); eve is provisioned on site-b, mismatched with her
	// hr_employee row on site-a (must surface, unenriched); dave is a plain
	// account with no hr_employee row at all (the intersection edge this fix
	// targets); svc.notify.bot is a bot account present ONLY in users, never
	// in hr_employee — the exact regression this fix targets: pre-fix, EVERY
	// bot/admin account was excluded because the old pipeline required an
	// hr_employee row to exist before a users row was even considered.
	_, err := db.Collection("users").InsertMany(ctx, []any{
		bson.M{"_id": "u-alice", "account": "alice", "siteId": "site-a"},
		bson.M{"_id": "u-bob", "account": "bob", "siteId": "site-b"},
		bson.M{"_id": "u-eve", "account": "eve", "siteId": "site-b"},
		bson.M{"_id": "u-dave", "account": "dave", "siteId": "site-a"},
		bson.M{"_id": "u-bot", "account": "svc.notify.bot", "siteId": "site-a", "roles": bson.A{"bot"}},
	})
	require.NoError(t, err)

	emps, err := store.ListEmployees(ctx)
	require.NoError(t, err)

	byAccount := make(map[string]employee, len(emps))
	for _, e := range emps {
		byAccount[e.Account] = e
	}

	// The core regression: a bot account with no hr_employee row at all must
	// now be in the directory, with the human-only enrichment fields empty.
	botEntry, ok := byAccount["svc.notify.bot"]
	require.True(t, ok, "bot/admin account absent from hr_employee must now be in the directory")
	assert.Equal(t, employee{
		Account: "svc.notify.bot", SiteID: "site-a", UserID: "u-bot",
		Roles: []model.UserRole{model.UserRoleBot},
	}, botEntry)

	// A human present in both collections (matching account+siteId) still
	// gets the rich hr_employee fields.
	assert.Equal(t, employee{
		Account: "alice", EmployeeID: "E001", SiteID: "site-a", UserID: "u-alice",
	}, byAccount["alice"])
	assert.Equal(t, employee{
		Account: "bob", EmployeeID: "E002", SiteID: "site-b", UserID: "u-bob",
	}, byAccount["bob"])

	// eve's hr_employee row is on a different site than her users row, so the
	// left-join must not enrich her — she still surfaces (users primary) but
	// with the enrichment fields empty.
	assert.Equal(t, employee{Account: "eve", SiteID: "site-b", UserID: "u-eve"}, byAccount["eve"])

	// dave has no hr_employee row at all — the plain intersection edge this
	// fix targets, independent of any bot/admin role — and must still surface.
	assert.Equal(t, employee{Account: "dave", SiteID: "site-a", UserID: "u-dave"}, byAccount["dave"])

	// carol is hr_employee-only (no users row) and must NOT appear — users,
	// not hr_employee, drives the directory now.
	_, ok = byAccount["carol"]
	assert.False(t, ok, "an hr_employee-only row with no matching users account must not surface")

	assert.Len(t, emps, 5, "alice, bob, eve, dave, svc.notify.bot surface; hr_employee-only carol is excluded")
}

func TestMongoDirectoryStore_ListEmployees_Empty(t *testing.T) {
	db := testutil.MongoDB(t, "portal")
	store := newMongoDirectoryStore(db)

	emps, err := store.ListEmployees(context.Background())
	require.NoError(t, err)
	assert.Empty(t, emps)
}

func TestMongoDirectoryStore_EnsureIndexes_UniqueAccount(t *testing.T) {
	db := testutil.MongoDB(t, "portal")
	store := newMongoDirectoryStore(db)
	ctx := context.Background()

	require.NoError(t, store.EnsureIndexes(ctx))
	require.NoError(t, store.EnsureIndexes(ctx), "EnsureIndexes must be idempotent")

	coll := db.Collection("hr_employee")
	_, err := coll.InsertOne(ctx, bson.M{"account": "alice", "employeeId": "E001", "siteId": "site-a"})
	require.NoError(t, err)

	_, err = coll.InsertOne(ctx, bson.M{"account": "alice", "employeeId": "E099", "siteId": "site-b"})
	require.Error(t, err, "a second row for the same account must be rejected at write time")
	assert.True(t, mongo.IsDuplicateKeyError(err))

	// A distinct account is unaffected by the unique index.
	_, err = coll.InsertOne(ctx, bson.M{"account": "bob", "employeeId": "E002", "siteId": "site-b"})
	require.NoError(t, err)
}
