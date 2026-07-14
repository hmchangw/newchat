//go:build integration

package mongorepo

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/user-service/models"
)

func seedApps(t *testing.T, db *mongo.Database) {
	t.Helper()
	seed(t, db, "apps",
		bson.M{"_id": "app-helper", "name": "Helper", "assistant": bson.M{"enabled": true, "name": "helper.bot"}},
		bson.M{"_id": "app-other", "name": "Other", "assistant": bson.M{"enabled": true, "name": "other.bot"}},
	)
	// alice is subscribed to helper.bot only.
	seed(t, db, "subscriptions",
		bson.M{"_id": "sub-helper", "u": bson.M{"_id": "u-alice", "account": "alice"}, "name": "helper.bot", "roomId": "r-helper",
			"roomType": "botDM", "siteId": "site-a", "isSubscribed": true},
		bson.M{"_id": "sub-other", "u": bson.M{"_id": "u-alice", "account": "alice"}, "name": "other.bot", "roomId": "r-other",
			"roomType": "botDM", "siteId": "site-a", "isSubscribed": false},
		// Collision: same name as other.bot but channel roomType — lookup must NOT count this toward Other's isSubscribed.
		bson.M{"_id": "sub-collision", "u": bson.M{"_id": "u-alice", "account": "alice"}, "name": "other.bot", "roomId": "r-collision",
			"roomType": "channel", "siteId": "site-a", "isSubscribed": true},
	)
}

func TestGetApp_Integration(t *testing.T) {
	r, db := newTestAppRepo(t)
	ctx := context.Background()
	seedApps(t, db)

	t.Run("found", func(t *testing.T) {
		app, err := r.GetApp(ctx, "app-helper")
		require.NoError(t, err)
		require.NotNil(t, app)
		assert.Equal(t, "Helper", app.Name)
		require.NotNil(t, app.Assistant)
		assert.Equal(t, "helper.bot", app.Assistant.Name)
	})

	t.Run("miss", func(t *testing.T) {
		app, err := r.GetApp(ctx, "nope")
		require.NoError(t, err)
		assert.Nil(t, app)
	})
}

func TestGetAppsByAssistants_Integration(t *testing.T) {
	r, db := newTestAppRepo(t)
	ctx := context.Background()
	seedApps(t, db)

	t.Run("maps bot accounts to full app docs", func(t *testing.T) {
		apps, err := r.GetAppsByAssistants(ctx, []string{"helper.bot", "other.bot"})
		require.NoError(t, err)
		require.Len(t, apps, 2)
		require.NotNil(t, apps["helper.bot"])
		assert.Equal(t, "app-helper", apps["helper.bot"].ID, "AppID must come from the doc _id")
		assert.Equal(t, "Helper", apps["helper.bot"].Name)
		require.NotNil(t, apps["helper.bot"].Assistant)
		assert.Equal(t, "helper.bot", apps["helper.bot"].Assistant.Name)
		require.NotNil(t, apps["other.bot"])
		assert.Equal(t, "Other", apps["other.bot"].Name)
	})

	t.Run("unknown bot omitted", func(t *testing.T) {
		apps, err := r.GetAppsByAssistants(ctx, []string{"helper.bot", "ghost.bot"})
		require.NoError(t, err)
		require.Len(t, apps, 1)
		require.NotNil(t, apps["helper.bot"])
		assert.Equal(t, "Helper", apps["helper.bot"].Name)
	})

	t.Run("empty input yields empty map", func(t *testing.T) {
		apps, err := r.GetAppsByAssistants(ctx, []string{})
		require.NoError(t, err)
		assert.Empty(t, apps)
	})
}

func TestListApps_Integration(t *testing.T) {
	r, db := newTestAppRepo(t)
	ctx := context.Background()
	seedApps(t, db)

	page, err := r.ListApps(ctx, "alice", mongoutil.NewOffsetPageRequest(0, 0))
	require.NoError(t, err)
	require.Len(t, page.Data, 2)
	assert.False(t, page.HasMore, "both apps fit on one page")
	// Sorted by name: Helper, Other.
	assert.Equal(t, "Helper", page.Data[0].Name)
	assert.True(t, page.Data[0].IsSubscribed, "helper.bot is subscribed")
	assert.Equal(t, "Other", page.Data[1].Name)
	assert.False(t, page.Data[1].IsSubscribed,
		"other.bot has no subscribed botDM; a same-name channel sub must NOT flip isSubscribed")
}

func TestListApps_Pagination_Integration(t *testing.T) {
	r, db := newTestAppRepo(t)
	ctx := context.Background()
	seedApps(t, db)
	// Three extras on top of Helper + Other; names sort after "Helper" and
	// before/after "Other" deterministically: Helper, Other, Zeta1, Zeta2, Zeta3.
	for i := 1; i <= 3; i++ {
		seed(t, db, "apps", bson.M{
			"_id":  fmt.Sprintf("app-zeta-%d", i),
			"name": fmt.Sprintf("Zeta%d", i),
		})
	}

	t.Run("middle page signals more", func(t *testing.T) {
		page, err := r.ListApps(ctx, "alice", mongoutil.NewOffsetPageRequest(1, 2))
		require.NoError(t, err)
		require.Len(t, page.Data, 2)
		assert.True(t, page.HasMore, "more apps remain after this page")
		assert.Equal(t, "Other", page.Data[0].Name)
		assert.Equal(t, "Zeta1", page.Data[1].Name)
	})

	t.Run("last partial page", func(t *testing.T) {
		page, err := r.ListApps(ctx, "alice", mongoutil.NewOffsetPageRequest(4, 2))
		require.NoError(t, err)
		require.Len(t, page.Data, 1)
		assert.Equal(t, "Zeta3", page.Data[0].Name)
		assert.False(t, page.HasMore, "final page")
	})

	t.Run("offset beyond catalog", func(t *testing.T) {
		page, err := r.ListApps(ctx, "alice", mongoutil.NewOffsetPageRequest(10, 2))
		require.NoError(t, err)
		require.NotNil(t, page.Data, "Data must be non-nil so JSON marshals to []")
		assert.Empty(t, page.Data)
		assert.False(t, page.HasMore)
	})
}

func TestListApps_FieldPathAccountTreatedAsLiteral_Integration(t *testing.T) {
	r, db := newTestAppRepo(t)
	ctx := context.Background()
	seedApps(t, db)

	// Without $literal, "$u.account" is a field path and $eq:["$u.account","$u.account"] holds for every doc, flipping isSubscribed incorrectly.
	page, err := r.ListApps(ctx, "$u.account", mongoutil.NewOffsetPageRequest(0, 0))
	require.NoError(t, err)
	require.Len(t, page.Data, 2)
	for _, app := range page.Data {
		assert.False(t, app.IsSubscribed, "field-path-shaped account must match no subscription (app %s)", app.Name)
	}
}

func TestListAppCategories_Integration(t *testing.T) {
	r, db := newTestAppRepo(t)
	ctx := context.Background()

	// Native ObjectID _ids mirror the legacy collection (exercising the ObjectID→hex
	// decode path). _id order deliberately OPPOSES name order (F02 carries the largest
	// OID) so this fails unless name — not _id — is the primary sort key. internalNote
	// is a stored-but-undeclared field — appCategoryDoc drops it during decode
	// regardless of the projection, so this test does not by itself prove WithProjection.
	oidF02 := mustObjectID(t, "6422644600000000000000c8")
	oidF14 := mustObjectID(t, "642264460000000000000064")
	oidF22 := mustObjectID(t, "642264460000000000000001")
	seed(t, db, "fab_domain_mapping",
		bson.M{"_id": oidF14, "name": "F14", "siteId": "00700000", "internalNote": "do-not-serve"},
		bson.M{"_id": oidF02, "name": "F02", "siteId": "00600000"},
		bson.M{"_id": oidF22, "name": "F22", "siteId": "00600000"},
	)

	got, err := r.ListAppCategories(ctx)
	require.NoError(t, err)
	assert.Equal(t, []models.AppCategory{
		{ID: oidF02.Hex(), Name: "F02", SiteID: "00600000"},
		{ID: oidF14.Hex(), Name: "F14", SiteID: "00700000"},
		{ID: oidF22.Hex(), Name: "F22", SiteID: "00600000"},
	}, got)
}

// mustObjectID parses a fixed hex ObjectID for deterministic sort fixtures.
func mustObjectID(t *testing.T, hex string) bson.ObjectID {
	t.Helper()
	oid, err := bson.ObjectIDFromHex(hex)
	require.NoError(t, err)
	return oid
}

func TestListAppCategories_Integration_DuplicateNamesOrderedByID(t *testing.T) {
	r, db := newTestAppRepo(t)
	ctx := context.Background()

	// Two rows share a name; the _id sort tiebreaker must order them deterministically.
	lo, hi := bson.NewObjectID(), bson.NewObjectID()
	if hi.Hex() < lo.Hex() {
		lo, hi = hi, lo
	}
	seed(t, db, "fab_domain_mapping",
		bson.M{"_id": hi, "name": "F30", "siteId": "00800000"},
		bson.M{"_id": lo, "name": "F30", "siteId": "00900000"},
	)

	got, err := r.ListAppCategories(ctx)
	require.NoError(t, err)
	assert.Equal(t, []models.AppCategory{
		{ID: lo.Hex(), Name: "F30", SiteID: "00900000"},
		{ID: hi.Hex(), Name: "F30", SiteID: "00800000"},
	}, got)
}

func TestListAppCategories_Integration_Empty(t *testing.T) {
	r, _ := newTestAppRepo(t)

	got, err := r.ListAppCategories(context.Background())
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestListAppCategories_Integration_CancelledContext(t *testing.T) {
	r, _ := newTestAppRepo(t)

	cctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := r.ListAppCategories(cctx)
	assert.Error(t, err)
}
