//go:build integration

package mongoutil

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/testutil"
)

// TestConnect_LazyBootsDuringOutageThenRecovers is the scenario this option
// exists for: a process starts while MongoDB is unreachable, serves (failing)
// requests, and then works normally once MongoDB returns — all without a
// restart. The default ping path cannot start at all here.
func TestConnect_LazyBootsDuringOutageThenRecovers(t *testing.T) {
	outage := testutil.NewMongoOutage(t, testutil.MongoURI(t))

	// Boot during the outage: this is the step that currently kills every service.
	bootCtx, bootCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer bootCancel()
	client, err := Connect(bootCtx, outage.URI(), "", "", WithLazyConnect())
	require.NoError(t, err, "lazy Connect must boot while MongoDB is unreachable")
	t.Cleanup(func() { Disconnect(context.Background(), client) })

	coll := client.Database("mongoutil_lazy_recovery_test").Collection("docs")
	t.Cleanup(func() {
		_ = client.Database("mongoutil_lazy_recovery_test").Drop(context.Background())
	})

	// While down, operations fail — bounded, not hanging.
	downCtx, downCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer downCancel()
	start := time.Now()
	_, err = coll.InsertOne(downCtx, bson.M{"_id": "during-outage"})
	require.Error(t, err, "operations must fail while MongoDB is unreachable")
	assert.Less(t, time.Since(start), 5*time.Second, "must fail bounded, not hang")

	// MongoDB comes back. The same client must recover with no restart.
	outage.Restore(t)

	upCtx, upCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer upCancel()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		_, err := coll.InsertOne(upCtx, bson.M{"_id": "after-recovery"})
		assert.NoError(c, err)
	}, 25*time.Second, 250*time.Millisecond, "lazy client must recover once MongoDB returns")

	n, err := coll.CountDocuments(upCtx, bson.M{"_id": "after-recovery"})
	require.NoError(t, err)
	assert.EqualValues(t, 1, n)
}

// TestConnect_LazyAgainstHealthyMongoWorksImmediately confirms skipping the
// ping costs nothing in the normal case — the usual startup, just unproven.
func TestConnect_LazyAgainstHealthyMongoWorksImmediately(t *testing.T) {
	ctx := context.Background()
	client, err := Connect(ctx, testutil.MongoURI(t), "", "", WithLazyConnect())
	require.NoError(t, err)
	t.Cleanup(func() { Disconnect(context.Background(), client) })

	db := client.Database("mongoutil_lazy_healthy_test")
	t.Cleanup(func() { _ = db.Drop(context.Background()) })

	opCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	_, err = db.Collection("docs").InsertOne(opCtx, bson.M{"_id": "x"})
	require.NoError(t, err)
	n, err := db.Collection("docs").CountDocuments(opCtx, bson.M{})
	require.NoError(t, err)
	assert.EqualValues(t, 1, n)
}
