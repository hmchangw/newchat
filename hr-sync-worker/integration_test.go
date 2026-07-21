//go:build integration

package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/hrstore"
	"github.com/hmchangw/chat/pkg/jsretry"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

// startWorker wires the real consume path (durable consumer + jsretry settle)
// against the given handler, mirroring startSiteConsumer without the o11y SDK.
func startWorker(t *testing.T, js jetstream.JetStream, h *Handler, siteID string) {
	t.Helper()
	ctx := context.Background()
	streamCfg := stream.OrgSyncStream(siteID)
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{Name: streamCfg.Name, Subjects: streamCfg.Subjects})
	require.NoError(t, err)
	t.Cleanup(func() { _ = js.DeleteStream(context.Background(), streamCfg.Name) })

	consCfg := stream.DurableConsumerDefaults(stream.ConsumerSettings{AckWait: 5 * time.Second, MaxDeliver: -1, MaxWaiting: 64})
	consCfg.Durable = durableName
	consCfg.MaxAckPending = 1
	cons, err := js.CreateOrUpdateConsumer(ctx, streamCfg.Name, consCfg)
	require.NoError(t, err)
	cc, err := cons.Consume(func(msg jetstream.Msg) {
		data, derr := decodePayload(msg)
		require.NoError(t, derr)
		jsretry.Settle(context.Background(), msg, jsretry.DefaultBackoff, h.HandleMessage(context.Background(), msg.Subject(), data))
	})
	require.NoError(t, err)
	t.Cleanup(cc.Stop)
}

// publishJSON publishes payload as a zstd-compressed message with the
// Nats-Encoding header, matching the producer's wire format.
func publishJSON(t *testing.T, js jetstream.JetStream, subj string, payload any) {
	t.Helper()
	data, err := json.Marshal(payload)
	require.NoError(t, err)
	enc, err := zstd.NewWriter(nil)
	require.NoError(t, err)
	msg := &nats.Msg{Subject: subj, Data: enc.EncodeAll(data, nil), Header: nats.Header{"Nats-Encoding": []string{"zstd"}}}
	enc.Close()
	_, err = js.PublishMsg(context.Background(), msg)
	require.NoError(t, err)
}

func TestWorker_EndToEnd(t *testing.T) {
	ctx := context.Background()
	db := testutil.MongoDB(t, "hr_sync_worker_e2e")
	natsURL := testutil.NATS(t)
	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	require.NoError(t, err)

	// pre-existing user with auth fields the identity upsert must not touch
	// carries employeeId E1 — the identity upsert keys on it, so this row is
	// the one alice's upsert must update in place (not a fresh insert).
	_, err = db.Collection(hrstore.UserCollection).InsertOne(ctx,
		bson.M{"_id": "u-alice", "account": "alice", "employeeId": "E1", "siteId": "old-site", "roles": []string{"admin"}, "services": bson.M{"password": bson.M{"bcrypt": "hash"}}})
	require.NoError(t, err)

	h := NewHandler(newMongoStore(db))
	startWorker(t, js, h, "site-a")

	emp := func(account string) model.EmployeeWithChange {
		return model.EmployeeWithChange{
			Employee: model.Employee{
				ID: "E-" + account, EmployeeID: "E-" + account,
				Account: account, SiteID: "site-a",
				EngName: "Name " + account,
				Org:     model.Org{SectID: "g1", SectName: "Engineering"},
			},
			ChangeType: model.ChangeTypeNewHire,
		}
	}
	batch := []model.EmployeeWithChange{emp("alice"), emp("bob")}
	publishJSON(t, js, "chat.hr.site-a.employees.upsert", batch)
	publishJSON(t, js, "chat.hr.site-a.users.upsert", []model.UserWithChange{
		{User: model.User{Account: "alice", SiteID: "site-a", EngName: "Name alice", EmployeeID: "E1"}, ChangeType: model.ChangeTypeNewHire},
		{User: model.User{Account: "carol", SiteID: "site-a", ChineseName: "卡蘿", EmployeeID: "E2"}, ChangeType: model.ChangeTypeNewHire},
		// no employeeId → skipped, never written (an empty key would match and
		// clobber every other keyless row); the count assertion below proves it
		{User: model.User{Account: "keyless", SiteID: "site-a"}, ChangeType: model.ChangeTypeNewHire},
	})

	awaitCount(t, ctx, db, hrstore.EmployeeCollection, bson.M{}, 2)
	awaitCount(t, ctx, db, hrstore.UserCollection, bson.M{}, 2) // alice updated in place, carol inserted

	// identity upsert: alice's auth fields intact, identity fields updated
	var alice bson.M
	require.NoError(t, db.Collection(hrstore.UserCollection).FindOne(ctx, bson.M{"account": "alice"}).Decode(&alice))
	assert.Equal(t, "u-alice", alice["_id"], "existing user keeps its _id")
	assert.Equal(t, "site-a", alice["siteId"])
	assert.Equal(t, "Name alice", alice["engName"])
	assert.NotNil(t, alice["roles"], "roles must never be touched")
	assert.NotNil(t, alice["services"], "services must never be touched")
	var carol bson.M
	require.NoError(t, db.Collection(hrstore.UserCollection).FindOne(ctx, bson.M{"account": "carol"}).Decode(&carol))
	assert.NotEmpty(t, carol["_id"], "inserted user gets a generated _id")
	assert.Equal(t, "卡蘿", carol["chineseName"])

	// re-delivery (same batches again) → no dupes
	publishJSON(t, js, "chat.hr.site-a.employees.upsert", batch)
	awaitCount(t, ctx, db, hrstore.EmployeeCollection, bson.M{}, 2)

	// quit: alice + bob deleted from hr_employee; users untouched
	publishJSON(t, js, "chat.hr.site-a.employees.quit", model.HRSyncEmployeeQuitBatch{
		Timestamp: 2, SiteID: "site-a", Accounts: []string{"alice", "bob"},
	})
	awaitCount(t, ctx, db, hrstore.EmployeeCollection, bson.M{}, 0)
	awaitCount(t, ctx, db, hrstore.UserCollection, bson.M{}, 2)
}

// awaitCount polls the collection until filter matches want (consume is async).
func awaitCount(t *testing.T, ctx context.Context, db *mongo.Database, col string, filter bson.M, want int64) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		n, err := db.Collection(col).CountDocuments(ctx, filter)
		require.NoError(t, err)
		if n == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("collection %s: want %d docs matching %v, still %d", col, want, filter, n)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
