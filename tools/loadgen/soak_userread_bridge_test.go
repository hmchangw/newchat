package main

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	soakuserread "github.com/hmchangw/chat/tools/loadgen/internal/soak/userread"
)

func newSoakUserReadFixture(
	t *testing.T,
	transport soakRPCTransport,
	seed int64,
) (*soakUserReader, *soakRoomReadRecorder) {
	t.Helper()
	recorder := &soakRoomReadRecorder{}
	reader, err := newSoakUserReader(
		soakUserReadConfig{SiteID: "site-a", PageLimit: 5, RequestTimeout: time.Second},
		&soakTopology{
			ActiveUsers: []model.User{
				{ID: "u1", Account: "user-a"},
				{ID: "u2", Account: "user-b"},
			},
			Rooms: []model.Room{
				{ID: "room-1", Type: model.RoomTypeChannel},
				{ID: "dm-1", Type: model.RoomTypeDM},
			},
			Subscriptions: []model.Subscription{
				{RoomID: "dm-1", RoomType: model.RoomTypeDM,
					User: model.SubscriptionUser{ID: "u1", Account: "user-a"}},
				{RoomID: "dm-1", RoomType: model.RoomTypeDM,
					User: model.SubscriptionUser{ID: "u2", Account: "user-b"}},
				{RoomID: "room-1", RoomType: model.RoomTypeChannel,
					User: model.SubscriptionUser{ID: "u1", Account: "user-a"}},
				{RoomID: "room-1", RoomType: model.RoomTypeChannel,
					User: model.SubscriptionUser{ID: "u2", Account: "user-b"}},
			},
		},
		newSoakRPCClient(
			transport, soakRetryConfig{MaxAttempts: 1}, &soakRecordingSleeper{}, nil,
		),
		recorder,
		rand.New(rand.NewSource(seed)),
		nil,
	)
	require.NoError(t, err)
	return reader, recorder
}

func TestSoakUserReadRecorderAdapter_MapsEverySampleField(t *testing.T) {
	recorder := &soakRoomReadRecorder{}
	adapter := soakUserReadRecorderAdapter{recorder: recorder}
	want := soakuserread.Sample{
		Action: soakRPCUserMe, Latency: 2 * time.Second,
		Messages: 3, RowsCounted: true, ReplyBytes: 4,
		ErrorClass: soakErrorTimeout, ErrorReason: soakErrorReason("reason"),
		Retries: 5, Skipped: true,
	}

	adapter.Record(&want)

	require.Len(t, recorder.samples, 1)
	got := recorder.samples[0]
	assert.Equal(t, want.Action, got.Action)
	assert.Equal(t, want.Latency, got.Latency)
	assert.Equal(t, want.Messages, got.Messages)
	assert.Equal(t, want.RowsCounted, got.RowsCounted)
	assert.Equal(t, want.ReplyBytes, got.ReplyBytes)
	assert.Equal(t, want.ErrorClass, got.ErrorClass)
	assert.Equal(t, want.ErrorReason, got.ErrorReason)
	assert.Equal(t, want.Retries, got.Retries)
	assert.Equal(t, want.Skipped, got.Skipped)
}

func TestSoakUserReadRecorderAdapter_AllowsNoRecorder(t *testing.T) {
	assert.NotPanics(t, func() {
		soakUserReadRecorderAdapter{}.Record(&soakuserread.Sample{})
	})
}
