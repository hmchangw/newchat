package main

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// TestSubscriptionMentionsFilter_GuardsAlreadyRead pins the mongo filter shape
// used by SetSubscriptionMentions. Regression for #467: a bare $lt would skip
// subs whose lastSeenAt is missing/null (never read), while the intent is
// "skip only if already read past this message".
func TestSubscriptionMentionsFilter_GuardsAlreadyRead(t *testing.T) {
	msgAt := time.Date(2026, 3, 26, 9, 0, 0, 0, time.UTC)

	got := subscriptionMentionsFilter("room-1", []string{"alice", "bob"}, msgAt)

	want := bson.M{
		"roomId":     "room-1",
		"u.account":  bson.M{"$in": []string{"alice", "bob"}},
		"lastSeenAt": bson.M{"$not": bson.M{"$gte": msgAt}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("subscriptionMentionsFilter mismatch:\n got=%#v\nwant=%#v", got, want)
	}
}

func TestThreadRoomInfo_ZeroParentCreatedAtIsUnknown(t *testing.T) {
	// A zero threadParentCreatedAt must surface as nil, never as the epoch:
	// mentionVisible treats a nil parent time as "not visible" (fail closed),
	// while time.Time{} would compare as older than every historySharedSince
	// and admit every mentionee.
	info := threadRoomInfoFrom([]string{"alice", "bob"}, time.Time{})
	assert.Nil(t, info.ParentCreatedAt)
	assert.Len(t, info.Followers, 2)
}

func TestThreadRoomInfo_RealParentCreatedAtIsCarried(t *testing.T) {
	at := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	info := threadRoomInfoFrom([]string{"alice", ""}, at)
	require.NotNil(t, info.ParentCreatedAt)
	assert.Equal(t, at, *info.ParentCreatedAt)
	assert.Len(t, info.Followers, 1, "empty accounts are skipped")
}
