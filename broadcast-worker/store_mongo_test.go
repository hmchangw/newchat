package main

import (
	"reflect"
	"testing"
	"time"

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
