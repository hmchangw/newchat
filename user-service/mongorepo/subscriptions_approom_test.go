package mongorepo

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/pipelines"
)

// Whole-map equality, not a structural walk: it proves the predicates are
// present AND that nothing else narrows the bucket.
func TestApplyListType(t *testing.T) {
	anyType := bson.M{"$in": bson.A{"dm", "channel", "botDM"}}
	tests := []struct {
		listType string
		want     bson.M
		why      string
	}{
		{
			listType: "current",
			want: bson.M{
				"u.account": "alice",
				"roomType":  anyType,
				"$nor":      bson.A{pipelines.UnsubscribedAppFilter()},
			},
			why: "everything but an app room the user unsubscribed from",
		},
		{
			listType: "rooms",
			want: bson.M{
				"u.account": "alice",
				"roomType":  anyType,
				"$nor":      bson.A{pipelines.AppRoomFilter()},
			},
			why: "chats only: a bot's own botDM rows belong here, real apps do not",
		},
		{
			listType: "apps",
			want: bson.M{
				"u.account":    "alice",
				"roomType":     "botDM",
				"name":         pipelines.AppRoomFilter()["name"],
				"isSubscribed": true,
			},
			why: "the App section holds subscribed .bot apps and nothing else",
		},
		{
			listType: "bogus",
			want:     bson.M{"u.account": "alice"},
			why:      "unreachable in production; must not widen the match",
		},
	}
	for _, tt := range tests {
		t.Run(tt.listType, func(t *testing.T) {
			got := bson.M{"u.account": "alice"}
			applyListType(got, tt.listType)
			assert.Equal(t, tt.want, got, tt.why)
		})
	}
}

// A bot asking for its own DM with a human must resolve the room: that row is
// stored roomType=botDM, so a hard roomType:"dm" match would 404 it.
func TestDMMatch(t *testing.T) {
	assert.Equal(t, bson.M{
		"u.account": "weather.bot",
		"name":      "alice",
		"roomType":  bson.M{"$in": bson.A{"dm", "botDM"}},
	}, dmMatch("weather.bot", "alice"))

	assert.Equal(t, bson.M{
		"u.account": "alice",
		"name":      "weather.bot",
		"roomType":  bson.M{"$in": bson.A{"dm"}},
	}, dmMatch("alice", "weather.bot"),
		"a .bot target is an app room, never a DM — no regex needed, the name is known here")
}
