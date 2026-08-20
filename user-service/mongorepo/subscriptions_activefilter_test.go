package mongorepo

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// activeSubscriptionFilter backs both the total and the unread subscription
// counts, and MUST select the same rows subscription.list shows the user.
// A room the user closed is excluded from the list, so counting it would make
// the client's locally folded badge permanently disagree with the server's —
// each disagreement costing a full sidebar resync that cannot converge.
func TestActiveSubscriptionFilter_ExcludesClosedRooms(t *testing.T) {
	f := activeSubscriptionFilter("alice")

	got, ok := f["open"]
	assert.True(t, ok, "filter must constrain `open`")
	assert.Equal(t, bson.M{"$ne": false}, got,
		"a missing `open` (legacy docs) and open:true must both pass; only open:false is excluded")
}
