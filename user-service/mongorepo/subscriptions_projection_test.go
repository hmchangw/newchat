package mongorepo

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// subscriptionProjection must surface the per-attribute subscription timestamps
// and must no longer include the dead _updatedAt field (nothing decodes it and
// no $match keys on it — the activity window keys on room.lastMsgAt).
func TestSubscriptionProjection_TimestampFields(t *testing.T) {
	proj := subscriptionProjection(nil)

	for _, k := range []string{"favoriteUpdatedAt", "muteUpdatedAt", "rolesUpdatedAt", "nameUpdatedAt", "restrictUpdatedAt"} {
		_, ok := proj[k]
		assert.True(t, ok, "projection must include %q", k)
	}

	_, hasOld := proj["_updatedAt"]
	assert.False(t, hasOld, "projection must not include dead _updatedAt")
}
