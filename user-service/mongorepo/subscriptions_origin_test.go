package mongorepo

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/model"
)

func TestOriginFilterStage_ShowTeamsRoomFalse_ExcludesTeams(t *testing.T) {
	r := &SubscriptionRepo{showTeamsRoom: false}
	stages := r.originFilterStage("alice")
	require.Len(t, stages, 1)
	assert.Equal(t, bson.M{"$match": bson.M{"origin": bson.M{"$ne": model.OriginTeams}}}, stages[0])
}

func TestOriginFilterStage_ShowTeamsRoomTrue_NoOp(t *testing.T) {
	r := &SubscriptionRepo{showTeamsRoom: true}
	stages := r.originFilterStage("alice")
	assert.Len(t, stages, 0)
}

func TestOriginFilterStage_AllowlistedAccount_NoOp(t *testing.T) {
	r := &SubscriptionRepo{showTeamsRoom: false, showTeamsAccounts: map[string]bool{"alice": true}}
	// Allowlisted account sees Teams rooms → no filter.
	assert.Len(t, r.originFilterStage("alice"), 0)
	// A non-allowlisted account is still filtered.
	require.Len(t, r.originFilterStage("bob"), 1)
}

func TestWithShowTeamsAccounts_TrimsWhitespace(t *testing.T) {
	s := applyOptions([]Option{WithShowTeamsAccounts([]string{"alice", " bob", "  ", ""})})
	r := &SubscriptionRepo{showTeamsAccounts: s.showTeamsAccounts}
	assert.Len(t, r.originFilterStage("alice"), 0, "alice allowlisted")
	assert.Len(t, r.originFilterStage("bob"), 0, "bob allowlisted despite leading space in env")
	require.Len(t, r.originFilterStage("carol"), 1, "non-listed still filtered")
}
