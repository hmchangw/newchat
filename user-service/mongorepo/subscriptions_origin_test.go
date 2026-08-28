package mongorepo

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/model"
)

func TestOriginFilter_ShowTeamsRoomFalse_ExcludesTeams(t *testing.T) {
	r := &SubscriptionRepo{showTeamsRoom: false}
	assert.Equal(t, bson.M{"$ne": model.OriginTeams}, r.originFilter("alice"))
}

func TestOriginFilter_ShowTeamsRoomTrue_NoOp(t *testing.T) {
	r := &SubscriptionRepo{showTeamsRoom: true}
	assert.Nil(t, r.originFilter("alice"))
}

func TestOriginFilter_AllowlistedAccount_NoOp(t *testing.T) {
	r := &SubscriptionRepo{showTeamsRoom: false, showTeamsAccounts: map[string]bool{"alice": true}}
	assert.Nil(t, r.originFilter("alice"), "allowlisted account sees Teams rooms")
	assert.NotNil(t, r.originFilter("bob"), "a non-allowlisted account is still filtered")
}

func TestWithShowTeamsAccounts_TrimsWhitespace(t *testing.T) {
	s := applyOptions([]Option{WithShowTeamsAccounts([]string{"alice", " bob", "  ", ""})})
	r := &SubscriptionRepo{showTeamsAccounts: s.showTeamsAccounts}
	assert.Nil(t, r.originFilter("alice"), "alice allowlisted")
	assert.Nil(t, r.originFilter("bob"), "bob allowlisted despite leading space in env")
	assert.NotNil(t, r.originFilter("carol"), "non-listed still filtered")
}
