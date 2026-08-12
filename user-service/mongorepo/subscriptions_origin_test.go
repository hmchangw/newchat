package mongorepo

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/model"
)

func TestOriginFilterStage_ShowTeamsRoomFalse_ExcludesTeams(t *testing.T) {
	r := &SubscriptionRepo{showTeamsRoom: false}
	stages := r.originFilterStage()
	assert.Len(t, stages, 1)
	assert.Equal(t, bson.M{"$match": bson.M{"origin": bson.M{"$ne": model.OriginTeams}}}, stages[0])
}

func TestOriginFilterStage_ShowTeamsRoomTrue_NoOp(t *testing.T) {
	r := &SubscriptionRepo{showTeamsRoom: true}
	stages := r.originFilterStage()
	assert.Len(t, stages, 0)
}
