package main

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
)

// The typed collections are only ever used for their BulkWrite, which supplies
// the empty-input guard, the unordered execution every stage here depends on,
// and a %w wrap that keeps errors.As(BulkWriteException) working in
// classifyFlushErr.
type mongoStore struct {
	roomCol *mongoutil.Collection[model.Room]
	subCol  *mongoutil.Collection[model.Subscription]
}

func NewMongoStore(roomCol, subCol *mongo.Collection) *mongoStore {
	return &mongoStore{
		roomCol: mongoutil.NewCollection[model.Room](roomCol),
		subCol:  mongoutil.NewCollection[model.Subscription](subCol),
	}
}

// roomLastMsgFilter matches a room only when its stored lastMsgAt is not already
// at or beyond at, so a redelivered older message cannot regress the pointer.
// $not/$gte (not $lt) so it still matches a missing or null lastMsgAt.
func roomLastMsgFilter(roomID string, at time.Time) bson.M {
	return bson.M{
		"_id":       roomID,
		"lastMsgAt": bson.M{"$not": bson.M{"$gte": at}},
	}
}

// mentionFilter matches a subscription that has NOT already read past at.
// Same $not/$gte reasoning as roomLastMsgFilter: a plain $lt would skip a
// never-read subscription whose lastSeenAt is missing.
func mentionFilter(k subKey, at time.Time) bson.M {
	return bson.M{
		"roomId":     k.roomID,
		"u.account":  k.account,
		"lastSeenAt": bson.M{"$not": bson.M{"$gte": at}},
	}
}

func (m *mongoStore) BulkUpdateRoomLastMessage(ctx context.Context, updates map[string]roomLastMsgUpdate) error {
	models := make([]mongo.WriteModel, 0, len(updates))
	for roomID, u := range updates {
		fields := bson.M{
			"lastMsgAt": u.at,
			"lastMsgId": u.msgID,
			"updatedAt": u.at,
		}
		if !u.lastMentionAllAt.IsZero() {
			fields["lastMentionAllAt"] = u.lastMentionAllAt
		}
		models = append(models, mongo.NewUpdateOneModel().
			SetFilter(roomLastMsgFilter(roomID, u.at)).
			SetUpdate(bson.M{"$set": fields}))
	}
	_, err := m.roomCol.BulkWrite(ctx, models)
	return err
}

func (m *mongoStore) BulkAdvanceLastSeen(ctx context.Context, updates map[subKey]time.Time) error {
	models := make([]mongo.WriteModel, 0, len(updates))
	for k, at := range updates {
		models = append(models, mongo.NewUpdateOneModel().
			SetFilter(bson.M{"roomId": k.roomID, "u.account": k.account}).
			SetUpdate(bson.M{"$max": bson.M{"lastSeenAt": at}}))
	}
	_, err := m.subCol.BulkWrite(ctx, models)
	return err
}

func (m *mongoStore) BulkSetMentions(ctx context.Context, updates map[subKey]time.Time) error {
	models := make([]mongo.WriteModel, 0, len(updates))
	for k, at := range updates {
		models = append(models, mongo.NewUpdateOneModel().
			SetFilter(mentionFilter(k, at)).
			SetUpdate(bson.M{"$set": bson.M{"hasMention": true}}))
	}
	_, err := m.subCol.BulkWrite(ctx, models)
	return err
}
