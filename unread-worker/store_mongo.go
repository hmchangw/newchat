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

// roomLastMsgFilter matches a room only when (at, msgID) sorts newer than the
// stored pointer, so a redelivered older message cannot regress it.
//
// This is msgbucket.NewerRow rendered in BSON, and it has to be: the in-memory
// coalescer breaks ties with that comparator, and so does broadcast-worker's
// preview writer, which stamps previewForMsgId. history-service serves a stored
// preview only while previewForMsgId equals lastMsgId, so a guard that ordered
// ties differently from those two would leave the pointer on whichever message
// of a same-millisecond pair happened to land first while the preview named the
// other — a permanent miss for that room until a newer message arrives. Two
// messages at one millisecond reach this separately whenever they fall in
// different flush batches or one is redelivered.
//
// The two clauses:
//   - strictly older (or absent): $not/$gte rather than $lt, so a room whose
//     lastMsgAt is missing or null still matches — $lt would skip it and the
//     pointer would never be set at all.
//   - same instant, lower id: the case the first clause refuses. BSON dates are
//     millisecond-precision and the driver truncates on encode, so this equality
//     compares at exactly the granularity NewerRow does.
func roomLastMsgFilter(roomID, msgID string, at time.Time) bson.M {
	return bson.M{
		"_id": roomID,
		"$or": []bson.M{
			{"lastMsgAt": bson.M{"$not": bson.M{"$gte": at}}},
			{"lastMsgAt": at, "lastMsgId": bson.M{"$lt": msgID}},
		},
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

// roomLastMsgModels builds the writes for one batch of per-room updates. Split
// out from BulkUpdateRoomLastMessage so the filter/update pairing is assertable
// without a live Mongo — the pairing is the whole correctness question here.
func roomLastMsgModels(updates map[string]roomLastMsgUpdate) []mongo.WriteModel {
	models := make([]mongo.WriteModel, 0, len(updates))
	for roomID, u := range updates {
		models = append(models, mongo.NewUpdateOneModel().
			SetFilter(roomLastMsgFilter(roomID, u.msgID, u.at)).
			SetUpdate(bson.M{"$set": bson.M{
				"lastMsgAt": u.at,
				"lastMsgId": u.msgID,
				"updatedAt": u.at,
			}}))
		if u.lastMentionAllAt.IsZero() {
			continue
		}
		// A SEPARATE write, matched on identity alone. lastMentionAllAt is not
		// part of the room pointer — it is its own monotonic dimension — so
		// gating it on the pointer's regression filter would silently discard
		// the @all badge whenever a redelivered batch lost the pointer race to
		// a later message. user-service derives HasGroupMention from this
		// field, and the batch Acks after the retry, so that loss is permanent.
		// $max supplies the monotonicity the dropped guard used to imply, and
		// still writes when the field is missing.
		models = append(models, mongo.NewUpdateOneModel().
			SetFilter(bson.M{"_id": roomID}).
			SetUpdate(bson.M{"$max": bson.M{"lastMentionAllAt": u.lastMentionAllAt}}))
	}
	return models
}

func (m *mongoStore) BulkUpdateRoomLastMessage(ctx context.Context, updates map[string]roomLastMsgUpdate) error {
	// No empty guard: the typed collection supplies it (see the file header), which
	// is why the two sibling methods below don't carry one either.
	_, err := m.roomCol.BulkWrite(ctx, roomLastMsgModels(updates))
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
