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

// roomLastMsgFilter matches a room only when (at, msgID) is newer than what is
// stored, so a redelivered older message cannot move the pointer backwards.
//
// This is msgbucket.NewerRow in BSON, and has to be: the coalescer and
// broadcast-worker's preview writer both order ties that way, and the reader
// serves a preview only while previewForMsgId equals lastMsgId. Ordering ties
// differently here would leave the two naming different messages, and that room
// never serves its preview again.
//
// Two clauses: strictly older or absent ($not/$gte, so a missing lastMsgAt still
// matches, which $lt would skip), and same instant with a lower id. BSON dates
// are millisecond-precision, the same granularity NewerRow compares at.
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
			SetUpdate(roomPointerUpdate(&u)))
		if !u.userAt.IsZero() {
			// A SEPARATE write, matched on identity alone, for the same reason
			// lastMentionAllAt takes one: the user position is its own monotonic
			// dimension. Gating it on the pointer's regression filter would
			// discard a newer user position whenever a redelivered batch lost
			// the pointer race to a later system message — and the sidebar would
			// then order the room by a staler user message than it has.
			models = append(models, mongo.NewUpdateOneModel().
				SetFilter(bson.M{"_id": roomID}).
				SetUpdate(bson.M{"$max": bson.M{"lastUserMsgAt": u.userAt}}))
		}
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

// roomPointerUpdate renders the room-pointer write. A window that carried a user
// message takes the plain $set it always did — its user position rides a separate
// $max above. A system-only window takes a pipeline instead, because freezing
// lastUserMsgAt needs to read the document before this write changes it.
//
// The freeze must share this model with the pointer: the bulk is unordered, so a
// separate model could read lastMsgAt after the pointer write had already set it
// and conclude the room had carried a message all along.
func roomPointerUpdate(u *roomLastMsgUpdate) any {
	fields := bson.M{
		"lastMsgAt": u.at,
		"lastMsgId": u.msgID,
		"updatedAt": u.at,
	}
	if !u.userAt.IsZero() {
		return bson.M{"$set": fields}
	}
	// A bare "$"-prefixed string reads as a field path inside a pipeline stage.
	fields["lastMsgId"] = bson.M{"$literal": u.msgID}
	// Sticky freeze, carried verbatim from #382's coalescer: keep whatever
	// lastUserMsgAt already says, and pin a floor ONCE for a room that has never
	// carried a message — its createdAt, which makes the room unread for members
	// who never opened it without re-flagging members who have. A room that
	// already has a lastMsgAt is left alone: that timestamp is unclassified (it
	// may itself be a system message), so promoting it would persist a system
	// position as user activity and the freeze would then keep it forever. Absent
	// is the safe state — readers coalesce to lastMsgAt, the pre-field behaviour.
	fields["lastUserMsgAt"] = bson.M{"$ifNull": bson.A{
		"$lastUserMsgAt",
		bson.M{"$cond": bson.A{
			bson.M{"$eq": bson.A{bson.M{"$ifNull": bson.A{"$lastMsgAt", nil}}, nil}},
			bson.M{"$ifNull": bson.A{"$createdAt", "$$REMOVE"}},
			"$$REMOVE",
		}},
	}}
	return mongo.Pipeline{{{Key: "$set", Value: fields}}}
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
