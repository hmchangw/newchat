package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
)

type threadStoreMongo struct {
	threadRooms         *mongo.Collection
	threadSubscriptions *mongo.Collection
	subscriptions       *mongo.Collection
	// parentIndex and subIndex gate the document-creating writes on their unique index.
	parentIndex *mongoutil.IndexGate
	subIndex    *mongoutil.IndexGate
}

// Compile-time assertion that *threadStoreMongo satisfies ThreadStore.
var _ ThreadStore = (*threadStoreMongo)(nil)

func newThreadStoreMongo(db *mongo.Database) *threadStoreMongo {
	threadRooms := db.Collection("thread_rooms")
	threadSubscriptions := db.Collection("thread_subscriptions")
	// Same specs as room-service, the owner; a conflicting index closes the gate until room-service repairs it.
	return &threadStoreMongo{
		threadRooms:         threadRooms,
		threadSubscriptions: threadSubscriptions,
		subscriptions:       db.Collection("subscriptions"),
		parentIndex:         mongoutil.NewUniqueIndexGate(threadRooms, bson.D{{Key: "parentMessageId", Value: 1}}),
		subIndex: mongoutil.NewUniqueIndexGate(threadSubscriptions,
			bson.D{{Key: "threadRoomId", Value: 1}, {Key: "userAccount", Value: 1}}),
	}
}

// EnsureIndexes co-creates both unique keys with room-service's spec: on a fresh site the first reply can
// beat the owner's start, and duplicates once landed block any later build. Best-effort; writes re-ask.
func (s *threadStoreMongo) EnsureIndexes(ctx context.Context) error {
	if err := s.parentIndex.Ready(ctx); err != nil {
		return err
	}
	return s.subIndex.Ready(ctx)
}

// EnsureThreadRoom resolves the thread room for room.ParentMessageID in one round trip:
// an upserting FindOneAndUpdate whose $setOnInsert seeds the room only when absent. The hot
// subsequent-reply path matches the existing room — no insert, no duplicate key — where the
// previous insert-then-read pattern paid a rejected write plus a follow-up find.
//
// created comes from the server rather than from the returned document: asking for the
// PRE-image means an insert has no document to return and reports ErrNoDocuments, while a
// match returns the room that was already there. Comparing the returned _id to the
// candidate's would instead make created mean "the stored room happens to carry the id I
// offered", which is the same answer only while every caller mints a fresh id — a
// precondition this contract should not depend on. On an insert the stored room IS the
// candidate, so returning it loses nothing; $setOnInsert changes nothing on a match, so the
// pre-image and post-image are identical there too.
func (s *threadStoreMongo) EnsureThreadRoom(ctx context.Context, room *model.ThreadRoom) (*model.ThreadRoom, bool, error) {
	// The unique parentMessageId index is what keeps one parent to one thread room: without
	// it two concurrent first replies both miss the filter and both insert. So it is confirmed
	// before the write, and a failure NAKs (see mongoutil.IndexGate). The subscriptions' key is
	// confirmed here too: the room insert is the point of no return for a first reply (a
	// redelivery takes the subsequent-reply path).
	if err := s.parentIndex.Ready(ctx); err != nil {
		return nil, false, err
	}
	if err := s.subIndex.Ready(ctx); err != nil {
		return nil, false, err
	}
	candidate := *room
	if candidate.ReplyAccounts == nil {
		candidate.ReplyAccounts = []string{}
	}
	// Deliberately unprojected, unlike the repo default. Both reads below hydrate a
	// whole *model.ThreadRoom for handler code whose field set grows, so a projection
	// naming today's fields would not fail when a new one is read — it would decode as
	// the zero value and change behaviour silently. One small document per reply is
	// the price; this matches the GetThreadRoomByParentMessageID read it replaces.
	filter := bson.M{"parentMessageId": candidate.ParentMessageID}
	opts := options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.Before)

	var existing model.ThreadRoom
	err := s.threadRooms.FindOneAndUpdate(ctx, filter, bson.M{"$setOnInsert": candidate}, opts).Decode(&existing)
	switch {
	case err == nil:
		// A pre-image existed, so this call matched rather than inserted.
		return &existing, false, nil
	case errors.Is(err, mongo.ErrNoDocuments):
		// No pre-image: the upsert inserted, and what it inserted is the candidate.
		return &candidate, true, nil
	case mongo.IsDuplicateKeyError(err):
		// Probably lost the insert race to a concurrent first reply, in which case the
		// room exists now and reading it back resolves this call.
		var stored model.ThreadRoom
		ferr := s.threadRooms.FindOne(ctx, filter).Decode(&stored)
		switch {
		case ferr == nil:
			return &stored, false, nil
		case errors.Is(ferr, mongo.ErrNoDocuments):
			// Nothing is stored for this parent, so the duplicate was not the race —
			// it came from some other unique key, an _id already owned by an unrelated
			// thread room being the likely one. Reporting that as a failed post-race
			// read would diagnose a genuine conflict as someone else's success.
			return nil, false, fmt.Errorf("ensure thread room for parent %s: %w", candidate.ParentMessageID, err)
		default:
			return nil, false, fmt.Errorf("read thread room after upsert race for parent %s: %w", candidate.ParentMessageID, ferr)
		}
	default:
		return nil, false, fmt.Errorf("ensure thread room for parent %s: %w", candidate.ParentMessageID, err)
	}
}

// MarkParentStamped sets parentStamped on the thread room after the parent's
// thread_room_id write succeeded. Safe to call twice: it writes the same value.
func (s *threadStoreMongo) MarkParentStamped(ctx context.Context, threadRoomID string) error {
	if _, err := s.threadRooms.UpdateOne(ctx,
		bson.M{"_id": threadRoomID},
		bson.M{"$set": bson.M{"parentStamped": true}},
	); err != nil {
		return fmt.Errorf("mark parent stamped on thread room %s: %w", threadRoomID, err)
	}
	return nil
}

func (s *threadStoreMongo) InsertThreadSubscription(ctx context.Context, sub *model.ThreadSubscription) error {
	if err := s.subIndex.Ready(ctx); err != nil {
		return err
	}
	if _, err := s.threadSubscriptions.InsertOne(ctx, sub); err != nil {
		return fmt.Errorf("insert thread subscription: %w", err)
	}
	return nil
}

// UpsertThreadSubscription inserts sub if no document exists for (threadRoomId, userAccount);
// otherwise it is a no-op. $setOnInsert ensures existing subscriptions are never overwritten.
func (s *threadStoreMongo) UpsertThreadSubscription(ctx context.Context, sub *model.ThreadSubscription) error {
	if err := s.subIndex.Ready(ctx); err != nil {
		return err
	}
	filter := bson.M{"threadRoomId": sub.ThreadRoomID, "userAccount": sub.UserAccount}
	update := bson.M{"$setOnInsert": sub}
	if _, err := s.threadSubscriptions.UpdateOne(ctx, filter, update, options.UpdateOne().SetUpsert(true)); err != nil {
		return fmt.Errorf("upsert thread subscription: %w", err)
	}
	return nil
}

// UpsertThreadSubscriptionAdvancingLastSeen creates the (threadRoomId, userAccount)
// subscription via $setOnInsert when missing and advances its lastSeenAt to at via $max,
// in one write. It folds a $setOnInsert upsert and AdvanceThreadSubscriptionLastSeen for
// the replier on the hot path. lastSeenAt is owned exclusively by $max and never appears
// under $setOnInsert, so the two operators do not conflict: a new subscription is seeded
// with lastSeenAt=at, an existing one only moves forward.
func (s *threadStoreMongo) UpsertThreadSubscriptionAdvancingLastSeen(ctx context.Context, sub *model.ThreadSubscription, at time.Time) error {
	if err := s.subIndex.Ready(ctx); err != nil {
		return err
	}
	filter := bson.M{"threadRoomId": sub.ThreadRoomID, "userAccount": sub.UserAccount}
	update := bson.M{
		"$setOnInsert": bson.M{
			"_id":             sub.ID,
			"parentMessageId": sub.ParentMessageID,
			"roomId":          sub.RoomID,
			"threadRoomId":    sub.ThreadRoomID,
			"userId":          sub.UserID,
			"userAccount":     sub.UserAccount,
			"siteId":          sub.SiteID,
			"hasMention":      sub.HasMention,
			"createdAt":       sub.CreatedAt,
			"updatedAt":       sub.UpdatedAt,
		},
		"$max": bson.M{"lastSeenAt": at},
	}
	if _, err := s.threadSubscriptions.UpdateOne(ctx, filter, update, options.UpdateOne().SetUpsert(true)); err != nil {
		if !mongo.IsDuplicateKeyError(err) {
			return fmt.Errorf("upsert thread subscription advancing lastSeen: %w", err)
		}
		// Lost the insert race to a concurrent reply by the same account in this thread:
		// both missed the filter, both tried to insert, and the unique
		// (threadRoomId, userAccount) index rejected this one. The subscription exists
		// now, so replay the $max alone — without it the reply would NAK and the
		// replier's lastSeenAt would ride on redelivery.
		res, raceErr := s.threadSubscriptions.UpdateOne(ctx, filter, bson.M{"$max": bson.M{"lastSeenAt": at}})
		if raceErr != nil {
			return fmt.Errorf("advance thread subscription lastSeen after upsert race: %w", raceErr)
		}
		// Nothing matched (threadRoomId, userAccount), so the duplicate came from some
		// other unique key — an _id already owned by an unrelated subscription, say — and
		// this is a genuine conflict rather than the race above. Swallowing it would drop
		// the write silently.
		if res.MatchedCount == 0 {
			return fmt.Errorf("upsert thread subscription advancing lastSeen: %w", err)
		}
	}
	return nil
}

// MarkThreadSubscriptionMention sets hasMention=true, skipping subs that already
// read past sub.CreatedAt (else this async write clobbers a read-clear, #467).
// New subs go via $setOnInsert on the upsert; existing ones get a separate
// guarded, non-upsert update, so an already-read sub can't be upserted twice.
func (s *threadStoreMongo) MarkThreadSubscriptionMention(ctx context.Context, sub *model.ThreadSubscription) error {
	if err := s.subIndex.Ready(ctx); err != nil {
		return err
	}
	filter := bson.M{"threadRoomId": sub.ThreadRoomID, "userAccount": sub.UserAccount}
	upsert := bson.M{
		"$setOnInsert": bson.M{
			"_id":             sub.ID,
			"parentMessageId": sub.ParentMessageID,
			"roomId":          sub.RoomID,
			"threadRoomId":    sub.ThreadRoomID,
			"userId":          sub.UserID,
			"userAccount":     sub.UserAccount,
			"siteId":          sub.SiteID,
			"lastSeenAt":      sub.LastSeenAt,
			"hasMention":      true,
			"createdAt":       sub.CreatedAt,
			"updatedAt":       sub.UpdatedAt,
		},
	}
	if _, err := s.threadSubscriptions.UpdateOne(ctx, filter, upsert, options.UpdateOne().SetUpsert(true)); err != nil {
		return fmt.Errorf("mark thread subscription mention: %w", err)
	}

	guardedFilter := bson.M{
		"threadRoomId": sub.ThreadRoomID,
		"userAccount":  sub.UserAccount,
		// $not/$gte (not $lt): $lt is type-bracketed and won't match a null
		// lastSeenAt (never-read sub), which must still be flagged.
		"lastSeenAt": bson.M{"$not": bson.M{"$gte": sub.CreatedAt}},
	}
	guardedSet := bson.M{"$set": bson.M{"hasMention": true, "updatedAt": sub.UpdatedAt}}
	if _, err := s.threadSubscriptions.UpdateOne(ctx, guardedFilter, guardedSet); err != nil {
		return fmt.Errorf("mark thread subscription mention (existing): %w", err)
	}
	return nil
}

func (s *threadStoreMongo) UpdateThreadRoomLastMessage(ctx context.Context, threadRoomID, lastMsgID string, replyAccounts []string, lastMsgAt time.Time) error {
	update := bson.M{
		"$set": bson.M{
			"lastMsgAt": lastMsgAt,
			"lastMsgId": lastMsgID,
			"updatedAt": lastMsgAt,
		},
	}
	if len(replyAccounts) > 0 {
		update["$addToSet"] = bson.M{"replyAccounts": bson.M{"$each": replyAccounts}}
	}
	if _, err := s.threadRooms.UpdateOne(ctx, bson.M{"_id": threadRoomID}, update); err != nil {
		return fmt.Errorf("update thread room last message: %w", err)
	}
	return nil
}

// AdvanceThreadSubscriptionLastSeen advances the replier's lastSeenAt via $max so it
// never regresses a replier who already read later; missing sub is a best-effort no-op.
func (s *threadStoreMongo) AdvanceThreadSubscriptionLastSeen(ctx context.Context, threadRoomID, account string, at time.Time) error {
	if _, err := s.threadSubscriptions.UpdateOne(ctx,
		bson.M{"threadRoomId": threadRoomID, "userAccount": account},
		bson.M{"$max": bson.M{"lastSeenAt": at}},
	); err != nil {
		return fmt.Errorf("advance thread lastSeenAt for %q in thread room %q: %w", account, threadRoomID, err)
	}
	return nil
}

// MarkParentSubscribed flips thread_rooms.parentSubscribed, the record that the
// parent author's subscription for this thread has been written. Idempotent: a
// re-run writes the same value.
func (s *threadStoreMongo) MarkParentSubscribed(ctx context.Context, threadRoomID string) error {
	_, err := s.threadRooms.UpdateOne(ctx, bson.M{"_id": threadRoomID}, bson.M{
		"$set": bson.M{"parentSubscribed": true},
	})
	if err != nil {
		return fmt.Errorf("mark parent subscribed on thread room %s: %w", threadRoomID, err)
	}
	return nil
}

func (s *threadStoreMongo) AddReplyAccounts(ctx context.Context, threadRoomID string, accounts []string) error {
	if len(accounts) == 0 {
		return nil
	}
	_, err := s.threadRooms.UpdateOne(ctx, bson.M{"_id": threadRoomID}, bson.M{
		"$addToSet": bson.M{"replyAccounts": bson.M{"$each": accounts}},
	})
	if err != nil {
		return fmt.Errorf("add reply accounts to thread room %s: %w", threadRoomID, err)
	}
	return nil
}

// AddThreadUnread marks parentMessageID unread for accounts' subscriptions in
// roomID via a single $addToSet UpdateMany. Idempotent under JetStream
// redelivery; accounts not subscribed simply match nothing.
func (s *threadStoreMongo) AddThreadUnread(ctx context.Context, roomID, parentMessageID string, accounts []string) error {
	if len(accounts) == 0 {
		return nil
	}
	if _, err := s.subscriptions.UpdateMany(ctx,
		bson.M{"roomId": roomID, "u.account": bson.M{"$in": accounts}},
		bson.M{"$addToSet": bson.M{"threadUnread": parentMessageID}},
	); err != nil {
		return fmt.Errorf("add thread unread %q in room %q: %w", parentMessageID, roomID, err)
	}
	return nil
}

func (s *threadStoreMongo) GetHistorySharedSince(ctx context.Context, roomID string, accounts []string) (map[string]*time.Time, error) {
	out := make(map[string]*time.Time, len(accounts))
	if len(accounts) == 0 {
		return out, nil
	}
	filter := bson.M{"roomId": roomID, "u.account": bson.M{"$in": accounts}}
	opts := options.Find().SetProjection(bson.M{"u.account": 1, "historySharedSince": 1, "_id": 0})
	cursor, err := s.subscriptions.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("query history windows for room %s: %w", roomID, err)
	}
	defer cursor.Close(ctx)
	// Minimal decode shape: the projection returns only u.account + historySharedSince,
	// so decode just those rather than the full model.SubscriptionUser (whose other
	// fields would silently be zero-valued).
	var rows []struct {
		User struct {
			Account string `bson:"account"`
		} `bson:"u"`
		HistorySharedSince *time.Time `bson:"historySharedSince"`
	}
	if err := cursor.All(ctx, &rows); err != nil {
		return nil, fmt.Errorf("decode history windows: %w", err)
	}
	for i := range rows {
		out[rows[i].User.Account] = rows[i].HistorySharedSince
	}
	return out, nil
}
