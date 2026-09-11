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

var (
	errThreadRoomExists   = errors.New("thread room already exists")
	errThreadRoomNotFound = errors.New("thread room not found")
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

func (s *threadStoreMongo) CreateThreadRoom(ctx context.Context, room *model.ThreadRoom) error {
	// The duplicate-key branch is the thread's identity guarantee, so the index is confirmed first;
	// a failure NAKs (see mongoutil.IndexGate). The subscriptions' key is confirmed here too: the room
	// insert is the point of no return for a first reply (a redelivery takes the subsequent-reply path).
	if err := s.parentIndex.Ready(ctx); err != nil {
		return err
	}
	if err := s.subIndex.Ready(ctx); err != nil {
		return err
	}
	toInsert := *room
	if toInsert.ReplyAccounts == nil {
		toInsert.ReplyAccounts = []string{}
	}
	_, err := s.threadRooms.InsertOne(ctx, &toInsert)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return fmt.Errorf("insert thread room: %w", errThreadRoomExists)
		}
		return fmt.Errorf("insert thread room: %w", err)
	}
	return nil
}

func (s *threadStoreMongo) GetThreadRoomByParentMessageID(ctx context.Context, parentMessageID string) (*model.ThreadRoom, error) {
	var room model.ThreadRoom
	if err := s.threadRooms.FindOne(ctx, bson.M{"parentMessageId": parentMessageID}).Decode(&room); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, fmt.Errorf("find thread room by parent %s: %w", parentMessageID, errThreadRoomNotFound)
		}
		return nil, fmt.Errorf("find thread room by parent %s: %w", parentMessageID, err)
	}
	return &room, nil
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
