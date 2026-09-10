package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"golang.org/x/sync/singleflight"

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
	// parentIndex and subIndex gate the document-creating writes on their unique index; see indexGate.
	parentIndex *indexGate
	subIndex    *indexGate
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
		parentIndex:         uniqueIndexGate(threadRooms, bson.D{{Key: "parentMessageId", Value: 1}}),
		subIndex: uniqueIndexGate(threadSubscriptions,
			bson.D{{Key: "threadRoomId", Value: 1}, {Key: "userAccount", Value: 1}}),
	}
}

// uniqueIndexGate gates on a unique index over keys on coll, created non-destructively.
func uniqueIndexGate(coll *mongo.Collection, keys bson.D) *indexGate {
	fields := make([]string, 0, len(keys))
	for _, k := range keys {
		fields = append(fields, k.Key)
	}
	name := coll.Name() + " (" + strings.Join(fields, ",") + ")"
	return newIndexGate(name, func(ctx context.Context) error {
		return mongoutil.EnsureIndex(ctx, coll, mongo.IndexModel{
			Keys:    keys,
			Options: options.Index().SetUnique(true),
		})
	})
}

// indexGate confirms an index on demand, so a degraded worker that resumes before room-service has
// rebuilt the key NAKs instead of inserting. All callers share one bounded, detached attempt (see wait).
type indexGate struct {
	name   string // names the index in errors
	ensure func(context.Context) error
	now    func() time.Time
	flight singleflight.Group
	ready  atomic.Bool

	// mu guards the remembered failure: written inside the flight, read by any caller's fast path.
	mu      sync.Mutex
	lastErr error
	retryAt time.Time
	backoff time.Duration
}

// indexRetryMin and indexRetryMax bound the backoff between probes after a failed ensure.
const (
	indexRetryMin = 5 * time.Second
	indexRetryMax = time.Minute
)

func newIndexGate(name string, ensure func(context.Context) error) *indexGate {
	return &indexGate{name: name, ensure: ensure, now: time.Now}
}

// Ready returns nil once the index is confirmed, ensuring it if needed.
func (g *indexGate) Ready(ctx context.Context) error {
	if g.ready.Load() {
		return nil
	}
	if err := g.wait(ctx); err != nil {
		return fmt.Errorf("confirm unique index %s: %w", g.name, err)
	}
	return nil
}

// wait returns the remembered failure inside its retry-after, else joins or leads one attempt. A waiter
// whose ctx ends is released while the attempt runs on for the rest, so one cancelled caller fails nobody.
func (g *indexGate) wait(ctx context.Context) error {
	if err := g.remembered(); err != nil {
		return err
	}
	results := g.flight.DoChan("ensure", func() (any, error) {
		if g.ready.Load() {
			return nil, nil
		}
		if err := g.remembered(); err != nil {
			return nil, err
		}
		ensureCtx, cancel := g.attemptContext(ctx)
		defer cancel()
		if err := g.ensure(ensureCtx); err != nil {
			g.remember(err)
			return nil, err
		}
		g.ready.Store(true)
		return nil, nil
	})
	select {
	case r := <-results:
		return r.Err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// remembered returns the last failure while its retry-after has not elapsed.
func (g *indexGate) remembered() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.lastErr != nil && g.now().Before(g.retryAt) {
		return g.lastErr
	}
	return nil
}

// remember records a failure and schedules the next probe, doubling the interval up to indexRetryMax,
// so duplicate data no index build gets past cannot turn every NAK redelivery into a createIndexes.
func (g *indexGate) remember(err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.backoff = min(max(2*g.backoff, indexRetryMin), indexRetryMax)
	g.lastErr = err
	g.retryAt = g.now().Add(g.backoff)
}

// attemptContext detaches the caller's cancellation; deadline = min(caller's, now+IndexEnsureTimeout).
func (g *indexGate) attemptContext(ctx context.Context) (context.Context, context.CancelFunc) {
	// Wall clock, not g.now: the driver enforces this deadline; g.now only paces the retry-after.
	deadline := time.Now().Add(mongoutil.IndexEnsureTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	return context.WithDeadline(context.WithoutCancel(ctx), deadline)
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
	// The duplicate-key branch is the thread's identity guarantee, so the index is confirmed first.
	if err := s.parentIndex.Ready(ctx); err != nil {
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
