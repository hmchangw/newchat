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
	// parentIndex gates CreateThreadRoom on thread_rooms.parentMessageId being
	// confirmed unique, and subIndex gates the thread_subscriptions writes that
	// create documents on (threadRoomId, userAccount); see indexGate.
	parentIndex *indexGate
	subIndex    *indexGate
}

// Compile-time assertion that *threadStoreMongo satisfies ThreadStore.
var _ ThreadStore = (*threadStoreMongo)(nil)

func newThreadStoreMongo(db *mongo.Database) *threadStoreMongo {
	threadRooms := db.Collection("thread_rooms")
	threadSubscriptions := db.Collection("thread_subscriptions")
	// Same specs as room-service, the owner, so the idempotent creates cannot
	// conflict. mongoutil.EnsureIndex is non-destructive, so a same-keys index
	// with a different spec closes the gate (NAK) until room-service repairs it.
	return &threadStoreMongo{
		threadRooms:         threadRooms,
		threadSubscriptions: threadSubscriptions,
		subscriptions:       db.Collection("subscriptions"),
		parentIndex:         uniqueIndexGate(threadRooms, bson.D{{Key: "parentMessageId", Value: 1}}),
		subIndex: uniqueIndexGate(threadSubscriptions,
			bson.D{{Key: "threadRoomId", Value: 1}, {Key: "userAccount", Value: 1}}),
	}
}

// uniqueIndexGate gates on a unique index over keys on coll, created
// non-destructively.
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

// indexGate confirms an index once, on demand, and retries until it does. A
// degraded start means the startup EnsureIndexes can fail and the worker keeps
// running; when MongoDB returns, parked replies resume before room-service —
// still crashlooping, then backing off — has restarted and built the key. A
// write that relies on the constraint therefore asks the gate first: it pays
// one CreateOne on the first call after recovery (a no-op when the index is
// already there) and an atomic load thereafter. A failed ensure is returned to
// the caller, whose NAK retries the message later, so no insert ever runs
// against an unconfirmed constraint.
//
// Callers that arrive while an attempt is in flight share its result instead
// of each repeating the round trip before they can NAK. A failure is then
// remembered for a backoff interval (indexRetryMin doubling to indexRetryMax):
// callers inside it get the remembered error and only the first caller after
// it probes again, so duplicate data that no index build can get past does
// not turn every write and NAK redelivery into another collection-wide
// createIndexes. The attempt runs under its own deadline — the caller's when
// that is sooner, so startup's shared budget is honoured across both gates,
// else mongoutil.IndexEnsureTimeout — detached from the caller's cancellation, so a
// MongoDB that answers server selection but stalls the command cannot hold
// every reply behind one open-ended call, and one cancelled caller cannot fail
// the attempt its waiters share. A waiter whose own context ends (a draining
// consumer) is released with its context error while the attempt runs on for
// the rest.
type indexGate struct {
	name   string // names the index in errors
	ensure func(context.Context) error
	now    func() time.Time
	flight singleflight.Group
	ready  atomic.Bool

	// mu guards the remembered failure: the attempt writes it inside the
	// flight, while Ready's fast path reads it from any caller.
	mu      sync.Mutex
	lastErr error
	retryAt time.Time
	backoff time.Duration
}

// indexRetryMin and indexRetryMax bound the backoff between probes after a
// failed ensure.
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

// wait joins or leads one attempt, or returns the remembered failure while its
// retry-after has not elapsed (checked before the flight too, so a write during
// the window does not start a goroutine only to read it).
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

// remember records a failure and schedules the next probe, doubling the
// interval up to indexRetryMax.
func (g *indexGate) remember(err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.backoff = min(max(2*g.backoff, indexRetryMin), indexRetryMax)
	g.lastErr = err
	g.retryAt = g.now().Add(g.backoff)
}

// attemptContext detaches the caller's cancellation and bounds the attempt by
// the caller's deadline when that is sooner than mongoutil.IndexEnsureTimeout.
func (g *indexGate) attemptContext(ctx context.Context) (context.Context, context.CancelFunc) {
	// Wall clock, not g.now: the deadline is enforced by the driver's timers,
	// while g.now only paces the retry-after (and is faked in tests).
	deadline := time.Now().Add(mongoutil.IndexEnsureTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	return context.WithDeadline(context.WithoutCancel(ctx), deadline)
}

// EnsureIndexes asserts the unique constraints this store's writes depend on.
// Both are owned by room-service, which fails fast when MongoDB is unreachable
// and so is the service relied on to assert them; this one starts degraded, so
// its own attempt is skipped exactly when an outage makes that most likely.
//
// thread_rooms.parentMessageId is also created here, with the identical spec
// (idempotent): on a fresh site nothing orders room-service's startup before
// the first thread reply, and CreateThreadRoom's duplicate-key branch is the
// only thing keeping a second reply from opening a second thread room — a hole
// that, once two rows land, no later index build can close without a manual
// dedupe. The rule is that a degradable service must not be the SOLE creator.
// This startup call is best-effort; the writes re-ask the same gates, so a
// failure here (MongoDB down) only defers the confirmation to the first write
// after recovery. thread_subscriptions gets the same treatment: without its
// unique key two concurrent upserts on one (threadRoomId, userAccount) both
// insert, and the duplicates then make the owner's later index build fail.
func (s *threadStoreMongo) EnsureIndexes(ctx context.Context) error {
	if err := s.parentIndex.Ready(ctx); err != nil {
		return err
	}
	return s.subIndex.Ready(ctx)
}

func (s *threadStoreMongo) CreateThreadRoom(ctx context.Context, room *model.ThreadRoom) error {
	// The duplicate-key branch below is the thread's identity guarantee, so the
	// index is confirmed before the insert; a failure NAKs (see indexGate).
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
