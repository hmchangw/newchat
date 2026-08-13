package main

import (
	"context"
	"errors"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/history-service/internal/mongorepo"
	"github.com/hmchangw/chat/history-service/internal/service"
	"github.com/hmchangw/chat/pkg/circuitbreaker"
)

// breakerRoomRepo fences every room-metadata read behind a circuit breaker.
//
// These reads already fail open — resolveRoomTimesOrError falls back to
// now/floor, readFloorInto logs and yields nil — but a fail-open only helps if
// the read actually FAILS. A stopped MongoDB does not return an error: it
// blocks on server selection, so without this fence the read consumes the
// entire request budget, the handler's deadline expires, and the fallback never
// runs. The symptom surfaces downstream, on whichever call inherits the dead
// context (typically the Cassandra page read).
//
// The breaker is what converts "hangs" into "errors quickly", which is the
// precondition for every fail-open on this path.
type breakerRoomRepo struct {
	inner   service.RoomRepository
	breaker *circuitbreaker.Breaker
}

func newBreakerRoomRepo(inner service.RoomRepository, b *circuitbreaker.Breaker) *breakerRoomRepo {
	return &breakerRoomRepo{inner: inner, breaker: b}
}

// roomBreakerFailure is the failure predicate this breaker must be built with.
// A missing room is a healthy answer that history-service turns into a 404;
// counting it would let requests for deleted rooms open the breaker and degrade
// every other room's read.
func roomBreakerFailure(err error) bool {
	return err != nil && !errors.Is(err, mongo.ErrNoDocuments)
}

func (r *breakerRoomRepo) GetRoomTimes(ctx context.Context, roomID string) (lastMsgAt, createdAt time.Time, err error) {
	var l, c time.Time
	err = r.breaker.Do(func() error {
		var e error
		l, c, e = r.inner.GetRoomTimes(ctx, roomID)
		return e
	})
	return l, c, err
}

func (r *breakerRoomRepo) GetMinUserLastSeenAt(ctx context.Context, roomID string) (*time.Time, error) {
	return circuitbreaker.Do1(r.breaker, func() (*time.Time, error) {
		return r.inner.GetMinUserLastSeenAt(ctx, roomID)
	})
}

func (r *breakerRoomRepo) GetRoomTimesByIDs(ctx context.Context, ids []string) (map[string]mongorepo.RoomTimes, error) {
	return circuitbreaker.Do1(r.breaker, func() (map[string]mongorepo.RoomTimes, error) {
		return r.inner.GetRoomTimesByIDs(ctx, ids)
	})
}

func (r *breakerRoomRepo) GetRoomUserCount(ctx context.Context, roomID string) (int, error) {
	return circuitbreaker.Do1(r.breaker, func() (int, error) {
		return r.inner.GetRoomUserCount(ctx, roomID)
	})
}
