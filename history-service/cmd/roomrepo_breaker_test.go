package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/history-service/internal/mongorepo"
	"github.com/hmchangw/chat/pkg/circuitbreaker"
)

type stubRoomRepo struct {
	err   error
	calls int
}

func (s *stubRoomRepo) GetRoomTimes(context.Context, string) (time.Time, time.Time, error) {
	s.calls++
	return time.Time{}, time.Time{}, s.err
}

func (s *stubRoomRepo) GetMinUserLastSeenAt(context.Context, string) (*time.Time, error) {
	s.calls++
	return nil, s.err
}

func (s *stubRoomRepo) GetRoomTimesByIDs(context.Context, []string) (map[string]mongorepo.RoomTimes, error) {
	s.calls++
	return nil, s.err
}

func (s *stubRoomRepo) GetRoomUserCount(context.Context, string) (int, error) {
	s.calls++
	return 0, s.err
}

func (s *stubRoomRepo) SetPreviewMessage(context.Context, string, models.PreviewMessage, string, int64) error {
	s.calls++
	return s.err
}

func (s *stubRoomRepo) UpdatePreviewBody(context.Context, string, models.PreviewMessage, string, int64) (bool, error) {
	s.calls++
	return false, s.err
}

func (s *stubRoomRepo) ClearPreview(context.Context, string, int64) (bool, error) {
	s.calls++
	return false, s.err
}

func (s *stubRoomRepo) InvalidatePreviewKey(context.Context, string, string, int64) error {
	s.calls++
	return s.err
}

// The whole point of fencing these reads: a stopped Mongo blocks rather than
// erroring, so without a breaker the first read consumes the request budget and
// the fail-open downstream never runs. Once the breaker is open the read must
// return immediately and never touch Mongo.
func TestBreakerRoomRepo_OpenBreakerFailsFastWithoutCallingMongo(t *testing.T) {
	inner := &stubRoomRepo{err: errors.New("mongo unreachable")}
	b := circuitbreaker.New(1, time.Minute, circuitbreaker.WithFailurePredicate(roomBreakerFailure))
	repo := newBreakerRoomRepo(inner, b)

	_, _, err := repo.GetRoomTimes(context.Background(), "r1")
	require.Error(t, err)
	require.Equal(t, circuitbreaker.StateOpen, b.State())
	before := inner.calls

	_, _, err = repo.GetRoomTimes(context.Background(), "r1")
	assert.ErrorIs(t, err, circuitbreaker.ErrOpen)
	assert.Equal(t, before, inner.calls, "an open breaker must not reach Mongo")

	_, err = repo.GetMinUserLastSeenAt(context.Background(), "r1")
	assert.ErrorIs(t, err, circuitbreaker.ErrOpen)
	assert.Equal(t, before, inner.calls)
}

// A room that does not exist is a healthy answer, and history-service turns it
// into a 404. Spending the failure budget on it would let requests for deleted
// rooms open the breaker and degrade every other room's read.
func TestBreakerRoomRepo_NotFoundDoesNotTripBreaker(t *testing.T) {
	inner := &stubRoomRepo{err: mongo.ErrNoDocuments}
	b := circuitbreaker.New(2, time.Minute, circuitbreaker.WithFailurePredicate(roomBreakerFailure))
	repo := newBreakerRoomRepo(inner, b)

	for range 5 {
		_, _, err := repo.GetRoomTimes(context.Background(), "gone")
		require.ErrorIs(t, err, mongo.ErrNoDocuments, "not-found must still reach the caller")
	}
	assert.Equal(t, circuitbreaker.StateClosed, b.State())
}

// Every optional preview write goes through the same fence. Each one runs on a
// request whose real answer is already in hand — a resolved page, an acked
// mutation — so a stalled write costs the caller the reply it had earned. A
// table rather than four tests: what is being pinned is that none of them is
// missing from the wrapper, which is exactly the mistake a per-method test
// would not catch.
func TestBreakerRoomRepo_PreviewWritesFailFastWhenOpen(t *testing.T) {
	writes := map[string]func(*breakerRoomRepo) error{
		"SetPreviewMessage": func(r *breakerRoomRepo) error {
			return r.SetPreviewMessage(context.Background(), "r1", models.PreviewMessage{}, "m1", 1)
		},
		"UpdatePreviewBody": func(r *breakerRoomRepo) error {
			_, err := r.UpdatePreviewBody(context.Background(), "r1", models.PreviewMessage{}, "m1", 1)
			return err
		},
		"ClearPreview": func(r *breakerRoomRepo) error {
			_, err := r.ClearPreview(context.Background(), "r1", 1)
			return err
		},
		"InvalidatePreviewKey": func(r *breakerRoomRepo) error {
			return r.InvalidatePreviewKey(context.Background(), "r1", "m1", 1)
		},
	}
	for name, write := range writes {
		t.Run(name, func(t *testing.T) {
			inner := &stubRoomRepo{err: errors.New("mongo unreachable")}
			b := circuitbreaker.New(1, time.Minute, circuitbreaker.WithFailurePredicate(roomBreakerFailure))
			repo := newBreakerRoomRepo(inner, b)

			require.Error(t, write(repo))
			require.Equal(t, circuitbreaker.StateOpen, b.State(), "a failed write must count against the fence")
			before := inner.calls

			assert.ErrorIs(t, write(repo), circuitbreaker.ErrOpen)
			assert.Equal(t, before, inner.calls, "an open breaker must not reach Mongo")
		})
	}
}
