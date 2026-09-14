package read

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/tools/loadgen/internal/soak/catalog"
)

type soakRPCFakeReply struct {
	data []byte
	err  error
}

type soakRecordingSleeper struct {
	delays []time.Duration
}

func (s *soakRecordingSleeper) Sleep(
	ctx context.Context,
	delay time.Duration,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.delays = append(s.delays, delay)
	return nil
}

type fakeSoakClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeSoakClock(now time.Time) *fakeSoakClock {
	return &fakeSoakClock{now: now}
}

func (c *fakeSoakClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeSoakClock) Advance(duration time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(duration)
}

func acceptedMutationMessage(
	t *testing.T,
	clock *fakeSoakClock,
	messageID string,
	author string,
) *catalog.Catalog {
	t.Helper()
	messageCatalog := catalog.New(16, 100, 0, clock)
	acceptMutationCatalogMessage(
		t,
		messageCatalog,
		clock,
		messageID,
		author,
	)
	return messageCatalog
}

func acceptMutationCatalogMessage(
	t *testing.T,
	messageCatalog *catalog.Catalog,
	clock *fakeSoakClock,
	messageID string,
	author string,
) {
	t.Helper()
	require.NoError(t, messageCatalog.TrackPublished(&catalog.Candidate{
		ID: messageID, RoomID: "room-1", Author: author, Content: "original",
		CreatedAt: clock.Now(), ThreadReplyLimit: 10,
	}))
	require.True(t, messageCatalog.Accept("room-1", messageID))
	clock.Advance(time.Millisecond)
}
