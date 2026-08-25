package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

func TestRemotePeers(t *testing.T) {
	tests := []struct {
		name string
		self string
		all  []string
		want []string
	}{
		{name: "drops self", self: "a", all: []string{"a", "b", "c"}, want: []string{"b", "c"}},
		{name: "single site has no peers", self: "a", all: []string{"a"}, want: nil},
		{name: "unset disables", self: "a", all: nil, want: nil},
		{name: "tolerates blanks and self repeated", self: "a", all: []string{"a", "", "b", "a"}, want: []string{"b"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, remotePeers(tt.self, tt.all))
		})
	}
}

type capturePublisher struct {
	subjects []string
	payloads [][]byte
	err      error
}

func (c *capturePublisher) Publish(_ context.Context, subj string, data []byte) error {
	if c.err != nil {
		return c.err
	}
	c.subjects = append(c.subjects, subj)
	c.payloads = append(c.payloads, data)
	return nil
}

func TestRoomActivityPublisher_OneSmallEventPerPeer(t *testing.T) {
	pub := &capturePublisher{}
	send := roomActivityPublisher(pub, "site-a", []string{"site-b", "site-c"})

	at := time.UnixMilli(1740000000000).UTC()
	require.NoError(t, send(context.Background(), roomActivityRefresh{roomID: "r1", at: at}))

	// One message per destination, each carrying a single room — payload size is
	// bounded by construction, so no batch can outgrow max_payload.
	require.Len(t, pub.subjects, 2)
	assert.Equal(t, subject.RoomActivity("site-b"), pub.subjects[0])
	assert.Equal(t, subject.RoomActivity("site-c"), pub.subjects[1])

	var evt model.RoomActivityEvent
	require.NoError(t, json.Unmarshal(pub.payloads[0], &evt))
	assert.Equal(t, "r1", evt.RoomID)
	assert.Equal(t, "site-a", evt.SiteID, "the room's home site, so the destination can attribute it")
	assert.Equal(t, at.UnixMilli(), evt.LastMsgAt)
	assert.NotZero(t, evt.Timestamp)
}

func TestRoomActivityPublisher_PropagatesPublishError(t *testing.T) {
	send := roomActivityPublisher(&capturePublisher{err: errors.New("no route")}, "site-a", []string{"site-b"})
	require.Error(t, send(context.Background(), roomActivityRefresh{roomID: "r1", at: time.Now()}))
}

// fakeActivityPublisher records the refreshes the refresher emits.
type fakeActivityPublisher struct {
	mu    sync.Mutex
	sent  []roomActivityRefresh
	fails bool
}

func (f *fakeActivityPublisher) publish(_ context.Context, r roomActivityRefresh) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fails {
		return errors.New("publish failed")
	}
	f.sent = append(f.sent, r)
	return nil
}

func (f *fakeActivityPublisher) all() []roomActivityRefresh {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]roomActivityRefresh(nil), f.sent...)
}

func TestRoomActivityRefresher_CrossSiteRoomsOnly(t *testing.T) {
	yes, no := true, false
	tests := []struct {
		name      string
		crossSite *bool
		want      int
	}{
		{name: "cross-site room refreshes", crossSite: &yes, want: 1},
		{name: "same-site room does not", crossSite: &no, want: 0},
		// The cost of a wrong guess is one wasted publish, and the destination
		// ignores rooms it holds no subscription for — so unclassified refreshes.
		{name: "unclassified refreshes", crossSite: nil, want: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pub := &fakeActivityPublisher{}
			r := newRoomActivityRefresher(pub.publish, 0)

			at := time.Unix(1700000000, 0).UTC()
			r.refresh(context.Background(), "r1", tt.crossSite, at)

			assert.Len(t, pub.all(), tt.want)
		})
	}
}

// A single-site deployment leaves the refresher nil; calling into it must be a
// no-op rather than a panic on the fan-out path.
func TestRoomActivityRefresher_NilIsInert(t *testing.T) {
	var r *roomActivityRefresher
	assert.NotPanics(t, func() {
		r.refresh(context.Background(), "r1", nil, time.Now())
	})
}

func TestRoomActivityRefresher_ThrottlesPerRoom(t *testing.T) {
	pub := &fakeActivityPublisher{}
	r := newRoomActivityRefresher(pub.publish, 5*time.Second)
	clock := time.Unix(1700000000, 0).UTC()
	r.now = func() time.Time { return clock }

	r.refresh(context.Background(), "r-x", nil, clock)
	require.Len(t, pub.all(), 1)

	// Nineteen more messages, landing at 4.75s — still inside the interval. The
	// room's position keeps moving, but it must not be re-announced.
	for range 19 {
		clock = clock.Add(250 * time.Millisecond)
		r.refresh(context.Background(), "r-x", nil, clock)
	}
	assert.Len(t, pub.all(), 1, "throttled within the refresh interval")

	clock = clock.Add(5 * time.Second)
	r.refresh(context.Background(), "r-x", nil, clock)
	sent := pub.all()
	require.Len(t, sent, 2, "publishes again once the interval elapses")
	assert.Equal(t, clock, sent[1].at, "and carries the latest position, not one it skipped")
}

func TestRoomActivityRefresher_ThrottlesRoomsIndependently(t *testing.T) {
	pub := &fakeActivityPublisher{}
	r := newRoomActivityRefresher(pub.publish, 5*time.Second)
	clock := time.Unix(1700000000, 0).UTC()
	r.now = func() time.Time { return clock }

	r.refresh(context.Background(), "r-a", nil, clock)

	// r-b is newly active — its own watermark is unset, so it publishes even
	// though r-a is mid-interval.
	clock = clock.Add(250 * time.Millisecond)
	r.refresh(context.Background(), "r-a", nil, clock)
	r.refresh(context.Background(), "r-b", nil, clock)

	var rooms []string
	for _, s := range pub.all() {
		rooms = append(rooms, s.roomID)
	}
	assert.Equal(t, []string{"r-a", "r-b"}, rooms)
}

func TestRoomActivityRefresher_ZeroIntervalPublishesEveryMessage(t *testing.T) {
	pub := &fakeActivityPublisher{}
	r := newRoomActivityRefresher(pub.publish, 0) // throttling disabled

	for range 3 {
		r.refresh(context.Background(), "r-x", nil, time.Now())
	}
	assert.Len(t, pub.all(), 3)
}

func TestRoomActivityRefresher_ForgetsWatermarksForQuietRooms(t *testing.T) {
	// The watermark map must stay bounded by rooms active within the interval,
	// not grow with every room ever seen.
	pub := &fakeActivityPublisher{}
	r := newRoomActivityRefresher(pub.publish, time.Second)
	clock := time.Unix(1700000000, 0).UTC()
	r.now = func() time.Time { return clock }

	for i := range 100 {
		r.refresh(context.Background(), fmt.Sprintf("r-%d", i), nil, clock)
	}
	assert.Len(t, r.lastRefreshed, 100)

	// Those rooms go quiet; the next announce past the interval prunes them.
	clock = clock.Add(10 * time.Second)
	r.refresh(context.Background(), "r-live", nil, clock)
	assert.Len(t, r.lastRefreshed, 1, "only the still-active room retains a watermark")
}

// The announce is decorative and self-healing on the room's next message. A
// publish failure must not surface as an error: this runs after the message has
// already been broadcast to every client, so returning would NAK a delivered
// message and re-broadcast it.
func TestRoomActivityRefresher_PublishFailureIsSwallowed(t *testing.T) {
	r := newRoomActivityRefresher((&fakeActivityPublisher{fails: true}).publish, 0)
	assert.NotPanics(t, func() {
		r.refresh(context.Background(), "r-x", nil, time.Now())
	})
}

// The refresher now sits on the concurrent fan-out path, where the coalescer's
// single-threaded flush used to shield the watermark map.
func TestRoomActivityRefresher_ConcurrentRefreshIsThreadSafe(t *testing.T) {
	pub := &fakeActivityPublisher{}
	r := newRoomActivityRefresher(pub.publish, time.Minute)

	var wg sync.WaitGroup
	for i := range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.refresh(context.Background(), fmt.Sprintf("r-%d", i%10), nil, time.Now())
		}()
	}
	wg.Wait()

	// Ten distinct rooms, each throttled to one announce inside the interval.
	assert.Len(t, pub.all(), 10)
}
