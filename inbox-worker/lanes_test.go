package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

// stubLaneMsg is the minimum jetstream.Msg the lane dispatcher touches: it
// routes on Subject() and never disposes the message itself.
type stubLaneMsg struct{ subject string }

func (s stubLaneMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{NumDelivered: 1}, nil
}
func (s stubLaneMsg) Data() []byte                     { return nil }
func (s stubLaneMsg) Headers() nats.Header             { return nats.Header{} }
func (s stubLaneMsg) Subject() string                  { return s.subject }
func (s stubLaneMsg) Reply() string                    { return "" }
func (s stubLaneMsg) Ack() error                       { return nil }
func (s stubLaneMsg) DoubleAck(context.Context) error  { return nil }
func (s stubLaneMsg) Nak() error                       { return nil }
func (s stubLaneMsg) NakWithDelay(time.Duration) error { return nil }
func (s stubLaneMsg) InProgress() error                { return nil }
func (s stubLaneMsg) Term() error                      { return nil }
func (s stubLaneMsg) TermWithReason(string) error      { return nil }

// scriptedIter yields a fixed sequence and then reports a terminal error, which
// is how dispatchLanes is told to stop.
type scriptedIter struct {
	msgs []jetstream.Msg
	i    int
}

func (s *scriptedIter) Next(...jetstream.NextOpt) (context.Context, jetstream.Msg, error) {
	if s.i >= len(s.msgs) {
		return nil, nil, errors.New("iterator closed")
	}
	m := s.msgs[s.i]
	s.i++
	return context.Background(), m, nil
}

func TestMembershipLaneCap(t *testing.T) {
	tests := []struct {
		name          string
		maxAckPending int
		want          int
	}{
		{"a budget above the floor is honoured exactly", 4096, 4096},
		{"the repo-default budget is raised to the floor", 1000, membershipLaneFloor},
		{"a small budget is raised to the floor", 20, membershipLaneFloor},
		{"unlimited (-1) falls back to the floor", -1, membershipLaneFloor},
		{"unlimited (0) falls back to the floor", 0, membershipLaneFloor},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := membershipLaneCap(tt.maxAckPending)
			assert.Equal(t, tt.want, got)
			// The invariant the fix rests on: the lane is never smaller than the
			// delivery budget, so it cannot fill before the server stops
			// delivering. Without this the dispatcher can park on a full lane.
			assert.GreaterOrEqual(t, got, tt.maxAckPending,
				"lane must be at least the ack-pending budget")
			assert.GreaterOrEqual(t, got, membershipLaneFloor)
		})
	}
}

// TestDispatchLanes_MembershipBacklogDoesNotStallConcurrentLane is the
// regression guard for the head-of-line stall: a membership backlog that nobody
// is draining must not stop the pump that also feeds the concurrent lane.
//
// Every queued membership message is un-acked and so counts against the
// consumer's MaxAckPending budget. A lane sized at that budget therefore cannot
// fill before the server stops delivering, which is what keeps the send
// non-blocking.
func TestDispatchLanes_MembershipBacklogDoesNotStallConcurrentLane(t *testing.T) {
	const (
		siteID        = "site-a"
		maxAckPending = 20
		maxWorkers    = 4
	)

	msgs := make([]jetstream.Msg, 0, maxAckPending+1)
	for i := 0; i < maxAckPending; i++ {
		msgs = append(msgs, stubLaneMsg{subject: subject.InboxExternal(siteID, model.InboxMemberAdded)})
	}
	// The one message the concurrent lane must still receive.
	msgs = append(msgs, stubLaneMsg{subject: subject.InboxExternal(siteID, model.InboxSubscriptionRead)})

	concurrent := make(chan string, 1)
	process := func(m laneMsg) { concurrent <- m.msg.Subject() }

	// Deliberately never drained, standing in for a membership lane blocked on a
	// slow Mongo write.
	membershipCh := make(chan laneMsg, membershipLaneCap(maxAckPending))
	sem := make(chan struct{}, maxWorkers)
	var wg sync.WaitGroup

	done := make(chan struct{})
	go func() {
		defer close(done)
		dispatchLanes(&scriptedIter{msgs: msgs},
			func(subj string) bool { return isMembershipSubject(subj, siteID) },
			membershipCh, sem, &wg, process)
	}()

	select {
	case got := <-concurrent:
		assert.Equal(t, subject.InboxExternal(siteID, model.InboxSubscriptionRead), got,
			"the concurrent lane must keep flowing while membership is backed up")
	case <-time.After(2 * time.Second):
		t.Fatal("pump stalled: the concurrent lane was starved by an undrained membership backlog")
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("dispatchLanes did not return after the iterator reported a terminal error")
	}

	require.Len(t, membershipCh, maxAckPending, "every membership message should be queued, none dropped")
	wg.Wait()
}

// TestDispatchLanes_RoutesBySubject pins the split itself: membership goes to
// the sequential lane, everything else to the worker pool.
func TestDispatchLanes_RoutesBySubject(t *testing.T) {
	const siteID = "site-a"

	tests := []struct {
		name          string
		eventType     string
		wantSequenced bool
	}{
		{"member added is sequenced", model.InboxMemberAdded, true},
		{"member removed is sequenced", model.InboxMemberRemoved, true},
		{"subscription read is concurrent", model.InboxSubscriptionRead, false},
		{"role updated is concurrent", model.InboxRoleUpdated, false},
		{"room renamed is concurrent", model.InboxRoomRenamed, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subj := subject.InboxExternal(siteID, tt.eventType)
			processed := make(chan string, 1)

			membershipCh := make(chan laneMsg, 1)
			sem := make(chan struct{}, 1)
			var wg sync.WaitGroup

			dispatchLanes(&scriptedIter{msgs: []jetstream.Msg{stubLaneMsg{subject: subj}}},
				func(s string) bool { return isMembershipSubject(s, siteID) },
				membershipCh, sem, &wg, func(m laneMsg) { processed <- m.msg.Subject() })
			wg.Wait()

			if tt.wantSequenced {
				require.Len(t, membershipCh, 1, "expected the message on the sequential lane")
				assert.Empty(t, processed, "a sequenced message must not run on the worker pool")
				return
			}
			require.Empty(t, membershipCh, "a concurrent message must not be sequenced")
			select {
			case got := <-processed:
				assert.Equal(t, subj, got)
			case <-time.After(2 * time.Second):
				t.Fatal("concurrent message was never processed")
			}
		})
	}
}
