package main

import (
	"sync"

	"github.com/hmchangw/chat/pkg/natsmetrics"
)

// membershipLaneFloor bounds the sequential lane when MaxAckPending carries no
// usable number — unlimited (-1 or 0, which JetStream normalizes to unlimited)
// or a budget so small that a buffer sized from it would fill on a trivial
// burst. It is a memory bound, not a throughput one: laneMsg is two words plus
// the message the server has already delivered to this process.
const membershipLaneFloor = 1024

// membershipLaneCap sizes the sequential membership lane so the dispatcher can
// never block on it.
//
// Every message sitting in that buffer is un-acked, so it counts against the
// consumer's MaxAckPending budget. Sizing the buffer at the budget means the
// server stops delivering before the channel can fill — the send in
// dispatchLanes is therefore non-blocking in every reachable state, and a slow
// membership write can no longer stop the pump that also feeds the concurrent
// lane. Backpressure still exists; it just lands on iter.Next(), where having
// nothing to hand the concurrent lane is the truth rather than an artifact.
func membershipLaneCap(maxAckPending int) int {
	if maxAckPending < membershipLaneFloor {
		return membershipLaneFloor
	}
	return maxAckPending
}

// dispatchLanes pumps iter and routes each message to one of two lanes:
// membership events onto the caller's sequential channel, everything else onto
// a bounded worker pool. It returns when the iterator reports a terminal error,
// closing membershipCh so the sequential drainer can finish.
//
// The membership send is deliberately a plain send rather than a select with a
// default: dropping or NAKing on a full lane would reorder add/remove for the
// same (room, account), which is the one thing the sequential lane exists to
// prevent. It is safe to block on because membershipLaneCap sizes the channel
// past the delivery budget — see that function.
func dispatchLanes(
	iter natsmetrics.Iterator,
	isMembership func(subject string) bool,
	membershipCh chan<- laneMsg,
	sem chan struct{},
	wg *sync.WaitGroup,
	process func(laneMsg),
) {
	defer close(membershipCh)
	for {
		msgCtx, msg, err := iter.Next()
		if err != nil {
			return
		}
		m := laneMsg{ctx: msgCtx, msg: msg}
		if isMembership(msg.Subject()) {
			membershipCh <- m
			continue
		}
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer func() {
				<-sem
				wg.Done()
			}()
			process(m)
		}()
	}
}
