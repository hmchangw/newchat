package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func fakeStreamInfo(msgs, lastSeq uint64, subjects map[string]uint64) *jetstream.StreamInfo {
	return &jetstream.StreamInfo{State: jetstream.StreamState{
		Msgs: msgs, Bytes: msgs * 100, FirstSeq: 1, LastSeq: lastSeq, Subjects: subjects,
	}}
}

func TestStatsPoller_TickAndRate(t *testing.T) {
	seq := uint64(100)
	si := func(ctx context.Context) (*jetstream.StreamInfo, error) {
		return fakeStreamInfo(seq, seq, map[string]uint64{"chat.migration.oplog.s1.c.insert": seq}), nil
	}
	ci := func(ctx context.Context, name string) (*jetstream.ConsumerInfo, error) {
		return &jetstream.ConsumerInfo{NumPending: 7, NumAckPending: 2}, nil
	}
	var ticks []StreamStats
	p := newStatsPoller("MIGRATION-OPLOG-s1", si, ci, []string{"transformer"}, time.Millisecond,
		func() bool { return true }, func(s StreamStats) { ticks = append(ticks, s) })

	// drive three polls manually via the exported-for-test pollOnce
	p.pollOnce(context.Background(), time.Unix(0, 0))
	seq = 200
	p.pollOnce(context.Background(), time.Unix(10, 0))
	seq = 300
	p.pollOnce(context.Background(), time.Unix(20, 0))

	require.Len(t, ticks, 3)
	assert.Equal(t, float64(0), ticks[0].RatePerSec)
	assert.InDelta(t, 10.0, ticks[2].RatePerSec, 0.001) // (300-100)/20s
	assert.Equal(t, uint64(300), ticks[2].Msgs)
	require.Len(t, ticks[2].Consumers, 1)
	assert.Equal(t, uint64(7), ticks[2].Consumers[0].NumPending)
	assert.True(t, ticks[2].WatcherLive)
	assert.Equal(t, ticks[2], p.Last())
}

func TestStatsPoller_PollErrorKeepsGoing(t *testing.T) {
	calls := 0
	si := func(ctx context.Context) (*jetstream.StreamInfo, error) {
		calls++
		if calls == 1 {
			return nil, fmt.Errorf("nats down")
		}
		return fakeStreamInfo(1, 1, nil), nil
	}
	var ticks []StreamStats
	p := newStatsPoller("S", si, nil, nil, time.Millisecond, func() bool { return false },
		func(s StreamStats) { ticks = append(ticks, s) })
	p.pollOnce(context.Background(), time.Unix(0, 0))
	p.pollOnce(context.Background(), time.Unix(5, 0))
	require.Len(t, ticks, 2)
	assert.Contains(t, ticks[0].Error, "nats down")
	assert.Empty(t, ticks[1].Error)
}

func TestStatsPoller_SequenceResetClearsWindow(t *testing.T) {
	seq := uint64(1000)
	si := func(ctx context.Context) (*jetstream.StreamInfo, error) {
		return fakeStreamInfo(seq, seq, nil), nil
	}
	var last StreamStats
	p := newStatsPoller("S", si, nil, nil, time.Millisecond, func() bool { return true },
		func(s StreamStats) { last = s })
	p.pollOnce(context.Background(), time.Unix(0, 0))
	seq = 5 // purge
	p.pollOnce(context.Background(), time.Unix(10, 0))
	assert.Equal(t, float64(0), last.RatePerSec)
}

func TestStatsPoller_ConsumerError(t *testing.T) {
	si := func(ctx context.Context) (*jetstream.StreamInfo, error) { return fakeStreamInfo(1, 1, nil), nil }
	ci := func(ctx context.Context, name string) (*jetstream.ConsumerInfo, error) {
		return nil, fmt.Errorf("no such consumer")
	}
	var last StreamStats
	p := newStatsPoller("S", si, ci, []string{"ghost"}, time.Millisecond, func() bool { return true },
		func(s StreamStats) { last = s })
	p.pollOnce(context.Background(), time.Unix(0, 0))
	require.Len(t, last.Consumers, 1)
	assert.Contains(t, last.Consumers[0].Error, "no such consumer")
}
