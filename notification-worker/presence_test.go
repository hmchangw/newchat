package main

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errnats"
	"github.com/hmchangw/chat/pkg/model"
)

func TestNoopPresence_EmptySnapshot(t *testing.T) {
	p := noopPresenceSnapshotter{}
	snap, err := p.Snapshot(context.Background(), []string{"alice", "bob"})
	require.NoError(t, err)
	assert.Empty(t, snap)
}

func TestShouldPush(t *testing.T) {
	tests := []struct {
		status string
		want   bool
	}{
		{"online", true},
		{"offline", true},
		{"away", true},
		{"busy", false},
		{"in-call", false},
		{"", true},        // missing → fail-open
		{"unknown", true}, // unknown → fail-open
	}
	for _, tt := range tests {
		t.Run(tt.status, func(t *testing.T) {
			assert.Equal(t, tt.want, shouldPush(model.Presence{AggregatedStatus: tt.status}))
		})
	}
}

type stubRequester struct {
	mu       sync.Mutex
	calls    int
	gotReqs  []model.PresenceSnapshotRequest
	reply    func(req model.PresenceSnapshotRequest) (model.PresenceSnapshotReply, error)
	rawReply func(req model.PresenceSnapshotRequest) ([]byte, error) // when set, bypasses reply and returns raw bytes
}

func (s *stubRequester) Request(_ context.Context, _ string, data []byte, _ time.Duration) (*nats.Msg, error) {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	var req model.PresenceSnapshotRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.gotReqs = append(s.gotReqs, req)
	s.mu.Unlock()
	if s.rawReply != nil {
		out, err := s.rawReply(req)
		if err != nil {
			return nil, err
		}
		return &nats.Msg{Data: out}, nil
	}
	reply, err := s.reply(req)
	if err != nil {
		return nil, err
	}
	out, err := json.Marshal(reply)
	if err != nil {
		return nil, err
	}
	return &nats.Msg{Data: out}, nil
}

func TestBulkPresence_Chunks(t *testing.T) {
	accounts := make([]string, 1500)
	for i := range accounts {
		accounts[i] = "u"
	}
	for i := range accounts {
		accounts[i] = string(rune('a'+i%26)) + "-" + string(rune('a'+i/26%26))
	}
	stub := &stubRequester{reply: func(req model.PresenceSnapshotRequest) (model.PresenceSnapshotReply, error) {
		out := model.PresenceSnapshotReply{Presences: map[string]model.Presence{}}
		for _, a := range req.Accounts {
			out.Presences[a] = model.Presence{AggregatedStatus: "online"}
		}
		return out, nil
	}}

	src := newBulkPresenceSource(stub, "site-a", 500, time.Second)
	got, err := src.Snapshot(context.Background(), accounts)
	require.NoError(t, err)
	assert.Equal(t, 3, stub.calls, "expect ceil(1500/500) chunks")
	assert.Len(t, got, len(uniqueStrings(accounts)))
}

func TestBulkPresence_FailOpenOnError(t *testing.T) {
	stub := &stubRequester{reply: func(model.PresenceSnapshotRequest) (model.PresenceSnapshotReply, error) {
		return model.PresenceSnapshotReply{}, errors.New("nats: timeout")
	}}
	src := newBulkPresenceSource(stub, "site-a", 100, 50*time.Millisecond)
	got, err := src.Snapshot(context.Background(), []string{"a", "b"})
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestBulkPresence_ErrorResponseLoggedAndFailOpen(t *testing.T) {
	stub := &stubRequester{
		rawReply: func(_ model.PresenceSnapshotRequest) ([]byte, error) {
			return errnats.MarshalQuiet(errcode.Internal("presence backend down")), nil
		},
	}
	src := newBulkPresenceSource(stub, "site-a", 100, 50*time.Millisecond)
	got, err := src.Snapshot(context.Background(), []string{"alice", "bob"})
	require.NoError(t, err) // fail-open: error envelope is swallowed
	assert.Empty(t, got)
}

func uniqueStrings(in []string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, s := range in {
		out[s] = struct{}{}
	}
	return out
}
