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
		name             string
		status           string
		ns               notifSettings
		isPrioritySender bool
		want             bool
	}{
		// Zero notifSettings must reproduce the pre-enforcement truth table exactly.
		{"zero settings online", "online", notifSettings{}, false, true},
		{"zero settings offline", "offline", notifSettings{}, false, true},
		{"zero settings away", "away", notifSettings{}, false, true},
		{"zero settings busy", "busy", notifSettings{}, false, false},
		{"zero settings in-call", "in-call", notifSettings{}, false, false},
		{"zero settings missing status", "", notifSettings{}, false, true},
		{"zero settings unknown status", "unknown", notifSettings{}, false, true},

		// muteAll suppresses unless a priority sender pierces it.
		{"muted, no pierce", "online", notifSettings{muteAll: true}, false, false},
		{"muted, priority sender but pierce disabled", "online", notifSettings{muteAll: true}, true, false},
		{"muted, pierce enabled but sender not priority", "online", notifSettings{muteAll: true, allowPriority: true}, false, false},
		{"muted, pierce enabled and sender is priority", "online", notifSettings{muteAll: true, allowPriority: true}, true, true},
		{"unmuted, pierce enabled, non-priority sender", "online", notifSettings{allowPriority: true}, false, true},

		// showNotificationsInCall governs both suppressed statuses.
		{"in-call, opted in", "in-call", notifSettings{showInCall: true}, false, true},
		{"busy, opted in", "busy", notifSettings{showInCall: true}, false, true},
		{"in-call, not opted in", "in-call", notifSettings{}, false, false},

		// The pierce does not cross the in-call gate.
		{"muted+pierced but in-call without opt-in", "in-call", notifSettings{muteAll: true, allowPriority: true}, true, false},
		{"muted+pierced and in-call with opt-in", "in-call", notifSettings{muteAll: true, allowPriority: true, showInCall: true}, true, true},

		// Both suppressors clear.
		{"muted+pierced, online", "online", notifSettings{muteAll: true, allowPriority: true, showInCall: true}, true, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := shouldPush(model.Presence{AggregatedStatus: tt.status}, tt.ns, tt.isPrioritySender)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestIsInCall(t *testing.T) {
	tests := []struct {
		status string
		want   bool
	}{
		{"busy", true},
		{"in-call", true},
		{"online", false},
		{"offline", false},
		{"away", false},
		{"", false},
		{"unknown", false},
	}
	for _, tt := range tests {
		t.Run(tt.status, func(t *testing.T) {
			assert.Equal(t, tt.want, isInCall(model.Presence{AggregatedStatus: tt.status}))
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
