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

// swapPresenceFlags installs both stubs for one test and restores them afterwards,
// so a test that flips them cannot leak into a sibling. Sole owner of the
// save/restore; the two helpers below differ only in the predicates they build.
func swapPresenceFlags(t *testing.T, dnd, presenting func(model.Presence) bool) {
	t.Helper()
	origDND, origPresenting := isDND, isPresenting
	isDND, isPresenting = dnd, presenting
	t.Cleanup(func() { isDND, isPresenting = origDND, origPresenting })
}

// stubPresenceFlags forces both stubs to a constant, for the shouldPush table.
func stubPresenceFlags(t *testing.T, dnd, presenting bool) {
	t.Helper()
	swapPresenceFlags(t,
		func(model.Presence) bool { return dnd },
		func(model.Presence) bool { return presenting })
}

// stubPresenceFlagsByStatus points each stub at one status. Lets a handler test
// prove the gate consults both without asserting any real status mapping; ""
// disables that stub.
func stubPresenceFlagsByStatus(t *testing.T, dndStatus, presentingStatus string) {
	t.Helper()
	swapPresenceFlags(t,
		func(p model.Presence) bool { return dndStatus != "" && p.AggregatedStatus == dndStatus },
		func(p model.Presence) bool { return presentingStatus != "" && p.AggregatedStatus == presentingStatus })
}

func TestShouldPush(t *testing.T) {
	tests := []struct {
		name             string
		status           string
		dnd              bool
		presenting       bool
		ns               notifSettings
		isPrioritySender bool
		want             bool
	}{
		// Zero notifSettings with both stubs inert must reproduce the pre-change
		// truth table exactly: no stored settings means no behaviour change.
		{name: "zero settings online", status: "online", want: true},
		{name: "zero settings offline", status: "offline", want: true},
		{name: "zero settings away", status: "away", want: true},
		{name: "zero settings busy", status: "busy", want: false},
		{name: "zero settings in-call", status: "in-call", want: false},
		{name: "zero settings missing status", status: "", want: true},
		{name: "zero settings unknown status", status: "unknown", want: true},

		// muteAll suppresses unless a priority sender pierces it.
		{name: "muted, no pierce", status: "online", ns: notifSettings{muteAll: true}, want: false},
		{name: "muted, priority sender but pierce disabled", status: "online", ns: notifSettings{muteAll: true}, isPrioritySender: true, want: false},
		{name: "muted, pierce enabled but sender not priority", status: "online", ns: notifSettings{muteAll: true, allowPriority: true}, want: false},
		{name: "muted, pierce enabled and sender is priority", status: "online", ns: notifSettings{muteAll: true, allowPriority: true}, isPrioritySender: true, want: true},
		{name: "unmuted, pierce enabled, non-priority sender", status: "online", ns: notifSettings{allowPriority: true}, want: true},

		// Rule 2: DND and presenting suppress on their own, and the in-call opt-in
		// does not rescue them — showNotificationsInCall governs in-call only.
		{name: "dnd", status: "online", dnd: true, want: false},
		{name: "dnd, in-call opt-in does not rescue", status: "online", dnd: true, ns: notifSettings{showInCall: true}, want: false},
		{name: "presenting", status: "online", presenting: true, want: false},
		{name: "presenting, in-call opt-in does not rescue", status: "online", presenting: true, ns: notifSettings{showInCall: true}, want: false},
		{name: "dnd and presenting together", status: "online", dnd: true, presenting: true, want: false},

		// showNotificationsInCall governs the in-call bucket, for non-priority senders.
		{name: "in-call, opted in", status: "in-call", ns: notifSettings{showInCall: true}, want: true},
		{name: "busy, opted in", status: "busy", ns: notifSettings{showInCall: true}, want: true},
		{name: "in-call, not opted in", status: "in-call", want: false},

		// The pierce crosses every suppressor, DND and presenting included.
		{name: "dnd, priority pierce", status: "online", dnd: true, ns: notifSettings{allowPriority: true}, isPrioritySender: true, want: true},
		{name: "presenting, priority pierce", status: "online", presenting: true, ns: notifSettings{allowPriority: true}, isPrioritySender: true, want: true},
		{name: "in-call, priority pierce without in-call opt-in", status: "in-call", ns: notifSettings{allowPriority: true}, isPrioritySender: true, want: true},
		{name: "muted+dnd, priority pierce", status: "in-call", dnd: true, ns: notifSettings{muteAll: true, allowPriority: true}, isPrioritySender: true, want: true},

		// ...but only with its opt-in. A priority sender alone pierces nothing.
		{name: "dnd, priority sender but pierce disabled", status: "online", dnd: true, isPrioritySender: true, want: false},
		{name: "presenting, priority sender but pierce disabled", status: "online", presenting: true, isPrioritySender: true, want: false},
		{name: "in-call, priority sender but pierce disabled", status: "in-call", isPrioritySender: true, want: false},

		// Every suppressor clear.
		{name: "muted+pierced, online", status: "online", ns: notifSettings{muteAll: true, allowPriority: true, showInCall: true}, isPrioritySender: true, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stubPresenceFlags(t, tt.dnd, tt.presenting)
			got := shouldPush(model.Presence{AggregatedStatus: tt.status}, tt.ns, tt.isPrioritySender)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestDNDAndPresentingStubsAreInert pins the stub contract: until the presence
// side ships the real predicates, neither may infer a status from what we
// currently receive. A stub that starts returning true for "busy" or "in-call"
// would silently change delivery, so every status we know about is asserted.
func TestDNDAndPresentingStubsAreInert(t *testing.T) {
	for _, status := range []string{"busy", "in-call", "online", "offline", "away", "", "unknown"} {
		t.Run(status, func(t *testing.T) {
			p := model.Presence{AggregatedStatus: status}
			assert.False(t, isDND(p), "isDND must stay inert until the presence side owns it")
			assert.False(t, isPresenting(p), "isPresenting must stay inert until the presence side owns it")
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
