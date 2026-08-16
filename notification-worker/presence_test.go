package main

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/hmchangw/chat/pkg/natsmetrics"

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

// stubPresenceFlags swaps the isDND/isPresenting stubs for one test and restores
// them afterwards, so a row that flips them cannot leak into a sibling test.
func stubPresenceFlags(t *testing.T, dnd, presenting bool) {
	t.Helper()
	origDND, origPresenting := isDND, isPresenting
	isDND = func(model.Presence) bool { return dnd }
	isPresenting = func(model.Presence) bool { return presenting }
	t.Cleanup(func() { isDND, isPresenting = origDND, origPresenting })
}

// stubPresenceFlagsByStatus points the isDND/isPresenting stubs at one status
// each, restoring them afterwards. Lets a handler test prove the gate consults
// both without asserting any real status mapping; "" disables that stub.
func stubPresenceFlagsByStatus(t *testing.T, dndStatus, presentingStatus string) {
	t.Helper()
	origDND, origPresenting := isDND, isPresenting
	isDND = func(p model.Presence) bool { return dndStatus != "" && p.AggregatedStatus == dndStatus }
	isPresenting = func(p model.Presence) bool {
		return presentingStatus != "" && p.AggregatedStatus == presentingStatus
	}
	t.Cleanup(func() { isDND, isPresenting = origDND, origPresenting })
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
		{"zero settings online", "online", false, false, notifSettings{}, false, true},
		{"zero settings offline", "offline", false, false, notifSettings{}, false, true},
		{"zero settings away", "away", false, false, notifSettings{}, false, true},
		{"zero settings busy", "busy", false, false, notifSettings{}, false, false},
		{"zero settings in-call", "in-call", false, false, notifSettings{}, false, false},
		{"zero settings missing status", "", false, false, notifSettings{}, false, true},
		{"zero settings unknown status", "unknown", false, false, notifSettings{}, false, true},

		// muteAll suppresses unless a priority sender pierces it.
		{"muted, no pierce", "online", false, false, notifSettings{muteAll: true}, false, false},
		{"muted, priority sender but pierce disabled", "online", false, false, notifSettings{muteAll: true}, true, false},
		{"muted, pierce enabled but sender not priority", "online", false, false, notifSettings{muteAll: true, allowPriority: true}, false, false},
		{"muted, pierce enabled and sender is priority", "online", false, false, notifSettings{muteAll: true, allowPriority: true}, true, true},
		{"unmuted, pierce enabled, non-priority sender", "online", false, false, notifSettings{allowPriority: true}, false, true},

		// Rule 2: DND and presenting suppress on their own, and the in-call opt-in
		// does not rescue them — showNotificationsInCall governs in-call only.
		{"dnd", "online", true, false, notifSettings{}, false, false},
		{"dnd, in-call opt-in does not rescue", "online", true, false, notifSettings{showInCall: true}, false, false},
		{"presenting", "online", false, true, notifSettings{}, false, false},
		{"presenting, in-call opt-in does not rescue", "online", false, true, notifSettings{showInCall: true}, false, false},
		{"dnd and presenting together", "online", true, true, notifSettings{}, false, false},

		// showNotificationsInCall governs the in-call bucket, for non-priority senders.
		{"in-call, opted in", "in-call", false, false, notifSettings{showInCall: true}, false, true},
		{"busy, opted in", "busy", false, false, notifSettings{showInCall: true}, false, true},
		{"in-call, not opted in", "in-call", false, false, notifSettings{}, false, false},

		// The pierce crosses every suppressor, DND and presenting included.
		{"dnd, priority pierce", "online", true, false, notifSettings{allowPriority: true}, true, true},
		{"presenting, priority pierce", "online", false, true, notifSettings{allowPriority: true}, true, true},
		{"in-call, priority pierce without in-call opt-in", "in-call", false, false, notifSettings{allowPriority: true}, true, true},
		{"muted+dnd, priority pierce", "in-call", true, false, notifSettings{muteAll: true, allowPriority: true}, true, true},

		// ...but only with its opt-in. A priority sender alone pierces nothing.
		{"dnd, priority sender but pierce disabled", "online", true, false, notifSettings{}, true, false},
		{"presenting, priority sender but pierce disabled", "online", false, true, notifSettings{}, true, false},
		{"in-call, priority sender but pierce disabled", "in-call", false, false, notifSettings{}, true, false},

		// Every suppressor clear.
		{"muted+pierced, online", "online", false, false, notifSettings{muteAll: true, allowPriority: true, showInCall: true}, true, true},
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

	src := newBulkPresenceSource(stub, "site-a", 500, time.Second, natsmetrics.Publisher{})
	got, err := src.Snapshot(context.Background(), accounts)
	require.NoError(t, err)
	assert.Equal(t, 3, stub.calls, "expect ceil(1500/500) chunks")
	assert.Len(t, got, len(uniqueStrings(accounts)))
}

func TestBulkPresence_FailOpenOnError(t *testing.T) {
	stub := &stubRequester{reply: func(model.PresenceSnapshotRequest) (model.PresenceSnapshotReply, error) {
		return model.PresenceSnapshotReply{}, errors.New("nats: timeout")
	}}
	src := newBulkPresenceSource(stub, "site-a", 100, 50*time.Millisecond, natsmetrics.Publisher{})
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
	src := newBulkPresenceSource(stub, "site-a", 100, 50*time.Millisecond, natsmetrics.Publisher{})
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
