package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roommetacache"
	"github.com/hmchangw/chat/pkg/roomsubcache"
)

type stubRoomMeta struct {
	out map[string]roommetacache.Meta
	err error
}

func (s *stubRoomMeta) Get(_ context.Context, roomID string) (roommetacache.Meta, error) {
	if s.err != nil {
		return roommetacache.Meta{}, s.err
	}
	return s.out[roomID], nil
}

type stubMembers struct {
	mu    sync.Mutex
	out   map[string][]roomsubcache.Member
	calls []string // recorded in order: "get:<roomID>" / "inval:<roomID>"
}

func (s *stubMembers) GetMembers(_ context.Context, roomID string) ([]roomsubcache.Member, error) {
	s.mu.Lock()
	s.calls = append(s.calls, "get:"+roomID)
	s.mu.Unlock()
	return s.out[roomID], nil
}

func (s *stubMembers) Invalidate(_ context.Context, roomID string) {
	s.mu.Lock()
	s.calls = append(s.calls, "inval:"+roomID)
	s.mu.Unlock()
}

type stubFollowers struct {
	out map[string]map[string]struct{}
}

func (s *stubFollowers) Lookup(_ context.Context, parentID string) (ThreadRoomInfo, error) {
	info := ThreadRoomInfo{Followers: map[string]struct{}{}}
	if v, ok := s.out[parentID]; ok {
		info.Followers = v
	}
	return info, nil
}

// stubParent is a ParentFetcher test double: FetchParent returns a fixed parent
// (or error). The zero value returns an empty ParentMessageInfo — fine for thread
// tests whose members carry no HistorySharedSince and that don't assert on the
// parent author.
type stubParent struct {
	info *ParentMessageInfo
	err  error
}

func (s stubParent) FetchParent(context.Context, string, string, string, string) (*ParentMessageInfo, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.info != nil {
		return s.info, nil
	}
	return &ParentMessageInfo{}, nil
}

type stubPresence struct {
	out map[string]model.Presence
}

func (s *stubPresence) Snapshot(_ context.Context, _ []string) (map[string]model.Presence, error) {
	return s.out, nil
}

type rejectHook struct{}

func (rejectHook) Allow(context.Context, *model.Message, roomsubcache.Member) (bool, error) {
	return false, nil
}

// stubSettings records the accounts slice it was called with so tests can pin
// where in the pipeline the fetch runs.
type stubSettings struct {
	mu       sync.Mutex
	out      map[string]notifSettings
	err      error
	gotCalls [][]string
}

func (s *stubSettings) Snapshot(_ context.Context, accounts []string) (map[string]notifSettings, error) {
	s.mu.Lock()
	got := make([]string, len(accounts))
	copy(got, accounts)
	s.gotCalls = append(s.gotCalls, got)
	s.mu.Unlock()
	if s.err != nil {
		return nil, s.err
	}
	return s.out, nil
}

func (s *stubSettings) lastAccounts() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.gotCalls) == 0 {
		return nil
	}
	return s.gotCalls[len(s.gotCalls)-1]
}

// accountVetoer rejects exactly the named accounts, so a test can exercise the
// hook-veto exclusion without rejectHook's all-or-nothing behaviour.
type accountVetoer struct {
	deny map[string]struct{}
}

func (a accountVetoer) Allow(_ context.Context, _ *model.Message, m roomsubcache.Member) (bool, error) { //nolint:gocritic // hugeParam: must match Vetoer interface value semantics
	_, denied := a.deny[m.Account]
	return !denied, nil
}

// newTestHandlerWithSettings mirrors newTestHandler but injects a settings
// snapshotter and a caller-supplied Vetoer.
func newTestHandlerWithSettings(members MemberCache, presence PresenceSnapshotter, settings UserSettingsSnapshotter, hook Vetoer, emit Emitter) *Handler {
	return NewHandler(HandlerDeps{
		Members:            members,
		Followers:          &stubFollowers{},
		Parent:             stubParent{},
		Presence:           presence,
		Settings:           settings,
		Hook:               hook,
		Emitter:            emit,
		LargeRoomThreshold: 500,
	})
}

type recordingEmitter struct {
	mu      sync.Mutex
	emitted []model.PushNotificationEvent
}

func (r *recordingEmitter) Emit(_ context.Context, evt model.PushNotificationEvent) error { //nolint:gocritic // hugeParam: must match Emitter interface value semantics
	r.mu.Lock()
	defer r.mu.Unlock()
	r.emitted = append(r.emitted, evt)
	return nil
}

// accounts flattens every recipient across every emitted batch so existing assertions
// can stay account-oriented even though Emit now receives batched events.
func (r *recordingEmitter) accounts() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for i := range r.emitted {
		out = append(out, r.emitted[i].Accounts...)
	}
	return out
}

func newTestHandler(members MemberCache, followers ThreadFollowerLister, presence PresenceSnapshotter, hook Vetoer, emit Emitter) *Handler {
	return NewHandler(HandlerDeps{
		Members:            members,
		Followers:          followers,
		Parent:             stubParent{},
		Presence:           presence,
		Hook:               hook,
		Emitter:            emit,
		LargeRoomThreshold: 500,
	})
}

func msgEvent(m *model.Message) []byte { //nolint:gocritic // hugeParam: test helper only; pointer avoids copy
	data, _ := json.Marshal(model.MessageEvent{Message: *m, SiteID: "site-a"})
	return data
}

func TestHandle_ImportantMessageNotifies(t *testing.T) {
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {{ID: "alice", Account: "alice"}, {ID: "bob", Account: "bob"}},
	}}
	emit := &recordingEmitter{}
	h := newTestHandler(members, &stubFollowers{}, noopPresenceSnapshotter{}, noopVetoer{}, emit)

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice",
		Type: model.MessageTypeImportant, CreatedAt: time.Now(),
	})))
	assert.Equal(t, []string{"bob"}, emit.accounts(), "important message notifies like a normal message")
}

func TestHandle_SystemMessageDoesNotNotify(t *testing.T) {
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {{ID: "alice", Account: "alice"}, {ID: "bob", Account: "bob"}},
	}}
	emit := &recordingEmitter{}
	h := newTestHandler(members, &stubFollowers{}, noopPresenceSnapshotter{}, noopVetoer{}, emit)

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice",
		Type: model.MessageTypeRoomRenamed, CreatedAt: time.Now(),
	})))
	assert.Empty(t, emit.accounts(), "system message must not notify")
}

func TestHandle_SkipsSender(t *testing.T) {
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice"},
			{ID: "bob", Account: "bob"},
		},
	}}
	emit := &recordingEmitter{}
	h := newTestHandler(members, &stubFollowers{}, noopPresenceSnapshotter{}, noopVetoer{}, emit)

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice",
		CreatedAt: time.Now(),
	})))
	assert.Equal(t, []string{"bob"}, emit.accounts())
}

func TestHandle_SkipsMuted(t *testing.T) {
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice"},
			{ID: "bob", Account: "bob", Muted: true},
			{ID: "carol", Account: "carol"},
		},
	}}
	emit := &recordingEmitter{}
	h := newTestHandler(members, &stubFollowers{}, noopPresenceSnapshotter{}, noopVetoer{}, emit)

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice",
		CreatedAt: time.Now(),
	})))
	assert.ElementsMatch(t, []string{"carol"}, emit.accounts(), "muted bob is skipped")
}

func TestHandle_SkipsRestrictedBeforeWindow(t *testing.T) {
	createdAt := time.Unix(0, 1700000000000*int64(time.Millisecond))
	afterWindow := int64(1700000000001)  // joined after message ms
	beforeWindow := int64(1699999999999) // joined before message ms
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice"},
			{ID: "bob", Account: "bob", HistorySharedSince: &afterWindow},      // joined after message → skip
			{ID: "carol", Account: "carol", HistorySharedSince: &beforeWindow}, // joined before → include
		},
	}}
	emit := &recordingEmitter{}
	h := newTestHandler(members, &stubFollowers{}, noopPresenceSnapshotter{}, noopVetoer{}, emit)

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice", CreatedAt: createdAt,
	})))
	assert.ElementsMatch(t, []string{"carol"}, emit.accounts())
}

func TestHandle_ThreadOnlyReply_SkipsNonFollowerNonMention(t *testing.T) {
	parentCreatedAt := time.Unix(0, 1700000000000*int64(time.Millisecond))
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice"},
			{ID: "bob", Account: "bob"},
			{ID: "carol", Account: "carol"},
		},
	}}
	followers := &stubFollowers{out: map[string]map[string]struct{}{
		"parent-1": {"bob": {}},
	}}
	emit := &recordingEmitter{}
	h := newTestHandler(members, followers, noopPresenceSnapshotter{}, noopVetoer{}, emit)

	msg := model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice", CreatedAt: time.Now(),
		ThreadParentMessageID:        "parent-1",
		ThreadParentMessageCreatedAt: &parentCreatedAt,
		TShow:                        false,
		Content:                      "thread reply",
	}
	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&msg)))
	assert.ElementsMatch(t, []string{"bob"}, emit.accounts(), "only thread follower receives")
}

func TestHandle_ThreadReply_TShow_TreatedAsChannelMessage(t *testing.T) {
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice"},
			{ID: "bob", Account: "bob"},
			{ID: "carol", Account: "carol"},
		},
	}}
	emit := &recordingEmitter{}
	h := newTestHandler(members, &stubFollowers{}, noopPresenceSnapshotter{}, noopVetoer{}, emit)

	msg := model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice", CreatedAt: time.Now(),
		ThreadParentMessageID: "parent-1",
		TShow:                 true,
		Content:               "shared with channel",
	}
	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&msg)))
	assert.ElementsMatch(t, []string{"bob", "carol"}, emit.accounts())
}

func TestHandle_HookVeto_DropsAll(t *testing.T) {
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice"},
			{ID: "bob", Account: "bob"},
		},
	}}
	emit := &recordingEmitter{}
	h := newTestHandler(members, &stubFollowers{}, noopPresenceSnapshotter{}, rejectHook{}, emit)

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice", CreatedAt: time.Now(),
	})))
	assert.Empty(t, emit.accounts())
}

func TestHandle_LargeRoomNonMention_DropsAll(t *testing.T) {
	roomMembers := make([]roomsubcache.Member, 600)
	for i := range roomMembers {
		roomMembers[i] = roomsubcache.Member{ID: "u", Account: "u" + string(rune(i))}
	}
	roomMembers[0] = roomsubcache.Member{ID: "alice", Account: "alice"}
	members := &stubMembers{out: map[string][]roomsubcache.Member{"r1": roomMembers}}
	emit := &recordingEmitter{}
	h := NewHandler(HandlerDeps{
		Members:            members,
		Followers:          &stubFollowers{},
		Presence:           noopPresenceSnapshotter{},
		Hook:               noopVetoer{},
		Emitter:            emit,
		LargeRoomThreshold: 500,
	})

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice", Content: "no mentions",
		CreatedAt: time.Now(),
	})))
	assert.Empty(t, emit.accounts(), "large room non-mention drops all")
}

func TestHandle_LargeRoomMention_OnlyMentionedPushed(t *testing.T) {
	roomMembers := []roomsubcache.Member{
		{ID: "alice", Account: "alice"},
		{ID: "bob", Account: "bob"},
		{ID: "carol", Account: "carol"},
	}
	for i := 0; i < 600; i++ {
		roomMembers = append(roomMembers, roomsubcache.Member{ID: "u" + string(rune(i)), Account: "u" + string(rune(i))})
	}
	members := &stubMembers{out: map[string][]roomsubcache.Member{"r1": roomMembers}}
	emit := &recordingEmitter{}
	h := newTestHandler(members, &stubFollowers{}, noopPresenceSnapshotter{}, noopVetoer{}, emit)

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice",
		Content: "hey @bob check this", CreatedAt: time.Now(),
	})))
	assert.ElementsMatch(t, []string{"bob"}, emit.accounts())
}

func TestHandle_LargeRoomAtAll_PushesAllNonSender(t *testing.T) {
	roomMembers := []roomsubcache.Member{
		{ID: "alice", Account: "alice"},
		{ID: "bob", Account: "bob"},
		{ID: "carol", Account: "carol"},
	}
	for i := 0; i < 500; i++ {
		roomMembers = append(roomMembers, roomsubcache.Member{ID: "u", Account: "u" + string(rune(i))})
	}
	members := &stubMembers{out: map[string][]roomsubcache.Member{"r1": roomMembers}}
	emit := &recordingEmitter{}
	h := newTestHandler(members, &stubFollowers{}, noopPresenceSnapshotter{}, noopVetoer{}, emit)

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice",
		Content: "@all heads up", CreatedAt: time.Now(),
	})))
	assert.Contains(t, emit.accounts(), "bob")
	assert.Contains(t, emit.accounts(), "carol")
	assert.NotContains(t, emit.accounts(), "alice")
}

func TestHandle_PresenceBusyDropsPush(t *testing.T) {
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice"},
			{ID: "bob", Account: "bob"},
			{ID: "carol", Account: "carol"},
		},
	}}
	presence := &stubPresence{out: map[string]model.Presence{
		"bob":   {AggregatedStatus: "busy"},
		"carol": {AggregatedStatus: "online"},
	}}
	emit := &recordingEmitter{}
	h := newTestHandler(members, &stubFollowers{}, presence, noopVetoer{}, emit)

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice", CreatedAt: time.Now(),
	})))
	assert.ElementsMatch(t, []string{"carol"}, emit.accounts())
}

func TestHandle_TwoMemberChannel_RoutesAsChannel(t *testing.T) {
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice", RoomType: model.RoomTypeChannel},
			{ID: "bob", Account: "bob", RoomType: model.RoomTypeChannel},
		},
	}}
	emit := &recordingEmitter{}
	h := newTestHandler(members, &stubFollowers{}, noopPresenceSnapshotter{}, noopVetoer{}, emit)

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice",
		Content: "hi", CreatedAt: time.Now(),
	})))
	require.Len(t, emit.emitted, 1)
	assert.Equal(t, "c", emit.emitted[0].Data.Type)
}

func TestHandle_PushPayloadSenderFromMemberRecord(t *testing.T) {
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice", RoomType: model.RoomTypeChannel},
			{ID: "bob", Account: "bob", RoomType: model.RoomTypeChannel},
		},
	}}
	emit := &recordingEmitter{}
	h := newTestHandler(members, &stubFollowers{}, noopPresenceSnapshotter{}, noopVetoer{}, emit)

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice",
		Content:   "hello",
		CreatedAt: time.Unix(0, 1700000000000*int64(time.Millisecond)),
	})))
	require.Len(t, emit.emitted, 1)
	got := emit.emitted[0]
	assert.Equal(t, "m1-b0", got.ID, "dedup-stable batch ID")
	assert.Equal(t, []string{"bob"}, got.Accounts)
	assert.Equal(t, "r1", got.RoomID)
	require.NotNil(t, got.Data.Sender)
	assert.Equal(t, "alice", got.Data.Sender.Account)
	assert.Equal(t, "m1", got.Data.MessageID)
	assert.NotEmpty(t, got.Data.PushTime)
	assert.Greater(t, got.Timestamp, int64(0))
}

func TestHandle_InvalidJSON(t *testing.T) {
	emit := &recordingEmitter{}
	h := newTestHandler(&stubMembers{}, &stubFollowers{}, noopPresenceSnapshotter{}, noopVetoer{}, emit)
	err := h.HandleMessage(context.Background(), []byte("not json"))
	assert.Error(t, err)
	_, perm := errcode.IsPermanent(err)
	assert.True(t, perm, "a malformed payload can never parse on redelivery — must be a permanent (drop) error")
}

type errHook struct{}

func (errHook) Allow(context.Context, *model.Message, roomsubcache.Member) (bool, error) {
	return false, fmt.Errorf("hook backend unavailable")
}

func TestHandle_HookError_FailOpen(t *testing.T) {
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice"},
			{ID: "bob", Account: "bob"},
		},
	}}
	emit := &recordingEmitter{}
	h := newTestHandler(members, &stubFollowers{}, noopPresenceSnapshotter{}, errHook{}, emit)

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice", CreatedAt: time.Now(),
	})))
	assert.ElementsMatch(t, []string{"bob"}, emit.accounts(), "hook error must fail-open")
}

// A parent fetch failure must NAK (return an error) so JetStream redelivers, rather
// than silently acking and dropping the thread reply's recipients.
func TestHandle_ThreadOnlyReply_ParentFetchError_NAKs(t *testing.T) {
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice"},
			{ID: "bob", Account: "bob"},
		},
	}}
	followers := &stubFollowers{out: map[string]map[string]struct{}{
		"parent-1": {"bob": {}},
	}}
	emit := &recordingEmitter{}
	h := NewHandler(HandlerDeps{
		Members:            members,
		Followers:          followers,
		Parent:             stubParent{err: errors.New("history timeout")},
		Presence:           noopPresenceSnapshotter{},
		Hook:               noopVetoer{},
		Emitter:            emit,
		LargeRoomThreshold: 500,
	})

	msg := model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice", CreatedAt: time.Now(),
		ThreadParentMessageID: "parent-1",
		TShow:                 false,
		Content:               "thread reply",
	}
	err := h.HandleMessage(context.Background(), msgEvent(&msg))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fetch thread parent")
	assert.Empty(t, emit.accounts(), "no notifications emitted when the parent fetch fails")
}

// The parent createdAt comes authoritatively from history-service (stubParent), not
// the event or thread_rooms: a member who joined before the parent is notified; one
// who joined after is suppressed.
func TestHandle_ThreadOnlyReply_ParentCreatedAtFromHistory(t *testing.T) {
	parentMillis := int64(1700000000000)
	parentCreatedAt := time.UnixMilli(parentMillis).UTC()
	before := parentMillis - 1000 // joined before the parent → not restricted
	after := parentMillis + 1000  // joined after the parent → restricted

	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice"},
			{ID: "bob", Account: "bob", HistorySharedSince: &before},
			{ID: "carol", Account: "carol", HistorySharedSince: &after},
		},
	}}
	followers := &stubFollowers{
		out: map[string]map[string]struct{}{"parent-1": {"bob": {}, "carol": {}}},
	}
	emit := &recordingEmitter{}
	h := NewHandler(HandlerDeps{
		Members:            members,
		Followers:          followers,
		Parent:             stubParent{info: &ParentMessageInfo{CreatedAt: parentCreatedAt}},
		Presence:           noopPresenceSnapshotter{},
		Hook:               noopVetoer{},
		Emitter:            emit,
		LargeRoomThreshold: 500,
	})

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice", CreatedAt: time.Now(),
		ThreadParentMessageID: "parent-1",
		TShow:                 false,
		Content:               "thread reply",
	})))
	assert.ElementsMatch(t, []string{"bob"}, emit.accounts(),
		"bob (joined before parent) notified; carol (joined after parent) suppressed")
}

// The parent author is always notified of replies to their own thread, even on the
// first reply (before thread_rooms exists, so they are not yet a follower) and when
// they were not @-mentioned.
func TestHandle_ThreadOnlyReply_ParentSenderAlwaysNotified(t *testing.T) {
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice"}, // reply sender
			{ID: "carol", Account: "carol"}, // parent author, not a follower, not mentioned
			{ID: "dave", Account: "dave"},   // uninvolved bystander
		},
	}}
	// thread_rooms not created yet → empty followers (the first-reply race).
	followers := &stubFollowers{}
	emit := &recordingEmitter{}
	h := NewHandler(HandlerDeps{
		Members:            members,
		Followers:          followers,
		Parent:             stubParent{info: &ParentMessageInfo{SenderAccount: "carol"}},
		Presence:           noopPresenceSnapshotter{},
		Hook:               noopVetoer{},
		Emitter:            emit,
		LargeRoomThreshold: 500,
	})

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice", CreatedAt: time.Now(),
		ThreadParentMessageID: "parent-1",
		TShow:                 false,
		Content:               "thread reply",
	})))
	assert.ElementsMatch(t, []string{"carol"}, emit.accounts(),
		"parent author carol notified; uninvolved dave excluded")
}

// failIfCalledParent is a ParentFetcher double that fails the test if FetchParent
// is ever invoked — used to prove the event-carried parent values are used and the
// history-service round-trip is skipped.
type failIfCalledParent struct{ t *testing.T }

func (p failIfCalledParent) FetchParent(context.Context, string, string, string, string) (*ParentMessageInfo, error) {
	p.t.Helper()
	p.t.Error("FetchParent must not be called when the event carries the parent createdAt + sender account")
	return &ParentMessageInfo{}, nil
}

// When the gatekeeper resolved the parent on the send path, the event carries both
// the parent createdAt and the parent sender account. notification-worker must use
// them directly and skip FetchParent — while still notifying the parent author
// (race-free) and gating restricted followers by the event's createdAt.
func TestHandle_ThreadOnlyReply_UsesEventParent_SkipsFetch(t *testing.T) {
	parentMillis := int64(1700000000000)
	parentCreatedAt := time.UnixMilli(parentMillis).UTC()
	before := parentMillis - 1000 // joined before the parent → not restricted
	after := parentMillis + 1000  // joined after the parent → restricted

	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice"},                          // reply sender
			{ID: "carol", Account: "carol"},                          // parent author (from event)
			{ID: "bob", Account: "bob", HistorySharedSince: &before}, // follower, unrestricted
			{ID: "eve", Account: "eve", HistorySharedSince: &after},  // follower, restricted
		},
	}}
	followers := &stubFollowers{
		out: map[string]map[string]struct{}{"parent-1": {"bob": {}, "eve": {}}},
	}
	emit := &recordingEmitter{}
	h := NewHandler(HandlerDeps{
		Members:            members,
		Followers:          followers,
		Parent:             failIfCalledParent{t}, // must NOT be called
		Presence:           noopPresenceSnapshotter{},
		Hook:               noopVetoer{},
		Emitter:            emit,
		LargeRoomThreshold: 500,
	})

	evt := model.MessageEvent{
		Message: model.Message{
			ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice", CreatedAt: time.Now(),
			ThreadParentMessageID:        "parent-1",
			ThreadParentMessageCreatedAt: &parentCreatedAt, // gatekeeper-resolved
			TShow:                        false,
			Content:                      "thread reply",
		},
		SiteID:                    "site-a",
		ThreadParentSenderAccount: "carol", // gatekeeper-resolved parent author
	}
	data, _ := json.Marshal(evt)
	require.NoError(t, h.HandleMessage(context.Background(), data))
	assert.ElementsMatch(t, []string{"carol", "bob"}, emit.accounts(),
		"parent author + unrestricted follower notified; restricted follower suppressed by event createdAt")
}

// When the event lacks the parent sender account (gatekeeper soft-fail, or an
// edit/delete event that bypassed the gatekeeper), notification-worker falls back to
// FetchParent even if createdAt is present — both values come from the same fetch.
func TestHandle_ThreadOnlyReply_MissingSenderAccount_FallsBackToFetch(t *testing.T) {
	parentCreatedAt := time.UnixMilli(1700000000000).UTC()
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice"}, // reply sender
			{ID: "carol", Account: "carol"}, // parent author, only reachable via fetch
		},
	}}
	followers := &stubFollowers{}
	emit := &recordingEmitter{}
	h := NewHandler(HandlerDeps{
		Members:            members,
		Followers:          followers,
		Parent:             stubParent{info: &ParentMessageInfo{SenderAccount: "carol", CreatedAt: parentCreatedAt}},
		Presence:           noopPresenceSnapshotter{},
		Hook:               noopVetoer{},
		Emitter:            emit,
		LargeRoomThreshold: 500,
	})

	// createdAt present on the event, but no sender account → fetch fallback.
	evt := model.MessageEvent{
		Message: model.Message{
			ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice", CreatedAt: time.Now(),
			ThreadParentMessageID:        "parent-1",
			ThreadParentMessageCreatedAt: &parentCreatedAt,
			TShow:                        false,
			Content:                      "thread reply",
		},
		SiteID: "site-a",
	}
	data, _ := json.Marshal(evt)
	require.NoError(t, h.HandleMessage(context.Background(), data))
	assert.ElementsMatch(t, []string{"carol"}, emit.accounts(),
		"parent author from fetch fallback is notified")
}

type errFollowers struct{}

func (errFollowers) Lookup(context.Context, string) (ThreadRoomInfo, error) {
	return ThreadRoomInfo{}, fmt.Errorf("mongo timeout")
}

func TestHandle_ThreadFollowersError_NAKs(t *testing.T) {
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice"},
			{ID: "bob", Account: "bob"},
		},
	}}
	emit := &recordingEmitter{}
	h := newTestHandler(members, errFollowers{}, noopPresenceSnapshotter{}, noopVetoer{}, emit)

	msg := &model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice",
		Content:               "thread reply",
		ThreadParentMessageID: "parent-1",
		TShow:                 false,
		CreatedAt:             time.Now(),
	}
	// An actual thread-room lookup failure (Mongo down) must NAK for redelivery, not
	// silently ack and drop follower-only recipients + restricted-room filtering.
	err := h.HandleMessage(context.Background(), msgEvent(msg))
	require.Error(t, err)
	assert.Empty(t, emit.accounts(), "no notifications emitted when the lookup fails")
}

func TestNewHandler_DefaultLargeRoomThreshold(t *testing.T) {
	h := NewHandler(HandlerDeps{
		Members:   &stubMembers{},
		Followers: &stubFollowers{},
		Presence:  noopPresenceSnapshotter{},
		Hook:      noopVetoer{},
		Emitter:   &recordingEmitter{},
		// LargeRoomThreshold + RecipientBatchSize zero → must default
	})
	assert.Equal(t, 500, h.deps.LargeRoomThreshold)
	assert.Equal(t, defaultRecipientBatchSize, h.deps.RecipientBatchSize)
}

// @here is no longer a push trigger (legacy FE doesn't render it). A large-room message
// containing ONLY @here must result in zero pushes — same as a non-mention large-room post.
func TestHandle_AtHere_LargeRoom_DropsAll(t *testing.T) {
	roomMembers := []roomsubcache.Member{{ID: "alice", Account: "alice"}}
	for i := 0; i < 600; i++ {
		roomMembers = append(roomMembers, roomsubcache.Member{ID: "u", Account: "u" + string(rune(i))})
	}
	members := &stubMembers{out: map[string][]roomsubcache.Member{"r1": roomMembers}}
	emit := &recordingEmitter{}
	h := newTestHandler(members, &stubFollowers{}, noopPresenceSnapshotter{}, noopVetoer{}, emit)

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice",
		Content: "@here heads up", CreatedAt: time.Now(),
	})))
	assert.Empty(t, emit.accounts(), "@here in large room must not push to anyone")
}

// @here in a thread-only reply must NOT bypass the follower check — only followers (and
// explicit @account mentions) should be pushed.
func TestHandle_AtHere_ThreadOnlyReply_DoesNotBypassFollowers(t *testing.T) {
	parentCreatedAt := time.Unix(0, 1700000000000*int64(time.Millisecond))
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice"},
			{ID: "bob", Account: "bob"},
			{ID: "carol", Account: "carol"},
		},
	}}
	followers := &stubFollowers{out: map[string]map[string]struct{}{
		"parent-1": {"bob": {}},
	}}
	emit := &recordingEmitter{}
	h := newTestHandler(members, followers, noopPresenceSnapshotter{}, noopVetoer{}, emit)

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice", CreatedAt: time.Now(),
		ThreadParentMessageID:        "parent-1",
		ThreadParentMessageCreatedAt: &parentCreatedAt,
		TShow:                        false,
		Content:                      "@here in thread",
	})))
	assert.ElementsMatch(t, []string{"bob"}, emit.accounts(),
		"only the thread follower receives; @here must not promote carol")
}

func TestHandle_BatchesRecipients(t *testing.T) {
	// 250 members + sender → 249 candidates; with batch=100 expect 3 events of 100/100/49.
	roomMembers := []roomsubcache.Member{{ID: "alice", Account: "alice"}}
	for i := 0; i < 250; i++ {
		roomMembers = append(roomMembers, roomsubcache.Member{ID: fmt.Sprintf("u%03d", i), Account: fmt.Sprintf("u%03d", i)})
	}
	members := &stubMembers{out: map[string][]roomsubcache.Member{"r1": roomMembers}}
	emit := &recordingEmitter{}
	h := NewHandler(HandlerDeps{
		Members:            members,
		Followers:          &stubFollowers{},
		Presence:           noopPresenceSnapshotter{},
		Hook:               noopVetoer{},
		Emitter:            emit,
		LargeRoomThreshold: 1000, // keep below threshold so all non-sender candidates remain
		RecipientBatchSize: 100,
	})

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice",
		Content: "hi", CreatedAt: time.Now(),
	})))

	require.Len(t, emit.emitted, 3, "250 recipients → ceil(250/100) = 3 batches")
	assert.Len(t, emit.emitted[0].Accounts, 100)
	assert.Len(t, emit.emitted[1].Accounts, 100)
	assert.Len(t, emit.emitted[2].Accounts, 50)
	assert.Equal(t, "m1-b0", emit.emitted[0].ID)
	assert.Equal(t, "m1-b1", emit.emitted[1].ID)
	assert.Equal(t, "m1-b2", emit.emitted[2].ID)

	// Same body, sender, room-level metadata replicated across batches.
	for _, e := range emit.emitted {
		assert.Equal(t, "hi", e.Body)
		assert.Equal(t, "m1", e.Data.MessageID)
		assert.Equal(t, "r1", e.RoomID)
	}

	// Survivor union covers every non-sender member; no duplicates across batches.
	all := emit.accounts()
	assert.Len(t, all, 250)
	seen := map[string]bool{}
	for _, a := range all {
		assert.False(t, seen[a], "account %s emitted in multiple batches", a)
		seen[a] = true
	}
}

// Sub-batch-size survivor count must still produce exactly one event.
func TestHandle_SingleBatch_WhenSurvivorsBelowBatchSize(t *testing.T) {
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice"},
			{ID: "bob", Account: "bob"},
			{ID: "carol", Account: "carol"},
		},
	}}
	emit := &recordingEmitter{}
	h := NewHandler(HandlerDeps{
		Members: members, Followers: &stubFollowers{},
		Presence: noopPresenceSnapshotter{}, Hook: noopVetoer{}, Emitter: emit,
		LargeRoomThreshold: 500, RecipientBatchSize: 100,
	})

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice", CreatedAt: time.Now(),
	})))
	require.Len(t, emit.emitted, 1)
	assert.ElementsMatch(t, []string{"bob", "carol"}, emit.emitted[0].Accounts)
	assert.Equal(t, "m1-b0", emit.emitted[0].ID)
}

// Emit failure must be returned so JetStream redelivers the canonical message.
// Logging-and-continuing would silently drop the push batch — push-stream dedup
// at {messageId}-b{N} protects against duplicates on redelivery.
type failingEmitter struct{ err error }

func (f failingEmitter) Emit(context.Context, model.PushNotificationEvent) error {
	return f.err
}

func TestHandle_EmitFailure_ReturnsError(t *testing.T) {
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice"},
			{ID: "bob", Account: "bob"},
		},
	}}
	emit := failingEmitter{err: fmt.Errorf("nats: full")}
	h := newTestHandler(members, &stubFollowers{}, noopPresenceSnapshotter{}, noopVetoer{}, emit)

	err := h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice", CreatedAt: time.Now(),
	}))
	require.Error(t, err, "emit failure must propagate so JetStream redelivers")
	assert.Contains(t, err.Error(), "emit push batches for message m1")
}

// Title resolution matches the legacy rule: room.Name when present, else sender.Account.
func TestHandle_Title_UsesRoomName(t *testing.T) {
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice"},
			{ID: "bob", Account: "bob"},
		},
	}}
	rooms := &stubRoomMeta{out: map[string]roommetacache.Meta{
		"r1": {ID: "r1", Name: "general", Type: model.RoomTypeChannel},
	}}
	emit := &recordingEmitter{}
	h := NewHandler(HandlerDeps{
		Members: members, Followers: &stubFollowers{}, Presence: noopPresenceSnapshotter{},
		Hook: noopVetoer{}, Emitter: emit, RoomMeta: rooms,
		LargeRoomThreshold: 500, RecipientBatchSize: 100,
	})

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice", CreatedAt: time.Now(),
	})))
	require.Len(t, emit.emitted, 1)
	assert.Equal(t, "general", emit.emitted[0].Title)
}

func TestHandle_Title_FallsBackToSenderWhenRoomNameEmpty(t *testing.T) {
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice", RoomType: model.RoomTypeDM},
			{ID: "bob", Account: "bob", RoomType: model.RoomTypeDM},
		},
	}}
	rooms := &stubRoomMeta{out: map[string]roommetacache.Meta{
		"r1": {ID: "r1", Name: "", Type: model.RoomTypeDM}, // DM rooms have no name
	}}
	emit := &recordingEmitter{}
	h := NewHandler(HandlerDeps{
		Members: members, Followers: &stubFollowers{}, Presence: noopPresenceSnapshotter{},
		Hook: noopVetoer{}, Emitter: emit, RoomMeta: rooms,
		LargeRoomThreshold: 500, RecipientBatchSize: 100,
	})

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice", CreatedAt: time.Now(),
	})))
	require.Len(t, emit.emitted, 1)
	assert.Equal(t, "alice", emit.emitted[0].Title, "empty room name → sender account")
}

func TestHandle_Title_RoomMetaErrorFallsBackToSender(t *testing.T) {
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice"},
			{ID: "bob", Account: "bob"},
		},
	}}
	rooms := &stubRoomMeta{err: errors.New("mongo timeout")}
	emit := &recordingEmitter{}
	h := NewHandler(HandlerDeps{
		Members: members, Followers: &stubFollowers{}, Presence: noopPresenceSnapshotter{},
		Hook: noopVetoer{}, Emitter: emit, RoomMeta: rooms,
		LargeRoomThreshold: 500, RecipientBatchSize: 100,
	})

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice", CreatedAt: time.Now(),
	})))
	require.Len(t, emit.emitted, 1)
	assert.Equal(t, "alice", emit.emitted[0].Title, "lookup error must not block delivery")
}

func TestHandle_Title_NilRoomMetaFallsBackToSender(t *testing.T) {
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice"},
			{ID: "bob", Account: "bob"},
		},
	}}
	emit := &recordingEmitter{}
	h := newTestHandler(members, &stubFollowers{}, noopPresenceSnapshotter{}, noopVetoer{}, emit)

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice", CreatedAt: time.Now(),
	})))
	require.Len(t, emit.emitted, 1)
	assert.Equal(t, "alice", emit.emitted[0].Title, "no RoomMeta dep → immediate sender fallback")
}

// Sender display name comes from the canonical message (gatekeeper composed it).
// Notification-worker just copies it through — no per-message lookup.
func TestHandle_Sender_DisplayNameFromCanonicalMessage(t *testing.T) {
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice"},
			{ID: "bob", Account: "bob"},
		},
	}}
	emit := &recordingEmitter{}
	h := newTestHandler(members, &stubFollowers{}, noopPresenceSnapshotter{}, noopVetoer{}, emit)

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice",
		UserDisplayName: "Alice Wang 愛麗絲",
		CreatedAt:       time.Now(),
	})))
	require.Len(t, emit.emitted, 1)
	s := emit.emitted[0].Data.Sender
	require.NotNil(t, s)
	assert.Equal(t, "alice", s.Account)
	assert.Equal(t, "Alice Wang 愛麗絲", s.DisplayName, "display name comes from canonical message verbatim")
}

// Backward compatibility: pre-rollout canonical messages without UserDisplayName
// must still produce a valid push event. Fallback is UserAccount.
func TestHandle_Sender_EmptyDisplayNameFallsBackToAccount(t *testing.T) {
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice"},
			{ID: "bob", Account: "bob"},
		},
	}}
	emit := &recordingEmitter{}
	h := newTestHandler(members, &stubFollowers{}, noopPresenceSnapshotter{}, noopVetoer{}, emit)

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice", CreatedAt: time.Now(),
		// UserDisplayName intentionally empty — legacy in-flight message shape
	})))
	require.Len(t, emit.emitted, 1)
	s := emit.emitted[0].Data.Sender
	require.NotNil(t, s)
	assert.Equal(t, "alice", s.Account)
	assert.Equal(t, "alice", s.DisplayName, "empty UserDisplayName → fall back to account")
}

// Sys-message drives invalidation under Option C. Coupling note: works because
// room-worker guards add/remove to channels — relaxing that requires re-keeping the publish.
func TestHandle_InvalidatesCacheOnMemberChangeSysMessage(t *testing.T) {
	for _, msgType := range []string{
		model.MessageTypeMembersAdded,
		model.MessageTypeMemberLeft,
		model.MessageTypeMemberRemoved,
	} {
		t.Run(msgType, func(t *testing.T) {
			members := &stubMembers{out: map[string][]roomsubcache.Member{
				"r1": {{ID: "alice", Account: "alice"}, {ID: "bob", Account: "bob"}},
			}}
			emit := &recordingEmitter{}
			h := newTestHandler(members, &stubFollowers{}, noopPresenceSnapshotter{}, noopVetoer{}, emit)

			require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
				ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice",
				Type: msgType, CreatedAt: time.Now(),
			})))

			assert.Equal(t, []string{"inval:r1"}, members.calls,
				"member-change sys-message invalidates but is gated as non-notifiable before fan-out (no GetMembers)")
			assert.Empty(t, emit.emitted, "system messages never push")
		})
	}
}

func TestIsNotifiable(t *testing.T) {
	tests := []struct {
		name    string
		msgType string
		want    bool
	}{
		{"regular message", "", true},
		{"important client type", model.MessageTypeImportant, true},
		{"room created", model.MessageTypeRoomCreated, false},
		{"members added", model.MessageTypeMembersAdded, false},
		{"member removed", model.MessageTypeMemberRemoved, false},
		{"member left", model.MessageTypeMemberLeft, false},
		{"room renamed", model.MessageTypeRoomRenamed, false},
		{"room restricted", model.MessageTypeRoomRestricted, false},
		{"teams meet started", model.MessageTypeTeamsMeetStarted, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isNotifiable(tt.msgType))
		})
	}
}

// System messages must never push — not in a small channel (where fan-out would
// otherwise notify every non-sender), nor when content parses as an @mention
// (the latent leak this gate closes).
func TestHandle_SystemMessageProducesNoPush(t *testing.T) {
	tests := []struct {
		name    string
		msgType string
		content string
	}{
		{"room created", model.MessageTypeRoomCreated, "system text"},
		{"members added", model.MessageTypeMembersAdded, "system text"},
		{"member removed", model.MessageTypeMemberRemoved, "system text"},
		{"member left", model.MessageTypeMemberLeft, "system text"},
		{"room renamed", model.MessageTypeRoomRenamed, "system text"},
		{"room restricted", model.MessageTypeRoomRestricted, "system text"},
		{"members added with @mention content", model.MessageTypeMembersAdded, "added @bob"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			members := &stubMembers{out: map[string][]roomsubcache.Member{
				"r1": {
					{ID: "alice", Account: "alice", RoomType: model.RoomTypeChannel},
					{ID: "bob", Account: "bob", RoomType: model.RoomTypeChannel},
				},
			}}
			emit := &recordingEmitter{}
			h := newTestHandler(members, &stubFollowers{}, noopPresenceSnapshotter{}, noopVetoer{}, emit)

			require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
				ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice",
				Type: tt.msgType, Content: tt.content, CreatedAt: time.Now(),
			})))

			assert.Empty(t, emit.emitted, "system message must not push")
		})
	}
}

// badgeCall records one Counts invocation for order/content assertions.
type badgeCall struct {
	siteID   string
	roomID   string
	accounts []string
}

// fakeBadgeClient is a badgeClient test double: per-siteID configurable
// response/error, records every call.
type fakeBadgeClient struct {
	mu    sync.Mutex
	calls []badgeCall
	resp  map[string]map[string]int
	err   map[string]error
}

func (f *fakeBadgeClient) Counts(_ context.Context, siteID, roomID string, accounts []string) (map[string]int, error) {
	f.mu.Lock()
	accCopy := append([]string(nil), accounts...)
	f.calls = append(f.calls, badgeCall{siteID: siteID, roomID: roomID, accounts: accCopy})
	f.mu.Unlock()

	if f.err != nil {
		if err, ok := f.err[siteID]; ok {
			return nil, err
		}
	}
	if f.resp != nil {
		return f.resp[siteID], nil
	}
	return nil, nil
}

func (f *fakeBadgeClient) callsFor(siteID string) []badgeCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []badgeCall
	for _, c := range f.calls {
		if c.siteID == siteID {
			out = append(out, c)
		}
	}
	return out
}

// TestHandle_BadgeAudience_WiderThanSurvivors: a member excluded from push by
// presence (busy) still has their unread state changed by the message — they
// must be in the badge RPC audience (their set gets bumped) while staying
// absent from the push payload's accounts.
func TestHandle_BadgeAudience_WiderThanSurvivors(t *testing.T) {
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice", HomeSiteID: "site-a"},
			{ID: "bob", Account: "bob", HomeSiteID: "site-a"},
			{ID: "carol", Account: "carol", HomeSiteID: "site-a"},
		},
	}}
	badge := &fakeBadgeClient{resp: map[string]map[string]int{"site-a": {"bob": 2, "carol": 3}}}
	presence := &stubPresence{out: map[string]model.Presence{
		"bob": {AggregatedStatus: "busy"}, // push-excluded, badge-included
	}}
	emit := &recordingEmitter{}
	h := NewHandler(HandlerDeps{
		Members: members, Followers: &stubFollowers{}, Presence: presence,
		Hook: noopVetoer{}, Emitter: emit, BadgeClient: badge, LargeRoomThreshold: 500,
	})

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice", CreatedAt: time.Now(),
	})))

	calls := badge.callsFor("site-a")
	require.Len(t, calls, 1)
	assert.ElementsMatch(t, []string{"bob", "carol"}, calls[0].accounts, "badge audience includes the push-excluded member")
	assert.ElementsMatch(t, []string{"carol"}, emit.accounts(), "push payload stays survivor-only")
}

// TestHandle_BadgeAudience_ExcludesMuted: muted members are outside the badge
// audience entirely — their room must never enter their set via a bump.
func TestHandle_BadgeAudience_ExcludesMuted(t *testing.T) {
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice", HomeSiteID: "site-a"},
			{ID: "bob", Account: "bob", HomeSiteID: "site-a", Muted: true},
			{ID: "carol", Account: "carol", HomeSiteID: "site-a"},
		},
	}}
	badge := &fakeBadgeClient{resp: map[string]map[string]int{"site-a": {"carol": 1}}}
	emit := &recordingEmitter{}
	h := NewHandler(HandlerDeps{
		Members: members, Followers: &stubFollowers{}, Presence: noopPresenceSnapshotter{},
		Hook: noopVetoer{}, Emitter: emit, BadgeClient: badge, LargeRoomThreshold: 500,
	})

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice", CreatedAt: time.Now(),
	})))

	calls := badge.callsFor("site-a")
	require.Len(t, calls, 1)
	assert.ElementsMatch(t, []string{"carol"}, calls[0].accounts, "muted member excluded from the badge audience")
}

// TestHandle_BadgeBumpWithoutPush: when every candidate is hook-vetoed (zero
// push survivors), the badge RPC must still fire — the members' unread state
// changed even though nobody gets pushed.
func TestHandle_BadgeBumpWithoutPush(t *testing.T) {
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice", HomeSiteID: "site-a"},
			{ID: "bob", Account: "bob", HomeSiteID: "site-a"},
		},
	}}
	badge := &fakeBadgeClient{resp: map[string]map[string]int{"site-a": {"bob": 1}}}
	emit := &recordingEmitter{}
	h := NewHandler(HandlerDeps{
		Members: members, Followers: &stubFollowers{}, Presence: noopPresenceSnapshotter{},
		Hook: rejectHook{}, Emitter: emit, BadgeClient: badge, LargeRoomThreshold: 500,
	})

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice", CreatedAt: time.Now(),
	})))

	calls := badge.callsFor("site-a")
	require.Len(t, calls, 1)
	assert.ElementsMatch(t, []string{"bob"}, calls[0].accounts, "hook-vetoed member still gets bumped")
	assert.Empty(t, emit.emitted, "no push survivors → no batches emitted")
}

// Survivors on two distinct home sites must trigger exactly one RPC per
// site, each carrying only that site's accounts, merged into one map
// stamped on the outgoing batch. HomeSiteID is the member's home site as
// resolved from the users collection at cache-fill time (mixed values within
// one room are the normal cross-site shape — the room's own subscription
// siteId would be identical for everyone and must not be used); the fill
// path itself is covered by TestMongoMemberLoader_Load_HomeSiteFromUsers and
// TestNotificationWorker_BadgeRPCs_GroupByUsersHomeSite (integration).
func TestHandle_BadgeCounts_TwoHomeSites_MergesPerSiteRPCs(t *testing.T) {
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice", HomeSiteID: "site-a"},
			{ID: "bob", Account: "bob", HomeSiteID: "site-a"},
			{ID: "carol", Account: "carol", HomeSiteID: "site-b"},
			{ID: "dave", Account: "dave", HomeSiteID: "site-b"},
		},
	}}
	badge := &fakeBadgeClient{resp: map[string]map[string]int{
		"site-a": {"bob": 2},
		"site-b": {"carol": 5, "dave": 9},
	}}
	emit := &recordingEmitter{}
	h := NewHandler(HandlerDeps{
		Members: members, Followers: &stubFollowers{}, Presence: noopPresenceSnapshotter{},
		Hook: noopVetoer{}, Emitter: emit, BadgeClient: badge, LargeRoomThreshold: 500,
	})

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice", CreatedAt: time.Now(),
	})))

	siteACalls := badge.callsFor("site-a")
	require.Len(t, siteACalls, 1)
	assert.Equal(t, []string{"bob"}, siteACalls[0].accounts)
	assert.Equal(t, "r1", siteACalls[0].roomID)

	siteBCalls := badge.callsFor("site-b")
	require.Len(t, siteBCalls, 1)
	assert.ElementsMatch(t, []string{"carol", "dave"}, siteBCalls[0].accounts)

	require.Len(t, emit.emitted, 1)
	assert.Equal(t, map[string]int{"bob": 2, "carol": 5, "dave": 9}, emit.emitted[0].UnreadCounts)
}

// A per-site RPC error must not fail the handler or drop the push — that
// site's accounts are simply absent from UnreadCounts.
func TestHandle_BadgeCounts_OneSiteErrors_AccountsAbsent(t *testing.T) {
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice", HomeSiteID: "site-a"},
			{ID: "bob", Account: "bob", HomeSiteID: "site-a"},
			{ID: "carol", Account: "carol", HomeSiteID: "site-b"},
		},
	}}
	badge := &fakeBadgeClient{
		resp: map[string]map[string]int{"site-b": {"carol": 3}},
		err:  map[string]error{"site-a": errors.New("user-service unreachable")},
	}
	emit := &recordingEmitter{}
	h := NewHandler(HandlerDeps{
		Members: members, Followers: &stubFollowers{}, Presence: noopPresenceSnapshotter{},
		Hook: noopVetoer{}, Emitter: emit, BadgeClient: badge, LargeRoomThreshold: 500,
	})

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice", CreatedAt: time.Now(),
	})), "badge RPC failure must not fail HandleMessage — the push ships without counts")

	require.Len(t, emit.emitted, 1)
	assert.ElementsMatch(t, []string{"bob", "carol"}, emit.emitted[0].Accounts, "publish still happens for both accounts")
	assert.Equal(t, map[string]int{"carol": 3}, emit.emitted[0].UnreadCounts, "bob's site errored — bob absent")
}

// A nil badge client (env-disabled or not wired) must skip the badge phase
// entirely: no RPCs, no UnreadCounts on the outgoing batch (Phase A compat).
func TestHandle_BadgeCounts_NilClient_NoRPCNoMap(t *testing.T) {
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice", HomeSiteID: "site-a"},
			{ID: "bob", Account: "bob", HomeSiteID: "site-a"},
		},
	}}
	emit := &recordingEmitter{}
	h := newTestHandler(members, &stubFollowers{}, noopPresenceSnapshotter{}, noopVetoer{}, emit) // BadgeClient unset → nil

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice", CreatedAt: time.Now(),
	})))

	require.Len(t, emit.emitted, 1)
	assert.Empty(t, emit.emitted[0].UnreadCounts, "nil badge client must produce no UnreadCounts")
}

// A survivor with no known home site (empty HomeSiteID — the account is
// missing from the users collection, or a stale pre-upgrade cache entry)
// degrades: no RPC is issued on its behalf and it is simply absent from
// UnreadCounts, but it still receives the push.
func TestHandle_BadgeCounts_EmptyHomeSiteIDMember_Skipped(t *testing.T) {
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice", HomeSiteID: "site-a"},
			{ID: "bob", Account: "bob", HomeSiteID: "site-a"},
			{ID: "carol", Account: "carol", HomeSiteID: ""},
		},
	}}
	badge := &fakeBadgeClient{resp: map[string]map[string]int{"site-a": {"bob": 4}}}
	emit := &recordingEmitter{}
	h := NewHandler(HandlerDeps{
		Members: members, Followers: &stubFollowers{}, Presence: noopPresenceSnapshotter{},
		Hook: noopVetoer{}, Emitter: emit, BadgeClient: badge, LargeRoomThreshold: 500,
	})

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice", CreatedAt: time.Now(),
	})))

	assert.Len(t, badge.calls, 1, "only the known-site account triggers an RPC")
	require.Len(t, emit.emitted, 1)
	assert.ElementsMatch(t, []string{"bob", "carol"}, emit.emitted[0].Accounts, "carol still receives the push")
	assert.Equal(t, map[string]int{"bob": 4}, emit.emitted[0].UnreadCounts, "carol (unknown site) absent from counts")
}

// UnreadCounts is filtered per batch: an account must only see counts data
// relevant to accounts actually present in that outgoing batch.
func TestHandle_BadgeCounts_StampedPerBatch_FilteredToBatchAccounts(t *testing.T) {
	roomMembers := []roomsubcache.Member{{ID: "alice", Account: "alice", HomeSiteID: "site-a"}}
	resp := map[string]int{}
	for i := 0; i < 150; i++ {
		account := fmt.Sprintf("u%03d", i)
		roomMembers = append(roomMembers, roomsubcache.Member{ID: account, Account: account, HomeSiteID: "site-a"})
		resp[account] = i % 10
	}
	members := &stubMembers{out: map[string][]roomsubcache.Member{"r1": roomMembers}}
	badge := &fakeBadgeClient{resp: map[string]map[string]int{"site-a": resp}}
	emit := &recordingEmitter{}
	h := NewHandler(HandlerDeps{
		Members: members, Followers: &stubFollowers{}, Presence: noopPresenceSnapshotter{},
		Hook: noopVetoer{}, Emitter: emit, BadgeClient: badge,
		LargeRoomThreshold: 1000, RecipientBatchSize: 100,
	})

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice", CreatedAt: time.Now(),
	})))

	require.Len(t, emit.emitted, 2, "150 recipients, batch size 100 → 2 batches")
	for _, batch := range emit.emitted {
		assert.Len(t, batch.UnreadCounts, len(batch.Accounts), "counts map must be filtered to exactly this batch's accounts")
		for _, account := range batch.Accounts {
			_, ok := batch.UnreadCounts[account]
			assert.True(t, ok, "account %s missing its own batch's UnreadCounts entry", account)
		}
	}
}

func TestHandle_DoesNotInvalidateOnRegularMessage(t *testing.T) {
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {{ID: "alice", Account: "alice"}, {ID: "bob", Account: "bob"}},
	}}
	emit := &recordingEmitter{}
	h := newTestHandler(members, &stubFollowers{}, noopPresenceSnapshotter{}, noopVetoer{}, emit)

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice",
		Content: "hello", CreatedAt: time.Now(),
	})))

	for _, c := range members.calls {
		assert.NotContains(t, c, "inval:", "regular messages must not invalidate cache")
	}
}

// TestHandle_SettingsFetchedOnlyForSurvivingCandidates pins the design: the fetch
// sits after the candidate loop, so it must never see accounts that an upstream
// filter already excluded. Hoisting it above the loop fails this loudly.
func TestHandle_SettingsFetchedOnlyForSurvivingCandidates(t *testing.T) {
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice"},              // sender — excluded
			{ID: "bob", Account: "bob", Muted: true},     // muted — excluded
			{ID: "carol", Account: "carol", IsBot: true}, // bot — excluded by EligibleForPush
			{ID: "dave", Account: "dave", HistorySharedSince: int64Ptr(time.Now().Add(time.Hour).UnixMilli())}, // restricted — excluded
			{ID: "frank", Account: "frank"}, // hook veto — excluded
			{ID: "erin", Account: "erin"},   // survivor
		},
	}}
	settings := &stubSettings{out: map[string]notifSettings{}}
	emit := &recordingEmitter{}
	hook := accountVetoer{deny: map[string]struct{}{"frank": {}}}
	h := newTestHandlerWithSettings(members, noopPresenceSnapshotter{}, settings, hook, emit)

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice",
		CreatedAt: time.Now(),
	})))

	assert.Equal(t, []string{"erin"}, settings.lastAccounts(),
		"settings must be fetched only for accounts that survived the candidate loop")
	assert.Equal(t, []string{"erin"}, emit.accounts())
}

func TestHandle_SettingsErrorFailsOpen(t *testing.T) {
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {{ID: "alice", Account: "alice"}, {ID: "bob", Account: "bob"}, {ID: "carol", Account: "carol"}},
	}}
	settings := &stubSettings{err: errors.New("mongo: connection refused")}
	emit := &recordingEmitter{}
	h := newTestHandlerWithSettings(members, noopPresenceSnapshotter{}, settings, noopVetoer{}, emit)

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice",
		CreatedAt: time.Now(),
	})))
	assert.Equal(t, []string{"bob", "carol"}, emit.accounts(),
		"a settings read failure must not silence pushes")
}

func TestHandle_SettingsPartialMapFailsOpenForAbsentAccounts(t *testing.T) {
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {{ID: "alice", Account: "alice"}, {ID: "bob", Account: "bob"}, {ID: "carol", Account: "carol"}},
	}}
	settings := &stubSettings{out: map[string]notifSettings{
		"bob": {muteAll: true}, // carol is absent → zero value → pushes
	}}
	emit := &recordingEmitter{}
	h := newTestHandlerWithSettings(members, noopPresenceSnapshotter{}, settings, noopVetoer{}, emit)

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice",
		CreatedAt: time.Now(),
	})))
	assert.Equal(t, []string{"carol"}, emit.accounts(),
		"bob is muted; carol is absent from the map and takes the zero value")
}

// TestHandle_PriorityContactSenderPiercesMute pins the Spec 1 affordance: a
// recipient who muted everything still gets pushed when the *sender's* account
// is in their priority-contact set (ns.isPriority(msg.UserAccount) — keyed on the
// sender, not the recipient, an easy-to-invert wiring detail). The sender here
// happens to be a .bot account, which works because priority contacts hold raw
// accounts — users and .bot alike — not because any bot-specific branch exists.
func TestHandle_PriorityContactSenderPiercesMute(t *testing.T) {
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "helper", Account: "helper.bot", IsBot: true},
			{ID: "bob", Account: "bob"},
			{ID: "carol", Account: "carol"},
		},
	}}
	settings := &stubSettings{out: map[string]notifSettings{
		"bob": {
			muteAll:          true,
			allowPriority:    true,
			priorityContacts: map[string]struct{}{"helper.bot": {}},
		},
		"carol": {muteAll: true, allowPriority: true}, // no priority contacts → stays muted
	}}
	emit := &recordingEmitter{}
	h := newTestHandlerWithSettings(members, noopPresenceSnapshotter{}, settings, noopVetoer{}, emit)

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "helper", UserAccount: "helper.bot",
		CreatedAt: time.Now(),
	})))
	assert.Equal(t, []string{"bob"}, emit.accounts(),
		"bob listed helper.bot as priority; carol did not")
}

// TestHandle_PriorityContactPiercesPresenceSuppression pins the reversal
// end-to-end: the pierce crosses the presence gate, and it is keyed on the
// sender's account — bob lists the sender, carol lists someone else, and both
// have the same suppressing presence.
func TestHandle_PriorityContactPiercesPresenceSuppression(t *testing.T) {
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice"},
			{ID: "bob", Account: "bob"},
			{ID: "carol", Account: "carol"},
		},
	}}
	presence := &stubPresence{out: map[string]model.Presence{
		"bob":   {AggregatedStatus: "busy"},
		"carol": {AggregatedStatus: "busy"},
	}}
	settings := &stubSettings{out: map[string]notifSettings{
		"bob":   {allowPriority: true, priorityContacts: map[string]struct{}{"alice": {}}},
		"carol": {allowPriority: true, priorityContacts: map[string]struct{}{"dave": {}}},
	}}
	emit := &recordingEmitter{}
	h := newTestHandlerWithSettings(members, presence, settings, noopVetoer{}, emit)

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice", CreatedAt: time.Now(),
	})))
	assert.Equal(t, []string{"bob"}, emit.accounts(),
		"bob lists the sender as a priority contact and is pierced out of presence suppression; carol does not")
}

// TestHandle_DNDAndPresentingSuppressPush proves the handler consults both stubs
// once they go live. Every recipient here has showNotificationsInCall set, so
// only a suppressor the opt-in does NOT govern can drop them — which is exactly
// rule 2. Uses invented statuses so the test asserts the wiring, not a mapping
// the presence side has yet to define.
func TestHandle_DNDAndPresentingSuppressPush(t *testing.T) {
	stubPresenceFlagsByStatus(t, "stub-dnd", "stub-presenting")

	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice"},
			{ID: "bob", Account: "bob"},
			{ID: "carol", Account: "carol"},
			{ID: "dave", Account: "dave"},
		},
	}}
	presence := &stubPresence{out: map[string]model.Presence{
		"bob":   {AggregatedStatus: "stub-dnd"},
		"carol": {AggregatedStatus: "stub-presenting"},
		"dave":  {AggregatedStatus: "online"},
	}}
	settings := &stubSettings{out: map[string]notifSettings{
		"bob":   {showInCall: true},
		"carol": {showInCall: true},
		"dave":  {showInCall: true},
	}}
	emit := &recordingEmitter{}
	h := newTestHandlerWithSettings(members, presence, settings, noopVetoer{}, emit)

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice", CreatedAt: time.Now(),
	})))
	assert.Equal(t, []string{"dave"}, emit.accounts(),
		"DND and presenting suppress regardless of showNotificationsInCall")
}

// TestHandle_PriorityContactPiercesDNDStub is the pierce counterpart: the same
// suppressed recipient survives when the sender is one of their priority contacts.
func TestHandle_PriorityContactPiercesDNDStub(t *testing.T) {
	stubPresenceFlagsByStatus(t, "stub-dnd", "")

	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice"},
			{ID: "bob", Account: "bob"},
			{ID: "carol", Account: "carol"},
		},
	}}
	presence := &stubPresence{out: map[string]model.Presence{
		"bob":   {AggregatedStatus: "stub-dnd"},
		"carol": {AggregatedStatus: "stub-dnd"},
	}}
	settings := &stubSettings{out: map[string]notifSettings{
		"bob":   {allowPriority: true, priorityContacts: map[string]struct{}{"alice": {}}},
		"carol": {allowPriority: true, priorityContacts: map[string]struct{}{"dave": {}}},
	}}
	emit := &recordingEmitter{}
	h := newTestHandlerWithSettings(members, presence, settings, noopVetoer{}, emit)

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice", CreatedAt: time.Now(),
	})))
	assert.Equal(t, []string{"bob"}, emit.accounts(),
		"bob lists the sender as a priority contact and is pierced out of DND; carol does not")
}

func int64Ptr(v int64) *int64 { return &v }

// stubMentionNames records the accounts it was asked to resolve so tests can pin
// both the substitution and where in the pipeline the lookup runs.
type stubMentionNames struct {
	mu       sync.Mutex
	out      map[string]string
	err      error
	gotCalls [][]string
}

func (s *stubMentionNames) Resolve(_ context.Context, accounts []string) (map[string]string, error) {
	s.mu.Lock()
	got := make([]string, len(accounts))
	copy(got, accounts)
	s.gotCalls = append(s.gotCalls, got)
	s.mu.Unlock()
	// out AND err together — the real resolver returns the names it collected
	// before failing, so nil-ing out here would hide the partial-result path.
	return s.out, s.err
}

func (s *stubMentionNames) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.gotCalls)
}

func (s *stubMentionNames) lastAccounts() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.gotCalls) == 0 {
		return nil
	}
	return s.gotCalls[len(s.gotCalls)-1]
}

// newTestHandlerWithMentions wires a mention resolver onto the default test rig.
func newTestHandlerWithMentions(members MemberCache, names MentionNameResolver, emit Emitter) *Handler {
	return NewHandler(HandlerDeps{
		Members:            members,
		Followers:          &stubFollowers{},
		Parent:             stubParent{},
		Presence:           noopPresenceSnapshotter{},
		Hook:               noopVetoer{},
		Emitter:            emit,
		MentionNames:       names,
		LargeRoomThreshold: 500,
	})
}

func mentionMembers() *stubMembers {
	return &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice"},
			{ID: "bob", Account: "bob"},
		},
	}}
}

func mentionMsg(content string) []byte {
	return msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice",
		Content: content, CreatedAt: time.Now(),
	})
}

func TestHandle_BodyReplacesMentionsWithDisplayNames(t *testing.T) {
	names := &stubMentionNames{out: map[string]string{"bob": "Bob Chen"}}
	emit := &recordingEmitter{}
	h := newTestHandlerWithMentions(mentionMembers(), names, emit)

	require.NoError(t, h.HandleMessage(context.Background(), mentionMsg("hey @bob look")))

	require.Len(t, emit.emitted, 1)
	assert.Equal(t, "hey Bob Chen look", emit.emitted[0].Body)
}

func TestHandle_BodyReplacesLiteralMentions(t *testing.T) {
	names := &stubMentionNames{out: map[string]string{"bob": "Bob Chen"}}
	emit := &recordingEmitter{}
	h := newTestHandlerWithMentions(mentionMembers(), names, emit)

	require.NoError(t, h.HandleMessage(context.Background(), mentionMsg("@all and @here and @bob")))

	require.Len(t, emit.emitted, 1)
	assert.Equal(t, "All and here and Bob Chen", emit.emitted[0].Body)
}

func TestHandle_BodyKeepsUnresolvedMentionVerbatim(t *testing.T) {
	names := &stubMentionNames{out: map[string]string{"bob": "Bob Chen"}}
	emit := &recordingEmitter{}
	h := newTestHandlerWithMentions(mentionMembers(), names, emit)

	require.NoError(t, h.HandleMessage(context.Background(), mentionMsg("@bob ping @ghost")))

	require.Len(t, emit.emitted, 1)
	assert.Equal(t, "Bob Chen ping @ghost", emit.emitted[0].Body,
		"an account with no display name keeps its @ marker rather than being invented")
}

func TestHandle_MentionResolverSeesOnlyAccountsNeedingLookup(t *testing.T) {
	names := &stubMentionNames{out: map[string]string{"bob": "Bob Chen"}}
	emit := &recordingEmitter{}
	h := newTestHandlerWithMentions(mentionMembers(), names, emit)

	require.NoError(t, h.HandleMessage(context.Background(), mentionMsg("@all @here @bob")))

	assert.Equal(t, []string{"bob"}, names.lastAccounts(),
		"@all/@here resolve from constants and must not hit the users collection")
}

func TestHandle_MentionResolverSkippedWithoutMentions(t *testing.T) {
	names := &stubMentionNames{}
	emit := &recordingEmitter{}
	h := newTestHandlerWithMentions(mentionMembers(), names, emit)

	require.NoError(t, h.HandleMessage(context.Background(), mentionMsg("no mentions here")))

	require.Len(t, emit.emitted, 1)
	assert.Equal(t, "no mentions here", emit.emitted[0].Body)
	assert.Zero(t, names.callCount(), "a message without mentions must not pay for a lookup")
}

func TestHandle_MentionResolverSkippedWhenNoSurvivors(t *testing.T) {
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {{ID: "alice", Account: "alice"}, {ID: "bob", Account: "bob", Muted: true}},
	}}
	names := &stubMentionNames{}
	emit := &recordingEmitter{}
	h := newTestHandlerWithMentions(members, names, emit)

	require.NoError(t, h.HandleMessage(context.Background(), mentionMsg("hey @bob")))

	assert.Empty(t, emit.emitted)
	assert.Zero(t, names.callCount(), "no surviving recipient means no lookup")
}

func TestHandle_MentionResolverFailsOpen(t *testing.T) {
	names := &stubMentionNames{err: errors.New("mongo down")}
	emit := &recordingEmitter{}
	h := newTestHandlerWithMentions(mentionMembers(), names, emit)

	require.NoError(t, h.HandleMessage(context.Background(), mentionMsg("hey @bob and @all")),
		"a lookup failure must not drop the push")

	require.Len(t, emit.emitted, 1)
	assert.Equal(t, "hey @bob and All", emit.emitted[0].Body,
		"accounts degrade to the raw token; literals still render")
}

func TestHandle_LiteralMentionsRenderWithoutResolver(t *testing.T) {
	emit := &recordingEmitter{}
	h := newTestHandlerWithMentions(mentionMembers(), nil, emit)

	require.NoError(t, h.HandleMessage(context.Background(), mentionMsg("@all hey @bob")))

	require.Len(t, emit.emitted, 1)
	assert.Equal(t, "All hey @bob", emit.emitted[0].Body,
		"a nil resolver still renders the literals and never panics")
}

func TestHandle_SubstitutedBodyRidesEveryBatch(t *testing.T) {
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice"},
			{ID: "bob", Account: "bob"},
			{ID: "carol", Account: "carol"},
		},
	}}
	names := &stubMentionNames{out: map[string]string{"bob": "Bob Chen"}}
	emit := &recordingEmitter{}
	h := NewHandler(HandlerDeps{
		Members:            members,
		Followers:          &stubFollowers{},
		Parent:             stubParent{},
		Presence:           noopPresenceSnapshotter{},
		Hook:               noopVetoer{},
		Emitter:            emit,
		MentionNames:       names,
		LargeRoomThreshold: 500,
		RecipientBatchSize: 1,
	})

	require.NoError(t, h.HandleMessage(context.Background(), mentionMsg("hey @bob")))

	require.Len(t, emit.emitted, 2, "two survivors at batch size 1")
	for i := range emit.emitted {
		assert.Equal(t, "hey Bob Chen", emit.emitted[i].Body, "batch %d", i)
	}
	assert.Equal(t, 1, names.callCount(), "one lookup per message, not per batch")
}

func TestHandle_MentionResolverCalledOnceForRepeatedMention(t *testing.T) {
	names := &stubMentionNames{out: map[string]string{"bob": "Bob Chen"}}
	emit := &recordingEmitter{}
	h := newTestHandlerWithMentions(mentionMembers(), names, emit)

	require.NoError(t, h.HandleMessage(context.Background(), mentionMsg("@bob then @BOB again")))

	require.Len(t, emit.emitted, 1)
	assert.Equal(t, "Bob Chen then Bob Chen again", emit.emitted[0].Body)
	assert.Equal(t, []string{"bob"}, names.lastAccounts(), "duplicate mentions dedup to one lookup key")
}

func TestHandle_MentionLookupIsCapped(t *testing.T) {
	var content string
	for i := 0; i < maxMentionLookups+25; i++ {
		content += fmt.Sprintf("@user%03d ", i)
	}
	names := &stubMentionNames{out: map[string]string{"user000": "User Zero"}}
	emit := &recordingEmitter{}
	h := newTestHandlerWithMentions(mentionMembers(), names, emit)

	require.NoError(t, h.HandleMessage(context.Background(), mentionMsg(content)))

	assert.Len(t, names.lastAccounts(), maxMentionLookups,
		"user-controlled content must not drive an unbounded $in")
	require.Len(t, emit.emitted, 1)
	assert.Contains(t, emit.emitted[0].Body, "User Zero", "tokens within the cap still resolve")
	assert.Contains(t, emit.emitted[0].Body, "@user070", "tokens past the cap keep their raw @token")
}

func TestHandle_MentionPartialResolutionOnError(t *testing.T) {
	// The production resolver returns names it did collect alongside the error;
	// the handler must substitute those rather than dropping the whole map.
	names := &stubMentionNames{
		out: map[string]string{"bob": "Bob Chen"},
		err: errors.New("mongo down"),
	}
	emit := &recordingEmitter{}
	h := newTestHandlerWithMentions(mentionMembers(), names, emit)

	require.NoError(t, h.HandleMessage(context.Background(), mentionMsg("@bob and @ghost and @all")))

	require.Len(t, emit.emitted, 1)
	assert.Equal(t, "Bob Chen and @ghost and All", emit.emitted[0].Body)
}

// TestHandle_MentionsAndBadgeCountsCoexist pins the two independently-added
// push-envelope phases against each other: substituted body and per-recipient
// unread counts must both survive on the same event.
func TestHandle_MentionsAndBadgeCountsCoexist(t *testing.T) {
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice", HomeSiteID: "site-a"},
			{ID: "bob", Account: "bob", HomeSiteID: "site-a"},
		},
	}}
	names := &stubMentionNames{out: map[string]string{"bob": "Bob Chen"}}
	emit := &recordingEmitter{}
	h := NewHandler(HandlerDeps{
		Members:            members,
		Followers:          &stubFollowers{},
		Parent:             stubParent{},
		Presence:           noopPresenceSnapshotter{},
		Hook:               noopVetoer{},
		Emitter:            emit,
		MentionNames:       names,
		BadgeClient:        &fakeBadgeClient{resp: map[string]map[string]int{"site-a": {"bob": 7}}},
		LargeRoomThreshold: 500,
	})

	require.NoError(t, h.HandleMessage(context.Background(), mentionMsg("hey @bob")))

	require.Len(t, emit.emitted, 1)
	assert.Equal(t, "hey Bob Chen", emit.emitted[0].Body, "body substitution survives the badge phase")
	assert.Equal(t, map[string]int{"bob": 7}, emit.emitted[0].UnreadCounts, "badge counts survive substitution")
}

// ---------------------------------------------------------------------------
// Performance: critical-path concurrency and badge chunking.
// ---------------------------------------------------------------------------

// arrivalBarrier releases every caller once n distinct callers are inside it.
// Concurrent callers pass immediately; a sequential pipeline never reaches n and
// the deadline fires, recording timedOut so the test fails with a clear message.
// The deadline is a failure escape, not goroutine synchronisation — the barrier
// itself is the synchronisation primitive.
type arrivalBarrier struct {
	n        int
	mu       sync.Mutex
	arrived  int
	release  chan struct{}
	timedOut atomic.Bool
}

func newArrivalBarrier(n int) *arrivalBarrier {
	return &arrivalBarrier{n: n, release: make(chan struct{})}
}

func (b *arrivalBarrier) arrive() {
	b.mu.Lock()
	b.arrived++
	if b.arrived == b.n {
		close(b.release)
	}
	b.mu.Unlock()
	select {
	case <-b.release:
	case <-time.After(3 * time.Second):
		b.timedOut.Store(true)
	}
}

// barrierSettings/barrierPresence/barrierBadge each park in the same barrier, so
// all three must be in flight simultaneously for HandleMessage to make progress.
type barrierSettings struct {
	b   *arrivalBarrier
	out map[string]notifSettings
}

func (s *barrierSettings) Snapshot(_ context.Context, _ []string) (map[string]notifSettings, error) {
	s.b.arrive()
	return s.out, nil
}

type barrierPresence struct {
	b   *arrivalBarrier
	out map[string]model.Presence
}

func (p *barrierPresence) Snapshot(_ context.Context, _ []string) (map[string]model.Presence, error) {
	p.b.arrive()
	return p.out, nil
}

type barrierBadge struct {
	b *arrivalBarrier
}

func (c *barrierBadge) Counts(_ context.Context, _, _ string, accounts []string) (map[string]int, error) {
	c.b.arrive()
	out := make(map[string]int, len(accounts))
	for _, a := range accounts {
		out[a] = 1
	}
	return out, nil
}

// TestHandle_SettingsPresenceBadgeRunConcurrently pins that the three independent
// critical-path lookups overlap. Run sequentially, none of them can reach the
// third arrival and the barrier times out.
func TestHandle_SettingsPresenceBadgeRunConcurrently(t *testing.T) {
	barrier := newArrivalBarrier(3)
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice", HomeSiteID: "site-a"},
			{ID: "bob", Account: "bob", HomeSiteID: "site-a"},
		},
	}}
	emit := &recordingEmitter{}
	h := NewHandler(HandlerDeps{
		Members:   members,
		Followers: &stubFollowers{},
		Presence:  &barrierPresence{b: barrier, out: map[string]model.Presence{}},
		Settings:  &barrierSettings{b: barrier, out: map[string]notifSettings{}},
		Hook:      noopVetoer{},
		Emitter:   emit,
		// BadgeClient is the third arrival; without it the barrier can't be met.
		BadgeClient:        &barrierBadge{b: barrier},
		LargeRoomThreshold: 500,
	})

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice", CreatedAt: time.Now(),
	})))

	assert.False(t, barrier.timedOut.Load(),
		"settings, presence and badge lookups must run concurrently, not back-to-back")
	assert.Equal(t, []string{"bob"}, emit.accounts(), "parallelising must not change the outcome")
}

// TestFetchUnreadCounts_ChunksLargeSiteAudience: one site with more accounts than
// BadgeBatchSize must be split across several bounded RPCs rather than sent as a
// single unbounded request, and every account's count must survive the merge.
func TestFetchUnreadCounts_ChunksLargeSiteAudience(t *testing.T) {
	const total = 1200
	const batch = 512

	accounts := make([]string, total)
	siteByAccount := make(map[string]string, total)
	for i := range accounts {
		accounts[i] = fmt.Sprintf("u%04d", i)
		siteByAccount[accounts[i]] = "site-a"
	}

	badge := &countingBadgeClient{}
	h := NewHandler(HandlerDeps{
		Members: &stubMembers{}, Followers: &stubFollowers{}, Presence: noopPresenceSnapshotter{},
		Hook: noopVetoer{}, Emitter: &recordingEmitter{},
		BadgeClient: badge, BadgeBatchSize: batch, LargeRoomThreshold: 500,
	})

	got := h.fetchUnreadCounts(context.Background(), "r1", accounts, siteByAccount)

	calls := badge.snapshot()
	assert.Len(t, calls, 3, "expect ceil(1200/512) badge RPCs")
	var seen []string
	for _, c := range calls {
		assert.LessOrEqual(t, len(c), batch, "no badge RPC may exceed BadgeBatchSize")
		seen = append(seen, c...)
	}
	assert.ElementsMatch(t, accounts, seen, "chunking must cover the whole audience exactly once")
	assert.Len(t, got, total, "every chunk's counts must be merged")
}

// TestFetchUnreadCounts_BoundsChunkConcurrency: chunk fan-out must stay inside
// BadgeConcurrency so a huge room can't spawn an unbounded RPC burst.
func TestFetchUnreadCounts_BoundsChunkConcurrency(t *testing.T) {
	const total = 2000
	const batch = 100
	const limit = 4

	accounts := make([]string, total)
	siteByAccount := make(map[string]string, total)
	for i := range accounts {
		accounts[i] = fmt.Sprintf("u%04d", i)
		siteByAccount[accounts[i]] = "site-a"
	}

	badge := &countingBadgeClient{hold: 20 * time.Millisecond}
	h := NewHandler(HandlerDeps{
		Members: &stubMembers{}, Followers: &stubFollowers{}, Presence: noopPresenceSnapshotter{},
		Hook: noopVetoer{}, Emitter: &recordingEmitter{},
		BadgeClient: badge, BadgeBatchSize: batch, BadgeConcurrency: limit, LargeRoomThreshold: 500,
	})

	h.fetchUnreadCounts(context.Background(), "r1", accounts, siteByAccount)

	assert.LessOrEqual(t, badge.maxInFlight.Load(), int64(limit),
		"badge chunk fan-out must respect BadgeConcurrency")
	assert.Greater(t, badge.maxInFlight.Load(), int64(1), "chunks should still overlap")
}

// countingBadgeClient records each call's account slice and tracks peak concurrency.
type countingBadgeClient struct {
	hold        time.Duration
	mu          sync.Mutex
	calls       [][]string
	inFlight    atomic.Int64
	maxInFlight atomic.Int64
}

func (c *countingBadgeClient) Counts(_ context.Context, _, _ string, accounts []string) (map[string]int, error) {
	n := c.inFlight.Add(1)
	for {
		peak := c.maxInFlight.Load()
		if n <= peak || c.maxInFlight.CompareAndSwap(peak, n) {
			break
		}
	}
	defer c.inFlight.Add(-1)
	if c.hold > 0 {
		// Widen the overlap window so peak concurrency is observable; this gates
		// the fake RPC's duration, it does not synchronise the goroutines.
		time.Sleep(c.hold)
	}

	c.mu.Lock()
	c.calls = append(c.calls, append([]string(nil), accounts...))
	c.mu.Unlock()

	out := make(map[string]int, len(accounts))
	for _, a := range accounts {
		out[a] = 1
	}
	return out, nil
}

func (c *countingBadgeClient) snapshot() [][]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([][]string(nil), c.calls...)
}
