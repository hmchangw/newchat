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

func (s *stubFollowers) Followers(_ context.Context, parentID string) (map[string]struct{}, error) {
	if v, ok := s.out[parentID]; ok {
		return v, nil
	}
	return map[string]struct{}{}, nil
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

func TestHandle_ThreadOnlyReply_NilParentCreatedAt_Restricted(t *testing.T) {
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
	h := newTestHandler(members, followers, noopPresenceSnapshotter{}, noopVetoer{}, emit)

	threshold := int64(1700000000000)
	members.out["r1"][1].HistorySharedSince = &threshold

	msg := model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice", CreatedAt: time.Now(),
		ThreadParentMessageID:        "parent-1",
		ThreadParentMessageCreatedAt: nil, // legacy: no parent ts
		TShow:                        false,
	}
	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&msg)))
	assert.Empty(t, emit.accounts(), "nil parent CreatedAt with HistorySharedSince must restrict bob")
}

type errFollowers struct{}

func (errFollowers) Followers(context.Context, string) (map[string]struct{}, error) {
	return nil, fmt.Errorf("mongo timeout")
}

func TestHandle_ThreadFollowersError_FailOpenEmptySet(t *testing.T) {
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
	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(msg)))
	assert.Empty(t, emit.accounts(), "non-mentioned non-followers are dropped when follower lookup fails")
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

			require.GreaterOrEqual(t, len(members.calls), 2)
			assert.Equal(t, []string{"inval:r1", "get:r1"}, members.calls[:2], "Invalidate must happen before GetMembers to avoid stale read")
		})
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
