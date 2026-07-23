package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/roomkeysender"
	"github.com/hmchangw/chat/pkg/roomkeystore"
	"github.com/hmchangw/chat/pkg/subject"
)

type fakeStore struct {
	InsertRoomFn             func(ctx context.Context, r *Room) error
	FindRoomFn               func(ctx context.Context, id string) (*Room, error)
	UpsertSubscriptionFn     func(ctx context.Context, s *Subscription) (bool, error)
	DeleteSubscriptionFn     func(ctx context.Context, r, u string) (bool, error)
	FindUserFn               func(ctx context.Context, id string) (*model.User, error)
	ListRoomMemberAccountsFn func(ctx context.Context, roomID string) ([]string, error)
}

func (f *fakeStore) InsertRoom(ctx context.Context, r *Room) error { return f.InsertRoomFn(ctx, r) }
func (f *fakeStore) FindRoom(ctx context.Context, id string) (*Room, error) {
	return f.FindRoomFn(ctx, id)
}
func (f *fakeStore) UpsertSubscription(ctx context.Context, s *Subscription) (bool, error) {
	return f.UpsertSubscriptionFn(ctx, s)
}
func (f *fakeStore) DeleteSubscription(ctx context.Context, r, u string) (bool, error) {
	return f.DeleteSubscriptionFn(ctx, r, u)
}
func (f *fakeStore) FindUser(ctx context.Context, id string) (*model.User, error) {
	return f.FindUserFn(ctx, id)
}
func (f *fakeStore) ListRoomMemberAccounts(ctx context.Context, roomID string) ([]string, error) {
	if f.ListRoomMemberAccountsFn != nil {
		return f.ListRoomMemberAccountsFn(ctx, roomID)
	}
	return nil, nil
}

type captureOutbox struct {
	mu    struct{ int32 }
	calls []struct{ Subj, MsgID string }
}

func (c *captureOutbox) publish(_ context.Context, subj string, _ []byte, msgID string) error {
	atomic.AddInt32(&c.mu.int32, 1)
	c.calls = append(c.calls, struct{ Subj, MsgID string }{subj, msgID})
	return nil
}

func ident() BotIdentity {
	return BotIdentity{ID: "bot-1", Account: "myapp.bot", SiteID: "site-a", EngName: "PayrollBot"}
}

func withIdentity(t *testing.T, roomID string, i BotIdentity) *natsrouter.Context { //nolint:gocritic // hugeParam
	t.Helper()
	c := natsrouter.NewContext(map[string]string{"roomID": roomID})
	msg := &nats.Msg{Header: nats.Header{}}
	body, _ := json.Marshal(i)
	msg.Header.Set(model.HeaderBotIdentity, string(body))
	c.Msg = msg
	return c
}

func TestHandleCreate_HappyPath(t *testing.T) {
	store := &fakeStore{
		InsertRoomFn:         func(_ context.Context, _ *Room) error { return nil },
		UpsertSubscriptionFn: func(_ context.Context, _ *Subscription) (bool, error) { return true, nil },
	}
	cap := &captureOutbox{}
	h := newHandler(store, "site-a", []string{"site-b"}, cap.publish, testKeyStore, testKeySender)

	c := withIdentity(t, "", ident())
	resp, err := h.handleCreate(c, BotCreateRoomRequest{Name: "deployments", Topic: "CI"})
	require.NoError(t, err)
	assert.NotEmpty(t, resp.ID)
	assert.Equal(t, "deployments", resp.Name)
	assert.Equal(t, "bot-1", resp.Owner.ID)
	assert.True(t, resp.Owner.IsBot)
	assert.Equal(t, []string{"bot-1"}, resp.Members)
}

func TestHandleCreate_MissingName(t *testing.T) {
	h := newHandler(&fakeStore{}, "site-a", nil, (&captureOutbox{}).publish, testKeyStore, testKeySender)
	c := withIdentity(t, "", ident())
	_, err := h.handleCreate(c, BotCreateRoomRequest{})
	require.Error(t, err)
	var ec *errcode.Error
	require.True(t, isErrcode(err, &ec))
	assert.Equal(t, errcode.CodeBadRequest, ec.Code)
	assert.Equal(t, "content_invalid", string(ec.Reason))
}

func TestHandleCreate_RemoteMemberFederates(t *testing.T) {
	store := &fakeStore{
		InsertRoomFn:         func(_ context.Context, _ *Room) error { return nil },
		UpsertSubscriptionFn: func(_ context.Context, _ *Subscription) (bool, error) { return true, nil },
		FindUserFn: func(_ context.Context, id string) (*model.User, error) {
			return &model.User{ID: id, Account: "alice", SiteID: "site-b"}, nil
		},
	}
	cap := &captureOutbox{}
	h := newHandler(store, "site-a", []string{"site-b"}, cap.publish, testKeyStore, testKeySender)
	c := withIdentity(t, "", ident())

	_, err := h.handleCreate(c, BotCreateRoomRequest{Name: "deployments", Members: []string{"alice-id"}})
	require.NoError(t, err)
	require.Len(t, cap.calls, 1, "remote member triggers one outbox publish")
	assert.Contains(t, cap.calls[0].Subj, "chat.outbox.site-a.site-b.member_added")
	assert.True(t, strings.HasPrefix(cap.calls[0].MsgID, "bot-add:"))
}

func TestHandleAdd_LocalMemberNoFederation(t *testing.T) {
	store := &fakeStore{
		FindRoomFn: func(_ context.Context, _ string) (*Room, error) {
			return &Room{ID: "r1", Type: "c", CreatedByBot: "bot-1"}, nil
		},
		UpsertSubscriptionFn: func(_ context.Context, _ *Subscription) (bool, error) { return true, nil },
		FindUserFn: func(_ context.Context, id string) (*model.User, error) {
			return &model.User{ID: id, Account: "bob", SiteID: "site-a"}, nil
		},
	}
	cap := &captureOutbox{}
	h := newHandler(store, "site-a", nil, cap.publish, testKeyStore, testKeySender)
	c := withIdentity(t, "r1", ident())

	resp, err := h.handleAdd(c, BotMembersBatchRequest{UserIDs: []string{"bob-id"}})
	require.NoError(t, err)
	assert.Equal(t, []string{"bob-id"}, resp.Added.UserIDs)
	assert.Empty(t, cap.calls, "local member does NOT federate")
}

func TestHandleAdd_DuplicateIsNoop(t *testing.T) {
	store := &fakeStore{
		FindRoomFn: func(_ context.Context, _ string) (*Room, error) {
			return &Room{ID: "r1", Type: "c", CreatedByBot: "bot-1"}, nil
		},
		UpsertSubscriptionFn: func(_ context.Context, _ *Subscription) (bool, error) {
			return false, nil
		},
		FindUserFn: func(_ context.Context, id string) (*model.User, error) {
			return &model.User{ID: id, Account: "bob", SiteID: "site-a"}, nil
		},
	}
	cap := &captureOutbox{}
	h := newHandler(store, "site-a", nil, cap.publish, testKeyStore, testKeySender)
	c := withIdentity(t, "r1", ident())

	resp, err := h.handleAdd(c, BotMembersBatchRequest{UserIDs: []string{"bob-id"}})
	require.NoError(t, err)
	assert.Empty(t, resp.Added.UserIDs, "already-present member is not in the diff")
	assert.Empty(t, cap.calls, "no outbox publish on dup")
}

func TestHandleRemove_CannotRemoveSelf(t *testing.T) {
	store := &fakeStore{
		FindRoomFn: func(_ context.Context, _ string) (*Room, error) {
			return &Room{ID: "r1", Type: "c", CreatedByBot: "bot-1"}, nil
		},
	}
	h := newHandler(store, "site-a", nil, (&captureOutbox{}).publish, testKeyStore, testKeySender)
	c := withIdentity(t, "r1", ident())

	_, err := h.handleRemove(c, BotMembersBatchRequest{UserIDs: []string{"bot-1"}})
	require.Error(t, err)
	var ec *errcode.Error
	require.True(t, isErrcode(err, &ec))
	assert.Equal(t, errcode.CodeForbidden, ec.Code)
	assert.Equal(t, "cannot_remove_self", string(ec.Reason))
}

func TestHandleGet_MissingIsNotFound(t *testing.T) {
	store := &fakeStore{
		FindRoomFn: func(_ context.Context, _ string) (*Room, error) { return nil, ErrNotFound },
	}
	h := newHandler(store, "site-a", nil, (&captureOutbox{}).publish, testKeyStore, testKeySender)
	c := natsrouter.NewContext(nil)
	c.Msg = &nats.Msg{Header: nats.Header{}}
	_, err := h.handleGet(c, BotRoomGetRequest{RoomID: "missing"})
	require.Error(t, err)
	var ec *errcode.Error
	require.True(t, isErrcode(err, &ec))
	assert.Equal(t, errcode.CodeNotFound, ec.Code)
}

// TestHandleDMEnsure_DeterministicRoomID: two Ensure calls for the same
// (bot, target) pair — regardless of argument order at the two originating
// sides — MUST derive the same room ID. Guards the design's DM race
// resolution: both sides converge on idgen.BuildDMRoomID(a, b).
func TestHandleDMEnsure_DeterministicRoomID(t *testing.T) {
	var inserted []string
	store := &fakeStore{
		InsertRoomFn: func(_ context.Context, r *Room) error {
			inserted = append(inserted, r.ID)
			return nil
		},
		UpsertSubscriptionFn: func(_ context.Context, _ *Subscription) (bool, error) { return true, nil },
		FindUserFn: func(_ context.Context, id string) (*model.User, error) {
			return &model.User{ID: id, Account: "alice", SiteID: "site-a"}, nil
		},
	}
	h := newHandler(store, "site-a", nil, (&captureOutboxPayload{}).publish, testKeyStore, testKeySender)

	resp1, err := h.handleDMEnsure(withIdentity(t, "", ident()), BotDMEnsureRequest{TargetUserID: "alice-id"})
	require.NoError(t, err)
	resp2, err := h.handleDMEnsure(withIdentity(t, "", ident()), BotDMEnsureRequest{TargetUserID: "alice-id"})
	require.NoError(t, err)

	assert.Equal(t, resp1.RoomID, resp2.RoomID, "same (bot, target) must produce same DM room ID")
	require.GreaterOrEqual(t, len(inserted), 1)
	for _, id := range inserted {
		assert.Equal(t, resp1.RoomID, id, "every InsertRoom carries the deterministic ID")
	}
}

// TestHandleDMEnsure_DuplicateInsertIsBenign: a second Ensure hitting an
// already-materialized DM must succeed silently rather than 500.
func TestHandleDMEnsure_DuplicateInsertIsBenign(t *testing.T) {
	store := &fakeStore{
		InsertRoomFn:         func(_ context.Context, _ *Room) error { return ErrDuplicate },
		UpsertSubscriptionFn: func(_ context.Context, _ *Subscription) (bool, error) { return false, nil },
		FindUserFn: func(_ context.Context, id string) (*model.User, error) {
			return &model.User{ID: id, Account: "alice", SiteID: "site-a"}, nil
		},
	}
	h := newHandler(store, "site-a", nil, (&captureOutboxPayload{}).publish, testKeyStore, testKeySender)
	resp, err := h.handleDMEnsure(withIdentity(t, "", ident()), BotDMEnsureRequest{TargetUserID: "alice-id"})
	require.NoError(t, err)
	assert.NotEmpty(t, resp.RoomID)
}

func TestHandleDMEnsure_RemoteTargetFederatesAsBotDM(t *testing.T) {
	store := &fakeStore{
		InsertRoomFn:         func(_ context.Context, _ *Room) error { return nil },
		UpsertSubscriptionFn: func(_ context.Context, _ *Subscription) (bool, error) { return true, nil },
		FindUserFn: func(_ context.Context, id string) (*model.User, error) {
			return &model.User{ID: id, Account: "alice", SiteID: "site-b"}, nil
		},
	}
	cap := &captureOutboxPayload{}
	h := newHandler(store, "site-a", []string{"site-b"}, cap.publish, testKeyStore, testKeySender)
	c := withIdentity(t, "", ident())

	_, err := h.handleDMEnsure(c, BotDMEnsureRequest{TargetUserID: "alice-id"})
	require.NoError(t, err)
	require.Len(t, cap.payloads, 1, "remote DM target triggers one member_added federation")

	var evt model.MemberAddEvent
	require.NoError(t, json.Unmarshal(cap.payloads[0], &evt))
	assert.Equal(t, model.RoomTypeBotDM, evt.RoomType, "DM federation must stamp roomType=botDM")
	assert.Equal(t, "myapp.bot", evt.RequesterAccount, "RequesterAccount is the bot's account for DM naming")
	assert.Equal(t, []string{"alice"}, evt.Accounts)
	assert.Equal(t, "site-a", evt.SiteID, "SiteID = DM origin (bot's site)")
}

func TestHandleCreate_ChannelFederationCarriesRoomName(t *testing.T) {
	store := &fakeStore{
		InsertRoomFn:         func(_ context.Context, _ *Room) error { return nil },
		UpsertSubscriptionFn: func(_ context.Context, _ *Subscription) (bool, error) { return true, nil },
		FindUserFn: func(_ context.Context, id string) (*model.User, error) {
			return &model.User{ID: id, Account: "alice", SiteID: "site-b"}, nil
		},
	}
	cap := &captureOutboxPayload{}
	h := newHandler(store, "site-a", []string{"site-b"}, cap.publish, testKeyStore, testKeySender)
	c := withIdentity(t, "", ident())

	_, err := h.handleCreate(c, BotCreateRoomRequest{Name: "deployments", Members: []string{"alice-id"}})
	require.NoError(t, err)
	require.Len(t, cap.payloads, 1)

	var evt model.MemberAddEvent
	require.NoError(t, json.Unmarshal(cap.payloads[0], &evt))
	assert.Equal(t, model.RoomTypeChannel, evt.RoomType)
	assert.Equal(t, "deployments", evt.RoomName, "channel federation carries RoomName for inbox-worker naming")
}

// captureOutboxPayload records raw OutboxEvent bodies so tests can decode inner payloads.
type captureOutboxPayload struct {
	payloads [][]byte
}

func (c *captureOutboxPayload) publish(_ context.Context, _ string, data []byte, _ string) error {
	var outboxEnv model.OutboxEvent
	if err := json.Unmarshal(data, &outboxEnv); err != nil {
		return err
	}
	var inbox model.InboxEvent
	if err := json.Unmarshal(outboxEnv.Envelope, &inbox); err != nil {
		return err
	}
	c.payloads = append(c.payloads, inbox.Payload)
	return nil
}

func TestHandleGet_ReturnsAuthoritativeRoom(t *testing.T) {
	created := time.Now().UTC()
	store := &fakeStore{
		FindRoomFn: func(_ context.Context, id string) (*Room, error) {
			return &Room{ID: id, Type: "c", Name: "ops", Topic: "on-call", SiteID: "site-a", CreatedAt: created}, nil
		},
	}
	h := newHandler(store, "site-a", nil, (&captureOutbox{}).publish, testKeyStore, testKeySender)
	c := natsrouter.NewContext(nil)
	c.Msg = &nats.Msg{Header: nats.Header{}}
	resp, err := h.handleGet(c, BotRoomGetRequest{RoomID: "r1"})
	require.NoError(t, err)
	assert.Equal(t, "r1", resp.ID)
	assert.Equal(t, "c", resp.Type)
	assert.Equal(t, "ops", resp.Name)
	assert.Equal(t, "site-a", resp.SiteID)
	assert.Equal(t, created, resp.CreatedAt)
}

// fakeSysmsgPub records every sysmsg publish for LOCAL-subject and payload assertions.
type fakeSysmsgPub struct {
	calls     int
	lastSubj  string
	lastData  []byte
	lastMsgID string
}

func (f *fakeSysmsgPub) PublishWithMsgID(_ context.Context, subj string, data []byte, msgID string) error {
	f.calls++
	f.lastSubj = subj
	f.lastData = append([]byte(nil), data...)
	f.lastMsgID = msgID
	return nil
}

func TestHandleCreate_EmitsLocalSysmsg(t *testing.T) {
	store := &fakeStore{
		InsertRoomFn:         func(_ context.Context, _ *Room) error { return nil },
		UpsertSubscriptionFn: func(_ context.Context, _ *Subscription) (bool, error) { return true, nil },
	}
	sys := &fakeSysmsgPub{}
	h := newHandler(store, "site-a", nil, (&captureOutbox{}).publish, testKeyStore, testKeySender)
	h.sysmsgPub = sys
	c := withIdentity(t, "", ident())

	_, err := h.handleCreate(c, BotCreateRoomRequest{Name: "deployments"})
	require.NoError(t, err)

	require.Equal(t, 1, sys.calls, "one sysmsg per create")
	assert.Equal(t, "chat.bot.canonical.site-a.created", sys.lastSubj, "LOCAL bot canonical only")
	assert.Contains(t, sys.lastMsgID, "bot-sysmsg:")

	var envelope model.Message
	require.NoError(t, json.Unmarshal(sys.lastData, &envelope))
	assert.Equal(t, model.MessageTypeMembersAdded, envelope.Type)
	assert.Equal(t, "bot-1", envelope.UserID)
	assert.Equal(t, "myapp.bot", envelope.UserAccount)

	var payload model.MembersAdded
	require.NoError(t, json.Unmarshal(envelope.SysMsgData, &payload))
	assert.Equal(t, 1, payload.AddedUsersCount, "seed is just the owner")
	assert.Equal(t, []string{"bot-1"}, payload.Individuals)
}

func TestHandleAdd_DuplicateBatchEmitsNoSysmsg(t *testing.T) {
	store := &fakeStore{
		FindRoomFn: func(_ context.Context, _ string) (*Room, error) {
			return &Room{ID: "r1", Type: "c", CreatedByBot: "bot-1"}, nil
		},
		UpsertSubscriptionFn: func(_ context.Context, _ *Subscription) (bool, error) {
			return false, nil
		},
		FindUserFn: func(_ context.Context, id string) (*model.User, error) {
			return &model.User{ID: id, Account: "bob", SiteID: "site-a"}, nil
		},
	}
	sys := &fakeSysmsgPub{}
	h := newHandler(store, "site-a", nil, (&captureOutbox{}).publish, testKeyStore, testKeySender)
	h.sysmsgPub = sys
	c := withIdentity(t, "r1", ident())

	resp, err := h.handleAdd(c, BotMembersBatchRequest{UserIDs: []string{"bob-id"}})
	require.NoError(t, err)
	assert.Empty(t, resp.Added.UserIDs)
	assert.Equal(t, 0, sys.calls, "no sysmsg on dup-only batch")
}

func TestHandleAdd_NonEmptyDiffEmitsSysmsg(t *testing.T) {
	store := &fakeStore{
		FindRoomFn: func(_ context.Context, _ string) (*Room, error) {
			return &Room{ID: "r1", Type: "c", CreatedByBot: "bot-1"}, nil
		},
		UpsertSubscriptionFn: func(_ context.Context, _ *Subscription) (bool, error) { return true, nil },
		FindUserFn: func(_ context.Context, id string) (*model.User, error) {
			return &model.User{ID: id, Account: "bob", SiteID: "site-a"}, nil
		},
	}
	sys := &fakeSysmsgPub{}
	h := newHandler(store, "site-a", nil, (&captureOutbox{}).publish, testKeyStore, testKeySender)
	h.sysmsgPub = sys
	c := withIdentity(t, "r1", ident())

	_, err := h.handleAdd(c, BotMembersBatchRequest{UserIDs: []string{"bob-id"}})
	require.NoError(t, err)
	require.Equal(t, 1, sys.calls)
	var envelope model.Message
	require.NoError(t, json.Unmarshal(sys.lastData, &envelope))
	assert.Equal(t, model.MessageTypeMembersAdded, envelope.Type)
}

func TestHandleRemove_EmitsSysmsgWhenSomethingChanged(t *testing.T) {
	store := &fakeStore{
		FindRoomFn: func(_ context.Context, _ string) (*Room, error) {
			return &Room{ID: "r1", Type: "c", CreatedByBot: "bot-1"}, nil
		},
		DeleteSubscriptionFn: func(_ context.Context, _, _ string) (bool, error) { return true, nil },
		FindUserFn: func(_ context.Context, id string) (*model.User, error) {
			return &model.User{ID: id, Account: "bob", SiteID: "site-a"}, nil
		},
	}
	sys := &fakeSysmsgPub{}
	h := newHandler(store, "site-a", nil, (&captureOutbox{}).publish, testKeyStore, testKeySender)
	h.sysmsgPub = sys
	c := withIdentity(t, "r1", ident())

	_, err := h.handleRemove(c, BotMembersBatchRequest{UserIDs: []string{"bob-id"}})
	require.NoError(t, err)
	require.Equal(t, 1, sys.calls)
	var envelope model.Message
	require.NoError(t, json.Unmarshal(sys.lastData, &envelope))
	assert.Equal(t, model.MessageTypeMemberRemoved, envelope.Type)
	var payload model.MemberRemoved
	require.NoError(t, json.Unmarshal(envelope.SysMsgData, &payload))
	assert.Equal(t, 1, payload.RemovedUsersCount)
}

func TestHandleAdd_RejectsNonOwner(t *testing.T) {
	store := &fakeStore{
		FindRoomFn: func(_ context.Context, _ string) (*Room, error) {
			return &Room{ID: "r1", Type: "c", CreatedByBot: "some-other-bot"}, nil
		},
	}
	h := newHandler(store, "site-a", nil, (&captureOutbox{}).publish, testKeyStore, testKeySender)
	c := withIdentity(t, "r1", ident())

	_, err := h.handleAdd(c, BotMembersBatchRequest{UserIDs: []string{"bob-id"}})
	require.Error(t, err)
	var ec *errcode.Error
	require.True(t, errors.As(err, &ec))
	assert.Equal(t, errcode.CodeForbidden, ec.Code)
	assert.Equal(t, string(errcode.BotNotARoomOwner), string(ec.Reason))
}

func TestHandleRemove_RejectsNonOwner(t *testing.T) {
	store := &fakeStore{
		FindRoomFn: func(_ context.Context, _ string) (*Room, error) {
			return &Room{ID: "r1", Type: "c", CreatedByBot: "some-other-bot"}, nil
		},
	}
	h := newHandler(store, "site-a", nil, (&captureOutbox{}).publish, testKeyStore, testKeySender)
	c := withIdentity(t, "r1", ident())

	_, err := h.handleRemove(c, BotMembersBatchRequest{UserIDs: []string{"bob-id"}})
	require.Error(t, err)
	var ec *errcode.Error
	require.True(t, errors.As(err, &ec))
	assert.Equal(t, errcode.CodeForbidden, ec.Code)
	assert.Equal(t, string(errcode.BotNotARoomOwner), string(ec.Reason))
}

// A sysmsg publish error is logged, not returned; state write already succeeded.
type failingSysmsgReal struct{ calls int }

func (f *failingSysmsgReal) PublishWithMsgID(_ context.Context, _ string, _ []byte, _ string) error {
	f.calls++
	return errors.New("nats down")
}

func TestSysmsgPublishFailureDoesNotFailOp(t *testing.T) {
	store := &fakeStore{
		InsertRoomFn:         func(_ context.Context, _ *Room) error { return nil },
		UpsertSubscriptionFn: func(_ context.Context, _ *Subscription) (bool, error) { return true, nil },
	}
	sys := &failingSysmsgReal{}
	h := newHandler(store, "site-a", nil, (&captureOutbox{}).publish, testKeyStore, testKeySender)
	h.sysmsgPub = sys
	c := withIdentity(t, "", ident())

	resp, err := h.handleCreate(c, BotCreateRoomRequest{Name: "deployments"})
	require.NoError(t, err, "sysmsg failure must not fail the outer op")
	assert.NotEmpty(t, resp.ID)
	assert.Equal(t, 1, sys.calls, "publish was attempted")
}

// TestHandleCreate_GeneratesAndFansOutRoomKey: CreateChannel generates a room
// key, stores it under the new room's ID, and fans it out to the owner's
// per-account key-update subject.
func TestHandleCreate_GeneratesAndFansOutRoomKey(t *testing.T) {
	store := &fakeStore{
		InsertRoomFn:         func(_ context.Context, _ *Room) error { return nil },
		UpsertSubscriptionFn: func(_ context.Context, _ *Subscription) (bool, error) { return true, nil },
	}
	var setRoomID string
	keyStore := &fakeKeyStore{
		SetFn: func(_ context.Context, roomID string, _ roomkeystore.RoomKeyPair) (int, error) {
			setRoomID = roomID
			return 1, nil
		},
	}
	pub := &fakePublisher{}
	keySender := roomkeysender.NewSender(pub)
	h := newHandler(store, "site-a", nil, (&captureOutbox{}).publish, keyStore, keySender)
	c := withIdentity(t, "", ident())

	resp, err := h.handleCreate(c, BotCreateRoomRequest{Name: "deployments"})
	require.NoError(t, err)
	assert.Equal(t, resp.ID, setRoomID, "room key is stored under the new room's ID")

	require.Len(t, pub.subjects, 1, "owner receives one key event")
	assert.Equal(t, subject.RoomKeyUpdate("myapp.bot"), pub.subjects[0], "fan-out targets the owner's account")

	var evt model.RoomKeyEvent
	require.NoError(t, json.Unmarshal(pub.payloads[0], &evt))
	assert.Equal(t, resp.ID, evt.RoomID)
	assert.Equal(t, 1, evt.Version)
	assert.NotEmpty(t, evt.PrivateKey)
}

// TestHandleCreate_KeyFanOutFailureDoesNotFailOp: the owner's key is already
// durably stored once keyStore.Set succeeds — a fan-out publish failure is
// logged, not surfaced as a room-creation error.
func TestHandleCreate_KeyFanOutFailureDoesNotFailOp(t *testing.T) {
	store := &fakeStore{
		InsertRoomFn:         func(_ context.Context, _ *Room) error { return nil },
		UpsertSubscriptionFn: func(_ context.Context, _ *Subscription) (bool, error) { return true, nil },
	}
	failPub := &failingPublisher{}
	h := newHandler(store, "site-a", nil, (&captureOutbox{}).publish, &fakeKeyStore{}, roomkeysender.NewSender(failPub))
	c := withIdentity(t, "", ident())

	resp, err := h.handleCreate(c, BotCreateRoomRequest{Name: "deployments"})
	require.NoError(t, err, "key fan-out failure must not fail room creation")
	assert.NotEmpty(t, resp.ID)
	assert.Equal(t, 1, failPub.calls, "fan-out was attempted")
}

// TestHandleCreate_KeyStoreSetErrorFailsOp: unlike fan-out, a failure to
// durably store the room key must fail the create — otherwise the room would
// exist with no way to ever encrypt/decrypt its messages.
func TestHandleCreate_KeyStoreSetErrorFailsOp(t *testing.T) {
	store := &fakeStore{
		InsertRoomFn:         func(_ context.Context, _ *Room) error { return nil },
		UpsertSubscriptionFn: func(_ context.Context, _ *Subscription) (bool, error) { return true, nil },
	}
	keyStore := &fakeKeyStore{
		SetFn: func(_ context.Context, _ string, _ roomkeystore.RoomKeyPair) (int, error) {
			return 0, errors.New("mongo down")
		},
	}
	h := newHandler(store, "site-a", nil, (&captureOutbox{}).publish, keyStore, testKeySender)
	c := withIdentity(t, "", ident())

	_, err := h.handleCreate(c, BotCreateRoomRequest{Name: "deployments"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "store room key")
}

// TestHandleAdd_NewMembersReceiveRoomKey: every newly-subscribed member
// (UpsertSubscription created=true) receives the room's current key via
// keySender.Send exactly once.
func TestHandleAdd_NewMembersReceiveRoomKey(t *testing.T) {
	store := &fakeStore{
		FindRoomFn: func(_ context.Context, _ string) (*Room, error) {
			return &Room{ID: "r1", Type: "c", CreatedByBot: "bot-1"}, nil
		},
		UpsertSubscriptionFn: func(_ context.Context, _ *Subscription) (bool, error) { return true, nil },
		FindUserFn: func(_ context.Context, id string) (*model.User, error) {
			return &model.User{ID: id, Account: id + "-acct", SiteID: "site-a"}, nil
		},
	}
	var getRoomID string
	keyStore := &fakeKeyStore{
		GetFn: func(_ context.Context, roomID string) (*roomkeystore.VersionedKeyPair, error) {
			getRoomID = roomID
			return &roomkeystore.VersionedKeyPair{
				Version: 3,
				KeyPair: roomkeystore.RoomKeyPair{PrivateKey: []byte("room-secret-key-bytes")},
			}, nil
		},
	}
	pub := &fakePublisher{}
	h := newHandler(store, "site-a", nil, (&captureOutbox{}).publish, keyStore, roomkeysender.NewSender(pub))
	c := withIdentity(t, "r1", ident())

	resp, err := h.handleAdd(c, BotMembersBatchRequest{UserIDs: []string{"bob-id", "carol-id"}})
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"bob-id", "carol-id"}, resp.Added.UserIDs)
	assert.Equal(t, "r1", getRoomID)

	require.Len(t, pub.subjects, 2, "one key event per newly-added member")
	assert.ElementsMatch(t, []string{
		subject.RoomKeyUpdate("bob-id-acct"),
		subject.RoomKeyUpdate("carol-id-acct"),
	}, pub.subjects)

	for _, payload := range pub.payloads {
		var evt model.RoomKeyEvent
		require.NoError(t, json.Unmarshal(payload, &evt))
		assert.Equal(t, "r1", evt.RoomID)
		assert.Equal(t, 3, evt.Version)
		assert.Equal(t, []byte("room-secret-key-bytes"), evt.PrivateKey)
	}
}

// TestHandleAdd_ExistingMembersNoKeyFanOut: a duplicate add (created=false)
// already has the key from its original add, so it must not trigger a
// keyStore.Get or a fresh key-event publish.
func TestHandleAdd_ExistingMembersNoKeyFanOut(t *testing.T) {
	store := &fakeStore{
		FindRoomFn: func(_ context.Context, _ string) (*Room, error) {
			return &Room{ID: "r1", Type: "c", CreatedByBot: "bot-1"}, nil
		},
		UpsertSubscriptionFn: func(_ context.Context, _ *Subscription) (bool, error) { return false, nil },
		FindUserFn: func(_ context.Context, id string) (*model.User, error) {
			return &model.User{ID: id, Account: "bob", SiteID: "site-a"}, nil
		},
	}
	getCalled := false
	keyStore := &fakeKeyStore{
		GetFn: func(_ context.Context, _ string) (*roomkeystore.VersionedKeyPair, error) {
			getCalled = true
			return &roomkeystore.VersionedKeyPair{}, nil
		},
	}
	pub := &fakePublisher{}
	h := newHandler(store, "site-a", nil, (&captureOutbox{}).publish, keyStore, roomkeysender.NewSender(pub))
	c := withIdentity(t, "r1", ident())

	resp, err := h.handleAdd(c, BotMembersBatchRequest{UserIDs: []string{"bob-id"}})
	require.NoError(t, err)
	assert.Empty(t, resp.Added.UserIDs)
	assert.False(t, getCalled, "keyStore.Get is skipped when nothing was newly added")
	assert.Empty(t, pub.subjects, "no key fan-out for a pre-existing member")
}

// TestHandleAdd_NoCurrentKeyDoesNotFailOp: a legacy/broken room with no
// stored key must not fail add-member — just skip the fan-out.
func TestHandleAdd_NoCurrentKeyDoesNotFailOp(t *testing.T) {
	store := &fakeStore{
		FindRoomFn: func(_ context.Context, _ string) (*Room, error) {
			return &Room{ID: "r1", Type: "c", CreatedByBot: "bot-1"}, nil
		},
		UpsertSubscriptionFn: func(_ context.Context, _ *Subscription) (bool, error) { return true, nil },
		FindUserFn: func(_ context.Context, id string) (*model.User, error) {
			return &model.User{ID: id, Account: "bob", SiteID: "site-a"}, nil
		},
	}
	keyStore := &fakeKeyStore{
		GetFn: func(_ context.Context, _ string) (*roomkeystore.VersionedKeyPair, error) {
			return nil, roomkeystore.ErrNoCurrentKey
		},
	}
	pub := &fakePublisher{}
	h := newHandler(store, "site-a", nil, (&captureOutbox{}).publish, keyStore, roomkeysender.NewSender(pub))
	c := withIdentity(t, "r1", ident())

	resp, err := h.handleAdd(c, BotMembersBatchRequest{UserIDs: []string{"bob-id"}})
	require.NoError(t, err, "missing key must not fail add-member")
	assert.Equal(t, []string{"bob-id"}, resp.Added.UserIDs)
	assert.Empty(t, pub.subjects, "nothing to fan out when there is no current key")
}

// TestHandleAdd_KeyStoreGetErrorFailsOp: unlike ErrNoCurrentKey, an infra
// error from keyStore.Get must fail the whole add-member op.
func TestHandleAdd_KeyStoreGetErrorFailsOp(t *testing.T) {
	store := &fakeStore{
		FindRoomFn: func(_ context.Context, _ string) (*Room, error) {
			return &Room{ID: "r1", Type: "c", CreatedByBot: "bot-1"}, nil
		},
		UpsertSubscriptionFn: func(_ context.Context, _ *Subscription) (bool, error) { return true, nil },
		FindUserFn: func(_ context.Context, id string) (*model.User, error) {
			return &model.User{ID: id, Account: "bob", SiteID: "site-a"}, nil
		},
	}
	keyStore := &fakeKeyStore{
		GetFn: func(_ context.Context, _ string) (*roomkeystore.VersionedKeyPair, error) {
			return nil, errors.New("mongo down")
		},
	}
	h := newHandler(store, "site-a", nil, (&captureOutbox{}).publish, keyStore, testKeySender)
	c := withIdentity(t, "r1", ident())

	_, err := h.handleAdd(c, BotMembersBatchRequest{UserIDs: []string{"bob-id"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "get room key")
}

// TestHandleAdd_KeyFanOutSendFailureDoesNotFailOp: a per-account Send
// failure is logged, not surfaced — best-effort fan-out.
func TestHandleAdd_KeyFanOutSendFailureDoesNotFailOp(t *testing.T) {
	store := &fakeStore{
		FindRoomFn: func(_ context.Context, _ string) (*Room, error) {
			return &Room{ID: "r1", Type: "c", CreatedByBot: "bot-1"}, nil
		},
		UpsertSubscriptionFn: func(_ context.Context, _ *Subscription) (bool, error) { return true, nil },
		FindUserFn: func(_ context.Context, id string) (*model.User, error) {
			return &model.User{ID: id, Account: "bob", SiteID: "site-a"}, nil
		},
	}
	failPub := &failingPublisher{}
	h := newHandler(store, "site-a", nil, (&captureOutbox{}).publish, &fakeKeyStore{}, roomkeysender.NewSender(failPub))
	c := withIdentity(t, "r1", ident())

	resp, err := h.handleAdd(c, BotMembersBatchRequest{UserIDs: []string{"bob-id"}})
	require.NoError(t, err, "fan-out failure must not fail add-member")
	assert.Equal(t, []string{"bob-id"}, resp.Added.UserIDs)
	assert.Equal(t, 1, failPub.calls, "fan-out was attempted")
}

func isErrcode(err error, out **errcode.Error) bool {
	if err == nil {
		return false
	}
	e, ok := err.(*errcode.Error) //nolint:errorlint // want the outer typed error, not the wrap chain
	if !ok {
		return false
	}
	*out = e
	return true
}
