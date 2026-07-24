package main

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

const (
	soakTestMessageID = "0123456789ABCDEFGHIJ"
	soakTestRequestID = "01970a4f-8c2d-7c9a-abcd-e0123456789f"
)

type soakRecordingPublisher struct {
	mu       sync.Mutex
	subjects []string
	payloads [][]byte
	errors   []error
}

func (p *soakRecordingPublisher) Publish(
	_ context.Context,
	subject string,
	data []byte,
) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.subjects = append(p.subjects, subject)
	p.payloads = append(p.payloads, append([]byte(nil), data...))
	if len(p.errors) == 0 {
		return nil
	}
	err := p.errors[0]
	p.errors = p.errors[1:]
	return err
}

func (p *soakRecordingPublisher) snapshot() ([]string, [][]byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.subjects...), append([][]byte(nil), p.payloads...)
}

func TestSoakSender_TopLevelUsesFrontdoorAndAdmitsOnlyAfterSuccessReply(t *testing.T) {
	clock := newFakeSoakClock(time.Unix(100, 0).UTC())
	catalog := newSoakCatalog(8, 100, 10*time.Second, clock)
	publisher := &soakRecordingPublisher{}
	sender := newTestSoakSender(catalog, publisher, clock, 0)

	pending, err := sender.Publish(context.Background(), soakSendTarget{
		UserID: "u-1", Account: "alice", RoomID: "room-1",
	}, "hello")
	require.NoError(t, err)

	assert.Equal(t, soakSendTopLevel, pending.Kind)
	assert.Equal(t, soakTestMessageID, pending.MessageID)
	assert.Equal(t, soakTestRequestID, pending.RequestID)
	assert.Zero(t, catalog.Size(), "publish acknowledgement is not persistence admission")

	subjects, payloads := publisher.snapshot()
	require.Len(t, subjects, 1)
	assert.Equal(t, subject.MsgSend("alice", "room-1", "site-1"), subjects[0])
	var request model.SendMessageRequest
	require.NoError(t, json.Unmarshal(payloads[0], &request))
	assert.Equal(t, soakTestMessageID, request.ID)
	assert.Equal(t, soakTestRequestID, request.RequestID)
	assert.Equal(t, "hello", request.Content)
	assert.Empty(t, request.ThreadParentMessageID)

	reply, err := json.Marshal(model.Message{
		ID: soakTestMessageID, RoomID: "room-1", UserID: "u-1",
		UserAccount: "alice", Content: "hello", CreatedAt: clock.Now(),
	})
	require.NoError(t, err)
	result := sender.HandleReply(
		subject.UserResponse("alice", soakTestRequestID),
		reply,
	)

	assert.Equal(t, soakSendReplyAccepted, result.Status)
	assert.Equal(t, 1, catalog.Size())
	got, ok := catalog.Get("room-1", soakTestMessageID)
	require.True(t, ok)
	assert.Equal(t, "hello", got.Content)
}

func TestSoakSender_ThreadReplyUsesEligibleParentFromSameRoom(t *testing.T) {
	clock := newFakeSoakClock(time.Unix(100, 0).UTC())
	catalog := newSoakCatalog(8, 100, 10*time.Second, clock)
	parentID := "AAAAAAAAAAAAAAAAAAAA"
	require.NoError(t, catalog.TrackPublished(&soakCatalogCandidate{
		ID: parentID, RoomID: "room-1", Author: "alice",
		Content: "parent", CreatedAt: clock.Now(), ThreadReplyLimit: 2,
	}))
	require.True(t, catalog.Accept("room-1", parentID))
	clock.Advance(10 * time.Second)

	publisher := &soakRecordingPublisher{}
	sender := newTestSoakSender(catalog, publisher, clock, 1)
	pending, err := sender.Publish(context.Background(), soakSendTarget{
		UserID: "u-2", Account: "bob", RoomID: "room-1",
	}, "reply")
	require.NoError(t, err)

	assert.Equal(t, soakSendThreadReply, pending.Kind)
	assert.Equal(t, parentID, pending.ThreadParentID)
	_, payloads := publisher.snapshot()
	var request model.SendMessageRequest
	require.NoError(t, json.Unmarshal(payloads[0], &request))
	assert.Equal(t, parentID, request.ThreadParentMessageID)

	reply, err := json.Marshal(model.Message{
		ID: soakTestMessageID, RoomID: "room-1", UserID: "u-2",
		UserAccount: "bob", Content: "reply", CreatedAt: clock.Now(),
		ThreadParentMessageID: parentID,
	})
	require.NoError(t, err)
	result := sender.HandleReply(subject.UserResponse("bob", soakTestRequestID), reply)
	assert.Equal(t, soakSendReplyAccepted, result.Status)

	child, ok := catalog.Get("room-1", soakTestMessageID)
	require.True(t, ok)
	assert.Equal(t, parentID, child.ThreadParentID)
}

func TestSoakSendPicker_ConfiguredNinetyTenMix(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	thread := 0
	const samples = 100000
	for range samples {
		if pickSoakSendKind(rng, 0.10) == soakSendThreadReply {
			thread++
		}
	}
	assert.InDelta(t, 0.10, float64(thread)/samples, 0.005)
}

func TestStartSoakSendResponses_SubscribesWildcardAndFlushes(t *testing.T) {
	source := &fakeSoakResponseSource{}
	sender := newTestSoakSender(
		newSoakCatalog(8, 100, 0, nil),
		&soakRecordingPublisher{},
		newFakeSoakClock(time.Unix(100, 0)),
		0,
	)

	subscription, err := startSoakSendResponses(source, sender)
	require.NoError(t, err)
	assert.Nil(t, subscription)
	assert.Equal(t, subject.UserResponseWildcard(), source.subject)
	assert.True(t, source.flushed)

	source.handler(&nats.Msg{
		Subject: subject.UserResponse("nobody", "unmatched"),
		Data:    []byte(`{"error":"missing","code":"not_found"}`),
	})
	assert.Equal(t, 1, source.handled)
}

func TestStartSoakSendResponses_FlushFailureUnsubscribes(t *testing.T) {
	subscription := &fakeSoakSubscription{}
	source := &fakeSoakResponseSource{
		subscription: subscription,
		flushErr:     errors.New("flush failed"),
	}

	_, err := startSoakSendResponses(source, newTestSoakSender(
		newSoakCatalog(8, 100, 0, nil),
		&soakRecordingPublisher{},
		newFakeSoakClock(time.Unix(100, 0)),
		0,
	))

	require.ErrorContains(t, err, "flush")
	assert.True(t, subscription.unsubscribed)
}

func TestNewNATSSoakResponseSource_WrapsConnection(t *testing.T) {
	source := newNATSSoakResponseSource(nil)
	assert.Nil(t, source.nc)
}

func TestSoakSender_ErrorAndMalformedRepliesRejectPendingCandidate(t *testing.T) {
	tests := []struct {
		name       string
		reply      []byte
		wantStatus soakSendReplyStatus
	}{
		{
			name:       "gatekeeper error",
			reply:      []byte(`{"error":"not subscribed","code":"forbidden"}`),
			wantStatus: soakSendReplyRejected,
		},
		{
			name:       "malformed success",
			reply:      []byte(`{"id":`),
			wantStatus: soakSendReplyMalformed,
		},
		{
			name:       "wrong message",
			reply:      []byte(`{"id":"wrong","roomId":"room-1","userId":"u-1","userAccount":"alice","content":"hello","createdAt":"1970-01-01T00:01:40Z"}`),
			wantStatus: soakSendReplyRejected,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clock := newFakeSoakClock(time.Unix(100, 0).UTC())
			catalog := newSoakCatalog(8, 100, 0, clock)
			sender := newTestSoakSender(catalog, &soakRecordingPublisher{}, clock, 0)
			_, err := sender.Publish(context.Background(), soakSendTarget{
				UserID: "u-1", Account: "alice", RoomID: "room-1",
			}, "hello")
			require.NoError(t, err)

			result := sender.HandleReply(
				subject.UserResponse("alice", soakTestRequestID),
				tt.reply,
			)
			assert.Equal(t, tt.wantStatus, result.Status)
			assert.Zero(t, catalog.Size())
			assert.Equal(t, 0, sender.Pending())
			assert.False(t, catalog.Accept("room-1", soakTestMessageID))
		})
	}
}

func TestSoakSender_ThreadParentRequiresRoomGraceAndAvailableCap(t *testing.T) {
	tests := []struct {
		name         string
		parentRoom   string
		graceAdvance time.Duration
		parentLimit  int
		preconsume   bool
	}{
		{"different room", "room-2", 10 * time.Second, 2, false},
		{"inside persistence grace", "room-1", 9 * time.Second, 2, false},
		{"reply cap exhausted", "room-1", 10 * time.Second, 1, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clock := newFakeSoakClock(time.Unix(100, 0).UTC())
			catalog := newSoakCatalog(8, 100, 10*time.Second, clock)
			parentID := "AAAAAAAAAAAAAAAAAAAA"
			require.NoError(t, catalog.TrackPublished(&soakCatalogCandidate{
				ID: parentID, RoomID: tt.parentRoom, Author: "alice",
				CreatedAt: clock.Now(), ThreadReplyLimit: tt.parentLimit,
			}))
			require.True(t, catalog.Accept(tt.parentRoom, parentID))
			clock.Advance(tt.graceAdvance)
			if tt.preconsume {
				require.True(t, catalog.IncrementThreadReplies(tt.parentRoom, parentID))
			}

			sender := newTestSoakSender(
				catalog,
				&soakRecordingPublisher{},
				clock,
				1,
			)
			pending, err := sender.Publish(context.Background(), soakSendTarget{
				UserID: "u-2", Account: "bob", RoomID: "room-1",
			}, "message")
			require.NoError(t, err)
			assert.Equal(t, soakSendTopLevel, pending.Kind)
			assert.Empty(t, pending.ThreadParentID)
		})
	}
}

func TestSoakSender_PublishFailureRetriesStableLogicalMessage(t *testing.T) {
	clock := newFakeSoakClock(time.Unix(100, 0).UTC())
	catalog := newSoakCatalog(8, 100, 0, clock)
	publisher := &soakRecordingPublisher{errors: []error{
		errors.New("NATS disconnected"),
		nil,
	}}
	messageCalls, requestCalls := 0, 0
	sender := newSoakSender(soakSendConfig{
		SiteID: "site-1", ReplyTimeout: 5 * time.Second,
	}, catalog, publisher, clock, rand.New(rand.NewSource(1)), &soakSendIDs{
		messageID: func() string {
			messageCalls++
			return soakTestMessageID
		},
		requestID: func() string {
			requestCalls++
			return soakTestRequestID
		},
	})

	pending, err := sender.Publish(context.Background(), soakSendTarget{
		UserID: "u-1", Account: "alice", RoomID: "room-1",
	}, "hello")
	require.ErrorContains(t, err, "publish")
	require.NotNil(t, pending)
	assert.Equal(t, 1, sender.Pending())

	require.NoError(t, sender.Retry(context.Background(), pending.RequestID))
	subjects, payloads := publisher.snapshot()
	require.Len(t, subjects, 2)
	assert.Equal(t, subjects[0], subjects[1])
	assert.Equal(t, payloads[0], payloads[1])
	assert.Equal(t, 1, messageCalls)
	assert.Equal(t, 1, requestCalls)

	clock.Advance(5 * time.Second)
	assert.Equal(t, 1, sender.Expire())
	assert.Zero(t, sender.Pending())
	assert.Zero(t, catalog.Size())
}

func TestSoakSender_UnmatchedReplyDoesNotTouchCatalog(t *testing.T) {
	sender := newTestSoakSender(
		newSoakCatalog(8, 100, 0, nil),
		&soakRecordingPublisher{},
		newFakeSoakClock(time.Unix(100, 0)),
		0,
	)
	result := sender.HandleReply(
		subject.UserResponse("alice", "unknown"),
		[]byte(`{"error":"missing","code":"not_found"}`),
	)
	assert.Equal(t, soakSendReplyUnmatched, result.Status)
}

func newTestSoakSender(
	catalog *soakCatalog,
	publisher Publisher,
	clock soakClock,
	threadShare float64,
) *soakSender {
	return newSoakSender(soakSendConfig{
		SiteID: "site-1", ThreadShare: threadShare, ReplyTimeout: 5 * time.Second,
	}, catalog, publisher, clock, rand.New(rand.NewSource(1)), &soakSendIDs{
		messageID: func() string { return soakTestMessageID },
		requestID: func() string { return soakTestRequestID },
	})
}

type fakeSoakResponseSource struct {
	subject      string
	handler      nats.MsgHandler
	subscription soakResponseSubscription
	flushErr     error
	flushed      bool
	handled      int
}

func (f *fakeSoakResponseSource) Subscribe(
	subject string,
	handler nats.MsgHandler,
) (soakResponseSubscription, error) {
	f.subject = subject
	f.handler = func(msg *nats.Msg) {
		f.handled++
		handler(msg)
	}
	return f.subscription, nil
}

func (f *fakeSoakResponseSource) Flush() error {
	f.flushed = true
	return f.flushErr
}

type fakeSoakSubscription struct {
	unsubscribed bool
}

func (s *fakeSoakSubscription) Unsubscribe() error {
	s.unsubscribed = true
	return nil
}
