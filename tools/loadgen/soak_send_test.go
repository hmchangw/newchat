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
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

const (
	soakTestMessageID = "0123456789ABCDEFGHIJ"
	soakTestRequestID = "01970a4f-8c2d-7c9a-abcd-e0123456789f"
)

type soakRecordingPublisher struct {
	mu        sync.Mutex
	subjects  []string
	payloads  [][]byte
	errors    []error
	onPublish func()
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
	if p.onPublish != nil {
		p.onPublish()
	}
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

func TestSoakSender_UsesGatekeeperCreatedAtForAcceptedCatalogEntry(t *testing.T) {
	publishedAt := time.Unix(100, 0).UTC()
	persistedAt := publishedAt.Add(250 * time.Millisecond)
	clock := newFakeSoakClock(publishedAt)
	catalog := newSoakCatalog(8, 100, 10*time.Second, clock)
	sender := newTestSoakSender(catalog, &soakRecordingPublisher{}, clock, 0)

	_, err := sender.Publish(context.Background(), soakSendTarget{
		UserID: "u-1", Account: "alice", RoomID: "room-1",
	}, "hello")
	require.NoError(t, err)
	reply, err := json.Marshal(model.Message{
		ID: soakTestMessageID, RoomID: "room-1", UserID: "u-1",
		UserAccount: "alice", Content: "hello", CreatedAt: persistedAt,
	})
	require.NoError(t, err)

	result := sender.HandleReply(
		subject.UserResponse("alice", soakTestRequestID),
		reply,
	)

	require.Equal(t, soakSendReplyAccepted, result.Status)
	got, ok := catalog.Get("room-1", soakTestMessageID)
	require.True(t, ok)
	assert.Equal(t, persistedAt, got.CreatedAt)
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
	_, readable := catalog.PickEligible("room-1", "bob", soakCatalogThreadRead)
	assert.False(t, readable, "a pre-publish reservation must not create a readable thread")
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
	_, readable = catalog.PickEligible("room-1", "bob", soakCatalogThreadRead)
	assert.False(t, readable, "the accepted reply still needs persistence grace")
	clock.Advance(10 * time.Second)
	_, readable = catalog.PickEligible("room-1", "bob", soakCatalogThreadRead)
	assert.True(t, readable)

	child, ok := catalog.Get("room-1", soakTestMessageID)
	require.True(t, ok)
	assert.Equal(t, parentID, child.ThreadParentID)
}

func TestSoakSender_ChannelThreadReplySnapshotsExactFollowerSet(t *testing.T) {
	clock := newFakeSoakClock(time.Unix(100, 0).UTC())
	catalog := newSoakCatalog(8, 100, 0, clock)
	parentID := "AAAAAAAAAAAAAAAAAAAA"
	require.NoError(t, catalog.TrackPublished(&soakCatalogCandidate{
		ID: parentID, RoomID: "room-1", Author: "alice", Content: "parent",
		CreatedAt: clock.Now(), ThreadReplyLimit: 4,
	}))
	require.True(t, catalog.Accept("room-1", parentID))
	require.NoError(t, catalog.TrackPublished(&soakCatalogCandidate{
		ID: "BBBBBBBBBBBBBBBBBBBB", RoomID: "room-1", Author: "bob", Content: "first",
		CreatedAt: clock.Now(), ThreadParentID: parentID,
	}))
	require.True(t, catalog.Accept("room-1", "BBBBBBBBBBBBBBBBBBBB"))

	lifecycle := NewMocksoakSendLifecycle(gomock.NewController(t))
	var started *soakPendingSend
	gomock.InOrder(
		lifecycle.EXPECT().Start(gomock.Any()).DoAndReturn(func(pending *soakPendingSend) error {
			started = cloneSoakPendingSend(pending)
			return nil
		}),
		lifecycle.EXPECT().Activate(gomock.Any()).Return(nil),
	)
	sender := newSoakSender(
		soakSendConfig{SiteID: "site-1", ThreadShare: 1, ReplyTimeout: time.Second},
		catalog, &soakRecordingPublisher{}, clock, rand.New(rand.NewSource(1)),
		&soakSendIDs{messageID: func() string { return soakTestMessageID }, requestID: func() string { return soakTestRequestID }},
		withSoakSendLifecycle(lifecycle, nil),
	)
	_, err := sender.Publish(context.Background(), soakSendTarget{
		UserID: "u-3", Account: "carol", RoomID: "room-1", RoomType: model.RoomTypeChannel,
		Recipients: []string{"alice", "bob", "carol", "not-a-thread-follower"},
	}, "next reply")
	require.NoError(t, err)
	require.NotNil(t, started)
	assert.Equal(t, []string{"alice", "bob", "carol"}, started.Target.Recipients)
}

func TestSoakSender_RejectedThreadReplyReleasesParentBudget(t *testing.T) {
	clock := newFakeSoakClock(time.Unix(100, 0).UTC())
	catalog := newSoakCatalog(8, 100, 0, clock)
	parentID := "AAAAAAAAAAAAAAAAAAAA"
	require.NoError(t, catalog.TrackPublished(&soakCatalogCandidate{
		ID: parentID, RoomID: "room-1", Author: "alice",
		Content: "parent", CreatedAt: clock.Now(), ThreadReplyLimit: 1,
	}))
	require.True(t, catalog.Accept("room-1", parentID))
	sender := newTestSoakSender(
		catalog,
		&soakRecordingPublisher{},
		clock,
		1,
	)

	first, err := sender.Publish(context.Background(), soakSendTarget{
		UserID: "u-2", Account: "bob", RoomID: "room-1",
	}, "reply")
	require.NoError(t, err)
	require.Equal(t, soakSendThreadReply, first.Kind)
	result := sender.HandleReply(
		subject.UserResponse("bob", first.RequestID),
		[]byte(`{"error":"rejected","code":"forbidden"}`),
	)
	require.Equal(t, soakSendReplyRejected, result.Status)

	parent, ok := catalog.Get("room-1", parentID)
	require.True(t, ok)
	assert.Zero(t, parent.ThreadReplies)
	second, err := sender.Publish(context.Background(), soakSendTarget{
		UserID: "u-2", Account: "bob", RoomID: "room-1",
	}, "retry reply")
	require.NoError(t, err)
	assert.Equal(t, soakSendThreadReply, second.Kind)
}

func TestSoakSender_TopLevelStoresConfiguredThreadReplyLimit(t *testing.T) {
	clock := newFakeSoakClock(time.Unix(100, 0).UTC())
	catalog := newSoakCatalog(8, 100, 0, clock)
	sender := newSoakSender(
		soakSendConfig{
			SiteID: "site-1",
			NextThreadReplyLimit: func() int {
				return 37
			},
		},
		catalog,
		&soakRecordingPublisher{},
		clock,
		rand.New(rand.NewSource(1)),
		&soakSendIDs{
			messageID: func() string { return soakTestMessageID },
			requestID: func() string { return soakTestRequestID },
		},
	)
	_, err := sender.Publish(context.Background(), soakSendTarget{
		UserID: "u-1", Account: "alice", RoomID: "room-1",
	}, "parent")
	require.NoError(t, err)
	reply, err := json.Marshal(model.Message{
		ID: soakTestMessageID, RoomID: "room-1", UserID: "u-1",
		UserAccount: "alice", Content: "parent", CreatedAt: clock.Now(),
	})
	require.NoError(t, err)
	assert.Equal(
		t,
		soakSendReplyAccepted,
		sender.HandleReply(
			subject.UserResponse("alice", soakTestRequestID),
			reply,
		).Status,
	)

	message, ok := catalog.Get("room-1", soakTestMessageID)
	require.True(t, ok)
	assert.Equal(t, 37, message.ThreadReplyLimit)
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
		wantClass  soakErrorClass
	}{
		{
			name:       "gatekeeper error",
			reply:      []byte(`{"error":"not subscribed","code":"forbidden"}`),
			wantStatus: soakSendReplyRejected,
			wantClass:  soakErrorForbidden,
		},
		{
			name:       "malformed success",
			reply:      []byte(`{"id":`),
			wantStatus: soakSendReplyMalformed,
			wantClass:  soakErrorDecode,
		},
		{
			name:       "wrong message",
			reply:      []byte(`{"id":"wrong","roomId":"room-1","userId":"u-1","userAccount":"alice","content":"hello","createdAt":"1970-01-01T00:01:40Z"}`),
			wantStatus: soakSendReplyRejected,
			wantClass:  soakErrorAssertion,
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
			assert.Equal(t, tt.wantClass, result.ErrorClass)
			assert.Zero(t, catalog.Size())
			assert.Equal(t, 0, sender.Pending())
			assert.False(t, catalog.Accept("room-1", soakTestMessageID))
		})
	}
}

func TestStartSoakSendResponses_ObserverIgnoresUnmatchedTraffic(t *testing.T) {
	source := &fakeSoakResponseSource{}
	sender := newTestSoakSender(
		newSoakCatalog(8, 100, 0, nil),
		&soakRecordingPublisher{},
		newFakeSoakClock(time.Unix(100, 0)),
		0,
	)
	observed := 0
	_, err := startSoakSendResponsesWithObserver(
		source,
		sender,
		func(soakSendReplyResult) { observed++ },
	)
	require.NoError(t, err)

	source.handler(&nats.Msg{
		Subject: subject.UserResponse("another-process", "request-id"),
		Data:    []byte(`{"id":"unrelated"}`),
	})

	assert.Zero(t, observed)
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
				require.True(t, catalog.ReserveThreadReply(tt.parentRoom, parentID))
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

func TestSoakSender_PersistsLifecycleBeforePublishing(t *testing.T) {
	clock := newFakeSoakClock(time.Unix(100, 0).UTC())
	var sequence []string
	publisher := &soakRecordingPublisher{onPublish: func() { sequence = append(sequence, "publish") }}
	lifecycle := NewMocksoakSendLifecycle(gomock.NewController(t))
	var started, activated *soakPendingSend
	gomock.InOrder(
		lifecycle.EXPECT().Start(gomock.Any()).DoAndReturn(func(pending *soakPendingSend) error {
			started = cloneSoakPendingSend(pending)
			sequence = append(sequence, "start")
			return nil
		}),
		lifecycle.EXPECT().Activate(gomock.Any()).DoAndReturn(func(pending *soakPendingSend) error {
			activated = cloneSoakPendingSend(pending)
			sequence = append(sequence, "activate")
			return nil
		}),
	)
	sender := newSoakSender(soakSendConfig{
		SiteID: "site-1", ReplyTimeout: 5 * time.Second,
	}, newSoakCatalog(8, 100, 0, clock), publisher, clock,
		rand.New(rand.NewSource(1)), &soakSendIDs{
			messageID: func() string { return soakTestMessageID },
			requestID: func() string { return soakTestRequestID },
		}, withSoakSendLifecycle(lifecycle, nil),
	)

	pending, err := sender.Publish(context.Background(), soakSendTarget{
		UserID: "u-1", Account: "alice", RoomID: "room-1",
	}, "hello")
	require.NoError(t, err)
	require.NotNil(t, started)
	require.NotNil(t, activated)
	assert.Equal(t, pending.MessageID, started.MessageID)
	assert.Equal(t, pending.MessageID, activated.MessageID)
	assert.Equal(t, []string{"start", "publish", "activate"}, sequence)
	assert.True(t, pending.Tracked)
	subjects, _ := publisher.snapshot()
	assert.Len(t, subjects, 1)
}

func TestSoakSender_LocalPublishFailureBecomesNotSent(t *testing.T) {
	now := time.Date(2026, 8, 15, 1, 2, 3, 0, time.UTC)
	ledger, err := newFailureLedger(failureLedgerConfig{Capacity: 1})
	require.NoError(t, err)
	tracker := newSoakFailureTracker(ledger, 0, time.Minute, func() time.Time { return now })
	publisher := &soakRecordingPublisher{errors: []error{nats.ErrConnectionClosed}}
	sender := newSoakSender(soakSendConfig{
		SiteID: "site-1", ReplyTimeout: 5 * time.Second,
	}, newSoakCatalog(8, 100, 0, nil), publisher, nil,
		rand.New(rand.NewSource(1)), &soakSendIDs{
			messageID: func() string { return soakTestMessageID },
			requestID: func() string { return soakTestRequestID },
		}, withSoakSendLifecycle(tracker, nil),
	)

	pending, err := sender.Publish(context.Background(), soakSendTarget{
		UserID: "u-1", Account: "alice", RoomID: "room-1",
	}, "hello")

	require.ErrorIs(t, err, nats.ErrConnectionClosed)
	require.NotNil(t, pending)
	snapshot := ledger.Snapshot()
	assert.Zero(t, snapshot.Active)
	assert.Equal(t, uint64(1), snapshot.Results[failureResultNotSent])
	assert.Zero(t, sender.Pending(), "a definite local rejection must not expire as a second admission result")
}

func TestSoakSender_AmbiguousPublishFailureRemainsActive(t *testing.T) {
	now := time.Date(2026, 8, 15, 1, 2, 3, 0, time.UTC)
	ledger, err := newFailureLedger(failureLedgerConfig{Capacity: 1})
	require.NoError(t, err)
	tracker := newSoakFailureTracker(ledger, 0, time.Minute, func() time.Time { return now })
	publisher := &soakRecordingPublisher{errors: []error{errors.New("ambiguous publish failure")}}
	sender := newSoakSender(soakSendConfig{
		SiteID: "site-1", ReplyTimeout: 5 * time.Second,
	}, newSoakCatalog(8, 100, 0, nil), publisher, nil,
		rand.New(rand.NewSource(1)), &soakSendIDs{
			messageID: func() string { return soakTestMessageID },
			requestID: func() string { return soakTestRequestID },
		}, withSoakSendLifecycle(tracker, nil),
	)

	_, err = sender.Publish(context.Background(), soakSendTarget{
		UserID: "u-1", Account: "alice", RoomID: "room-1",
	}, "hello")

	require.Error(t, err)
	operation, ok := ledger.Active(soakTestMessageID)
	require.True(t, ok)
	assert.Equal(t, failureOperationActive, operation.LifecycleState)
}

func TestSoakSender_LifecycleFailureKeepsTrafficFlowing(t *testing.T) {
	wantErr := errors.New("ledger disk full")
	publisher := &soakRecordingPublisher{}
	lifecycle := NewMocksoakSendLifecycle(gomock.NewController(t))
	lifecycle.EXPECT().Start(gomock.Any()).Return(wantErr)
	var observed []error
	sender := newSoakSender(soakSendConfig{
		SiteID: "site-1", ReplyTimeout: 5 * time.Second,
	}, newSoakCatalog(8, 100, 0, nil), publisher, nil,
		rand.New(rand.NewSource(1)), &soakSendIDs{
			messageID: func() string { return soakTestMessageID },
			requestID: func() string { return soakTestRequestID },
		}, withSoakSendLifecycle(lifecycle, func(err error) {
			observed = append(observed, err)
		}),
	)

	pending, err := sender.Publish(context.Background(), soakSendTarget{
		UserID: "u-1", Account: "alice", RoomID: "room-1",
	}, "hello")
	require.NoError(t, err, "observation is best-effort and must not stop the workload")
	require.NotNil(t, pending)
	assert.False(t, pending.Tracked)
	require.Len(t, observed, 1)
	assert.ErrorIs(t, observed[0], wantErr)
	subjects, _ := publisher.snapshot()
	assert.Len(t, subjects, 1)
	assert.Equal(t, 1, sender.Pending())
}

func TestSoakSender_ActivationFailureKeepsTrafficFlowing(t *testing.T) {
	wantErr := errors.New("ledger activation failed")
	publisher := &soakRecordingPublisher{}
	lifecycle := NewMocksoakSendLifecycle(gomock.NewController(t))
	var started, activated *soakPendingSend
	gomock.InOrder(
		lifecycle.EXPECT().Start(gomock.Any()).DoAndReturn(func(pending *soakPendingSend) error {
			started = cloneSoakPendingSend(pending)
			return nil
		}),
		lifecycle.EXPECT().Activate(gomock.Any()).DoAndReturn(func(pending *soakPendingSend) error {
			activated = cloneSoakPendingSend(pending)
			return wantErr
		}),
	)
	var observed []error
	sender := newSoakSender(soakSendConfig{
		SiteID: "site-1", ReplyTimeout: 5 * time.Second,
	}, newSoakCatalog(8, 100, 0, nil), publisher, nil,
		rand.New(rand.NewSource(1)), &soakSendIDs{
			messageID: func() string { return soakTestMessageID },
			requestID: func() string { return soakTestRequestID },
		}, withSoakSendLifecycle(lifecycle, func(err error) {
			observed = append(observed, err)
		}),
	)

	pending, err := sender.Publish(context.Background(), soakSendTarget{
		UserID: "u-1", Account: "alice", RoomID: "room-1",
	}, "hello")

	require.NoError(t, err)
	require.NotNil(t, pending)
	assert.True(t, pending.Tracked)
	require.NotNil(t, started)
	require.NotNil(t, activated)
	require.Len(t, observed, 1)
	assert.ErrorIs(t, observed[0], wantErr)
	subjects, _ := publisher.snapshot()
	assert.Len(t, subjects, 1)
}

func TestSoakSender_ExpireResultsRetainsCorrelation(t *testing.T) {
	clock := newFakeSoakClock(time.Unix(100, 0).UTC())
	sender := newTestSoakSender(
		newSoakCatalog(8, 100, 0, clock),
		&soakRecordingPublisher{},
		clock,
		0,
	)
	_, err := sender.Publish(context.Background(), soakSendTarget{
		UserID: "u-1", Account: "alice", RoomID: "room-1",
	}, "hello")
	require.NoError(t, err)
	clock.Advance(5 * time.Second)

	results := sender.ExpireResults()
	require.Len(t, results, 1)
	assert.Equal(t, soakTestMessageID, results[0].MessageID)
	assert.Equal(t, soakTestRequestID, results[0].RequestID)
	assert.Equal(t, soakErrorTimeout, results[0].ErrorClass)
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

func TestSoakSender_DiscardDropsNeverPublishedSend(t *testing.T) {
	clock := newFakeSoakClock(time.Unix(100, 0).UTC())
	publisher := &soakRecordingPublisher{}
	sender := newSoakSender(soakSendConfig{
		SiteID: "site-1", ReplyTimeout: 5 * time.Second,
	}, newSoakCatalog(8, 100, 0, clock), publisher, clock,
		rand.New(rand.NewSource(1)), &soakSendIDs{
			messageID: func() string { return soakTestMessageID },
			requestID: func() string { return soakTestRequestID },
		},
	)
	pending, err := sender.Publish(context.Background(), soakSendTarget{
		UserID: "u-1", Account: "alice", RoomID: "room-1",
	}, "hello")
	require.NoError(t, err)
	require.Equal(t, 1, sender.Pending())

	sender.Discard(pending.RequestID)

	assert.Zero(t, sender.Pending())
	clock.Advance(10 * time.Second)
	assert.Empty(
		t, sender.ExpireResults(),
		"a discarded send must not be counted again at its reply deadline",
	)

	sender.Discard("unknown-request")
	assert.Zero(t, sender.Pending())
}
