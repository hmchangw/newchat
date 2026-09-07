package send

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand" // #nosec G404 -- load generator randomness, never used for secrets // nosemgrep: math-random-used
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
	soakcatalog "github.com/hmchangw/chat/tools/loadgen/internal/soak/catalog"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/rpc"
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
	clock := newFakeClock(time.Unix(100, 0).UTC())
	catalog := soakcatalog.New(8, 100, 10*time.Second, clock)
	publisher := &soakRecordingPublisher{}
	sender := newTestSender(catalog, publisher, clock, 0)

	pending, err := sender.Publish(context.Background(), Target{
		UserID: "u-1", Account: "alice", RoomID: "room-1",
	}, "hello")
	require.NoError(t, err)

	assert.Equal(t, KindTopLevel, pending.Kind)
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

	assert.Equal(t, ReplyAccepted, result.Status)
	assert.Equal(t, 1, catalog.Size())
	got, ok := catalog.Get("room-1", soakTestMessageID)
	require.True(t, ok)
	assert.Equal(t, soakcatalog.ContentDigest("hello"), got.ContentSHA256)
}

func TestSoakSender_UsesGatekeeperCreatedAtForAcceptedCatalogEntry(t *testing.T) {
	publishedAt := time.Unix(100, 0).UTC()
	persistedAt := publishedAt.Add(250 * time.Millisecond)
	clock := newFakeClock(publishedAt)
	catalog := soakcatalog.New(8, 100, 10*time.Second, clock)
	sender := newTestSender(catalog, &soakRecordingPublisher{}, clock, 0)

	_, err := sender.Publish(context.Background(), Target{
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

	require.Equal(t, ReplyAccepted, result.Status)
	got, ok := catalog.Get("room-1", soakTestMessageID)
	require.True(t, ok)
	assert.Equal(t, persistedAt, got.CreatedAt)
}

func TestSoakSender_ThreadReplyUsesEligibleParentFromSameRoom(t *testing.T) {
	clock := newFakeClock(time.Unix(100, 0).UTC())
	catalog := soakcatalog.New(8, 100, 10*time.Second, clock)
	parentID := "AAAAAAAAAAAAAAAAAAAA"
	require.NoError(t, catalog.TrackPublished(&soakcatalog.Candidate{
		ID: parentID, RoomID: "room-1", Author: "alice",
		Content: "parent", CreatedAt: clock.Now(), ThreadReplyLimit: 2,
	}))
	require.True(t, catalog.Accept("room-1", parentID))
	clock.Advance(10 * time.Second)

	publisher := &soakRecordingPublisher{}
	sender := newTestSender(catalog, publisher, clock, 1)
	pending, err := sender.Publish(context.Background(), Target{
		UserID: "u-2", Account: "bob", RoomID: "room-1",
	}, "reply")
	require.NoError(t, err)

	assert.Equal(t, KindThreadReply, pending.Kind)
	assert.Equal(t, parentID, pending.ThreadParentID)
	_, readable := catalog.PickEligible("room-1", "bob", soakcatalog.ActionThreadRead)
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
	assert.Equal(t, ReplyAccepted, result.Status)
	_, readable = catalog.PickEligible("room-1", "bob", soakcatalog.ActionThreadRead)
	assert.False(t, readable, "the accepted reply still needs persistence grace")
	clock.Advance(10 * time.Second)
	_, readable = catalog.PickEligible("room-1", "bob", soakcatalog.ActionThreadRead)
	assert.True(t, readable)

	child, ok := catalog.Get("room-1", soakTestMessageID)
	require.True(t, ok)
	assert.Equal(t, parentID, child.ThreadParentID)
}

func TestSoakSender_ChannelThreadReplySnapshotsExactFollowerSet(t *testing.T) {
	clock := newFakeClock(time.Unix(100, 0).UTC())
	catalog := soakcatalog.New(8, 100, 0, clock)
	parentID := "AAAAAAAAAAAAAAAAAAAA"
	require.NoError(t, catalog.TrackPublished(&soakcatalog.Candidate{
		ID: parentID, RoomID: "room-1", Author: "alice", Content: "parent",
		CreatedAt: clock.Now(), ThreadReplyLimit: 4,
	}))
	require.True(t, catalog.Accept("room-1", parentID))
	require.NoError(t, catalog.TrackPublished(&soakcatalog.Candidate{
		ID: "BBBBBBBBBBBBBBBBBBBB", RoomID: "room-1", Author: "bob", Content: "first",
		CreatedAt: clock.Now(), ThreadParentID: parentID,
	}))
	require.True(t, catalog.Accept("room-1", "BBBBBBBBBBBBBBBBBBBB"))

	lifecycle := NewMockLifecycle(gomock.NewController(t))
	var started *Pending
	gomock.InOrder(
		lifecycle.EXPECT().Start(gomock.Any()).DoAndReturn(func(pending *Pending) error {
			started = ClonePending(pending)
			return nil
		}),
		lifecycle.EXPECT().Activate(gomock.Any()).Return(nil),
	)
	sender := New(
		Config{SiteID: "site-1", ThreadShare: 1, ReplyTimeout: time.Second},
		catalog, &soakRecordingPublisher{}, clock, rand.New(rand.NewSource(1)),
		&IDs{MessageID: func() string { return soakTestMessageID }, RequestID: func() string { return soakTestRequestID }},
		WithLifecycle(lifecycle, nil),
	)
	_, err := sender.Publish(context.Background(), Target{
		UserID: "u-3", Account: "carol", RoomID: "room-1", RoomType: model.RoomTypeChannel,
		Recipients: []string{"alice", "bob", "carol", "not-a-thread-follower"},
	}, "next reply")
	require.NoError(t, err)
	require.NotNil(t, started)
	assert.Equal(t, []string{"alice", "bob", "carol"}, started.Target.Recipients)
	assert.Equal(t, RecipientSourceThreadFollowers, started.Target.RecipientSetSource)
	assert.True(t, started.Target.RecipientSetComplete)
	assert.Equal(t, RecipientRouteUser, started.Target.RecipientRoute)
}

func TestSoakSender_RejectedThreadReplyReleasesParentBudget(t *testing.T) {
	clock := newFakeClock(time.Unix(100, 0).UTC())
	catalog := soakcatalog.New(8, 100, 0, clock)
	parentID := "AAAAAAAAAAAAAAAAAAAA"
	require.NoError(t, catalog.TrackPublished(&soakcatalog.Candidate{
		ID: parentID, RoomID: "room-1", Author: "alice",
		Content: "parent", CreatedAt: clock.Now(), ThreadReplyLimit: 1,
	}))
	require.True(t, catalog.Accept("room-1", parentID))
	sender := newTestSender(
		catalog,
		&soakRecordingPublisher{},
		clock,
		1,
	)

	first, err := sender.Publish(context.Background(), Target{
		UserID: "u-2", Account: "bob", RoomID: "room-1",
	}, "reply")
	require.NoError(t, err)
	require.Equal(t, KindThreadReply, first.Kind)
	result := sender.HandleReply(
		subject.UserResponse("bob", first.RequestID),
		[]byte(`{"error":"rejected","code":"forbidden"}`),
	)
	require.Equal(t, ReplyRejected, result.Status)

	parent, ok := catalog.Get("room-1", parentID)
	require.True(t, ok)
	assert.Zero(t, parent.ThreadReplies)
	second, err := sender.Publish(context.Background(), Target{
		UserID: "u-2", Account: "bob", RoomID: "room-1",
	}, "retry reply")
	require.NoError(t, err)
	assert.Equal(t, KindThreadReply, second.Kind)
}

func TestSoakSender_TopLevelStoresConfiguredThreadReplyLimit(t *testing.T) {
	clock := newFakeClock(time.Unix(100, 0).UTC())
	catalog := soakcatalog.New(8, 100, 0, clock)
	sender := New(
		Config{
			SiteID: "site-1",
			NextThreadReplyLimit: func() int {
				return 37
			},
		},
		catalog,
		&soakRecordingPublisher{},
		clock,
		rand.New(rand.NewSource(1)),
		&IDs{
			MessageID: func() string { return soakTestMessageID },
			RequestID: func() string { return soakTestRequestID },
		},
	)
	_, err := sender.Publish(context.Background(), Target{
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
		ReplyAccepted,
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
		if PickKind(rng, 0.10) == KindThreadReply {
			thread++
		}
	}
	assert.InDelta(t, 0.10, float64(thread)/samples, 0.005)
}

func TestStartSoakSendResponses_SubscribesWildcardAndFlushes(t *testing.T) {
	source := &fakeSoakResponseSource{}
	sender := newTestSender(
		soakcatalog.New(8, 100, 0, nil),
		&soakRecordingPublisher{},
		newFakeClock(time.Unix(100, 0)),
		0,
	)

	subscription, err := StartResponses(source, sender)
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

	_, err := StartResponses(source, newTestSender(
		soakcatalog.New(8, 100, 0, nil),
		&soakRecordingPublisher{},
		newFakeClock(time.Unix(100, 0)),
		0,
	))

	require.ErrorContains(t, err, "flush")
	assert.True(t, subscription.unsubscribed)
}

func TestNewNATSSoakResponseSource_WrapsConnection(t *testing.T) {
	source := NewNATSResponseSource(nil)
	assert.NotNil(t, source)
}

func TestSoakSender_ErrorAndMalformedRepliesRejectPendingCandidate(t *testing.T) {
	tests := []struct {
		name       string
		reply      []byte
		wantStatus ReplyStatus
		wantClass  rpc.ErrorClass
	}{
		{
			name:       "gatekeeper error",
			reply:      []byte(`{"error":"not subscribed","code":"forbidden"}`),
			wantStatus: ReplyRejected,
			wantClass:  rpc.ErrorForbidden,
		},
		{
			name:       "malformed success",
			reply:      []byte(`{"id":`),
			wantStatus: ReplyMalformed,
			wantClass:  rpc.ErrorResponseDecode,
		},
		{
			name:       "wrong message",
			reply:      []byte(`{"id":"wrong","roomId":"room-1","userId":"u-1","userAccount":"alice","content":"hello","createdAt":"1970-01-01T00:01:40Z"}`),
			wantStatus: ReplyRejected,
			wantClass:  rpc.ErrorAssertion,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clock := newFakeClock(time.Unix(100, 0).UTC())
			catalog := soakcatalog.New(8, 100, 0, clock)
			sender := newTestSender(catalog, &soakRecordingPublisher{}, clock, 0)
			_, err := sender.Publish(context.Background(), Target{
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
	sender := newTestSender(
		soakcatalog.New(8, 100, 0, nil),
		&soakRecordingPublisher{},
		newFakeClock(time.Unix(100, 0)),
		0,
	)
	observed := 0
	_, err := StartResponsesWithObserver(
		source,
		sender,
		func(ReplyResult) { observed++ },
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
			clock := newFakeClock(time.Unix(100, 0).UTC())
			catalog := soakcatalog.New(8, 100, 10*time.Second, clock)
			parentID := "AAAAAAAAAAAAAAAAAAAA"
			require.NoError(t, catalog.TrackPublished(&soakcatalog.Candidate{
				ID: parentID, RoomID: tt.parentRoom, Author: "alice",
				CreatedAt: clock.Now(), ThreadReplyLimit: tt.parentLimit,
			}))
			require.True(t, catalog.Accept(tt.parentRoom, parentID))
			clock.Advance(tt.graceAdvance)
			if tt.preconsume {
				require.True(t, catalog.ReserveThreadReply(tt.parentRoom, parentID))
			}

			sender := newTestSender(
				catalog,
				&soakRecordingPublisher{},
				clock,
				1,
			)
			pending, err := sender.Publish(context.Background(), Target{
				UserID: "u-2", Account: "bob", RoomID: "room-1",
			}, "message")
			require.NoError(t, err)
			assert.Equal(t, KindTopLevel, pending.Kind)
			assert.Empty(t, pending.ThreadParentID)
		})
	}
}

func TestSoakSender_PublishFailureRetriesStableLogicalMessage(t *testing.T) {
	clock := newFakeClock(time.Unix(100, 0).UTC())
	catalog := soakcatalog.New(8, 100, 0, clock)
	publisher := &soakRecordingPublisher{errors: []error{
		errors.New("NATS disconnected"),
		nil,
	}}
	messageCalls, requestCalls := 0, 0
	sender := New(Config{
		SiteID: "site-1", ReplyTimeout: 5 * time.Second,
	}, catalog, publisher, clock, rand.New(rand.NewSource(1)), &IDs{
		MessageID: func() string {
			messageCalls++
			return soakTestMessageID
		},
		RequestID: func() string {
			requestCalls++
			return soakTestRequestID
		},
	})

	pending, err := sender.Publish(context.Background(), Target{
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
	sender := newTestSender(
		soakcatalog.New(8, 100, 0, nil),
		&soakRecordingPublisher{},
		newFakeClock(time.Unix(100, 0)),
		0,
	)
	result := sender.HandleReply(
		subject.UserResponse("alice", "unknown"),
		[]byte(`{"error":"missing","code":"not_found"}`),
	)
	assert.Equal(t, ReplyUnmatched, result.Status)
}

func TestSoakSender_PersistsLifecycleBeforePublishing(t *testing.T) {
	clock := newFakeClock(time.Unix(100, 0).UTC())
	var sequence []string
	publisher := &soakRecordingPublisher{onPublish: func() { sequence = append(sequence, "publish") }}
	lifecycle := NewMockLifecycle(gomock.NewController(t))
	var started, activated *Pending
	gomock.InOrder(
		lifecycle.EXPECT().Start(gomock.Any()).DoAndReturn(func(pending *Pending) error {
			started = ClonePending(pending)
			sequence = append(sequence, "start")
			return nil
		}),
		lifecycle.EXPECT().Activate(gomock.Any()).DoAndReturn(func(pending *Pending) error {
			activated = ClonePending(pending)
			sequence = append(sequence, "activate")
			return nil
		}),
	)
	sender := New(Config{
		SiteID: "site-1", ReplyTimeout: 5 * time.Second,
	}, soakcatalog.New(8, 100, 0, clock), publisher, clock,
		rand.New(rand.NewSource(1)), &IDs{
			MessageID: func() string { return soakTestMessageID },
			RequestID: func() string { return soakTestRequestID },
		}, WithLifecycle(lifecycle, nil),
	)

	pending, err := sender.Publish(context.Background(), Target{
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

func TestSoakSender_LifecycleFailureKeepsTrafficFlowing(t *testing.T) {
	wantErr := errors.New("ledger disk full")
	publisher := &soakRecordingPublisher{}
	lifecycle := NewMockLifecycle(gomock.NewController(t))
	lifecycle.EXPECT().Start(gomock.Any()).Return(wantErr)
	var observed []error
	sender := New(Config{
		SiteID: "site-1", ReplyTimeout: 5 * time.Second,
	}, soakcatalog.New(8, 100, 0, nil), publisher, nil,
		rand.New(rand.NewSource(1)), &IDs{
			MessageID: func() string { return soakTestMessageID },
			RequestID: func() string { return soakTestRequestID },
		}, WithLifecycle(lifecycle, func(err error) {
			observed = append(observed, err)
		}),
	)

	pending, err := sender.Publish(context.Background(), Target{
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
	lifecycle := NewMockLifecycle(gomock.NewController(t))
	var started, activated *Pending
	gomock.InOrder(
		lifecycle.EXPECT().Start(gomock.Any()).DoAndReturn(func(pending *Pending) error {
			started = ClonePending(pending)
			return nil
		}),
		lifecycle.EXPECT().Activate(gomock.Any()).DoAndReturn(func(pending *Pending) error {
			activated = ClonePending(pending)
			return wantErr
		}),
	)
	var observed []error
	sender := New(Config{
		SiteID: "site-1", ReplyTimeout: 5 * time.Second,
	}, soakcatalog.New(8, 100, 0, nil), publisher, nil,
		rand.New(rand.NewSource(1)), &IDs{
			MessageID: func() string { return soakTestMessageID },
			RequestID: func() string { return soakTestRequestID },
		}, WithLifecycle(lifecycle, func(err error) {
			observed = append(observed, err)
		}),
	)

	pending, err := sender.Publish(context.Background(), Target{
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
	clock := newFakeClock(time.Unix(100, 0).UTC())
	sender := newTestSender(
		soakcatalog.New(8, 100, 0, clock),
		&soakRecordingPublisher{},
		clock,
		0,
	)
	_, err := sender.Publish(context.Background(), Target{
		UserID: "u-1", Account: "alice", RoomID: "room-1",
	}, "hello")
	require.NoError(t, err)
	clock.Advance(5 * time.Second)

	results := sender.ExpireResults()
	require.Len(t, results, 1)
	assert.Equal(t, soakTestMessageID, results[0].MessageID)
	assert.Equal(t, soakTestRequestID, results[0].RequestID)
	assert.Equal(t, rpc.ErrorTimeout, results[0].ErrorClass)
}

func newTestSender(
	catalog *soakcatalog.Catalog,
	publisher Publisher,
	clock soakcatalog.TimeProvider,
	threadShare float64,
) *Sender {
	return New(Config{
		SiteID: "site-1", ThreadShare: threadShare, ReplyTimeout: 5 * time.Second,
	}, catalog, publisher, clock, rand.New(rand.NewSource(1)), &IDs{
		MessageID: func() string { return soakTestMessageID },
		RequestID: func() string { return soakTestRequestID },
	})
}

type fakeSoakResponseSource struct {
	subject      string
	handler      nats.MsgHandler
	subscription ResponseSubscription
	subscribeErr error
	flushErr     error
	flushed      bool
	handled      int
}

func (f *fakeSoakResponseSource) Subscribe(
	subject string,
	handler nats.MsgHandler,
) (ResponseSubscription, error) {
	f.subject = subject
	f.handler = func(msg *nats.Msg) {
		f.handled++
		handler(msg)
	}
	if f.subscribeErr != nil {
		return nil, f.subscribeErr
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
	clock := newFakeClock(time.Unix(100, 0).UTC())
	publisher := &soakRecordingPublisher{}
	sender := New(Config{
		SiteID: "site-1", ReplyTimeout: 5 * time.Second,
	}, soakcatalog.New(8, 100, 0, clock), publisher, clock,
		rand.New(rand.NewSource(1)), &IDs{
			MessageID: func() string { return soakTestMessageID },
			RequestID: func() string { return soakTestRequestID },
		},
	)
	pending, err := sender.Publish(context.Background(), Target{
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

func TestSoakSender_ReplyLatencyExcludesIntentJournalDelay(t *testing.T) {
	const journalDelay = 10 * time.Millisecond
	const replyLatency = 3 * time.Millisecond
	clock := newFakeClock(time.Unix(100, 0).UTC())
	catalog := soakcatalog.New(8, 100, 10*time.Second, clock)
	publisher := &soakRecordingPublisher{}
	sender := newTestSender(catalog, publisher, clock, 0)

	lifecycle := NewMockLifecycle(gomock.NewController(t))
	gomock.InOrder(
		lifecycle.EXPECT().Start(gomock.Any()).DoAndReturn(func(*Pending) error {
			// The WAL group commit holds the intent until its batch is fsynced.
			clock.Advance(journalDelay)
			return nil
		}),
		lifecycle.EXPECT().Activate(gomock.Any()).Return(nil),
	)
	WithLifecycle(lifecycle, func(error) {})(sender)

	published, err := sender.Publish(context.Background(), Target{
		UserID: "u-1", Account: "alice", RoomID: "room-1",
	}, "hello")
	require.NoError(t, err)
	assert.Equal(t, clock.Now(), published.PublishedAt,
		"the reply clock starts when the publish leaves the process")

	clock.Advance(replyLatency)
	reply, err := json.Marshal(model.Message{
		ID: soakTestMessageID, RoomID: "room-1", UserID: "u-1",
		UserAccount: "alice", Content: "hello", CreatedAt: clock.Now(),
	})
	require.NoError(t, err)
	result := sender.HandleReply(subject.UserResponse("alice", soakTestRequestID), reply)

	require.Equal(t, ReplyAccepted, result.Status)
	assert.Equal(t, replyLatency, result.Latency,
		"send latency must measure the reply, not the load generator's own intent journal wait")
}

func TestSoakSender_DefaultsAndRejectsInvalidInputs(t *testing.T) {
	ids := ProductionIDs()
	assert.NotEmpty(t, ids.MessageID())
	assert.NotEmpty(t, ids.RequestID())

	sender := New(
		Config{}, soakcatalog.New(8, 100, 0, nil), &soakRecordingPublisher{},
		nil, nil, nil,
	)
	assert.NotNil(t, sender.clock)
	assert.NotNil(t, sender.rng)
	assert.NotNil(t, sender.ids)
	assert.Equal(t, 10*time.Second, sender.cfg.ReplyTimeout)

	fixed := newTestSender(
		soakcatalog.New(8, 100, 0, nil), &soakRecordingPublisher{},
		newFakeClock(time.Unix(100, 0)), 0,
	)
	for _, target := range []Target{
		{},
		{UserID: "u-1", Account: "alice"},
	} {
		_, err := fixed.Publish(context.Background(), target, "hello")
		require.ErrorContains(t, err, "target")
	}
	_, err := fixed.Publish(context.Background(), Target{
		UserID: "u-1", Account: "alice", RoomID: "room-1",
	}, "")
	require.ErrorContains(t, err, "content")

	emptyIDs := New(
		Config{}, soakcatalog.New(8, 100, 0, nil), &soakRecordingPublisher{},
		nil, rand.New(rand.NewSource(1)), &IDs{
			MessageID: func() string { return "" }, RequestID: func() string { return "" },
		},
	)
	_, err = emptyIDs.Publish(context.Background(), Target{
		UserID: "u-1", Account: "alice", RoomID: "room-1",
	}, "hello")
	require.ErrorContains(t, err, "identity")
	require.ErrorContains(t, fixed.Retry(context.Background(), "missing"), "not found")
}

func TestSoakSender_RejectsDuplicateMessageAndRequestIDs(t *testing.T) {
	target := Target{UserID: "u-1", Account: "alice", RoomID: "room-1"}
	clock := newFakeClock(time.Unix(100, 0))

	messageCalls := 0
	duplicateMessage := New(
		Config{}, soakcatalog.New(8, 100, 0, clock), &soakRecordingPublisher{},
		clock, rand.New(rand.NewSource(1)), &IDs{
			MessageID: func() string { return "same-message" },
			RequestID: func() string {
				messageCalls++
				return fmt.Sprintf("request-%d", messageCalls)
			},
		},
	)
	_, err := duplicateMessage.Publish(context.Background(), target, "first")
	require.NoError(t, err)
	_, err = duplicateMessage.Publish(context.Background(), target, "second")
	require.ErrorContains(t, err, "already tracked")

	requestCalls := 0
	duplicateRequest := New(
		Config{}, soakcatalog.New(8, 100, 0, clock), &soakRecordingPublisher{},
		clock, rand.New(rand.NewSource(1)), &IDs{
			MessageID: func() string {
				requestCalls++
				return fmt.Sprintf("message-%d", requestCalls)
			},
			RequestID: func() string { return "same-request" },
		},
	)
	_, err = duplicateRequest.Publish(context.Background(), target, "first")
	require.NoError(t, err)
	_, err = duplicateRequest.Publish(context.Background(), target, "second")
	require.ErrorContains(t, err, "already pending")
}

func TestSoakSender_CoversPendingFailureBoundaries(t *testing.T) {
	target := Target{UserID: "u-1", Account: "alice", RoomID: "room-1"}
	clock := newFakeClock(time.Unix(100, 0))

	retryErr := errors.New("retry failed")
	publisher := &soakRecordingPublisher{errors: []error{
		errors.New("ambiguous publish"), retryErr,
	}}
	sender := newTestSender(soakcatalog.New(8, 100, 0, clock), publisher, clock, 0)
	pending, err := sender.Publish(context.Background(), target, "hello")
	require.Error(t, err)
	require.ErrorIs(t, sender.Retry(context.Background(), pending.RequestID), retryErr)
	assert.Empty(t, sender.ExpireResults(), "a pending request must not expire before its deadline")
	assert.Nil(t, sender.markPendingDispatched("missing", clock.Now()))

	catalog := soakcatalog.New(8, 100, 0, clock)
	sender = newTestSender(catalog, &soakRecordingPublisher{}, clock, 0)
	pending, err = sender.Publish(context.Background(), target, "hello")
	require.NoError(t, err)
	require.True(t, catalog.Reject(target.RoomID, pending.MessageID))
	reply, err := json.Marshal(model.Message{
		ID: pending.MessageID, RoomID: target.RoomID, UserID: target.UserID,
		UserAccount: target.Account, Content: "hello", CreatedAt: clock.Now(),
	})
	require.NoError(t, err)
	result := sender.HandleReply(subject.UserResponse(target.Account, pending.RequestID), reply)
	assert.Equal(t, ReplyRejected, result.Status)
	assert.Equal(t, rpc.ErrorAssertion, result.ErrorClass)
}

func TestSoakSender_ReportsAbandonAndSubscribeFailures(t *testing.T) {
	wantErr := errors.New("abandon failed")
	lifecycle := NewMockLifecycle(gomock.NewController(t))
	lifecycle.EXPECT().Start(gomock.Any()).Return(nil)
	lifecycle.EXPECT().AbandonUnsent(gomock.Any()).Return(wantErr)
	var observed []error
	sender := New(
		Config{}, soakcatalog.New(8, 100, 0, nil),
		&soakRecordingPublisher{errors: []error{nats.ErrConnectionClosed}},
		nil, rand.New(rand.NewSource(1)), &IDs{
			MessageID: func() string { return soakTestMessageID },
			RequestID: func() string { return soakTestRequestID },
		}, WithLifecycle(lifecycle, func(err error) { observed = append(observed, err) }),
	)
	_, err := sender.Publish(context.Background(), Target{
		UserID: "u-1", Account: "alice", RoomID: "room-1",
	}, "hello")
	require.ErrorIs(t, err, nats.ErrConnectionClosed)
	require.Len(t, observed, 1)
	assert.ErrorIs(t, observed[0], wantErr)

	subscribeErr := errors.New("subscribe failed")
	_, err = StartResponses(&fakeSoakResponseSource{subscribeErr: subscribeErr}, sender)
	require.ErrorIs(t, err, subscribeErr)
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(now time.Time) *fakeClock {
	return &fakeClock{now: now}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(duration time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(duration)
}
