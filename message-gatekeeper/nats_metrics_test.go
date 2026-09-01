package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/broadcastpath"
	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsmetrics"
	"github.com/hmchangw/chat/pkg/roommetacache"
	"github.com/hmchangw/chat/pkg/testutil"
)

func TestCanonicalPublishError_PreservesClassificationAndCause(t *testing.T) {
	cause := errors.New("stream unavailable")
	err := fmt.Errorf("publish to MESSAGES-CANONICAL: %w", errors.Join(errCanonicalPublish, cause))

	assert.ErrorIs(t, err, errCanonicalPublish)
	assert.ErrorIs(t, err, cause)
}

func gatekeeperCounts(t *testing.T, reader sdkmetric.Reader) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	got := map[string]int64{}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "message_gatekeeper_messages_total" {
				continue
			}
			for _, point := range m.Data.(metricdata.Sum[int64]).DataPoints {
				values := map[string]string{}
				for _, attr := range point.Attributes.ToSlice() {
					values[string(attr.Key)] = attr.Value.AsString()
				}
				got[values["result"]+"/"+values["reason"]] = point.Value
			}
		}
	}
	return got
}

func TestGatekeeperMetrics_BoundedResults(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	m := newGatekeeperMetrics(mp.Meter("test"))

	m.Record(context.Background(), resultAccepted, reasonNone)
	// A value outside the enum can only arrive via a conversion; it must still
	// collapse instead of minting an unbounded series.
	m.Record(context.Background(), gatekeeperResult("dynamic"), gatekeeperReasonCode("secret error text"))

	assert.Equal(t, map[string]int64{"accepted/none": 1, "failed/unknown": 1}, gatekeeperCounts(t, reader))
}

func TestHandler_HandleJetStreamMsg_RecordsRejectedOutcome(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	metrics := newGatekeeperMetrics(mp.Meter("test"))
	h := NewHandler(nil, nil, nil, func(context.Context, *nats.Msg) error { return nil }, "site-a", nil, 500, 1, 8192, "", withGatekeeperMetrics(metrics))
	msg := &fakeJSMsg{subject: "chat.invalid", data: []byte(`{}`)}

	h.HandleJetStreamMsg(context.Background(), msg)

	assert.True(t, msg.acked)
	assert.False(t, msg.naked)
	assert.Equal(t, map[string]int64{"rejected/invalid_subject": 1}, gatekeeperCounts(t, reader))
}

func TestHandler_HandleJetStreamMsg_RecordsAcceptedAndRetryOutcomes(t *testing.T) {
	for _, tt := range []struct {
		name       string
		publishErr error
		wantMetric map[string]int64
		wantAck    bool
		wantNak    bool
		final      bool
	}{
		{name: "accepted", wantMetric: map[string]int64{"accepted/none": 1}, wantAck: true},
		{name: "canonical publish retry", publishErr: errors.New("stream unavailable"), wantMetric: map[string]int64{"retry/canonical_publish": 1}, wantNak: true},
		{name: "canonical publish exhausted", publishErr: errors.New("stream unavailable"), wantMetric: map[string]int64{"failed/canonical_publish": 1}, wantNak: true, final: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			reader := sdkmetric.NewManualReader()
			mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			metrics := newGatekeeperMetrics(mp.Meter("test"))
			ctrl := gomock.NewController(t)
			store := NewMockStore(ctrl)
			store.EXPECT().GetSubscription(gomock.Any(), "weather.bot", "room-1").Return(&model.Subscription{
				User:  model.SubscriptionUser{ID: "u-bot", Account: "weather.bot"},
				Roles: []model.Role{model.RoleMember},
			}, nil)
			store.EXPECT().GetRoomMeta(gomock.Any(), "room-1").
				Return(roommetacache.Meta{ID: "room-1", Type: model.RoomTypeChannel, UserCount: 1}, nil)
			h := NewHandler(store, nil, makePublishFunc(nil, tt.publishErr), func(context.Context, *nats.Msg) error { return nil }, "site-a", nil, 500, 1, 8192, "", withGatekeeperMetrics(metrics))
			request := model.SendMessageRequest{
				ID: idgen.GenerateMessageID(), Content: "hello",
				RequestID: "01970a4f-8c2d-7c9a-abcd-e0123456789f",
			}
			data, err := json.Marshal(request)
			require.NoError(t, err)
			msg := &fakeJSMsg{subject: "chat.user.weather_bot.room.room-1.site-a.msg.send", data: data}
			ctx := context.Background()
			var delivery jetstream.Msg = msg
			if tt.final {
				msg.numDelivered = 1
				consumer := natsmetrics.New(mp.Meter("shared")).Consumer(natsmetrics.ConsumerConfig{
					Site: "site-a", Stream: "MESSAGES_site-a", Consumer: "message-gatekeeper",
				})
				consumer.LoopStarted(ctx)
				tracked := consumer.Track(ctx, msg, natsmetrics.EventSend, 1)
				delivery = tracked
				ctx = tracked.Context(ctx)
			}

			h.HandleJetStreamMsg(ctx, delivery)

			assert.Equal(t, tt.wantAck, msg.acked)
			assert.Equal(t, tt.wantNak, msg.naked)
			assert.Equal(t, tt.wantMetric, gatekeeperCounts(t, reader))
		})
	}
}

func TestGatekeeperMetrics_Record_NilReceiverIsSafe(t *testing.T) {
	var metrics *gatekeeperMetrics
	metrics.Record(context.Background(), resultAccepted, reasonNone)
}

// A permanent server-side fault (e.g. a value that cannot be marshaled) is
// undeliverable work, not a client rejection: it must Ack — retrying can never
// succeed — but count as failed/internal so the rejection series stays a pure
// signal of client errors.
func TestHandler_HandleJetStreamMsg_PermanentFault_RecordsFailedNotRejected(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	metrics := newGatekeeperMetrics(mp.Meter("test"))
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "room-1").
		Return(nil, errcode.MarshalFailed("message event", errors.New("json: unsupported value: NaN")))

	h := NewHandler(store, nil, nil, func(context.Context, *nats.Msg) error { return nil },
		"site-a", nil, 500, 1, 8192, "", withGatekeeperMetrics(metrics))
	request := model.SendMessageRequest{
		ID: idgen.GenerateMessageID(), Content: "hello",
		RequestID: "01970a4f-8c2d-7c9a-abcd-e0123456789f",
	}
	data, err := json.Marshal(request)
	require.NoError(t, err)
	msg := &fakeJSMsg{subject: "chat.user.alice.room.room-1.site-a.msg.send", data: data}

	h.HandleJetStreamMsg(context.Background(), msg)

	assert.True(t, msg.acked, "a permanent fault can never succeed on redelivery — Ack-drop it")
	assert.False(t, msg.naked)
	assert.Equal(t, map[string]int64{"failed/internal": 1}, gatekeeperCounts(t, reader))
}

// A canonical publish rejected for exceeding max_payload fails identically on
// every redelivery: Ack-drop it (with a reply) instead of Nakking to MaxDeliver.
func TestHandler_HandleJetStreamMsg_OversizedCanonicalPublish_IsPermanent(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	metrics := newGatekeeperMetrics(mp.Meter("test"))
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().GetSubscription(gomock.Any(), "weather.bot", "room-1").Return(&model.Subscription{
		User:  model.SubscriptionUser{ID: "u-bot", Account: "weather.bot"},
		Roles: []model.Role{model.RoleMember},
	}, nil)
	store.EXPECT().GetRoomMeta(gomock.Any(), "room-1").
		Return(roommetacache.Meta{ID: "room-1", Type: model.RoomTypeChannel, UserCount: 1}, nil)
	h := NewHandler(store, nil, makePublishFunc(nil, nats.ErrMaxPayload),
		func(context.Context, *nats.Msg) error { return nil }, "site-a", nil, 500, 1, 8192, "",
		withGatekeeperMetrics(metrics))
	request := model.SendMessageRequest{
		ID: idgen.GenerateMessageID(), Content: "hello",
		RequestID: "01970a4f-8c2d-7c9a-abcd-e0123456789f",
	}
	data, err := json.Marshal(request)
	require.NoError(t, err)
	msg := &fakeJSMsg{subject: "chat.user.weather_bot.room.room-1.site-a.msg.send", data: data}

	h.HandleJetStreamMsg(context.Background(), msg)

	assert.True(t, msg.acked, "an oversized message can never be published — Ack-drop it")
	assert.False(t, msg.naked)
	assert.Equal(t, map[string]int64{"failed/internal": 1}, gatekeeperCounts(t, reader))
}

// canonicalPublishCounts reads the two SLO-1a/1b denominator families: the
// per-path publish counter keyed by its broadcast_path label, and the unlabelled
// duplicate counter under the key "duplicate".
func canonicalPublishCounts(t *testing.T, reader sdkmetric.Reader) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	got := map[string]int64{}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			switch m.Name {
			case "messages_canonical_published_total":
				for _, point := range m.Data.(metricdata.Sum[int64]).DataPoints {
					path, ok := point.Attributes.Value("broadcast_path")
					require.True(t, ok, "messages_canonical_published_total point carries no broadcast_path")
					assert.Equal(t, 1, point.Attributes.Len(), "broadcast_path must be the only label")
					got[path.AsString()] = point.Value
				}
			case "messages_canonical_publish_duplicate_total":
				for _, point := range m.Data.(metricdata.Sum[int64]).DataPoints {
					assert.Equal(t, 0, point.Attributes.Len(), "the duplicate counter carries no labels")
					got["duplicate"] = point.Value
				}
			}
		}
	}
	return got
}

// TestGatekeeperMetrics_CanonicalPublished_BoundedPaths pins the exact series
// name and label values, including the two a live smoke check can never produce
// (`unknown` and the duplicate counter) — see the contract's §13.3 note on why
// those have to be pinned by unit test rather than by scraping.
func TestGatekeeperMetrics_CanonicalPublished_BoundedPaths(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	m := newGatekeeperMetrics(mp.Meter("test"))
	ctx := context.Background()

	for _, p := range broadcastpath.All {
		m.RecordCanonicalPublished(ctx, p)
	}
	// A value outside the enum can only arrive via a conversion; it must collapse
	// onto unknown instead of minting an unbounded series.
	m.RecordCanonicalPublished(ctx, broadcastpath.Path("something-new"))
	m.RecordCanonicalPublishDuplicate(ctx)

	assert.Equal(t, map[string]int64{
		"room_subject": 1, "thread": 1, "dm": 1, "unknown": 2, "duplicate": 1,
	}, canonicalPublishCounts(t, reader))
}

func TestGatekeeperMetrics_CanonicalPublished_NilReceiverIsSafe(t *testing.T) {
	var metrics *gatekeeperMetrics
	metrics.RecordCanonicalPublished(context.Background(), broadcastpath.RoomSubject)
	metrics.RecordCanonicalPublishDuplicate(context.Background())
}

// TestHandler_processMessage_RecordsBroadcastPath drives the shared
// classification table through the real handler. The table is shared with
// broadcast-worker's dispatch test on purpose: SLO-1b's denominator is emitted
// here and its numerator there, so two tables that drift give a ratio whose
// halves count different messages.
func TestHandler_processMessage_RecordsBroadcastPath(t *testing.T) {
	for _, tc := range testutil.BroadcastPathCases() {
		t.Run(tc.Name, func(t *testing.T) {
			reader := sdkmetric.NewManualReader()
			mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			metrics := newGatekeeperMetrics(mp.Meter("test"))
			ctrl := gomock.NewController(t)
			store := NewMockStore(ctrl)
			store.EXPECT().GetSubscription(gomock.Any(), "alice", "room-1").
				Return(&model.Subscription{User: model.SubscriptionUser{ID: "u-alice", Account: "alice"}}, nil)
			if tc.Want == broadcastpath.Thread {
				// The thread test is free — a hidden thread reply must not pay
				// for a room-meta lookup it cannot use.
				store.EXPECT().GetRoomMeta(gomock.Any(), gomock.Any()).Times(0)
			} else {
				store.EXPECT().GetRoomMeta(gomock.Any(), "room-1").
					Return(roommetacache.Meta{ID: "room-1", Type: tc.RoomType, UserCount: 1}, nil)
			}
			var published []publishedMsg
			h := NewHandler(store, nil, makePublishFunc(&published, nil),
				func(context.Context, *nats.Msg) error { return nil }, "site-a", nil, 500, 1, 8192, "",
				withGatekeeperMetrics(metrics))

			req := model.SendMessageRequest{
				ID: idgen.GenerateMessageID(), Content: "hello",
				RequestID:             "01970a4f-8c2d-7c9a-abcd-e0123456789f",
				ThreadParentMessageID: tc.ThreadParentMessageID,
				TShow:                 tc.TShow,
			}
			_, err := h.processMessage(context.Background(), "alice", "room-1", "site-a", &req)
			require.NoError(t, err)

			assert.Equal(t, map[string]int64{string(tc.Want): 1}, canonicalPublishCounts(t, reader))

			// Close the loop on the wire: broadcast-worker classifies the
			// message it receives, not the request the gatekeeper saw. The
			// canonical event must therefore carry the fields that reproduce
			// the same route — this is what catches a normalization applied to
			// the label but not to the published message, or vice versa.
			require.Len(t, published, 1)
			var evt model.MessageEvent
			require.NoError(t, json.Unmarshal(published[0].data, &evt))
			assert.Equal(t, tc.Want, broadcastpath.Classify(
				evt.Message.ThreadParentMessageID, evt.Message.TShow, tc.RoomType),
				"the published message reproduces a different route than the label")
		})
	}
}

func TestHandler_processMessage_RoomMetaErrorFailsOpenToUnknown(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	metrics := newGatekeeperMetrics(mp.Meter("test"))
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "room-1").
		Return(&model.Subscription{User: model.SubscriptionUser{ID: "u-alice", Account: "alice"}}, nil)
	store.EXPECT().GetRoomMeta(gomock.Any(), "room-1").
		Return(roommetacache.Meta{}, errors.New("mongo unavailable"))
	var published []publishedMsg
	h := NewHandler(store, nil, makePublishFunc(&published, nil),
		func(context.Context, *nats.Msg) error { return nil }, "site-a", nil, 500, 1, 8192, "",
		withGatekeeperMetrics(metrics))

	req := model.SendMessageRequest{
		ID: idgen.GenerateMessageID(), Content: "hello",
		RequestID: "01970a4f-8c2d-7c9a-abcd-e0123456789f",
	}
	_, err := h.processMessage(context.Background(), "alice", "room-1", "site-a", &req)

	require.NoError(t, err, "a metric must never fail a message")
	assert.Len(t, published, 1, "the message is still published")
	assert.Equal(t, map[string]int64{"unknown": 1}, canonicalPublishCounts(t, reader))
}

func TestHandler_processMessage_DuplicateAckIsExcluded(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	metrics := newGatekeeperMetrics(mp.Meter("test"))
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "room-1").
		Return(&model.Subscription{User: model.SubscriptionUser{ID: "u-alice", Account: "alice"}}, nil)
	store.EXPECT().GetRoomMeta(gomock.Any(), "room-1").
		Return(roommetacache.Meta{ID: "room-1", Type: model.RoomTypeChannel, UserCount: 1}, nil)
	dup := func(context.Context, *nats.Msg, ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
		return &jetstream.PubAck{Duplicate: true}, nil
	}
	h := NewHandler(store, nil, dup, func(context.Context, *nats.Msg) error { return nil },
		"site-a", nil, 500, 1, 8192, "", withGatekeeperMetrics(metrics))

	req := model.SendMessageRequest{
		ID: idgen.GenerateMessageID(), Content: "hello",
		RequestID: "01970a4f-8c2d-7c9a-abcd-e0123456789f",
	}
	_, err := h.processMessage(context.Background(), "alice", "room-1", "site-a", &req)
	require.NoError(t, err)

	assert.Equal(t, map[string]int64{"duplicate": 1}, canonicalPublishCounts(t, reader),
		"the stream deduplicated this publish, so it is not a new canonical message")
}

func TestHandler_processMessage_PublishFailureCountsNeither(t *testing.T) {
	for _, tt := range []struct {
		name       string
		publishErr error
	}{
		{name: "transient", publishErr: errors.New("stream unavailable")},
		{name: "oversized", publishErr: nats.ErrMaxPayload},
	} {
		t.Run(tt.name, func(t *testing.T) {
			reader := sdkmetric.NewManualReader()
			mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			metrics := newGatekeeperMetrics(mp.Meter("test"))
			ctrl := gomock.NewController(t)
			store := NewMockStore(ctrl)
			store.EXPECT().GetSubscription(gomock.Any(), "alice", "room-1").
				Return(&model.Subscription{User: model.SubscriptionUser{ID: "u-alice", Account: "alice"}}, nil)
			store.EXPECT().GetRoomMeta(gomock.Any(), "room-1").
				Return(roommetacache.Meta{ID: "room-1", Type: model.RoomTypeChannel, UserCount: 1}, nil)
			h := NewHandler(store, nil, makePublishFunc(nil, tt.publishErr),
				func(context.Context, *nats.Msg) error { return nil }, "site-a", nil, 500, 1, 8192, "",
				withGatekeeperMetrics(metrics))

			req := model.SendMessageRequest{
				ID: idgen.GenerateMessageID(), Content: "hello",
				RequestID: "01970a4f-8c2d-7c9a-abcd-e0123456789f",
			}
			_, err := h.processMessage(context.Background(), "alice", "room-1", "site-a", &req)

			require.Error(t, err)
			assert.Empty(t, canonicalPublishCounts(t, reader))
		})
	}
}

// A message rejected before the publish must not reach either counter — the
// denominator means "published", not "accepted by the handler".
func TestHandler_processMessage_RejectedBeforePublishCountsNeither(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	metrics := newGatekeeperMetrics(mp.Meter("test"))
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "room-1").Return(nil, errNotSubscribed)
	h := NewHandler(store, nil, makePublishFunc(nil, nil),
		func(context.Context, *nats.Msg) error { return nil }, "site-a", nil, 500, 1, 8192, "",
		withGatekeeperMetrics(metrics))

	req := model.SendMessageRequest{
		ID: idgen.GenerateMessageID(), Content: "hello",
		RequestID: "01970a4f-8c2d-7c9a-abcd-e0123456789f",
	}
	_, err := h.processMessage(context.Background(), "alice", "room-1", "site-a", &req)

	require.Error(t, err)
	assert.Empty(t, canonicalPublishCounts(t, reader))
}

// The large-room cap must keep its exact pre-existing scope. Classifying now
// fetches room meta on paths that previously skipped it — a bypass-eligible
// sender, and a tshow thread reply — and the cap must still not apply to either.
func TestHandler_processMessage_ClassificationDoesNotWidenLargeRoomCap(t *testing.T) {
	for _, tt := range []struct {
		name      string
		account   string
		roles     []model.Role
		threadPID string
		tShow     bool
		wantPath  string
	}{
		{name: "bot sender bypasses the cap", account: "weather.bot", roles: []model.Role{model.RoleMember}, wantPath: "room_subject"},
		{name: "owner bypasses the cap", account: "alice", roles: []model.Role{model.RoleOwner}, wantPath: "room_subject"},
		{name: "tshow thread reply is exempt", account: "alice", roles: []model.Role{model.RoleMember}, threadPID: idgen.GenerateMessageID(), tShow: true, wantPath: "room_subject"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			reader := sdkmetric.NewManualReader()
			mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			metrics := newGatekeeperMetrics(mp.Meter("test"))
			ctrl := gomock.NewController(t)
			store := NewMockStore(ctrl)
			store.EXPECT().GetSubscription(gomock.Any(), tt.account, "room-1").
				Return(&model.Subscription{
					User:  model.SubscriptionUser{ID: "u-1", Account: tt.account},
					Roles: tt.roles,
				}, nil)
			// Well over the threshold: a message that reaches the cap is rejected.
			store.EXPECT().GetRoomMeta(gomock.Any(), "room-1").
				Return(roommetacache.Meta{ID: "room-1", Type: model.RoomTypeChannel, UserCount: 10_000}, nil)
			h := NewHandler(store, nil, makePublishFunc(nil, nil),
				func(context.Context, *nats.Msg) error { return nil }, "site-a", nil, 500, 1, 8192, "",
				withGatekeeperMetrics(metrics))

			req := model.SendMessageRequest{
				ID: idgen.GenerateMessageID(), Content: "hello",
				RequestID:             "01970a4f-8c2d-7c9a-abcd-e0123456789f",
				ThreadParentMessageID: tt.threadPID,
				TShow:                 tt.tShow,
			}
			_, err := h.processMessage(context.Background(), tt.account, "room-1", "site-a", &req)

			require.NoError(t, err, "the cap must not have newly applied to this sender")
			assert.Equal(t, map[string]int64{tt.wantPath: 1}, canonicalPublishCounts(t, reader))
		})
	}
}
