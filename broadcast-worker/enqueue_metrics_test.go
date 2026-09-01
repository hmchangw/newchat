package main

import (
	"context"
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/broadcastpath"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
)

// enqueueCounts reads broadcast_channel_enqueue_total keyed by its outcome
// label, asserting on the way through that outcome is the only label it carries.
func enqueueCounts(t *testing.T, reader sdkmetric.Reader) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	got := map[string]int64{}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "broadcast_channel_enqueue_total" {
				continue
			}
			for _, point := range m.Data.(metricdata.Sum[int64]).DataPoints {
				outcome, ok := point.Attributes.Value("outcome")
				require.True(t, ok, "broadcast_channel_enqueue_total point carries no outcome")
				assert.Equal(t, 1, point.Attributes.Len(), "outcome must be the only label")
				got[outcome.AsString()] = point.Value
			}
		}
	}
	return got
}

func TestBroadcastMetrics_ChannelEnqueue_BoundedOutcomes(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	m := newBroadcastMetrics(mp.Meter("test"))
	ctx := context.Background()

	m.ChannelEnqueue(ctx, nil)
	m.ChannelEnqueue(ctx, errors.New("broker down"))
	// A value outside the enum can only arrive via a conversion; it must collapse
	// rather than mint a series nothing closed.
	m.recordChannelEnqueue(ctx, enqueueOutcome("weird"))

	assert.Equal(t, map[string]int64{"ok": 1, "failed": 2}, enqueueCounts(t, reader))
}

func TestBroadcastMetrics_ChannelEnqueue_NilReceiverIsSafe(t *testing.T) {
	var m *broadcastMetrics
	m.ChannelEnqueue(context.Background(), nil)
}

// enqueueHandler builds a handler wired to a manual reader, plus the mocks a
// channel create needs.
func enqueueHandler(t *testing.T, encrypt bool, pub Publisher) (*Handler, *MockStore, *MockUserStore, *MockRoomKeyProvider, sdkmetric.Reader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	keyStore := NewMockRoomKeyProvider(ctrl)
	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, encrypt, subject.RouteGlobal,
		withBroadcastMetrics(newBroadcastMetrics(mp.Meter("test"))))
	return h, store, us, keyStore, reader
}

func createdChannelEvent(t *testing.T) []byte {
	t.Helper()
	evt := model.MessageEvent{
		Event:  model.EventCreated,
		SiteID: "site-a",
		Message: model.Message{
			ID: "msg-1", RoomID: "room-1", UserID: "user-1", UserAccount: "sender",
			Content: "hello", CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		},
	}
	data, err := json.Marshal(evt)
	require.NoError(t, err)
	return data
}

func TestHandler_ChannelEnqueue_RecordsOutcomes(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		pub := &mockPublisher{}
		h, store, us, keyStore, reader := enqueueHandler(t, true, pub)
		keyStore.EXPECT().Get(gomock.Any(), "room-1").Return(testRoomKey(t), nil)
		store.EXPECT().GetRoomMeta(gomock.Any(), "room-1").Return(metaOf(testChannelRoom), nil)
		us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"sender"}).Return(nil, nil)

		require.NoError(t, h.HandleMessage(context.Background(), createdChannelEvent(t)))

		assert.Equal(t, map[string]int64{"ok": 1}, enqueueCounts(t, reader))
	})

	// The encryption failure is the reason this is a defer on a named return
	// rather than a wrapper around the final publish. It is a real enqueue
	// failure that returns two statements early, and counting only the publish
	// would leave it out of SLO-1b's numerator — biasing the ratio green on
	// exactly the path where the room key is unavailable.
	t.Run("failed on encryption", func(t *testing.T) {
		pub := &mockPublisher{}
		h, store, us, keyStore, reader := enqueueHandler(t, true, pub)
		keyStore.EXPECT().Get(gomock.Any(), "room-1").
			Return(nil, errors.New("key store unavailable"))
		store.EXPECT().GetRoomMeta(gomock.Any(), "room-1").Return(metaOf(testChannelRoom), nil)
		us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"sender"}).Return(nil, nil)

		require.Error(t, h.HandleMessage(context.Background(), createdChannelEvent(t)))

		assert.Equal(t, map[string]int64{"failed": 1}, enqueueCounts(t, reader))
		assert.Empty(t, pub.records, "nothing was enqueued")
	})

	t.Run("failed on publish", func(t *testing.T) {
		pub := &mockPublisher{failOn: map[string]error{
			subject.RoomEvent("room-1", true): errors.New("broker down"),
		}}
		h, store, us, keyStore, reader := enqueueHandler(t, true, pub)
		keyStore.EXPECT().Get(gomock.Any(), "room-1").Return(testRoomKey(t), nil)
		store.EXPECT().GetRoomMeta(gomock.Any(), "room-1").Return(metaOf(testChannelRoom), nil)
		us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"sender"}).Return(nil, nil)

		require.Error(t, h.HandleMessage(context.Background(), createdChannelEvent(t)))

		assert.Equal(t, map[string]int64{"failed": 1}, enqueueCounts(t, reader))
	})
}

func TestHandler_ChannelEnqueue_DualRoutePartialFailureIsOneLogicalFailure(t *testing.T) {
	pub := &mockPublisher{failOn: map[string]error{
		subject.RoomEvent("room-1", false): errors.New("local lane down"),
	}}
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	keyStore := NewMockRoomKeyProvider(ctrl)
	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, subject.RouteDual,
		withBroadcastMetrics(newBroadcastMetrics(mp.Meter("test"))))
	keyStore.EXPECT().Get(gomock.Any(), "room-1").Return(testRoomKey(t), nil)
	meta := metaOf(testChannelRoom)
	meta.CrossSite = ptrBool(false)
	store.EXPECT().GetRoomMeta(gomock.Any(), "room-1").Return(meta, nil)
	us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"sender"}).Return(nil, nil)

	require.Error(t, h.HandleMessage(context.Background(), createdChannelEvent(t)))
	assert.Equal(t, []string{
		subject.RoomEvent("room-1", false),
		subject.RoomEvent("room-1", true),
	}, subjectsOf(pub), "all required targets must be attempted")
	assert.Equal(t, map[string]int64{"failed": 1}, enqueueCounts(t, reader),
		"one canonical message produces one logical outcome, not one outcome per target")
}

// TestHandler_ChannelEnqueue_MatchesTheGatekeeperDenominator is the other half
// of message-gatekeeper's TestHandler_processMessage_RecordsBroadcastPath. Both
// drive the same shared table: the numerator must fire exactly when the
// gatekeeper labelled the message room_subject, and never otherwise. If the two
// drift, SLO-1b's halves count different messages and the ratio is worthless.
func TestHandler_ChannelEnqueue_MatchesTheGatekeeperDenominator(t *testing.T) {
	for _, tc := range testutil.BroadcastPathCases() {
		t.Run(tc.Name, func(t *testing.T) {
			pub := &mockPublisher{}
			h, store, us, keyStore, reader := enqueueHandler(t, false, pub)
			room := &model.Room{ID: "room-1", Name: "r", Type: tc.RoomType, SiteID: "site-a", UserCount: 2}
			store.EXPECT().GetRoomMeta(gomock.Any(), "room-1").Return(metaOf(room), nil).AnyTimes()
			us.EXPECT().FindUsersByAccounts(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
			keyStore.EXPECT().Get(gomock.Any(), gomock.Any()).Return(testRoomKey(t), nil).AnyTimes()
			store.EXPECT().GetThreadFollowers(gomock.Any(), gomock.Any()).
				Return(map[string]struct{}{"alice": {}}, nil).AnyTimes()
			// Real members, not nil: with an empty room every non-room_subject
			// route publishes nothing, and "no ok was recorded" would then be
			// satisfied by a fan-out that never happened.
			store.EXPECT().ListRoomMembers(gomock.Any(), gomock.Any()).
				Return(testDMSubs, nil).AnyTimes()
			store.EXPECT().GetHistorySharedSince(gomock.Any(), gomock.Any(), gomock.Any()).
				Return(nil, nil).AnyTimes()

			evt := model.MessageEvent{
				Event:  model.EventCreated,
				SiteID: "site-a",
				Message: model.Message{
					ID: "msg-1", RoomID: "room-1", UserID: "user-1", UserAccount: "sender",
					Content: "hello", CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
					// The canonical field, not the request one: this worker
					// receives what the gatekeeper already normalized.
					ThreadParentMessageID: tc.ThreadParentMessageID,
					TShow:                 tc.TShow,
				},
			}
			data, err := json.Marshal(evt)
			require.NoError(t, err)
			require.NoError(t, h.HandleMessage(context.Background(), data))

			want := map[string]int64{}
			switch tc.Want {
			case broadcastpath.RoomSubject:
				want["ok"] = 1
				assert.NotEmpty(t, pub.records, "the message was never dispatched anywhere")
			case broadcastpath.Unknown:
				// The second producer of `unknown`: a room type the worker's
				// dispatch has no branch for. It logs and returns nil — the
				// message is dropped, not merely unmeasured — which is why the
				// contract calls the label a validity signal rather than a
				// bucket. Pinned here so a future dispatch branch for a new room
				// type cannot land without the gatekeeper's label learning it.
				assert.Empty(t, pub.records, "an unrecognised room type must reach no fan-out at all")
			default:
				// Without the publish count, "no ok was recorded" is satisfied
				// by a handler that returned before dispatching at all, and the
				// case would prove nothing about routing.
				assert.NotEmpty(t, pub.records, "the message was never dispatched anywhere")
			}
			assert.Equal(t, want, enqueueCounts(t, reader),
				"the numerator must fire exactly on the route the gatekeeper labels room_subject")
		})
	}
}

// TestPublishChannelEvent_MutationsDoNotCount checks the current dispatch
// match. broadcast_channel_enqueue_total counts created channel messages on the
// room-subject path, which is what
// messages_canonical_published_total{broadcast_path="room_subject"} counts
// upstream — and that equality holds only while publishChannelEvent is reached
// from handleCreated alone. A mutation (edit, pin, react) reaches the room
// subject through publishMutation → publishRoomEvent, bypassing it; a second
// caller would silently add messages to the numerator that the denominator
// never saw, pushing SLO-1b above 1 for a reason that is not redelivery.
func TestPublishChannelEvent_MutationsDoNotCount(t *testing.T) {
	pub := &mockPublisher{}
	h, store, us, keyStore, reader := enqueueHandler(t, false, pub)
	store.EXPECT().GetRoomMeta(gomock.Any(), "room-1").Return(metaOf(testChannelRoom), nil).AnyTimes()
	store.EXPECT().GetRoom(gomock.Any(), "room-1").Return(testChannelRoom, nil).AnyTimes()
	us.EXPECT().FindUsersByAccounts(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	keyStore.EXPECT().Get(gomock.Any(), gomock.Any()).Return(testRoomKey(t), nil).AnyTimes()

	require.NoError(t, h.HandleMessage(context.Background(), createdChannelEvent(t)))
	assert.Equal(t, map[string]int64{"ok": 1}, enqueueCounts(t, reader), "the create counts")

	// Every mutation reaches the same room subject, and none of them may count.
	edited := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	for _, event := range []model.EventType{
		model.EventUpdated, model.EventDeleted, model.EventPinned, model.EventUnpinned,
	} {
		evt := model.MessageEvent{
			Event:  event,
			SiteID: "site-a",
			Message: model.Message{
				ID: "msg-1", RoomID: "room-1", UserID: "user-1", UserAccount: "sender",
				Content: "hello", CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
				EditedAt: &edited, UpdatedAt: &edited, PinnedAt: &edited,
			},
		}
		data, err := json.Marshal(evt)
		require.NoError(t, err)
		require.NoError(t, h.HandleMessage(context.Background(), data), "event %q", event)
	}

	// Without this the assertion below is vacuous: a mutation that errored out
	// before reaching the room subject proves nothing about who calls
	// publishChannelEvent.
	assert.Greater(t, len(pub.records), 1, "the mutations must actually have reached the room subject")
	assert.Equal(t, map[string]int64{"ok": 1}, enqueueCounts(t, reader),
		"a mutation reached publishChannelEvent — the numerator now counts messages the upstream denominator never saw")
}

// TestPublishChannelEvent_HasOneCaller is the structural guard behind the
// denominator match. A runtime event sample cannot prove that an untested event
// or future branch did not add another caller, so parse the production source
// and require exactly one call site, owned by handleCreated.
func TestPublishChannelEvent_HasOneCaller(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "handler.go", nil, 0)
	require.NoError(t, err)

	var callers []string
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if ok && sel.Sel.Name == "publishChannelEvent" {
				callers = append(callers, fn.Name.Name)
			}
			return true
		})
	}

	assert.Equal(t, []string{"handleCreated"}, callers,
		"SLO-1b's numerator must have exactly one production call site")
}
