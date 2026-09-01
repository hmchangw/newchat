package main

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roommetacache"
)

// benchStore is a hand-written stub rather than the generated mock: gomock's
// call bookkeeping costs more per call than the store lookup this benchmark
// exists to measure, and would bury the signal under its own overhead.
type benchStore struct {
	sub       *model.Subscription
	meta      roommetacache.Meta
	metaCalls atomic.Int64
}

func (s *benchStore) GetSubscription(context.Context, string, string) (*model.Subscription, error) {
	return s.sub, nil
}

func (s *benchStore) GetRoomMeta(context.Context, string) (roommetacache.Meta, error) {
	s.metaCalls.Add(1)
	return s.meta, nil
}

// benchHandler builds a handler over store, with the publish and reply paths
// stubbed to nothing so the benchmark measures processMessage rather than NATS.
func benchHandler(store Store) *Handler {
	publish := func(context.Context, *nats.Msg, ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
		return &jetstream.PubAck{}, nil
	}
	reply := func(context.Context, *nats.Msg) error { return nil }
	return NewHandler(store, nil, publish, reply, "site-a", nil, 500, 1, 8192, "")
}

func newBenchStore(roles ...model.Role) *benchStore {
	return &benchStore{
		sub: &model.Subscription{
			User:     model.SubscriptionUser{ID: "u-1", Account: "alice"},
			Roles:    roles,
			RoomType: model.RoomTypeChannel,
		},
		meta: roommetacache.Meta{ID: "room-1", Type: model.RoomTypeChannel, UserCount: 5},
	}
}

func runProcessMessage(b *testing.B, h *Handler) {
	b.Helper()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := model.SendMessageRequest{
			ID:        idgen.GenerateMessageID(),
			Content:   "hello world",
			RequestID: "01970a4f-8c2d-7c9a-abcd-e0123456789f",
		}
		if _, err := h.processMessage(ctx, "alice", "room-1", "site-a", &req); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkProcessMessage_Member is the path that already paid for the
// room-meta read before broadcast_path existed: an ordinary member, no bypass.
// It is the control — this number must not move.
func BenchmarkProcessMessage_Member(b *testing.B) {
	store, err := newCachedMetaStore(newBenchStore(model.RoleMember), 1024, time.Minute)
	if err != nil {
		b.Fatal(err)
	}
	runProcessMessage(b, benchHandler(store))
}

// BenchmarkProcessMessage_BypassSender guards the owner fast path: route
// classification reuses the subscription projection and must not add a
// room-meta lookup.
func BenchmarkProcessMessage_BypassSender(b *testing.B) {
	inner := newBenchStore(model.RoleOwner)
	store, err := newCachedMetaStore(inner, 1024, time.Minute)
	if err != nil {
		b.Fatal(err)
	}
	runProcessMessage(b, benchHandler(store))
	if calls := inner.metaCalls.Load(); calls != 0 {
		b.Fatalf("room meta read %d times on the bypass fast path", calls)
	}
}

// BenchmarkProcessMessage_BypassSenderLegacyTypeFallback covers the temporary
// compatibility path for a subscription projection with no room type. It is an
// in-process floor; a real L2/Mongo miss is intentionally outside this microbench.
func BenchmarkProcessMessage_BypassSenderLegacyTypeFallback(b *testing.B) {
	store := newBenchStore(model.RoleOwner)
	store.sub.RoomType = ""
	h := benchHandler(store)
	runProcessMessage(b, h)
	if store.metaCalls.Load() < int64(b.N) {
		b.Fatalf("only %d of %d messages reached the store — this is not the miss path",
			store.metaCalls.Load(), b.N)
	}
}
