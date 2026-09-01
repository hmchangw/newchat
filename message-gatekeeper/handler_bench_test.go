package main

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roommetacache"
	"github.com/hmchangw/chat/pkg/stream"
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
			User:  model.SubscriptionUser{ID: "u-1", Account: "alice"},
			Roles: roles,
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

// BenchmarkProcessMessage_BypassSender is the path this change actually widens.
// An owner skipped the room-meta read entirely before, and now takes it to
// classify. Warm cache: the steady state of a running service, where the room
// has been seen recently.
func BenchmarkProcessMessage_BypassSender(b *testing.B) {
	store, err := newCachedMetaStore(newBenchStore(model.RoleOwner), 1024, time.Minute)
	if err != nil {
		b.Fatal(err)
	}
	runProcessMessage(b, benchHandler(store))
}

// BenchmarkProcessMessage_BypassSenderColdMiss times the same path with the
// cache removed, so every message goes through to the store.
//
// It is a floor, not the real cold cost: the store here returns immediately,
// where a genuine miss is a Mongo round-trip that dwarfs everything measured in
// this file. What this isolates is the in-process overhead of the miss — the
// cache machinery and the call itself — which is the part a benchmark can
// honestly attribute to this change.
func BenchmarkProcessMessage_BypassSenderColdMiss(b *testing.B) {
	// The store unwrapped: no cache, so every message reaches GetRoomMeta.
	store := newBenchStore(model.RoleOwner)
	h := benchHandler(store)
	runProcessMessage(b, h)
	if store.metaCalls.Load() < int64(b.N) {
		b.Fatalf("only %d of %d messages reached the store — this is not the miss path",
			store.metaCalls.Load(), b.N)
	}
}

// BenchmarkProcessMessage_RealPublish exists to give the numbers above a
// denominator that means something.
//
// The benchmarks above stub the canonical publish and the reply to an immediate
// return, so their per-message wall time is processMessage minus both of its
// network hops. A percentage measured against that is not the "1% of per-message
// wall time" any budget is written in terms of — it is a percentage of the part
// that happens to be cheap. This one runs the canonical publish against an
// in-process JetStream server, so the ratio between the two is the correction
// factor to apply.
//
// It is still a floor: the server is in the same process, so there is no wire.
func BenchmarkProcessMessage_RealPublish(b *testing.B) {
	opts := &natsserver.Options{Port: -1, JetStream: true, StoreDir: b.TempDir()}
	ns, err := natsserver.NewServer(opts)
	if err != nil {
		b.Fatal(err)
	}
	ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		b.Fatal("nats server did not become ready")
	}
	b.Cleanup(ns.Shutdown)

	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		b.Fatal(err)
	}
	sc := stream.MessagesCanonical("site-a")
	if _, err := js.CreateOrUpdateStream(context.Background(),
		jetstream.StreamConfig{Name: sc.Name, Subjects: sc.Subjects}); err != nil {
		b.Fatal(err)
	}

	store, err := newCachedMetaStore(newBenchStore(model.RoleOwner), 1024, time.Minute)
	if err != nil {
		b.Fatal(err)
	}
	publish := func(ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
		return js.PublishMsg(ctx, msg, opts...)
	}
	reply := func(context.Context, *nats.Msg) error { return nil }
	runProcessMessage(b, NewHandler(store, nil, publish, reply, "site-a", nil, 500, 1, 8192, ""))
}
