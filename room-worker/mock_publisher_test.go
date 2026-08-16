package main

import (
	"bytes"
	"context"
	"sync"

	"github.com/hmchangw/chat/pkg/roomkeysender"
	"github.com/hmchangw/chat/pkg/roomkeystore"
)

// mockPublisher captures NATS publishes for use in unit tests.
type mockPublisher struct {
	mu       sync.Mutex
	subjects []string
	payloads [][]byte
}

func (p *mockPublisher) Publish(subj string, data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.subjects = append(p.subjects, subj)
	p.payloads = append(p.payloads, append([]byte(nil), data...))
	return nil
}

// published snapshots the capture as []publishedMsg for the shared
// subject-filter helpers (subscriptionUpdates, messagesCanonical).
func (p *mockPublisher) published() []publishedMsg {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]publishedMsg, 0, len(p.subjects))
	for i, s := range p.subjects {
		out = append(out, publishedMsg{subj: s, data: p.payloads[i]})
	}
	return out
}

func (p *mockPublisher) publishCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.subjects)
}

// stubRoomKeyStore is a zero-config RoomKeyStore for tests that don't exercise
// key behavior (production always wires a Mongo-backed key store, so the Handler
// can no longer be constructed with a nil keyStore). Tests that DO exercise key
// behavior should build their own MockRoomKeyStore with explicit EXPECTations
// rather than using this stub.
type stubRoomKeyStore struct{}

// Get returns a synthetic version-0 `roomkeystore.VersionedKeyPair` whose
// `roomkeystore.RoomKeyPair` byte fields are placeholder fill (0x04, 0x05) —
// they are NOT valid P-256 key material and MUST NOT be used by any test that
// performs real crypto. Use a real `generateRoomKeyPair`/`roomcrypto`-based
// keypair for those.
func (stubRoomKeyStore) Get(_ context.Context, _ string) (*roomkeystore.VersionedKeyPair, error) {
	return &roomkeystore.VersionedKeyPair{
		Version: 0,
		KeyPair: roomkeystore.RoomKeyPair{
			PrivateKey: bytes.Repeat([]byte{0x05}, 32),
		},
	}, nil
}

func (stubRoomKeyStore) Set(_ context.Context, _ string, _ roomkeystore.RoomKeyPair) (int, error) {
	return 0, nil
}

func (stubRoomKeyStore) Rotate(_ context.Context, _ string, _ roomkeystore.RoomKeyPair) (int, error) {
	return 1, nil
}

// testKeyStore and testKeySender provide the default wiring used by tests that
// don't override key behavior. See stubRoomKeyStore above.
var (
	testKeyStore  RoomKeyStore          = stubRoomKeyStore{}
	testKeySender *roomkeysender.Sender = roomkeysender.NewSender(&mockPublisher{})
)
