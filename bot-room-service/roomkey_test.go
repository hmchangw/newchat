package main

import (
	"context"
	"errors"

	"github.com/hmchangw/chat/pkg/roomkeysender"
	"github.com/hmchangw/chat/pkg/roomkeystore"
)

// fakeKeyStore is a hand-written RoomKeyStore fake, consistent with this
// package's existing fakeStore/captureOutbox/fakeSysmsgPub test doubles
// (bot-room-service has no gomock/mockgen infrastructure).
type fakeKeyStore struct {
	SetFn            func(ctx context.Context, roomID string, pair roomkeystore.RoomKeyPair) (int, error)
	GetFn            func(ctx context.Context, roomID string) (*roomkeystore.VersionedKeyPair, error)
	RotateFn         func(ctx context.Context, roomID string, newPair roomkeystore.RoomKeyPair) (int, error)
	SetWithVersionFn func(ctx context.Context, roomID string, newPair roomkeystore.RoomKeyPair, version int) error
}

func (f *fakeKeyStore) Set(ctx context.Context, roomID string, pair roomkeystore.RoomKeyPair) (int, error) {
	if f.SetFn != nil {
		return f.SetFn(ctx, roomID, pair)
	}
	return 1, nil
}

func (f *fakeKeyStore) Get(ctx context.Context, roomID string) (*roomkeystore.VersionedKeyPair, error) {
	if f.GetFn != nil {
		return f.GetFn(ctx, roomID)
	}
	return &roomkeystore.VersionedKeyPair{
		Version: 1,
		KeyPair: roomkeystore.RoomKeyPair{PrivateKey: []byte("default-fake-key")},
	}, nil
}

func (f *fakeKeyStore) Rotate(ctx context.Context, roomID string, newPair roomkeystore.RoomKeyPair) (int, error) {
	if f.RotateFn != nil {
		return f.RotateFn(ctx, roomID, newPair)
	}
	return 2, nil
}

func (f *fakeKeyStore) SetWithVersion(ctx context.Context, roomID string, newPair roomkeystore.RoomKeyPair, version int) error {
	if f.SetWithVersionFn != nil {
		return f.SetWithVersionFn(ctx, roomID, newPair, version)
	}
	return nil
}

// fakePublisher captures roomkeysender publishes (subject + payload) for assertions.
type fakePublisher struct {
	subjects []string
	payloads [][]byte
}

func (p *fakePublisher) Publish(subj string, data []byte) error {
	p.subjects = append(p.subjects, subj)
	p.payloads = append(p.payloads, append([]byte(nil), data...))
	return nil
}

// failingPublisher always errors, for asserting that a key fan-out failure is
// logged but does not fail the outer create-room op.
type failingPublisher struct{ calls int }

func (p *failingPublisher) Publish(_ string, _ []byte) error {
	p.calls++
	return errors.New("publish failed")
}

// testKeyStore and testKeySender provide the default wiring used by tests
// that don't exercise room-key behavior directly.
var (
	testKeyStore  RoomKeyStore          = &fakeKeyStore{}
	testKeySender *roomkeysender.Sender = roomkeysender.NewSender(&fakePublisher{})
)
