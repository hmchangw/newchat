package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

// The lane fakes below only need identity — the test asserts which instance
// reached the handler, never what it does.
type laneParent struct{ ParentFetcher }
type laneBadge struct{ badgeClient }
type lanePresence struct{ PresenceSnapshotter }
type laneEmitter struct{ lane string }

func (e *laneEmitter) Emit(context.Context, model.PushNotificationEvent) error { return nil } //nolint:gocritic // hugeParam: the Emitter interface requires value semantics

func laneFor(name string) natsLane {
	return natsLane{
		Parent:   &laneParent{},
		Presence: &lanePresence{},
		Badge:    &laneBadge{},
		Emitter:  &laneEmitter{lane: name},
	}
}

// Every dependency that speaks over NATS has to be rebound per lane. The
// failover lane runs because this site's own NATS is unreachable: a lane built
// by copying the home deps and swapping only the emitter would still fetch its
// thread parent, its presence snapshot and its badge counts over the dead home
// connection, so each notification would stall for the RPC timeout and then be
// sent with the wrong content.
func TestNatsLane_Bind_ReplacesEveryConnectionBoundDep(t *testing.T) {
	home := laneFor("home")
	base := home.bind(&HandlerDeps{LargeRoomThreshold: 500, RecipientBatchSize: 7})

	buddy := laneFor("buddy")
	got := buddy.bind(&base)

	assert.Same(t, buddy.Parent, got.Parent, "thread-parent lookup must use the lane's own connection")
	assert.Same(t, buddy.Presence, got.Presence, "presence RPC must use the lane's own connection")
	assert.Same(t, buddy.Badge, got.BadgeClient, "badge-count RPC must use the lane's own connection")
	assert.Same(t, buddy.Emitter, got.Emitter, "push emit must use the lane's own connection")

	// The site-local dependencies and plain config are shared, not rebuilt:
	// Mongo and Valkey are still up when NATS is not.
	assert.Equal(t, base.LargeRoomThreshold, got.LargeRoomThreshold)
	assert.Equal(t, base.RecipientBatchSize, got.RecipientBatchSize)
}

// bind must not leave the previous lane's values in place for a dependency the
// caller deliberately disabled by env (nil badge client, nil settings source).
func TestNatsLane_Bind_CarriesNilDepsThrough(t *testing.T) {
	base := laneFor("home").bind(&HandlerDeps{})

	got := natsLane{}.bind(&base)

	require.Nil(t, got.Parent)
	require.Nil(t, got.Presence)
	require.Nil(t, got.BadgeClient)
	require.Nil(t, got.Emitter)
}
