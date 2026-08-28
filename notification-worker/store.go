package main

import (
	"context"

	"github.com/hmchangw/chat/pkg/roomsubcache"
)

//go:generate mockgen -destination=mock_store_test.go -package=main . MemberStore,ThreadFollowerLister,UserSettingsSnapshotter

// MemberStore reads a room's canonical member list from the operational store.
// Consumed through cachedMemberLookup, which fronts it with Valkey and
// single-flight; this interface is the cold-miss path behind that cache.
type MemberStore interface {
	ListMembers(ctx context.Context, roomID string) ([]roomsubcache.Member, error)
}

// ThreadRoomInfo is the per-thread metadata read from thread_rooms in one query.
// The parent's createdAt is no longer read here — it comes authoritatively from
// history-service (see ParentFetcher), which is race-free on the first reply.
type ThreadRoomInfo struct {
	Followers map[string]struct{}
}

// ThreadFollowerLister reads thread metadata for the thread rooted at parentMessageID.
type ThreadFollowerLister interface {
	Lookup(ctx context.Context, parentMessageID string) (ThreadRoomInfo, error)
}

// UserSettingsSnapshotter batches notification-settings lookups for push-eligible
// accounts. Errors are swallowed; an absent account defaults to current behaviour.
type UserSettingsSnapshotter interface {
	Snapshot(ctx context.Context, accounts []string) (map[string]notifSettings, error)
}
