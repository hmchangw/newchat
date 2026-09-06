package main

import (
	"github.com/hmchangw/chat/pkg/model"
	soaktopology "github.com/hmchangw/chat/tools/loadgen/internal/soak/topology"
)

// These aliases keep failure-aware room state and observation adapters stable.
// The production entry points compose topology directly; the aliases leave
// with the failure runtime in the follow-up PR.
type soakTopology = soaktopology.Topology
type soakIDs = soaktopology.IdentitySource

func buildSoakTopology(
	users []model.User,
	cfg *soakConfig,
	siteID string,
	seed int64,
	ids *soakIDs,
) (soakTopology, error) {
	return soaktopology.Build(users, &soaktopology.BuildConfig{
		RunID:          cfg.RunID,
		MaxUsers:       cfg.MaxUsers,
		ActiveUsers:    cfg.ActiveUsers,
		RoomCount:      cfg.RoomCount,
		ChannelRatio:   cfg.ChannelRatio,
		ChannelMembers: cfg.ChannelMembers,
	}, siteID, seed, ids)
}

func isSoakRoomMember(subscription *model.Subscription) bool {
	return soaktopology.IsRoomMember(subscription)
}
