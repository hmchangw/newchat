package main

import (
	"github.com/hmchangw/chat/pkg/model"
	soaktopology "github.com/hmchangw/chat/tools/loadgen/internal/soak/topology"
)

// These aliases keep the existing runtime call sites stable while topology is
// extracted. They are removed after the remaining soak packages own their
// dependencies directly.
type soakTopology = soaktopology.Topology
type soakIDs = soaktopology.IdentitySource

func newProductionSoakIDs() *soakIDs {
	return soaktopology.NewProductionIdentitySource()
}

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

func activeSoakUserIDs(topology *soakTopology) map[string]struct{} {
	return soaktopology.ActiveUserIDs(topology)
}

func isActiveSoakSubscription(
	subscription *model.Subscription,
	active map[string]struct{},
) bool {
	return soaktopology.IsActiveSubscription(subscription, active)
}

func isSoakRoomMember(subscription *model.Subscription) bool {
	return soaktopology.IsRoomMember(subscription)
}
