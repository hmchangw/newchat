package main

import (
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

type soakTopology struct {
	BorrowedUsers []model.User
	ActiveUsers   []model.User
	Rooms         []model.Room
	Subscriptions []model.Subscription
}

// soakIDs isolates random project identity generation from deterministic
// topology selection. Tests inject a repeatable sequence; production uses
// pkg/idgen so persisted entities follow the repository identity contract.
type soakIDs struct {
	channelRoomID  func() string
	subscriptionID func() string
}

func newProductionSoakIDs() *soakIDs {
	return &soakIDs{
		channelRoomID:  idgen.GenerateID,
		subscriptionID: idgen.GenerateUUIDv7,
	}
}

func eligibleSoakUsers(users []model.User, siteID string) []model.User {
	eligible := make([]model.User, 0, len(users))
	for i := range users {
		user := &users[i]
		if user.ID == "" ||
			user.SiteID != siteID ||
			user.Deactivated ||
			!subject.IsValidAccountToken(user.Account) ||
			model.IsBot(user.Account) ||
			model.IsPlatformAdminAccount(user.Account) ||
			model.HasLoginRole(user.Roles) {
			continue
		}
		eligible = append(eligible, cloneSoakUser(user))
	}
	return eligible
}

func selectSoakUsers(
	users []model.User,
	siteID string,
	maxUsers int,
	activeUsers int,
	seed int64,
) ([]model.User, []model.User, error) {
	if maxUsers <= 0 || maxUsers > maxBorrowedSoakUsers {
		return nil, nil, fmt.Errorf("max borrowed users must be between 1 and %d", maxBorrowedSoakUsers)
	}
	if activeUsers <= 0 || activeUsers > maxUsers {
		return nil, nil, fmt.Errorf("active users must be between 1 and max borrowed users")
	}

	eligible := eligibleSoakUsers(users, siteID)
	if len(eligible) < activeUsers {
		return nil, nil, fmt.Errorf("active users requested=%d eligible=%d", activeUsers, len(eligible))
	}

	rng := rand.New(rand.NewSource(seed))
	rng.Shuffle(len(eligible), func(i, j int) {
		eligible[i], eligible[j] = eligible[j], eligible[i]
	})
	if len(eligible) > maxUsers {
		eligible = eligible[:maxUsers]
	}

	borrowed := cloneSoakUsers(eligible)
	active := cloneSoakUsers(eligible[:activeUsers])
	return borrowed, active, nil
}

func buildSoakTopology(
	users []model.User,
	cfg *soakConfig,
	siteID string,
	seed int64,
	ids *soakIDs,
) (soakTopology, error) {
	if ids == nil || ids.channelRoomID == nil || ids.subscriptionID == nil {
		return soakTopology{}, fmt.Errorf("soak identity generator is required")
	}
	borrowed, active, err := selectSoakUsers(users, siteID, cfg.MaxUsers, cfg.ActiveUsers, seed)
	if err != nil {
		return soakTopology{}, fmt.Errorf("select soak users: %w", err)
	}

	channelCount := int(math.Round(float64(cfg.RoomCount) * cfg.ChannelRatio))
	channelCount = max(0, min(channelCount, cfg.RoomCount))
	dmCount := cfg.RoomCount - channelCount
	maxDMPairs := len(borrowed) * (len(borrowed) - 1) / 2
	if dmCount > maxDMPairs {
		return soakTopology{}, fmt.Errorf("requested %d DM rooms but only %d unique DM pairs are available", dmCount, maxDMPairs)
	}
	channelMembers := min(cfg.ChannelMembers, len(borrowed))
	if channelCount*channelMembers+dmCount*2 < len(active) {
		return soakTopology{}, fmt.Errorf(
			"room membership capacity=%d cannot cover %d active users",
			channelCount*channelMembers+dmCount*2,
			len(active),
		)
	}

	topology := soakTopology{
		BorrowedUsers: borrowed,
		ActiveUsers:   active,
		Rooms:         make([]model.Room, 0, cfg.RoomCount),
	}
	createdAt := time.Unix(0, 0).UTC()
	covered := make(map[string]bool, len(active))
	activeCursor := 0
	fillCursor := 0

	for roomIndex := range channelCount {
		members := make([]model.User, 0, channelMembers)
		memberIDs := make(map[string]struct{}, channelMembers)
		for len(members) < channelMembers && activeCursor < len(active) {
			user := active[activeCursor]
			activeCursor++
			members = append(members, user)
			memberIDs[user.ID] = struct{}{}
			covered[user.ID] = true
		}
		for len(members) < channelMembers {
			user := borrowed[fillCursor%len(borrowed)]
			fillCursor++
			if _, exists := memberIDs[user.ID]; exists {
				continue
			}
			members = append(members, user)
			memberIDs[user.ID] = struct{}{}
			covered[user.ID] = true
		}

		room := model.Room{
			ID:        ids.channelRoomID(),
			Name:      fmt.Sprintf("soak-%s-channel-%06d", cfg.RunID, roomIndex),
			Type:      model.RoomTypeChannel,
			SiteID:    siteID,
			UserCount: len(members),
			CreatedAt: createdAt,
			UpdatedAt: createdAt,
		}
		topology.Rooms = append(topology.Rooms, room)
		topology.Subscriptions = append(
			topology.Subscriptions,
			buildSoakSubscriptions(&room, members, ids, createdAt)...,
		)
	}

	usedPairs := make(map[string]struct{}, dmCount)
	pairI, pairJ := 0, 1
	for range dmCount {
		a, b, ok := nextSoakDMPair(active, borrowed, covered, usedPairs, &pairI, &pairJ)
		if !ok {
			return soakTopology{}, fmt.Errorf("find unique DM pair")
		}
		roomID := idgen.BuildDMRoomID(a.ID, b.ID)
		uids, accounts := model.BuildDMParticipants(&a, &b)
		room := model.Room{
			ID:        roomID,
			Type:      model.RoomTypeDM,
			SiteID:    siteID,
			UserCount: 2,
			UIDs:      uids,
			Accounts:  accounts,
			CreatedAt: createdAt,
			UpdatedAt: createdAt,
		}
		topology.Rooms = append(topology.Rooms, room)
		topology.Subscriptions = append(
			topology.Subscriptions,
			buildSoakSubscriptions(&room, []model.User{a, b}, ids, createdAt)...,
		)
		covered[a.ID] = true
		covered[b.ID] = true
	}

	for i := range active {
		if !covered[active[i].ID] {
			return soakTopology{}, fmt.Errorf("active user %q has no writable room", active[i].ID)
		}
	}
	return topology, nil
}

func nextSoakDMPair(
	active []model.User,
	borrowed []model.User,
	covered map[string]bool,
	used map[string]struct{},
	pairI *int,
	pairJ *int,
) (model.User, model.User, bool) {
	for i := range active {
		if covered[active[i].ID] {
			continue
		}
		for j := i + 1; j < len(active); j++ {
			if covered[active[j].ID] {
				continue
			}
			if reserveSoakPair(&active[i], &active[j], used) {
				return active[i], active[j], true
			}
		}
		for offset := range borrowed {
			candidate := borrowed[(i+offset)%len(borrowed)]
			if reserveSoakPair(&active[i], &candidate, used) {
				return active[i], candidate, true
			}
		}
	}

	for *pairI < len(borrowed)-1 {
		if *pairJ >= len(borrowed) {
			(*pairI)++
			*pairJ = *pairI + 1
			continue
		}
		a, b := borrowed[*pairI], borrowed[*pairJ]
		(*pairJ)++
		if reserveSoakPair(&a, &b, used) {
			return a, b, true
		}
	}
	return model.User{}, model.User{}, false
}

func reserveSoakPair(a, b *model.User, used map[string]struct{}) bool {
	if a.ID == b.ID {
		return false
	}
	key := a.ID + "\x00" + b.ID
	if a.ID > b.ID {
		key = b.ID + "\x00" + a.ID
	}
	if _, exists := used[key]; exists {
		return false
	}
	used[key] = struct{}{}
	return true
}

func buildSoakSubscriptions(
	room *model.Room,
	members []model.User,
	ids *soakIDs,
	joinedAt time.Time,
) []model.Subscription {
	subscriptions := make([]model.Subscription, len(members))
	for i := range members {
		roles := []model.Role{model.RoleMember}
		name := room.Name
		if room.Type == model.RoomTypeChannel && i == 0 {
			roles = []model.Role{model.RoleOwner}
		}
		if room.Type == model.RoomTypeDM {
			name = members[(i+1)%len(members)].Account
		}
		subscriptions[i] = model.Subscription{
			ID: ids.subscriptionID(),
			User: model.SubscriptionUser{
				ID:      members[i].ID,
				Account: members[i].Account,
			},
			RoomID:       room.ID,
			SiteID:       room.SiteID,
			Roles:        roles,
			Name:         name,
			RoomType:     room.Type,
			IsSubscribed: true,
			JoinedAt:     joinedAt,
		}
	}
	return subscriptions
}

func cloneSoakUsers(users []model.User) []model.User {
	cloned := make([]model.User, len(users))
	for i := range users {
		cloned[i] = cloneSoakUser(&users[i])
	}
	return cloned
}

func cloneSoakUser(user *model.User) model.User {
	cloned := *user
	cloned.Roles = append([]model.UserRole(nil), user.Roles...)
	return cloned
}
