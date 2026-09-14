package topology

import (
	"fmt"
	"math"
	"math/rand" // #nosec G404 -- load generator randomness, never used for secrets // nosemgrep: math-random-used
	"time"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

const MaxBorrowedUsers = 20000

type Topology struct {
	BorrowedUsers []model.User
	ActiveUsers   []model.User
	Rooms         []model.Room
	Subscriptions []model.Subscription
}

type BuildConfig struct {
	RunID          string
	MaxUsers       int
	ActiveUsers    int
	RoomCount      int
	ChannelRatio   float64
	ChannelMembers int
}

// IdentitySource isolates random project identity generation from deterministic
// topology selection. Tests inject a repeatable sequence; production uses
// pkg/idgen so persisted entities follow the repository identity contract.
type IdentitySource struct {
	NewChannelRoomID  func() string
	NewSubscriptionID func() string
}

func NewProductionIdentitySource() *IdentitySource {
	return &IdentitySource{
		NewChannelRoomID:  idgen.GenerateID,
		NewSubscriptionID: idgen.GenerateUUIDv7,
	}
}

func eligibleUsers(users []model.User, siteID string) []model.User {
	eligible := make([]model.User, 0, len(users))
	for i := range users {
		user := &users[i]
		if user.ID == "" ||
			user.SiteID != siteID ||
			!user.IsActive() ||
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

func selectUsers(
	users []model.User,
	siteID string,
	maxUsers int,
	activeUsers int,
	seed int64,
) ([]model.User, []model.User, error) {
	if maxUsers <= 0 || maxUsers > MaxBorrowedUsers {
		return nil, nil, fmt.Errorf("max borrowed users must be between 1 and %d", MaxBorrowedUsers)
	}
	if activeUsers <= 0 || activeUsers > maxUsers {
		return nil, nil, fmt.Errorf("active users must be between 1 and max borrowed users")
	}

	eligible := eligibleUsers(users, siteID)
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

func Build(
	users []model.User,
	cfg *BuildConfig,
	siteID string,
	seed int64,
	ids *IdentitySource,
) (Topology, error) {
	if ids == nil || ids.NewChannelRoomID == nil || ids.NewSubscriptionID == nil {
		return Topology{}, fmt.Errorf("soak identity generator is required")
	}
	borrowed, active, err := selectUsers(users, siteID, cfg.MaxUsers, cfg.ActiveUsers, seed)
	if err != nil {
		return Topology{}, fmt.Errorf("select soak users: %w", err)
	}

	channelCount := int(math.Round(float64(cfg.RoomCount) * cfg.ChannelRatio))
	channelCount = max(0, min(channelCount, cfg.RoomCount))
	dmCount := cfg.RoomCount - channelCount
	inactiveBorrowed := len(borrowed) - len(active)
	maxDMPairs := len(active)*(len(active)-1)/2 +
		len(active)*inactiveBorrowed
	if dmCount > maxDMPairs {
		return Topology{}, fmt.Errorf("requested %d DM rooms but only %d unique DM pairs are available", dmCount, maxDMPairs)
	}
	channelMembers := min(cfg.ChannelMembers, len(borrowed))
	if channelCount*channelMembers+dmCount*2 < len(active) {
		return Topology{}, fmt.Errorf(
			"room membership capacity=%d cannot cover %d active users",
			channelCount*channelMembers+dmCount*2,
			len(active),
		)
	}

	topology := Topology{
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
		anchor := active[roomIndex%len(active)]
		members = append(members, anchor)
		memberIDs[anchor.ID] = struct{}{}
		covered[anchor.ID] = true
		for len(members) < channelMembers && activeCursor < len(active) {
			user := active[activeCursor]
			activeCursor++
			if _, exists := memberIDs[user.ID]; exists {
				continue
			}
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
			ID:        ids.NewChannelRoomID(),
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
			buildSubscriptions(&room, members, ids, createdAt)...,
		)
	}

	usedPairs := make(map[string]struct{}, dmCount)
	for dmIndex := range dmCount {
		a, b, ok := nextSoakDMPair(
			active,
			borrowed,
			covered,
			usedPairs,
			dmIndex,
		)
		if !ok {
			return Topology{}, fmt.Errorf("find unique DM pair")
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
			buildSubscriptions(&room, []model.User{a, b}, ids, createdAt)...,
		)
		covered[a.ID] = true
		covered[b.ID] = true
	}

	for i := range active {
		if !covered[active[i].ID] {
			return Topology{}, fmt.Errorf("active user %q has no writable room", active[i].ID)
		}
	}
	return topology, nil
}

func nextSoakDMPair(
	active []model.User,
	borrowed []model.User,
	covered map[string]bool,
	used map[string]struct{},
	sequence int,
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
		if partner, ok := nextSoakDMPartner(
			&active[i],
			borrowed,
			used,
			sequence+i,
		); ok {
			return active[i], partner, true
		}
	}

	for offset := range active {
		index := (sequence + offset) % len(active)
		if partner, ok := nextSoakDMPartner(
			&active[index],
			borrowed,
			used,
			sequence+offset,
		); ok {
			return active[index], partner, true
		}
	}
	return model.User{}, model.User{}, false
}

func nextSoakDMPartner(
	active *model.User,
	borrowed []model.User,
	used map[string]struct{},
	sequence int,
) (model.User, bool) {
	for offset := range borrowed {
		index := (sequence + offset) % len(borrowed)
		if reserveSoakPair(active, &borrowed[index], used) {
			return borrowed[index], true
		}
	}
	return model.User{}, false
}

func ActiveUserIDs(topology *Topology) map[string]struct{} {
	if topology == nil {
		return nil
	}
	active := make(map[string]struct{}, len(topology.ActiveUsers))
	for i := range topology.ActiveUsers {
		if topology.ActiveUsers[i].ID != "" {
			active[topology.ActiveUsers[i].ID] = struct{}{}
		}
	}
	return active
}

func IsActiveSubscription(
	subscription *model.Subscription,
	active map[string]struct{},
) bool {
	if !IsRoomMember(subscription) {
		return false
	}
	if len(active) == 0 {
		return true
	}
	_, ok := active[subscription.User.ID]
	return ok
}

// IsRoomMember follows the production subscription model: channel and DM
// membership is represented by row existence because leave deletes the row.
// Only an app room (a botDM facing a ".bot" counterpart) keeps the row and uses
// IsSubscribed as a soft toggle.
func IsRoomMember(subscription *model.Subscription) bool {
	if subscription == nil {
		return false
	}
	return !model.IsAppRoom(subscription.RoomType, subscription.Name) || subscription.IsSubscribed
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

func buildSubscriptions(
	room *model.Room,
	members []model.User,
	ids *IdentitySource,
	joinedAt time.Time,
) []model.Subscription {
	subscriptions := make([]model.Subscription, len(members))
	for i := range members {
		roles := []model.Role{model.RoleUser}
		name := room.Name
		if room.Type == model.RoomTypeChannel && i == 0 {
			roles = []model.Role{model.RoleOwner}
		}
		if room.Type == model.RoomTypeDM {
			name = members[(i+1)%len(members)].Account
		}
		subscriptions[i] = model.Subscription{
			ID: ids.NewSubscriptionID(),
			User: model.SubscriptionUser{
				ID:      members[i].ID,
				Account: members[i].Account,
			},
			RoomID:   room.ID,
			SiteID:   room.SiteID,
			Roles:    roles,
			Name:     name,
			RoomType: room.Type,
			Open:     true,
			JoinedAt: joinedAt,
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
