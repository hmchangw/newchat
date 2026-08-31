package main

import (
	"fmt"
	"math/rand" // #nosec G404 -- load generator randomness, never used for secrets // nosemgrep: math-random-used
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roomkeystore"
)

// BotRoomPreset is a fully-deterministic spec for the large-room bot workload.
// It seeds RoomsPerSize channel rooms at each size in Sizes, each owned by a
// single bot account that also acts as the message sender.
type BotRoomPreset struct {
	Name         string
	Users        int   // shared member pool (excludes the bot)
	Sizes        []int // room sizes to seed
	RoomsPerSize int   // rooms seeded per size
	BotAccount   string
}

// BotRoomLayout is the in-memory map from size to the seeded room IDs of that
// size, derived deterministically from (preset, seed). Not persisted.
type BotRoomLayout struct {
	Sizes       []int
	RoomsBySize map[int][]string
	BotAccount  string
}

var builtinBotRoomPresets = map[string]BotRoomPreset{
	"botroom-small": {
		Name: "botroom-small", Users: 300, RoomsPerSize: 4,
		Sizes: []int{50, 100, 200}, BotAccount: "bot-0",
	},
	"botroom-medium": {
		Name: "botroom-medium", Users: 5500, RoomsPerSize: 4,
		Sizes: []int{100, 500, 1000, 2000, 5000}, BotAccount: "bot-0",
	},
}

// BuiltinBotRoomPreset looks up a preset by name.
func BuiltinBotRoomPreset(name string) (BotRoomPreset, bool) {
	p, ok := builtinBotRoomPresets[name]
	return p, ok
}

// BuildBotRoomFixtures is a pure function of (preset, seed, siteID) producing
// the full botroom fixture set plus the size→roomIDs layout. Equal inputs
// produce equal outputs.
func BuildBotRoomFixtures(p *BotRoomPreset, seed int64, siteID string) (Fixtures, BotRoomLayout) {
	r := rand.New(rand.NewSource(seed))
	now := time.Unix(0, 0).UTC()

	// Bot sits at users[0] and is excluded from the member pool (users[1:]).
	bot := model.User{
		ID:      "b-000000",
		Account: p.BotAccount,
		SiteID:  siteID,
		EngName: "bot", ChineseName: "bot",
	}
	users := make([]model.User, p.Users+1)
	users[0] = bot
	for i := 0; i < p.Users; i++ {
		users[i+1] = model.User{
			ID:      fmt.Sprintf("u-%06d", i),
			Account: fmt.Sprintf("user-%d", i),
			SiteID:  siteID,
			EngName: "member", ChineseName: "member",
		}
	}
	memberPool := users[1:]

	var rooms []model.Room
	var subs []model.Subscription
	roomKeys := make(map[string]roomkeystore.RoomKeyPair)
	layout := BotRoomLayout{
		Sizes:       append([]int(nil), p.Sizes...),
		RoomsBySize: make(map[int][]string, len(p.Sizes)),
		BotAccount:  p.BotAccount,
	}

	for _, size := range p.Sizes {
		for ri := 0; ri < p.RoomsPerSize; ri++ {
			roomID := fmt.Sprintf("broom-s%d-r%d", size, ri)
			rooms = append(rooms, model.Room{
				ID:        roomID,
				Name:      roomID,
				Type:      model.RoomTypeChannel,
				SiteID:    siteID,
				UserCount: size,
				CreatedAt: now,
				UpdatedAt: now,
			})
			layout.RoomsBySize[size] = append(layout.RoomsBySize[size], roomID)

			// Bot owner subscription first.
			subs = append(subs, model.Subscription{
				ID:       fmt.Sprintf("sub-%s-%s", roomID, bot.ID),
				User:     model.SubscriptionUser{ID: bot.ID, Account: bot.Account},
				RoomID:   roomID,
				SiteID:   siteID,
				Roles:    []model.Role{model.RoleOwner},
				JoinedAt: now,
			})
			// size-1 members drawn from the shared pool.
			perm := r.Perm(len(memberPool))
			need := size - 1
			// UserCount on the room records the target size; if a custom preset's pool is smaller than the target, the room simply gets fewer member subscriptions (consistent with buildBandedFixtures).
			if need > len(perm) {
				need = len(perm)
			}
			for _, idx := range perm[:need] {
				m := memberPool[idx]
				subs = append(subs, model.Subscription{
					ID:       fmt.Sprintf("sub-%s-%s", roomID, m.ID),
					User:     model.SubscriptionUser{ID: m.ID, Account: m.Account},
					RoomID:   roomID,
					SiteID:   siteID,
					Roles:    []model.Role{model.RoleUser},
					JoinedAt: now,
				})
			}
			roomKeys[roomID] = deterministicRoomKeyPair(r)
		}
	}

	return Fixtures{Users: users, Rooms: rooms, Subscriptions: subs, RoomKeys: roomKeys}, layout
}

// botRoomRoomIDs flattens the layout's per-size room IDs into one slice.
func botRoomRoomIDs(l *BotRoomLayout) []string {
	var out []string
	for _, size := range l.Sizes {
		out = append(out, l.RoomsBySize[size]...)
	}
	return out
}
