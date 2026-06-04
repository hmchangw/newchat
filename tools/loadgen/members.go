package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roomkeystore"
)

// Shape selects what an add-member request carries: individual users, orgs,
// channel-refs, or a mix. v1 implements ShapeUsers; the other values exist so
// the flag is forward-compatible.
type Shape string

const (
	ShapeUsers    Shape = "users"
	ShapeOrgs     Shape = "orgs"
	ShapeChannels Shape = "channels"
	ShapeMixed    Shape = "mixed"
)

// ParseShape converts a CLI flag value to a Shape.
func ParseShape(s string) (Shape, error) {
	switch Shape(s) {
	case ShapeUsers, ShapeOrgs, ShapeChannels, ShapeMixed:
		return Shape(s), nil
	default:
		return "", fmt.Errorf("unknown shape %q (want users|orgs|channels|mixed)", s)
	}
}

// MembersPreset is a fully-deterministic spec for the members workload.
type MembersPreset struct {
	Name          string
	Users         int // global user pool
	Rooms         int // rooms to seed
	BaselineSize  int // members per room at seed time (incl. owner)
	CandidatePool int // unused-but-eligible users tagged per room
}

var builtinMembersPresets = map[string]MembersPreset{
	"members-small": {
		Name: "members-small", Users: 200, Rooms: 5,
		BaselineSize: 10, CandidatePool: 50,
	},
	"members-medium": {
		Name: "members-medium", Users: 5000, Rooms: 100,
		BaselineSize: 100, CandidatePool: 900, // baseline+pool = 1000 = MAX_ROOM_SIZE
	},
	"members-capacity": {
		Name: "members-capacity", Users: 12000, Rooms: 5,
		BaselineSize: 1, CandidatePool: 990, // fits under MAX_ROOM_SIZE=1000
	},
	"members-heavy": {
		Name: "members-heavy", Users: 5000, Rooms: 700,
		BaselineSize: 10, CandidatePool: 990, // 700 × ⌊990/10⌋ = 69,300 ops ≈ 69s at 1000/s
	},
}

// BuiltinMembersPreset looks up a preset by name.
func BuiltinMembersPreset(name string) (MembersPreset, bool) {
	p, ok := builtinMembersPresets[name]
	return p, ok
}

// CandidatePools maps roomID to the list of accounts eligible to be added to
// that room (i.e., users not already seeded as members). Each generator pops
// from this list to build add-member requests.
type CandidatePools map[string][]string

// BuildMembersFixtures is a pure function of (preset, seed, siteID) producing
// the full members-workload fixture set plus per-room candidate pools.
// Two calls with equal inputs produce equal outputs.
func BuildMembersFixtures(p *MembersPreset, seed int64, siteID string) (Fixtures, CandidatePools) {
	r := rand.New(rand.NewSource(seed))
	now := time.Unix(0, 0).UTC()

	users := make([]model.User, p.Users)
	for i := 0; i < p.Users; i++ {
		users[i] = model.User{
			ID:          fmt.Sprintf("u-%06d", i),
			Account:     fmt.Sprintf("user-%d", i),
			SiteID:      siteID,
			EngName:     engNameBank[i%len(engNameBank)],
			ChineseName: chineseNameBank[i%len(chineseNameBank)],
		}
	}

	rooms := make([]model.Room, p.Rooms)
	for i := 0; i < p.Rooms; i++ {
		rooms[i] = model.Room{
			ID:        fmt.Sprintf("mroom-%06d", i),
			Name:      fmt.Sprintf("mroom-%d", i),
			Type:      model.RoomTypeChannel,
			SiteID:    siteID,
			UserCount: p.BaselineSize,
			CreatedAt: now,
			UpdatedAt: now,
		}
	}

	var subs []model.Subscription
	pools := make(CandidatePools, len(rooms))
	for i := range rooms {
		perm := r.Perm(len(users))
		need := p.BaselineSize + p.CandidatePool
		if need > len(perm) {
			need = len(perm)
		}
		chosen := perm[:need]
		memberSlice := chosen[:p.BaselineSize]
		candidateSlice := chosen[p.BaselineSize:need]

		for j, idx := range memberSlice {
			roles := []model.Role{model.RoleMember}
			if j == 0 {
				roles = []model.Role{model.RoleOwner}
			}
			subs = append(subs, model.Subscription{
				ID:       fmt.Sprintf("sub-%s-%s", rooms[i].ID, users[idx].ID),
				User:     model.SubscriptionUser{ID: users[idx].ID, Account: users[idx].Account},
				RoomID:   rooms[i].ID,
				SiteID:   siteID,
				Roles:    roles,
				JoinedAt: now,
			})
		}

		candidates := make([]string, len(candidateSlice))
		for k, idx := range candidateSlice {
			candidates[k] = users[idx].Account
		}
		pools[rooms[i].ID] = candidates
	}

	roomKeys := make(map[string]roomkeystore.RoomKeyPair, len(rooms))
	for i := range rooms {
		roomKeys[rooms[i].ID] = deterministicRoomKeyPair(r)
	}

	return Fixtures{
		Users:         users,
		Rooms:         rooms,
		Subscriptions: subs,
		RoomKeys:      roomKeys,
	}, pools
}

// OwnersByRoom returns a roomID -> owner account map from the fixture's
// subscription set. Used by the publisher to address frontdoor requests as
// "the owner asking room-service to add new members".
func OwnersByRoom(f *Fixtures) map[string]string {
	owners := make(map[string]string, len(f.Rooms))
	for i := range f.Subscriptions {
		s := &f.Subscriptions[i]
		for _, r := range s.Roles {
			if r == model.RoleOwner {
				owners[s.RoomID] = s.User.Account
				break
			}
		}
	}
	return owners
}

// ValidateInjectShape enforces compatibility between --inject and --shape.
// canonical+channels is explicitly rejected (room-service owns channel
// expansion); v1 also rejects everything except shape=users until the
// org/channel pre-resolution work in v2.
func ValidateInjectShape(inject InjectMode, shape Shape) error {
	if inject == InjectCanonical && shape == ShapeChannels {
		return fmt.Errorf("--shape=channels incompatible with --inject=canonical (channel expansion lives in room-service)")
	}
	if shape != ShapeUsers {
		return fmt.Errorf("--shape=%s not supported (only shape=users is implemented)", shape)
	}
	return nil
}
