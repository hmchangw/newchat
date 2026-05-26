// Package main: fixture builders.
//
// All builders return pure data — no I/O, no clocks, no random sources —
// so unit tests can assert exact counts/IDs and so re-runs of `make seed`
// produce the same final state.
package main

import (
	"crypto/sha256"
	"time"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roomkeystore"
)

const (
	siteLocal  = "site-local"
	siteRemote = "site-remote"
)

// seedBaseTime is the wall-clock anchor every seeded timestamp derives
// from. Fixed so re-running the seeder does not drift `CreatedAt`,
// `JoinedAt`, `LastSeenAt`, or message timestamps.
var seedBaseTime = time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC)

// BuildUsers returns the seed roster. alice and bob match the Keycloak
// realm in auth-service/deploy/keycloak/realm-export.json; the rest
// populate rooms so member lists look realistic.
func BuildUsers() []model.User {
	return []model.User{
		{ID: "u-alice", Account: "alice", SiteID: siteLocal, SectID: "eng", SectName: "Engineering", SectTCName: "工程部", DeptID: "eng-backend", DeptName: "Backend", DeptTCName: "後端組", EngName: "Alice Engineer", ChineseName: "王小愛", EmployeeID: "E001"},
		{ID: "u-bob", Account: "bob", SiteID: siteLocal, SectID: "eng", SectName: "Engineering", SectTCName: "工程部", DeptID: "eng-frontend", DeptName: "Frontend", DeptTCName: "前端組", EngName: "Bob Developer", ChineseName: "陳大寶", EmployeeID: "E002"},
		{ID: "u-carol", Account: "carol", SiteID: siteLocal, SectID: "eng", SectName: "Engineering", SectTCName: "工程部", DeptID: "eng-backend", DeptName: "Backend", DeptTCName: "後端組", EngName: "Carol Coder", ChineseName: "林小卡", EmployeeID: "E003"},
		{ID: "u-dave", Account: "dave", SiteID: siteLocal, SectID: "prod", SectName: "Product", SectTCName: "產品部", DeptID: "prod-pm", DeptName: "Product Management", DeptTCName: "產品經理", EngName: "Dave PM", ChineseName: "張小達", EmployeeID: "E004"},
		{ID: "u-eve", Account: "eve", SiteID: siteLocal, SectID: "prod", SectName: "Product", SectTCName: "產品部", DeptID: "prod-pm", DeptName: "Product Management", DeptTCName: "產品經理", EngName: "Eve Manager", ChineseName: "黃小夜", EmployeeID: "E005"},
		{ID: "u-frank", Account: "frank", SiteID: siteLocal, SectID: "design", SectName: "Design", SectTCName: "設計部", DeptID: "design-ux", DeptName: "UX", DeptTCName: "使用者體驗", EngName: "Frank Designer", ChineseName: "吳小法", EmployeeID: "E006"},
		{ID: "u-grace", Account: "grace", SiteID: siteLocal, SectID: "design", SectName: "Design", SectTCName: "設計部", DeptID: "design-ux", DeptName: "UX", DeptTCName: "使用者體驗", EngName: "Grace UX", ChineseName: "蔡小恩", EmployeeID: "E007"},
		{ID: "u-heidi", Account: "heidi", SiteID: siteLocal, SectID: "ops", SectName: "Operations", SectTCName: "營運部", DeptID: "ops-sre", DeptName: "SRE", DeptTCName: "站點可靠性", EngName: "Heidi Ops", ChineseName: "周小海", EmployeeID: "E008"},
		{ID: "u-ivan", Account: "ivan", SiteID: siteRemote, SectID: "eng", SectName: "Engineering (Remote)", SectTCName: "工程部（遠端）", DeptID: "eng-backend", DeptName: "Backend", DeptTCName: "後端組", EngName: "Ivan Remote", ChineseName: "鄭小宜", EmployeeID: "R001"},
		{ID: "u-judy", Account: "judy", SiteID: siteRemote, SectID: "prod", SectName: "Product (Remote)", SectTCName: "產品部（遠端）", DeptID: "prod-pm", DeptName: "Product Management", DeptTCName: "產品經理", EngName: "Judy Cross", ChineseName: "高小朱", EmployeeID: "R002"},
	}
}

// usersByAccount indexes BuildUsers by account for fast lookup in other builders.
func usersByAccount() map[string]model.User {
	users := BuildUsers()
	out := make(map[string]model.User, len(users))
	for i := range users {
		out[users[i].Account] = users[i]
	}
	return out
}

// channelRoom builds a channel-type Room with deterministic created/updated
// timestamps and synthesized UIDs/Accounts from the supplied accounts.
// LastMsgAt and LastMsgID are populated by BuildRoomsWithLastMsg later;
// the bare BuildRooms returns rooms with zero LastMsg fields.
func channelRoom(id, name, siteID string, restricted bool, accounts []string) model.Room {
	users := usersByAccount()
	uids := make([]string, len(accounts))
	accs := make([]string, len(accounts))
	for i, a := range accounts {
		u, ok := users[a]
		if !ok {
			panic("seed-sample-data: channelRoom references unknown account " + a)
		}
		uids[i] = u.ID
		accs[i] = u.Account
	}
	return model.Room{
		ID:         id,
		Name:       name,
		Type:       model.RoomTypeChannel,
		SiteID:     siteID,
		UserCount:  len(uids),
		AppCount:   0,
		CreatedAt:  seedBaseTime,
		UpdatedAt:  seedBaseTime,
		Restricted: restricted,
		UIDs:       uids,
		Accounts:   accs,
	}
}

// dmRoom builds a DM-type Room. Uses idgen.BuildDMRoomID for the deterministic
// sorted-concat ID so DM lookups in handlers match.
func dmRoom(accountA, accountB string) model.Room {
	users := usersByAccount()
	a, b := users[accountA], users[accountB]
	uids, accs := model.BuildDMParticipants(&a, &b)
	return model.Room{
		ID:        idgen.BuildDMRoomID(a.ID, b.ID),
		Name:      "",
		Type:      model.RoomTypeDM,
		SiteID:    siteLocal,
		UserCount: 2,
		CreatedAt: seedBaseTime,
		UpdatedAt: seedBaseTime,
		UIDs:      uids,
		Accounts:  accs,
	}
}

// BuildRooms returns the seed room set: 3 local channels, 2 local DMs, 1 remote channel.
func BuildRooms() []model.Room {
	return []model.Room{
		channelRoom("r-general", "general", siteLocal, false,
			[]string{"alice", "bob", "carol", "dave", "eve", "frank", "grace", "heidi", "ivan"}),
		channelRoom("r-eng", "engineering", siteLocal, true,
			[]string{"alice", "bob", "carol", "ivan"}),
		channelRoom("r-design", "design", siteLocal, false,
			[]string{"frank", "grace", "dave"}),
		dmRoom("alice", "bob"),
		dmRoom("carol", "eve"),
		channelRoom("r-remote-announce", "remote-announce", siteRemote, false,
			[]string{"ivan", "judy", "alice"}),
	}
}

// roomsByID indexes BuildRooms for cross-collection lookups.
func roomsByID() map[string]model.Room {
	rooms := BuildRooms()
	out := make(map[string]model.Room, len(rooms))
	for i := range rooms {
		out[rooms[i].ID] = rooms[i]
	}
	return out
}

// messageStep is the time gap between consecutive seeded messages within
// a single room. Fixed at 5 minutes so timestamps are strictly monotonic
// and easy to reason about in tests.
const messageStep = 5 * time.Minute

// seedScript drives BuildMessages. Each entry produces one root message
// in roomID by accountAuthor at index n (n = position within that room
// in seedScript order); CreatedAt = rootStart + n*messageStep.
type seedMessage struct {
	roomID  string
	author  string
	content string
}

var seedScript = []seedMessage{
	{roomID: "r-general", author: "alice", content: "Hi everyone, welcome to general 👋"},
	{roomID: "r-general", author: "bob", content: "Good morning"},
	{roomID: "r-general", author: "dave", content: "Reminder: planning at 10"},
	{roomID: "r-general", author: "eve", content: "On it"},
	{roomID: "r-general", author: "heidi", content: "Ops update: zero incidents overnight."},

	{roomID: "r-eng", author: "carol", content: "Pushed the auth refactor; PR up."},
	{roomID: "r-eng", author: "alice", content: "Should we adopt UUIDv7 for all entity IDs?"},
	{roomID: "r-eng", author: "bob", content: "Quick reminder to rebase before merge."},
	{roomID: "r-eng", author: "ivan", content: "Remote side is green on the latest build."},

	{roomID: "r-design", author: "frank", content: "Posted v2 mocks in the design folder."},
	{roomID: "r-design", author: "grace", content: "Looks great — minor spacing notes inline."},
	{roomID: "r-design", author: "dave", content: "Thanks both, will share with stakeholders."},

	{roomID: dmIDOf("alice", "bob"), author: "alice", content: "Lunch?"},
	{roomID: dmIDOf("alice", "bob"), author: "bob", content: "Sure, 12:30"},
	{roomID: dmIDOf("alice", "bob"), author: "alice", content: "Perfect"},

	{roomID: dmIDOf("carol", "eve"), author: "carol", content: "Slides ready for the demo"},
	{roomID: dmIDOf("carol", "eve"), author: "eve", content: "Ship it"},

	{roomID: "r-remote-announce", author: "ivan", content: "Remote site weekly: highlights below."},
	{roomID: "r-remote-announce", author: "judy", content: "Product update: Q2 roadmap finalized."},
	{roomID: "r-remote-announce", author: "alice", content: "Thanks for the cross-site visibility."},
}

// dmIDOf returns the BuildDMRoomID for two seed accounts.
func dmIDOf(a, b string) string {
	users := usersByAccount()
	return idgen.BuildDMRoomID(users[a].ID, users[b].ID)
}

// threadParentIndex is the index in seedScript of the alice "UUIDv7?" message.
// Kept as a function so a slice reorder breaks the build, not silently behavior.
func threadParentIndex() int {
	for i, m := range seedScript {
		if m.roomID == "r-eng" && m.author == "alice" {
			return i
		}
	}
	panic("seed-sample-data: r-eng alice thread-parent message not found in seedScript")
}

// BuildMessages materializes every message (root + thread replies). Per-room
// timestamps step monotonically from seedBaseTime + 1h by messageStep.
// Message IDs are derived via idgen.MessageIDFromRequestID for idempotency.
func BuildMessages() []model.Message {
	users := usersByAccount()

	rootStart := seedBaseTime.Add(1 * time.Hour)
	perRoomIdx := map[string]int{}

	roots := make([]model.Message, 0, len(seedScript))
	for _, s := range seedScript {
		idx := perRoomIdx[s.roomID]
		perRoomIdx[s.roomID] = idx + 1
		author := users[s.author]
		id := idgen.MessageIDFromRequestID("seed:"+s.roomID, itoa(idx))
		roots = append(roots, model.Message{
			ID:          id,
			RoomID:      s.roomID,
			UserID:      author.ID,
			UserAccount: author.Account,
			Content:     s.content,
			CreatedAt:   rootStart.Add(time.Duration(idx) * messageStep),
		})
	}

	threadParentID := roots[threadParentIndex()].ID
	threadParentCreatedAt := roots[threadParentIndex()].CreatedAt

	threadReplies := []struct {
		author  string
		content string
	}{
		{"bob", "+1, current 32-char hex is fine but v7 sort-ability is nice."},
		{"carol", "Subscriptions already use v7; channel rooms are still base62."},
		{"bob", "Let's draft a migration note."},
	}
	replies := make([]model.Message, 0, len(threadReplies))
	engRootCount := perRoomIdx["r-eng"]
	for i, tr := range threadReplies {
		idx := engRootCount + i
		author := users[tr.author]
		id := idgen.MessageIDFromRequestID("seed:r-eng", itoa(idx))
		created := rootStart.Add(time.Duration(idx) * messageStep)
		replies = append(replies, model.Message{
			ID:                           id,
			RoomID:                       "r-eng",
			UserID:                       author.ID,
			UserAccount:                  author.Account,
			Content:                      tr.content,
			CreatedAt:                    created,
			ThreadParentMessageID:        threadParentID,
			ThreadParentMessageCreatedAt: ptrTime(threadParentCreatedAt),
			TShow:                        false,
		})
	}

	return append(roots, replies...)
}

func itoa(i int) string {
	const digits = "0123456789"
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{digits[i%10]}, b...)
		i /= 10
	}
	return string(b)
}

func ptrTime(t time.Time) *time.Time { return &t }

// BuildRoomsWithLastMsg returns BuildRooms() but with each room's
// LastMsgAt/LastMsgID populated from the latest message in that room.
// This is what mongo.go upserts into the rooms collection — not the bare
// BuildRooms output, which carries zero LastMsg fields.
func BuildRoomsWithLastMsg() []model.Room {
	msgs := BuildMessages()
	latest := map[string]model.Message{}
	for i := range msgs {
		l, ok := latest[msgs[i].RoomID]
		if !ok || msgs[i].CreatedAt.After(l.CreatedAt) {
			latest[msgs[i].RoomID] = msgs[i]
		}
	}
	rooms := BuildRooms()
	for i := range rooms {
		if m, ok := latest[rooms[i].ID]; ok {
			created := m.CreatedAt
			rooms[i].LastMsgAt = &created
			rooms[i].LastMsgID = m.ID
		}
	}
	return rooms
}

// threadParentMessage returns the (id, createdAt) of the alice "UUIDv7?"
// message in r-eng. Centralized so BuildThreadRooms and BuildMessages
// stay in lockstep.
func threadParentMessage() (id string, createdAt time.Time) {
	msgs := BuildMessages()
	for i := range msgs {
		if msgs[i].RoomID == "r-eng" && msgs[i].UserAccount == "alice" && msgs[i].ThreadParentMessageID == "" {
			return msgs[i].ID, msgs[i].CreatedAt
		}
	}
	panic("seed-sample-data: r-eng alice thread-parent message not found in BuildMessages")
}

// threadLastMessage returns the (id, createdAt) of the latest thread reply
// in r-eng — used to populate ThreadRoom.LastMsg{ID,At}.
func threadLastMessage() (id string, createdAt time.Time) {
	msgs := BuildMessages()
	var latest model.Message
	for i := range msgs {
		if msgs[i].ThreadParentMessageID == "" || msgs[i].RoomID != "r-eng" {
			continue
		}
		if latest.ID == "" || msgs[i].CreatedAt.After(latest.CreatedAt) {
			latest = msgs[i]
		}
	}
	if latest.ID == "" {
		panic("seed-sample-data: no thread replies found in BuildMessages")
	}
	return latest.ID, latest.CreatedAt
}

// BuildThreadRooms returns the seed thread set: one entry under the
// alice "UUIDv7?" message in r-eng with bob + carol as repliers.
func BuildThreadRooms() []model.ThreadRoom {
	parentID, parentCreated := threadParentMessage()
	lastID, lastAt := threadLastMessage()
	return []model.ThreadRoom{
		{
			ID:                    "tr-uuidv7-debate",
			ParentMessageID:       parentID,
			ThreadParentCreatedAt: parentCreated,
			RoomID:                "r-eng",
			SiteID:                siteLocal,
			LastMsgAt:             lastAt,
			LastMsgID:             lastID,
			ReplyAccounts:         []string{"bob", "carol"},
			CreatedAt:             seedBaseTime,
			UpdatedAt:             seedBaseTime,
		},
	}
}

// BuildThreadSubscriptions returns the seed thread subscriptions for bob
// and carol on the single seeded thread.
func BuildThreadSubscriptions() []model.ThreadSubscription {
	users := usersByAccount()
	parentID, _ := threadParentMessage()
	out := make([]model.ThreadSubscription, 0, 2)
	for _, account := range []string{"bob", "carol"} {
		u := users[account]
		out = append(out, model.ThreadSubscription{
			ID:              "tsub:" + u.ID + ":tr-uuidv7-debate",
			ParentMessageID: parentID,
			RoomID:          "r-eng",
			ThreadRoomID:    "tr-uuidv7-debate",
			UserID:          u.ID,
			UserAccount:     u.Account,
			SiteID:          siteLocal,
			LastSeenAt:      nil,
			HasMention:      false,
			CreatedAt:       seedBaseTime,
			UpdatedAt:       seedBaseTime,
		})
	}
	return out
}

// roomOwners maps roomID -> account of the seeded owner.
// Used by BuildSubscriptions to assign RoleOwner.
var roomOwners = map[string]string{
	"r-general":         "alice",
	"r-eng":             "alice",
	"r-design":          "frank",
	"r-remote-announce": "ivan",
}

// BuildSubscriptions returns one Subscription per (user, room) the user
// is a member of. DM subscriptions are emitted as plain Subscription rows
// with Name set to the counterpart's account (matches how dm rooms are
// labeled on the client). Roles are ["owner"] for the seeded owner of
// each channel and ["member"] otherwise; DM members get ["member"].
func BuildSubscriptions() []model.Subscription {
	users := usersByAccount()
	rooms := BuildRooms()
	out := make([]model.Subscription, 0, 23)
	for ri := range rooms {
		r := &rooms[ri]
		for i, account := range r.Accounts {
			u := users[account]
			roles := []model.Role{model.RoleMember}
			if owner, ok := roomOwners[r.ID]; ok && owner == account {
				roles = []model.Role{model.RoleOwner}
			}
			name := r.Name
			if r.Type == model.RoomTypeDM {
				other := r.Accounts[1-i]
				name = other
			}
			out = append(out, model.Subscription{
				ID:           "sub:" + u.ID + ":" + r.ID,
				User:         model.SubscriptionUser{ID: u.ID, Account: u.Account, IsBot: false},
				RoomID:       r.ID,
				SiteID:       r.SiteID,
				Roles:        roles,
				Name:         name,
				RoomType:     r.Type,
				IsSubscribed: true,
				JoinedAt:     seedBaseTime,
				HasMention:   false,
				Alert:        true,
				Muted:        false,
			})
		}
	}
	return out
}

// BuildRoomMembers returns one RoomMember per (channel, user) pair across
// all four channel rooms. DM rooms are intentionally excluded — room-service
// derives DM membership from subscriptions when no room_members document
// exists for the room (see room-service/store_mongo.go:329).
func BuildRoomMembers() []model.RoomMember {
	users := usersByAccount()
	rooms := BuildRooms()
	out := make([]model.RoomMember, 0, 19)
	for ri := range rooms {
		r := &rooms[ri]
		if r.Type != model.RoomTypeChannel {
			continue
		}
		for _, account := range r.Accounts {
			u := users[account]
			out = append(out, model.RoomMember{
				ID:     r.ID + ":" + u.ID,
				RoomID: r.ID,
				Ts:     seedBaseTime,
				Member: model.RoomMemberEntry{
					ID:      u.ID,
					Type:    model.RoomMemberIndividual,
					Account: u.Account,
				},
			})
		}
	}
	return out
}

// RoomKeyEntry pairs a room ID with the 32-byte secret to write at
// room:{roomID}:key. The seeder derives the secret from sha256("seed-room-key:" + roomID)
// so re-runs always produce the same key material.
type RoomKeyEntry struct {
	RoomID  string
	KeyPair roomkeystore.RoomKeyPair
}

// BuildRoomKeys returns one stable 32-byte room key per seeded room.
// Order matches BuildRooms.
func BuildRoomKeys() []RoomKeyEntry {
	rooms := BuildRooms()
	out := make([]RoomKeyEntry, 0, len(rooms))
	for i := range rooms {
		sum := sha256.Sum256([]byte("seed-room-key:" + rooms[i].ID))
		key := make([]byte, 32)
		copy(key, sum[:])
		out = append(out, RoomKeyEntry{
			RoomID:  rooms[i].ID,
			KeyPair: roomkeystore.RoomKeyPair{PrivateKey: key},
		})
	}
	return out
}

// RestrictedCacheEntry is a single (account, rooms) entry to write at
// searchservice:restrictedrooms:<account>.
type RestrictedCacheEntry struct {
	Account string
	Rooms   map[string]int64
}

// BuildRestrictedCache emits one entry per member of every Restricted
// room in BuildRooms. Currently that's just r-eng (members: alice, bob,
// carol, ivan), so this returns four entries. The join timestamp is
// seedBaseTime in unix-millis so the cache content is deterministic.
func BuildRestrictedCache() []RestrictedCacheEntry {
	joinMs := seedBaseTime.UnixMilli()

	rooms := BuildRooms()
	byAccount := map[string]map[string]int64{}
	for i := range rooms {
		r := &rooms[i]
		if !r.Restricted {
			continue
		}
		for _, account := range r.Accounts {
			if _, ok := byAccount[account]; !ok {
				byAccount[account] = map[string]int64{}
			}
			byAccount[account][r.ID] = joinMs
		}
	}

	out := make([]RestrictedCacheEntry, 0, len(byAccount))
	users := BuildUsers()
	for i := range users {
		rooms, ok := byAccount[users[i].Account]
		if !ok {
			continue
		}
		out = append(out, RestrictedCacheEntry{Account: users[i].Account, Rooms: rooms})
	}
	return out
}
