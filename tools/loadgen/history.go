package main

import (
	"fmt"
	"math/rand" // #nosec G404 -- load generator randomness, never used for secrets // nosemgrep: math-random-used
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roomkeystore"
)

// HistoryPreset is a fully-deterministic spec for the history-read workload.
type HistoryPreset struct {
	Name             string
	Users            int     // global user pool
	Rooms            int     // rooms to seed
	BaselineSize     int     // members per room
	MessagesPerRoom  int     // top-level messages per room (thread replies are additional)
	MessageSpanDays  int     // span from now back over which to spread messages
	ThreadRate       float64 // 0..1 fraction of top-level messages that become thread parents
	RepliesPerThread int     // replies per thread parent
	ContentBytes     int
}

var builtinHistoryPresets = map[string]HistoryPreset{
	"history-small": {
		Name: "history-small", Users: 50, Rooms: 5, BaselineSize: 10,
		MessagesPerRoom: 100, MessageSpanDays: 1, ThreadRate: 0.0,
		RepliesPerThread: 0, ContentBytes: 200,
	},
	"history-medium": {
		Name: "history-medium", Users: 1000, Rooms: 100, BaselineSize: 20,
		MessagesPerRoom: 5000, MessageSpanDays: 7, ThreadRate: 0.05,
		RepliesPerThread: 3, ContentBytes: 200,
	},
	"history-large": {
		Name: "history-large", Users: 10000, Rooms: 1000, BaselineSize: 30,
		MessagesPerRoom: 50000, MessageSpanDays: 30, ThreadRate: 0.10,
		RepliesPerThread: 3, ContentBytes: 200,
	},
}

// BuiltinHistoryPreset looks up a preset by name.
func BuiltinHistoryPreset(name string) (HistoryPreset, bool) {
	p, ok := builtinHistoryPresets[name]
	return p, ok
}

// ThreadParentRef identifies a thread-parent message in a room. The generator
// uses this to pick valid threadMessageId values for GetThreadMessages requests.
type ThreadParentRef struct {
	MessageID    string
	ThreadRoomID string
}

// plannedMessage is one row queued for Cassandra write.
// Thread replies have ThreadParentID != "" and ThreadRoomID == parent's
// ThreadRoomID. Thread parents (top-level messages with replies) have
// ThreadRoomID set and ThreadParentID == "". Plain top-level messages have
// both fields empty.
type plannedMessage struct {
	RoomID         string
	MessageID      string
	SenderID       string
	SenderAccount  string
	SenderEngName  string
	Content        string
	CreatedAt      time.Time
	ThreadRoomID   string // parent's thread room (set on both parent and replies)
	ThreadParentID string // empty for top-level; parent message ID for replies
	TCount         int    // parent-only: number of replies (0 for plain top-level + replies)
}

// MessagePlan is the deterministic schedule of every message the seeder will
// write. Includes top-level messages and thread replies. Ordering is
// (room, asc by CreatedAt) — except FullPlan concatenates rooms in fixture
// order so callers that need cross-room ordering must re-sort.
type MessagePlan struct {
	Messages []plannedMessage
}

// roomSeed splits per-room randomness into two independent streams so a
// metadata-only walk can stay aligned with the full walk without paying the
// O(MessagesPerRoom × ContentBytes) cost of regenerating content.
//
// structural drives sender picks, CreatedAt jitter, thread-parent permutation,
// and reply offsets/senders. content drives only the message body bytes.
type roomSeed struct {
	structural int64
	content    int64
}

// HistoryFixtures bundles every artifact a history-workload seed produces.
// Plan is intentionally absent: on the history-large preset the full plan is
// ~50 GB. Stream via IterateRoomMessages, or materialize via FullPlan for
// small/medium presets where the cost is bounded.
type HistoryFixtures struct {
	Fixtures      Fixtures
	ThreadParents map[string][]ThreadParentRef // roomID -> parents, in room order

	// Iterator state. roomIDs/membersByRoom/roomSeeds are parallel-indexed.
	preset        *HistoryPreset
	roomIDs       []string
	membersByRoom [][]model.User
	roomSeeds     []roomSeed
	now           time.Time
}

// BuildHistoryFixtures is a pure function of (preset, seed, siteID, now)
// producing the fixture set + iterator state. `now` is the wall-clock anchor
// used for message timestamps: timestamps are anchored to now so the
// history-service floor doesn't clip them, but user/room/subscription identity
// remains deterministic on seed.
//
// The returned fixtures DO NOT contain the message plan in memory. Use
// IterateRoomMessages(fn) to stream per-room plans, or FullPlan() to
// materialize the full plan (bounded only by the preset size — DO NOT call on
// history-large).
func BuildHistoryFixtures(p *HistoryPreset, seed int64, siteID string, now time.Time) HistoryFixtures {
	r := rand.New(rand.NewSource(seed))
	now = now.UTC()
	epoch := time.Unix(0, 0).UTC()

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
			ID:        fmt.Sprintf("hroom-%06d", i),
			Name:      fmt.Sprintf("hroom-%d", i),
			Type:      model.RoomTypeChannel,
			SiteID:    siteID,
			UserCount: p.BaselineSize,
			CreatedAt: epoch,
			UpdatedAt: epoch,
		}
	}

	// Per-room members.
	subs := make([]model.Subscription, 0, p.Rooms*p.BaselineSize)
	membersByRoom := make([][]model.User, p.Rooms)
	for i := range rooms {
		size := p.BaselineSize
		if size > len(users) {
			size = len(users)
		}
		perm := r.Perm(len(users))[:size]
		members := make([]model.User, size)
		for j, idx := range perm {
			members[j] = users[idx]
			roles := []model.Role{model.RoleUser}
			if j == 0 {
				roles = []model.Role{model.RoleOwner}
			}
			subs = append(subs, model.Subscription{
				ID:       fmt.Sprintf("sub-%s-%s", rooms[i].ID, users[idx].ID),
				User:     model.SubscriptionUser{ID: users[idx].ID, Account: users[idx].Account},
				RoomID:   rooms[i].ID,
				SiteID:   siteID,
				Roles:    roles,
				JoinedAt: epoch,
			})
		}
		membersByRoom[i] = members
	}

	// Room keys for the room-key seed step.
	roomKeys := make(map[string]roomkeystore.RoomKeyPair, len(rooms))
	for i := range rooms {
		roomKeys[rooms[i].ID] = deterministicRoomKeyPair(r)
	}

	// Per-room seed split: two Int63 draws from the global RNG per room,
	// fixed up-front so the streaming iterator and the metadata walk can
	// regenerate identical structural/content sequences independently.
	roomSeeds := make([]roomSeed, len(rooms))
	for i := range roomSeeds {
		roomSeeds[i] = roomSeed{structural: r.Int63(), content: r.Int63()}
	}

	// Cheap metadata walk: derive each room's latest top-level CreatedAt and
	// the ordered list of thread parents WITHOUT materializing any message
	// content. Stamps Room.LastMsgAt so history-service's `before` cap lands
	// at the true latest message rather than 1970 (clipped by floor clamp).
	span := time.Duration(p.MessageSpanDays) * 24 * time.Hour
	threadParents := make(map[string][]ThreadParentRef, len(rooms))
	roomIDs := make([]string, len(rooms))
	for i := range rooms {
		roomIDs[i] = rooms[i].ID
		latest, parents := summarizeRoomPlan(p, rooms[i].ID, len(membersByRoom[i]), now, span, roomSeeds[i].structural)
		if !latest.IsZero() {
			t := latest.UTC()
			rooms[i].LastMsgAt = &t
		}
		if len(parents) > 0 {
			threadParents[rooms[i].ID] = parents
		}
	}

	return HistoryFixtures{
		Fixtures: Fixtures{
			Users:         users,
			Rooms:         rooms,
			Subscriptions: subs,
			RoomKeys:      roomKeys,
		},
		ThreadParents: threadParents,
		preset:        p,
		roomIDs:       roomIDs,
		membersByRoom: membersByRoom,
		roomSeeds:     roomSeeds,
		now:           now,
	}
}

// IterateRoomMessages calls fn once per room with that room's full message
// slice (top-level + replies, in room-local creation order: top-levels indexed
// 0..N-1, each followed inline by its replies if any). The slice is freshly
// allocated per call and goes out of scope when fn returns, so total RAM stays
// bounded by a single room's plan size.
//
// Returning a non-nil error from fn stops the iteration and propagates the
// error.
func (h *HistoryFixtures) IterateRoomMessages(fn func(messages []plannedMessage) error) error {
	if h.preset == nil {
		return nil
	}
	span := time.Duration(h.preset.MessageSpanDays) * 24 * time.Hour
	for i := range h.roomIDs {
		msgs := buildRoomMessages(h.preset, h.roomIDs[i], h.membersByRoom[i], h.now, span, h.roomSeeds[i])
		if err := fn(msgs); err != nil {
			return err
		}
	}
	return nil
}

// FullPlan materializes the entire message plan into a single slice. Use only
// on small/medium presets — history-large would need ~50 GB. Returned messages
// are in (room, room-local order) — the same order IterateRoomMessages yields.
func (h *HistoryFixtures) FullPlan() MessagePlan {
	var out MessagePlan
	_ = h.IterateRoomMessages(func(msgs []plannedMessage) error {
		out.Messages = append(out.Messages, msgs...)
		return nil
	})
	return out
}

// maxReplyOffset bounds how far after the parent a thread reply may land.
// Replies are placed 1..maxReplyOffset minutes after the parent, which keeps
// them in the same Cassandra bucket as the parent for all production bucket
// sizes.
const maxReplyOffset = 10 * time.Minute

// topLevelMeta is the structural metadata for one top-level message slot:
// who sends it and when. Computed via structRNG only — no content allocation.
type topLevelMeta struct {
	senderIdx int
	createdAt time.Time
}

// computeTopLevels walks structRNG to produce per-index sender + createdAt
// and the set of indices eligible to be thread parents (createdAt + reply
// window still fits before `now`). Both the metadata and full builders share
// this so they agree on every structural value.
func computeTopLevels(structR *rand.Rand, p *HistoryPreset, membersCount int, now time.Time, span time.Duration) ([]topLevelMeta, []int) {
	gap := span / time.Duration(p.MessagesPerRoom)
	if gap < 2*time.Millisecond {
		gap = 2 * time.Millisecond
	}
	jitter := gap / 2

	tops := make([]topLevelMeta, p.MessagesPerRoom)
	eligible := make([]int, 0, p.MessagesPerRoom)
	for i := 0; i < p.MessagesPerRoom; i++ {
		senderIdx := 0
		if membersCount > 0 {
			senderIdx = structR.Intn(membersCount)
		}
		baseOffset := span - (time.Duration(i)+1)*gap + gap/2
		j := time.Duration(structR.Int63n(int64(2*jitter)+1)) - jitter
		createdAt := now.Add(-baseOffset).Add(j).UTC()
		tops[i] = topLevelMeta{senderIdx: senderIdx, createdAt: createdAt}
		if createdAt.Add(maxReplyOffset + time.Minute).Before(now) {
			eligible = append(eligible, i)
		}
	}
	return tops, eligible
}

// selectThreadSet picks which eligible indices become thread parents.
// Consumes one Perm draw from structRNG — must be called immediately after
// computeTopLevels so both builders see the same RNG position.
func selectThreadSet(structR *rand.Rand, p *HistoryPreset, eligible []int) map[int]bool {
	threadCount := int(float64(p.MessagesPerRoom) * p.ThreadRate)
	if threadCount > len(eligible) {
		threadCount = len(eligible)
	}
	threadSet := make(map[int]bool, threadCount)
	if threadCount > 0 && p.RepliesPerThread > 0 {
		perm := structR.Perm(len(eligible))[:threadCount]
		for _, k := range perm {
			threadSet[eligible[k]] = true
		}
	}
	return threadSet
}

// summarizeRoomPlan derives a room's (latest top-level CreatedAt, ordered
// ThreadParentRefs) WITHOUT materializing message content or replies. RNG
// alignment with buildRoomMessages comes from sharing computeTopLevels +
// selectThreadSet on the structural RNG; content RNG is not consumed here.
func summarizeRoomPlan(p *HistoryPreset, roomID string, membersCount int, now time.Time, span time.Duration, structSeed int64) (time.Time, []ThreadParentRef) {
	structR := rand.New(rand.NewSource(structSeed))
	tops, eligible := computeTopLevels(structR, p, membersCount, now, span)
	threadSet := selectThreadSet(structR, p, eligible)

	var latest time.Time
	for i := range tops {
		if tops[i].createdAt.After(latest) {
			latest = tops[i].createdAt
		}
	}
	parents := make([]ThreadParentRef, 0, len(threadSet))
	for i := 0; i < p.MessagesPerRoom; i++ {
		if threadSet[i] {
			parents = append(parents, ThreadParentRef{
				MessageID:    fmt.Sprintf("hmsg-%s-%06d", roomID, i),
				ThreadRoomID: fmt.Sprintf("tr-%s-%06d", roomID, i),
			})
		}
	}
	return latest, parents
}

// buildRoomMessages materializes one room's full plan (top-levels + replies)
// from its roomSeed. Pure function of inputs — safe to call concurrently for
// different rooms, but the caller currently iterates serially to keep memory
// flat.
func buildRoomMessages(p *HistoryPreset, roomID string, members []model.User, now time.Time, span time.Duration, seeds roomSeed) []plannedMessage {
	if len(members) == 0 {
		return nil
	}
	structR := rand.New(rand.NewSource(seeds.structural))
	contentR := rand.New(rand.NewSource(seeds.content))
	tops, eligible := computeTopLevels(structR, p, len(members), now, span)
	threadSet := selectThreadSet(structR, p, eligible)

	// Capacity: top-levels + an upper bound on replies.
	out := make([]plannedMessage, 0, p.MessagesPerRoom+len(threadSet)*p.RepliesPerThread)
	for i := 0; i < p.MessagesPerRoom; i++ {
		top := tops[i]
		sender := members[top.senderIdx]
		msgID := fmt.Sprintf("hmsg-%s-%06d", roomID, i)
		pm := plannedMessage{
			RoomID:        roomID,
			MessageID:     msgID,
			SenderID:      sender.ID,
			SenderAccount: sender.Account,
			SenderEngName: sender.EngName,
			Content:       deterministicContent(contentR, p.ContentBytes),
			CreatedAt:     top.createdAt,
		}
		if threadSet[i] {
			pm.ThreadRoomID = fmt.Sprintf("tr-%s-%06d", roomID, i)
			pm.TCount = p.RepliesPerThread
			out = append(out, pm)
			for k := 0; k < p.RepliesPerThread; k++ {
				offset := time.Duration(1+structR.Intn(int(maxReplyOffset/time.Minute))) * time.Minute
				replyAt := top.createdAt.Add(offset).UTC()
				replySender := members[structR.Intn(len(members))]
				out = append(out, plannedMessage{
					RoomID:         roomID,
					MessageID:      fmt.Sprintf("hreply-%s-%06d-%02d", roomID, i, k),
					SenderID:       replySender.ID,
					SenderAccount:  replySender.Account,
					SenderEngName:  replySender.EngName,
					Content:        deterministicContent(contentR, p.ContentBytes),
					CreatedAt:      replyAt,
					ThreadRoomID:   pm.ThreadRoomID,
					ThreadParentID: msgID,
				})
			}
		} else {
			out = append(out, pm)
		}
	}
	return out
}

// deterministicContent fills a fixed-size string with deterministic alphanum
// bytes from r. Avoids non-printable bytes so logs stay readable.
func deterministicContent(r *rand.Rand, size int) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	if size <= 0 {
		return ""
	}
	buf := make([]byte, size)
	for i := range buf {
		buf[i] = alphabet[r.Intn(len(alphabet))]
	}
	return string(buf)
}
