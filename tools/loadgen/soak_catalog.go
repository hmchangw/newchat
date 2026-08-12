package main

import (
	"container/list"
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
	"time"
)

const soakCatalogShardCount = 64

type soakCatalogAction string

const (
	soakCatalogEdit         soakCatalogAction = "edit"
	soakCatalogDelete       soakCatalogAction = "delete"
	soakCatalogPin          soakCatalogAction = "pin"
	soakCatalogReaction     soakCatalogAction = "reaction"
	soakCatalogThreadParent soakCatalogAction = "thread_parent"
	// soakCatalogThreadRead picks a message whose thread actually exists.
	// Deliberately not the same predicate as soakCatalogThreadParent: that one
	// asks "can a new reply be attached here?", which is true of a message with
	// zero replies — precisely the case that has no thread room yet.
	soakCatalogThreadRead soakCatalogAction = "thread_read"
)

type soakClock interface {
	Now() time.Time
}

type soakRealClock struct{}

func (soakRealClock) Now() time.Time { return time.Now().UTC() }

type soakCatalogCandidate struct {
	ID               string
	RoomID           string
	Author           string
	Content          string
	CreatedAt        time.Time
	ThreadParentID   string
	ThreadReplyLimit int
}

type soakCatalogMessage struct {
	soakCatalogCandidate
	AcceptedAt    time.Time
	Edited        bool
	Deleted       bool
	Pinned        bool
	Reactions     map[string][]string
	ThreadReplies int
}

type soakCatalogEntry struct {
	soakCatalogCandidate
	acceptedAt time.Time
	edited     bool
	deleted    bool
	pinned     bool
	reactions  map[string]map[string]struct{}
	// threadReservations cap concurrent reply publishes before their
	// gatekeeper responses arrive. threadReplies counts only accepted replies;
	// keeping them separate prevents a reservation from making a non-existent
	// thread eligible for reads.
	threadReservations int
	threadReplies      int
	threadReadableAt   time.Time
	globalElement      *list.Element
}

type soakCatalogRoom struct {
	messages map[string]*soakCatalogEntry
	order    []*soakCatalogEntry
}

type soakCatalogShard struct {
	mu    sync.RWMutex
	rooms map[string]*soakCatalogRoom
}

type soakPendingEntry struct {
	key       string
	candidate soakCatalogCandidate
}

type soakCatalog struct {
	perRoomCap   int
	globalCap    int
	persistGrace time.Duration
	clock        soakClock

	shards [soakCatalogShardCount]soakCatalogShard

	globalMu     sync.Mutex
	globalOrder  list.List
	size         int
	pending      map[string]*list.Element
	pendingOrder list.List
}

func newSoakCatalog(
	perRoomCap int,
	globalCap int,
	persistGrace time.Duration,
	clock soakClock,
) *soakCatalog {
	if clock == nil {
		clock = soakRealClock{}
	}
	return &soakCatalog{
		perRoomCap:   max(1, perRoomCap),
		globalCap:    max(1, globalCap),
		persistGrace: max(0, persistGrace),
		clock:        clock,
		pending:      make(map[string]*list.Element),
	}
}

func (c *soakCatalog) TrackPublished(candidate *soakCatalogCandidate) error {
	if candidate == nil {
		return fmt.Errorf("published message candidate is required")
	}
	if candidate.ID == "" || candidate.RoomID == "" || candidate.Author == "" {
		return fmt.Errorf("published message requires ID, room ID, and author")
	}
	tracked := *candidate
	if tracked.ThreadReplyLimit <= 0 {
		tracked.ThreadReplyLimit = soakThreadReplyHardCap
	}
	tracked.ThreadReplyLimit = min(tracked.ThreadReplyLimit, soakThreadReplyHardCap)
	key := soakCatalogKey(tracked.RoomID, tracked.ID)

	c.globalMu.Lock()
	defer c.globalMu.Unlock()
	if _, exists := c.pending[key]; exists || c.messageExists(tracked.RoomID, tracked.ID) {
		return fmt.Errorf("message %q already tracked in room %q", tracked.ID, tracked.RoomID)
	}
	element := c.pendingOrder.PushBack(&soakPendingEntry{key: key, candidate: tracked})
	c.pending[key] = element
	for len(c.pending) > c.globalCap {
		oldest := c.pendingOrder.Front()
		pending := oldest.Value.(*soakPendingEntry)
		delete(c.pending, pending.key)
		c.pendingOrder.Remove(oldest)
	}
	return nil
}

func (c *soakCatalog) Accept(roomID, messageID string) bool {
	return c.AcceptAt(roomID, messageID, time.Time{})
}

func (c *soakCatalog) AcceptAt(
	roomID string,
	messageID string,
	createdAt time.Time,
) bool {
	key := soakCatalogKey(roomID, messageID)
	c.globalMu.Lock()
	element, exists := c.pending[key]
	if !exists {
		c.globalMu.Unlock()
		return false
	}
	pending := element.Value.(*soakPendingEntry)
	delete(c.pending, key)
	c.pendingOrder.Remove(element)
	if !createdAt.IsZero() {
		pending.candidate.CreatedAt = createdAt
	}

	entry := &soakCatalogEntry{
		soakCatalogCandidate: pending.candidate,
		acceptedAt:           c.clock.Now(),
		reactions:            make(map[string]map[string]struct{}),
	}
	shard := c.shard(roomID)
	shard.mu.Lock()
	room := shard.room(roomID)
	if _, duplicate := room.messages[messageID]; duplicate {
		shard.mu.Unlock()
		c.globalMu.Unlock()
		return false
	}
	entry.globalElement = c.globalOrder.PushBack(entry)
	room.messages[messageID] = entry
	room.order = append(room.order, entry)
	c.size++
	if len(room.order) > c.perRoomCap {
		if !c.removeOldestUnpinnedRoomLocked(room) {
			c.removeRoomIndexLocked(room, 0)
		}
	}
	shard.mu.Unlock()

	for c.size > c.globalCap {
		if !c.removeOldestUnpinnedGlobalLocked() {
			c.removeOldestGlobalLocked()
		}
	}
	c.globalMu.Unlock()
	return true
}

func (c *soakCatalog) Reject(roomID, messageID string) bool {
	key := soakCatalogKey(roomID, messageID)
	c.globalMu.Lock()
	defer c.globalMu.Unlock()
	element, exists := c.pending[key]
	if !exists {
		return false
	}
	delete(c.pending, key)
	c.pendingOrder.Remove(element)
	return true
}

func (c *soakCatalog) PickEligible(
	roomID string,
	actor string,
	action soakCatalogAction,
) (soakCatalogMessage, bool) {
	shard := c.shard(roomID)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	room := shard.rooms[roomID]
	if room == nil {
		return soakCatalogMessage{}, false
	}
	now := c.clock.Now()
	for i := len(room.order) - 1; i >= 0; i-- {
		entry := room.order[i]
		if !c.eligible(entry, actor, action, now) {
			continue
		}
		return snapshotSoakCatalogEntry(entry), true
	}
	return soakCatalogMessage{}, false
}

func (c *soakCatalog) PickAnyEligible(
	roomID string,
	action soakCatalogAction,
) (soakCatalogMessage, bool) {
	shard := c.shard(roomID)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	room := shard.rooms[roomID]
	if room == nil {
		return soakCatalogMessage{}, false
	}
	now := c.clock.Now()
	for i := len(room.order) - 1; i >= 0; i-- {
		entry := room.order[i]
		if c.eligible(entry, entry.Author, action, now) {
			return snapshotSoakCatalogEntry(entry), true
		}
	}
	return soakCatalogMessage{}, false
}

func (c *soakCatalog) GetEligible(
	roomID string,
	messageID string,
	action soakCatalogAction,
) (soakCatalogMessage, bool) {
	shard := c.shard(roomID)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	room := shard.rooms[roomID]
	if room == nil {
		return soakCatalogMessage{}, false
	}
	entry := room.messages[messageID]
	if entry == nil || !c.eligible(entry, entry.Author, action, c.clock.Now()) {
		return soakCatalogMessage{}, false
	}
	return snapshotSoakCatalogEntry(entry), true
}

func (c *soakCatalog) PickPinCandidate(
	roomID string,
	pinned bool,
) (soakCatalogMessage, bool) {
	shard := c.shard(roomID)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	room := shard.rooms[roomID]
	if room == nil {
		return soakCatalogMessage{}, false
	}
	now := c.clock.Now()
	for i := len(room.order) - 1; i >= 0; i-- {
		entry := room.order[i]
		if entry.pinned == pinned &&
			c.eligible(entry, entry.Author, soakCatalogPin, now) {
			return snapshotSoakCatalogEntry(entry), true
		}
	}
	return soakCatalogMessage{}, false
}

func (c *soakCatalog) PinnedCount(roomID string) int {
	shard := c.shard(roomID)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	room := shard.rooms[roomID]
	if room == nil {
		return 0
	}
	count := 0
	for _, entry := range room.order {
		if entry.pinned {
			count++
		}
	}
	return count
}

func (c *soakCatalog) PickVerificationCandidate(
	roomID string,
	preferMutated bool,
) (soakCatalogMessage, bool) {
	shard := c.shard(roomID)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	room := shard.rooms[roomID]
	if room == nil {
		return soakCatalogMessage{}, false
	}
	now := c.clock.Now()
	var fallback *soakCatalogEntry
	for i := len(room.order) - 1; i >= 0; i-- {
		entry := room.order[i]
		if !c.persistenceEligible(entry, now) {
			continue
		}
		if fallback == nil {
			fallback = entry
		}
		mutated := entry.edited || entry.deleted
		if mutated == preferMutated {
			return snapshotSoakCatalogEntry(entry), true
		}
	}
	if fallback != nil {
		return snapshotSoakCatalogEntry(fallback), true
	}
	return soakCatalogMessage{}, false
}

func (c *soakCatalog) PickHistoryVerificationCandidate(
	roomID string,
) (soakCatalogMessage, bool) {
	shard := c.shard(roomID)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	room := shard.rooms[roomID]
	if room == nil {
		return soakCatalogMessage{}, false
	}
	now := c.clock.Now()
	for i := len(room.order) - 1; i >= 0; i-- {
		entry := room.order[i]
		if entry.ThreadParentID == "" && c.persistenceEligible(entry, now) {
			return snapshotSoakCatalogEntry(entry), true
		}
	}
	return soakCatalogMessage{}, false
}

func (c *soakCatalog) GetVerificationCandidate(
	roomID string,
	messageID string,
) (soakCatalogMessage, bool) {
	shard := c.shard(roomID)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	room := shard.rooms[roomID]
	if room == nil {
		return soakCatalogMessage{}, false
	}
	entry := room.messages[messageID]
	if entry == nil || !c.persistenceEligible(entry, c.clock.Now()) {
		return soakCatalogMessage{}, false
	}
	return snapshotSoakCatalogEntry(entry), true
}

func (c *soakCatalog) Get(roomID, messageID string) (soakCatalogMessage, bool) {
	shard := c.shard(roomID)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	room := shard.rooms[roomID]
	if room == nil {
		return soakCatalogMessage{}, false
	}
	entry, exists := room.messages[messageID]
	if !exists {
		return soakCatalogMessage{}, false
	}
	return snapshotSoakCatalogEntry(entry), true
}

func (c *soakCatalog) MarkEdited(roomID, messageID, content string) bool {
	return c.update(roomID, messageID, func(entry *soakCatalogEntry) bool {
		if entry.deleted || entry.Author == "" {
			return false
		}
		entry.edited = true
		entry.Content = content
		return true
	})
}

func (c *soakCatalog) MarkDeleted(roomID, messageID string) bool {
	return c.update(roomID, messageID, func(entry *soakCatalogEntry) bool {
		if entry.deleted {
			return false
		}
		entry.deleted = true
		return true
	})
}

func (c *soakCatalog) SetPinned(roomID, messageID string, pinned bool) bool {
	return c.update(roomID, messageID, func(entry *soakCatalogEntry) bool {
		if entry.deleted {
			return false
		}
		entry.pinned = pinned
		return true
	})
}

func (c *soakCatalog) ObservePinned(message *soakWireMessage) bool {
	if message == nil || message.RoomID == "" || message.MessageID == "" ||
		message.Sender.Account == "" {
		return false
	}

	c.globalMu.Lock()
	shard := c.shard(message.RoomID)
	shard.mu.Lock()
	room := shard.room(message.RoomID)
	if entry := room.messages[message.MessageID]; entry != nil {
		entry.pinned = true
		shard.mu.Unlock()
		c.globalMu.Unlock()
		return true
	}

	entry := &soakCatalogEntry{
		soakCatalogCandidate: soakCatalogCandidate{
			ID: message.MessageID, RoomID: message.RoomID,
			Author: message.Sender.Account, Content: message.Msg,
			CreatedAt: message.CreatedAt, ThreadParentID: message.ThreadParentID,
			ThreadReplyLimit: soakThreadReplyHardCap,
		},
		acceptedAt: c.clock.Now().Add(-c.persistGrace),
		edited:     message.EditedAt != nil,
		deleted:    message.Deleted,
		pinned:     true,
		reactions:  make(map[string]map[string]struct{}),
	}
	entry.globalElement = c.globalOrder.PushBack(entry)
	room.messages[message.MessageID] = entry
	room.order = append(room.order, entry)
	c.size++
	if len(room.order) > c.perRoomCap {
		if !c.removeOldestUnpinnedRoomLocked(room) {
			c.removeRoomIndexLocked(room, 0)
		}
	}
	shard.mu.Unlock()
	for c.size > c.globalCap {
		if !c.removeOldestUnpinnedGlobalLocked() {
			c.removeOldestGlobalLocked()
		}
	}
	c.globalMu.Unlock()
	return true
}

func (c *soakCatalog) SetReaction(
	roomID, messageID, emoji, account string,
	present bool,
) bool {
	return c.update(roomID, messageID, func(entry *soakCatalogEntry) bool {
		if entry.deleted || emoji == "" || account == "" {
			return false
		}
		accounts := entry.reactions[emoji]
		if present {
			if accounts == nil {
				accounts = make(map[string]struct{})
				entry.reactions[emoji] = accounts
			}
			accounts[account] = struct{}{}
			return true
		}
		if accounts == nil {
			return false
		}
		delete(accounts, account)
		if len(accounts) == 0 {
			delete(entry.reactions, emoji)
		}
		return true
	})
}

func (c *soakCatalog) ReserveThreadReply(roomID, messageID string) bool {
	return c.update(roomID, messageID, func(entry *soakCatalogEntry) bool {
		if entry.deleted || entry.ThreadParentID != "" ||
			entry.threadReplies+entry.threadReservations >= entry.ThreadReplyLimit {
			return false
		}
		entry.threadReservations++
		return true
	})
}

func (c *soakCatalog) ReleaseThreadReplyReservation(roomID, messageID string) bool {
	return c.update(roomID, messageID, func(entry *soakCatalogEntry) bool {
		if entry.threadReservations <= 0 {
			return false
		}
		entry.threadReservations--
		return true
	})
}

// ConfirmThreadReply converts one pending reservation into an accepted reply.
// The first reply becomes readable only after persistGrace, giving the async
// message-worker time to create the thread room and persist its first row.
func (c *soakCatalog) ConfirmThreadReply(roomID, messageID string) bool {
	return c.update(roomID, messageID, func(entry *soakCatalogEntry) bool {
		if entry.threadReservations <= 0 {
			return false
		}
		entry.threadReservations--
		if entry.deleted || entry.ThreadParentID != "" {
			return false
		}
		if entry.threadReplies == 0 {
			entry.threadReadableAt = c.clock.Now().Add(c.persistGrace)
		}
		entry.threadReplies++
		return true
	})
}

func (c *soakCatalog) Size() int {
	c.globalMu.Lock()
	defer c.globalMu.Unlock()
	return c.size
}

func (c *soakCatalog) update(
	roomID, messageID string,
	fn func(*soakCatalogEntry) bool,
) bool {
	shard := c.shard(roomID)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	room := shard.rooms[roomID]
	if room == nil {
		return false
	}
	entry, exists := room.messages[messageID]
	if !exists {
		return false
	}
	return fn(entry)
}

func (c *soakCatalog) eligible(
	entry *soakCatalogEntry,
	actor string,
	action soakCatalogAction,
	now time.Time,
) bool {
	if entry.deleted || now.Before(entry.acceptedAt) ||
		now.Sub(entry.acceptedAt) < c.persistGrace {
		return false
	}
	switch action {
	case soakCatalogEdit, soakCatalogDelete:
		return entry.Author == actor
	case soakCatalogThreadParent:
		return entry.ThreadParentID == "" &&
			entry.threadReplies+entry.threadReservations < entry.ThreadReplyLimit
	case soakCatalogThreadRead:
		// A thread room is created by message-worker when the first reply
		// lands, so a zero-reply message has none. Reading it makes
		// history-service log `empty thread_room_id` and short-circuit before
		// touching the Cassandra thread partition — a fast no-op that would sit
		// in the GetThreadMessages latency tape and pull the percentiles down.
		// The reply budget is irrelevant here: a full thread is still readable.
		return entry.ThreadParentID == "" && entry.threadReplies > 0 &&
			!now.Before(entry.threadReadableAt)
	case soakCatalogPin, soakCatalogReaction:
		return true
	default:
		return false
	}
}

func (c *soakCatalog) persistenceEligible(
	entry *soakCatalogEntry,
	now time.Time,
) bool {
	return !now.Before(entry.acceptedAt) &&
		now.Sub(entry.acceptedAt) >= c.persistGrace
}

func (c *soakCatalog) removeRoomIndexLocked(room *soakCatalogRoom, index int) {
	entry := room.order[index]
	delete(room.messages, entry.ID)
	copy(room.order[index:], room.order[index+1:])
	room.order = room.order[:len(room.order)-1]
	c.globalOrder.Remove(entry.globalElement)
	c.size--
}

func (c *soakCatalog) removeOldestUnpinnedRoomLocked(
	room *soakCatalogRoom,
) bool {
	for i, entry := range room.order {
		if entry.pinned {
			continue
		}
		c.removeRoomIndexLocked(room, i)
		return true
	}
	return false
}

func (c *soakCatalog) removeOldestUnpinnedGlobalLocked() bool {
	for element := c.globalOrder.Front(); element != nil; element = element.Next() {
		entry := element.Value.(*soakCatalogEntry)
		shard := c.shard(entry.RoomID)
		shard.mu.Lock()
		room := shard.rooms[entry.RoomID]
		if room == nil || entry.pinned {
			shard.mu.Unlock()
			continue
		}
		for i := range room.order {
			if room.order[i] == entry {
				c.removeRoomIndexLocked(room, i)
				shard.mu.Unlock()
				return true
			}
		}
		shard.mu.Unlock()
	}
	return false
}

func (c *soakCatalog) removeOldestGlobalLocked() bool {
	element := c.globalOrder.Front()
	if element == nil {
		return false
	}
	entry := element.Value.(*soakCatalogEntry)
	shard := c.shard(entry.RoomID)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	room := shard.rooms[entry.RoomID]
	if room == nil {
		c.globalOrder.Remove(element)
		return false
	}
	for i := range room.order {
		if room.order[i] == entry {
			c.removeRoomIndexLocked(room, i)
			return true
		}
	}
	c.globalOrder.Remove(element)
	return false
}

func (c *soakCatalog) messageExists(roomID, messageID string) bool {
	shard := c.shard(roomID)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	room := shard.rooms[roomID]
	if room == nil {
		return false
	}
	_, exists := room.messages[messageID]
	return exists
}

func (c *soakCatalog) shard(roomID string) *soakCatalogShard {
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(roomID))
	return &c.shards[hasher.Sum32()%soakCatalogShardCount]
}

func (s *soakCatalogShard) room(roomID string) *soakCatalogRoom {
	if s.rooms == nil {
		s.rooms = make(map[string]*soakCatalogRoom)
	}
	room := s.rooms[roomID]
	if room == nil {
		room = &soakCatalogRoom{messages: make(map[string]*soakCatalogEntry)}
		s.rooms[roomID] = room
	}
	return room
}

func snapshotSoakCatalogEntry(entry *soakCatalogEntry) soakCatalogMessage {
	reactions := make(map[string][]string, len(entry.reactions))
	for emoji, accounts := range entry.reactions {
		users := make([]string, 0, len(accounts))
		for account := range accounts {
			users = append(users, account)
		}
		sort.Strings(users)
		reactions[emoji] = users
	}
	return soakCatalogMessage{
		soakCatalogCandidate: entry.soakCatalogCandidate,
		AcceptedAt:           entry.acceptedAt,
		Edited:               entry.edited,
		Deleted:              entry.deleted,
		Pinned:               entry.pinned,
		Reactions:            reactions,
		ThreadReplies:        entry.threadReplies,
	}
}

func soakCatalogKey(roomID, messageID string) string {
	return roomID + "\x00" + messageID
}
