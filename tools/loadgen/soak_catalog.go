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
	acceptedAt    time.Time
	edited        bool
	deleted       bool
	pinned        bool
	reactions     map[string]map[string]struct{}
	threadReplies int
	globalElement *list.Element
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
		c.removeRoomIndexLocked(room, 0)
	}
	shard.mu.Unlock()

	for c.size > c.globalCap {
		oldest := c.globalOrder.Front()
		if oldest == nil {
			break
		}
		victim := oldest.Value.(*soakCatalogEntry)
		victimShard := c.shard(victim.RoomID)
		victimShard.mu.Lock()
		victimRoom := victimShard.rooms[victim.RoomID]
		if victimRoom != nil {
			for i := range victimRoom.order {
				if victimRoom.order[i] == victim {
					c.removeRoomIndexLocked(victimRoom, i)
					break
				}
			}
		}
		victimShard.mu.Unlock()
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

func (c *soakCatalog) IncrementThreadReplies(roomID, messageID string) bool {
	return c.update(roomID, messageID, func(entry *soakCatalogEntry) bool {
		if entry.deleted || entry.ThreadParentID != "" ||
			entry.threadReplies >= entry.ThreadReplyLimit {
			return false
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
		return entry.ThreadParentID == "" && entry.threadReplies < entry.ThreadReplyLimit
	case soakCatalogPin, soakCatalogReaction:
		return true
	default:
		return false
	}
}

func (c *soakCatalog) removeRoomIndexLocked(room *soakCatalogRoom, index int) {
	entry := room.order[index]
	delete(room.messages, entry.ID)
	copy(room.order[index:], room.order[index+1:])
	room.order = room.order[:len(room.order)-1]
	c.globalOrder.Remove(entry.globalElement)
	c.size--
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
