package catalog

import (
	"container/list"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/hmchangw/chat/tools/loadgen/internal/soak/distribution"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/wire"
)

const shardCount = 64

type Action string

const (
	ActionEdit         Action = "edit"
	ActionDelete       Action = "delete"
	ActionPin          Action = "pin"
	ActionReaction     Action = "reaction"
	ActionThreadParent Action = "thread_parent"
	// ActionThreadRead picks a message whose thread actually exists.
	// Deliberately not the same predicate as ActionThreadParent: that one
	// asks "can a new reply be attached here?", which is true of a message with
	// zero replies — precisely the case that has no thread room yet.
	ActionThreadRead Action = "thread_read"
	// ActionReadReceipt picks a persisted message to ask "who has read
	// this?". The caller must address the request as that message's own author:
	// room-service serves read receipts only to the sender, so any other
	// identity is refused. This comment previously claimed the opposite, and the
	// lane it misled failed every request it ever sent.
	ActionReadReceipt Action = "read_receipt"
)

type TimeProvider interface {
	Now() time.Time
}

type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now().UTC() }

// Candidate carries what the catalogue keeps about a message, which
// deliberately excludes the body. Nothing that reads the catalogue needs it —
// the verifiers compare by digest and the search probe wants a single term —
// and at the sizes this runs at the bodies were the largest thing the harness
// held. Build the content fields with setContent.
type Candidate struct {
	ID     string
	RoomID string
	Author string
	// Content is input only. TrackPublished reduces it to the fields below and
	// then clears it, so a message read back out of the catalogue never carries
	// a body; compare against ContentSHA256 instead.
	Content          string
	ContentSHA256    string
	ContentLength    int
	SearchTerm       string
	CreatedAt        time.Time
	ThreadParentID   string
	ThreadReplyLimit int
}

// setContent reduces a message body to what the catalogue keeps: the
// digest the read-back verifiers compare against, its length, and the one term
// the search-index probe queries with.
func setContent(candidate *Candidate, body string) {
	candidate.ContentSHA256 = ContentDigest(body)
	candidate.ContentLength = len(body)
	candidate.SearchTerm = SearchTerm(body)
}

// reduceContentLocked is the single place a body enters the catalogue. Every
// path that admits a message goes through it, so none of them can forget to
// record the digest and leave later read-backs comparing against an empty one.
func (c *Catalog) reduceContentLocked(candidate *Candidate, body string) {
	setContent(candidate, body)
	candidate.Content = ""
	if !c.retainSearchTerms {
		candidate.SearchTerm = ""
	}
}

// ContentDigest is the form every read-back comparison uses, so a verifier
// never has to hold a body to check one.
func ContentDigest(body string) string {
	digest := sha256.Sum256([]byte(body))
	return hex.EncodeToString(digest[:])
}

// SearchTerm reduces a payload to a term the analyzer will match. The returned
// term is cloned so retaining it does not retain the complete message body.
func SearchTerm(content string) string {
	for field := range strings.FieldsSeq(content) {
		if len(field) >= 3 {
			return strings.Clone(field)
		}
	}
	return "soak"
}

type Message struct {
	Candidate
	AcceptedAt    time.Time
	Edited        bool
	Deleted       bool
	Pinned        bool
	Reactions     map[string][]string
	ThreadReplies int
}

type entry struct {
	Candidate
	acceptedAt time.Time
	edited     bool
	deleted    bool
	pinned     bool
	reactions  map[string]map[string]struct{}
	// threadReservations cap concurrent reply publishes before their
	// gatekeeper responses arrive. threadReplies counts only accepted replies;
	// keeping them separate prevents a reservation from making a non-existent
	// thread eligible for reads.
	threadReservations      int
	threadReplies           int
	threadReadableAt        time.Time
	threadFollowers         map[string]struct{}
	threadFollowersComplete bool
	globalElement           *list.Element
}

type room struct {
	messages map[string]*entry
	order    []*entry
}

type shard struct {
	mu    sync.RWMutex
	rooms map[string]*room
}

type pendingEntry struct {
	key       string
	candidate Candidate
}

type Catalog struct {
	// retainSearchTerms keeps the term the search-index probe queries with. The
	// soak's bodies are one repeated character with no word boundaries, so that
	// term is the whole body — the thing this catalogue exists not to hold. It
	// is kept only when the observer that needs it is configured.
	retainSearchTerms bool
	perRoomCap        int
	globalCap         int
	persistGrace      time.Duration
	clock             TimeProvider

	shards [shardCount]shard

	globalMu     sync.Mutex
	globalOrder  list.List
	size         int
	pending      map[string]*list.Element
	pendingOrder list.List
}

func New(
	perRoomCap int,
	globalCap int,
	persistGrace time.Duration,
	clock TimeProvider,
) *Catalog {
	if clock == nil {
		clock = RealClock{}
	}
	return &Catalog{
		perRoomCap:   max(1, perRoomCap),
		globalCap:    max(1, globalCap),
		persistGrace: max(0, persistGrace),
		clock:        clock,
		pending:      make(map[string]*list.Element),
	}
}

// RetainSearchTerms is set when the search-index observer is configured, which
// is the only reader that needs the query term.
func (c *Catalog) RetainSearchTerms(retain bool) {
	c.retainSearchTerms = retain
}

func (c *Catalog) TrackPublished(candidate *Candidate) error {
	if candidate == nil {
		return fmt.Errorf("published message candidate is required")
	}
	if candidate.ID == "" || candidate.RoomID == "" || candidate.Author == "" {
		return fmt.Errorf("published message requires ID, room ID, and author")
	}
	tracked := *candidate
	// Reduce the body here so nothing downstream — not the pending entry, not
	// the stored one — ever holds it.
	c.reduceContentLocked(&tracked, tracked.Content)
	if tracked.ThreadReplyLimit <= 0 {
		tracked.ThreadReplyLimit = distribution.ThreadReplyHardCap
	}
	tracked.ThreadReplyLimit = min(tracked.ThreadReplyLimit, distribution.ThreadReplyHardCap)
	key := key(tracked.RoomID, tracked.ID)

	c.globalMu.Lock()
	defer c.globalMu.Unlock()
	if _, exists := c.pending[key]; exists || c.messageExists(tracked.RoomID, tracked.ID) {
		return fmt.Errorf("message %q already tracked in room %q", tracked.ID, tracked.RoomID)
	}
	element := c.pendingOrder.PushBack(&pendingEntry{key: key, candidate: tracked})
	c.pending[key] = element
	for len(c.pending) > c.globalCap {
		oldest := c.pendingOrder.Front()
		pending := oldest.Value.(*pendingEntry)
		delete(c.pending, pending.key)
		c.pendingOrder.Remove(oldest)
	}
	return nil
}

func (c *Catalog) Accept(roomID, messageID string) bool {
	return c.AcceptAt(roomID, messageID, time.Time{})
}

func (c *Catalog) AcceptAt(
	roomID string,
	messageID string,
	createdAt time.Time,
) bool {
	key := key(roomID, messageID)
	c.globalMu.Lock()
	element, exists := c.pending[key]
	if !exists {
		c.globalMu.Unlock()
		return false
	}
	pending := element.Value.(*pendingEntry)
	delete(c.pending, key)
	c.pendingOrder.Remove(element)
	if !createdAt.IsZero() {
		pending.candidate.CreatedAt = createdAt
	}

	entry := &entry{
		Candidate:  pending.candidate,
		acceptedAt: c.clock.Now(),
		reactions:  make(map[string]map[string]struct{}),
	}
	if entry.ThreadParentID == "" {
		entry.threadFollowers = map[string]struct{}{entry.Author: {}}
		entry.threadFollowersComplete = true
	}
	shard := c.shard(roomID)
	shard.mu.Lock()
	room := shard.room(roomID)
	if _, duplicate := room.messages[messageID]; duplicate {
		shard.mu.Unlock()
		c.globalMu.Unlock()
		return false
	}
	if entry.ThreadParentID != "" {
		if parent := room.messages[entry.ThreadParentID]; parent != nil && entry.Author != "" {
			if parent.threadFollowers == nil {
				parent.threadFollowers = make(map[string]struct{})
			}
			parent.threadFollowers[entry.Author] = struct{}{}
		}
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

func (c *Catalog) Reject(roomID, messageID string) bool {
	key := key(roomID, messageID)
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

func (c *Catalog) PickEligible(
	roomID string,
	actor string,
	action Action,
) (Message, bool) {
	shard := c.shard(roomID)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	room := shard.rooms[roomID]
	if room == nil {
		return Message{}, false
	}
	now := c.clock.Now()
	for i := len(room.order) - 1; i >= 0; i-- {
		entry := room.order[i]
		if !c.eligible(entry, actor, action, now) {
			continue
		}
		return snapshot(entry), true
	}
	return Message{}, false
}

func (c *Catalog) PickAnyEligible(
	roomID string,
	action Action,
) (Message, bool) {
	shard := c.shard(roomID)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	room := shard.rooms[roomID]
	if room == nil {
		return Message{}, false
	}
	now := c.clock.Now()
	for i := len(room.order) - 1; i >= 0; i-- {
		entry := room.order[i]
		if c.eligible(entry, entry.Author, action, now) {
			return snapshot(entry), true
		}
	}
	return Message{}, false
}

func (c *Catalog) GetEligible(
	roomID string,
	messageID string,
	action Action,
) (Message, bool) {
	shard := c.shard(roomID)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	room := shard.rooms[roomID]
	if room == nil {
		return Message{}, false
	}
	entry := room.messages[messageID]
	if entry == nil || !c.eligible(entry, entry.Author, action, c.clock.Now()) {
		return Message{}, false
	}
	return snapshot(entry), true
}

func (c *Catalog) PickPinCandidate(
	roomID string,
	pinned bool,
) (Message, bool) {
	shard := c.shard(roomID)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	room := shard.rooms[roomID]
	if room == nil {
		return Message{}, false
	}
	now := c.clock.Now()
	for i := len(room.order) - 1; i >= 0; i-- {
		entry := room.order[i]
		if entry.pinned == pinned &&
			c.eligible(entry, entry.Author, ActionPin, now) {
			return snapshot(entry), true
		}
	}
	return Message{}, false
}

func (c *Catalog) PinnedCount(roomID string) int {
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

func (c *Catalog) PickVerificationCandidate(
	roomID string,
	preferMutated bool,
) (Message, bool) {
	shard := c.shard(roomID)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	room := shard.rooms[roomID]
	if room == nil {
		return Message{}, false
	}
	now := c.clock.Now()
	var fallback *entry
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
			return snapshot(entry), true
		}
	}
	if fallback != nil {
		return snapshot(fallback), true
	}
	return Message{}, false
}

func (c *Catalog) PickHistoryVerificationCandidate(
	roomID string,
) (Message, bool) {
	shard := c.shard(roomID)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	room := shard.rooms[roomID]
	if room == nil {
		return Message{}, false
	}
	now := c.clock.Now()
	for i := len(room.order) - 1; i >= 0; i-- {
		entry := room.order[i]
		if entry.ThreadParentID == "" && c.persistenceEligible(entry, now) {
			return snapshot(entry), true
		}
	}
	return Message{}, false
}

func (c *Catalog) GetVerificationCandidate(
	roomID string,
	messageID string,
) (Message, bool) {
	shard := c.shard(roomID)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	room := shard.rooms[roomID]
	if room == nil {
		return Message{}, false
	}
	entry := room.messages[messageID]
	if entry == nil || !c.persistenceEligible(entry, c.clock.Now()) {
		return Message{}, false
	}
	return snapshot(entry), true
}

func (c *Catalog) Get(roomID, messageID string) (Message, bool) {
	shard := c.shard(roomID)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	room := shard.rooms[roomID]
	if room == nil {
		return Message{}, false
	}
	entry, exists := room.messages[messageID]
	if !exists {
		return Message{}, false
	}
	return snapshot(entry), true
}

func (c *Catalog) MarkEdited(roomID, messageID, content string) bool {
	return c.update(roomID, messageID, func(entry *entry) bool {
		if entry.deleted || entry.Author == "" {
			return false
		}
		entry.edited = true
		c.reduceContentLocked(&entry.Candidate, content)
		return true
	})
}

func (c *Catalog) MarkDeleted(roomID, messageID string) bool {
	return c.update(roomID, messageID, func(entry *entry) bool {
		if entry.deleted {
			return false
		}
		entry.deleted = true
		return true
	})
}

func (c *Catalog) SetPinned(roomID, messageID string, pinned bool) bool {
	return c.update(roomID, messageID, func(entry *entry) bool {
		if entry.deleted {
			return false
		}
		entry.pinned = pinned
		return true
	})
}

func (c *Catalog) ObservePinned(message *wire.Message) bool {
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

	entry := &entry{
		Candidate: Candidate{
			ID: message.MessageID, RoomID: message.RoomID,
			Author:    message.Sender.Account,
			CreatedAt: message.CreatedAt, ThreadParentID: message.ThreadParentID,
			ThreadReplyLimit: distribution.ThreadReplyHardCap,
		},
		acceptedAt: c.clock.Now().Add(-c.persistGrace),
		edited:     message.EditedAt != nil,
		deleted:    message.Deleted,
		pinned:     true,
		reactions:  make(map[string]map[string]struct{}),
	}
	c.reduceContentLocked(&entry.Candidate, message.Msg)
	if entry.ThreadParentID == "" {
		entry.threadFollowers = map[string]struct{}{entry.Author: {}}
		entry.threadFollowersComplete = false
	} else if parent := room.messages[entry.ThreadParentID]; parent != nil && entry.Author != "" {
		if parent.threadFollowers == nil {
			parent.threadFollowers = make(map[string]struct{})
		}
		parent.threadFollowers[entry.Author] = struct{}{}
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

func (c *Catalog) SetReaction(
	roomID, messageID, emoji, account string,
	present bool,
) bool {
	return c.update(roomID, messageID, func(entry *entry) bool {
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

func (c *Catalog) ReserveThreadReply(roomID, messageID string) bool {
	return c.update(roomID, messageID, func(entry *entry) bool {
		if entry.deleted || entry.ThreadParentID != "" ||
			entry.threadReplies+entry.threadReservations >= entry.ThreadReplyLimit {
			return false
		}
		entry.threadReservations++
		return true
	})
}

func (c *Catalog) ReleaseThreadReplyReservation(roomID, messageID string) bool {
	return c.update(roomID, messageID, func(entry *entry) bool {
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
func (c *Catalog) ConfirmThreadReply(roomID, messageID string) bool {
	return c.update(roomID, messageID, func(entry *entry) bool {
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

// ThreadRecipients snapshots the recipient accounts that the broadcast worker
// will include for a channel thread reply without mentions: the sender, parent
// author, and authors of accepted replies that already follow the thread.
func (c *Catalog) ThreadRecipients(roomID, parentID, sender string) []string {
	recipients, _ := c.ThreadRecipientSet(roomID, parentID, sender)
	return recipients
}

func (c *Catalog) ThreadRecipientSet(roomID, parentID, sender string) ([]string, bool) {
	shard := c.shard(roomID)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	room := shard.rooms[roomID]
	if room == nil {
		return nil, false
	}
	recipients := make(map[string]struct{})
	if sender != "" {
		recipients[sender] = struct{}{}
	}
	parent := room.messages[parentID]
	if parent == nil {
		return nil, false
	}
	if len(parent.threadFollowers) == 0 && parent.Author != "" {
		recipients[parent.Author] = struct{}{}
	} else {
		for account := range parent.threadFollowers {
			recipients[account] = struct{}{}
		}
	}
	result := make([]string, 0, len(recipients))
	for account := range recipients {
		result = append(result, account)
	}
	sort.Strings(result)
	return result, parent.threadFollowersComplete
}

func (c *Catalog) Size() int {
	c.globalMu.Lock()
	defer c.globalMu.Unlock()
	return c.size
}

func (c *Catalog) update(
	roomID, messageID string,
	fn func(*entry) bool,
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

func (c *Catalog) eligible(
	entry *entry,
	actor string,
	action Action,
	now time.Time,
) bool {
	if entry.deleted || now.Before(entry.acceptedAt) ||
		now.Sub(entry.acceptedAt) < c.persistGrace {
		return false
	}
	switch action {
	case ActionEdit, ActionDelete:
		return entry.Author == actor
	case ActionThreadParent:
		return entry.ThreadParentID == "" &&
			entry.threadReplies+entry.threadReservations < entry.ThreadReplyLimit
	case ActionThreadRead:
		// A thread room is created by message-worker when the first reply
		// lands, so a zero-reply message has none. Reading it makes
		// history-service log `empty thread_room_id` and short-circuit before
		// touching the Cassandra thread partition — a fast no-op that would sit
		// in the GetThreadMessages latency tape and pull the percentiles down.
		// The reply budget is irrelevant here: a full thread is still readable.
		return entry.ThreadParentID == "" && entry.threadReplies > 0 &&
			!now.Before(entry.threadReadableAt)
	case ActionPin, ActionReaction, ActionReadReceipt:
		return true
	default:
		return false
	}
}

func (c *Catalog) persistenceEligible(
	entry *entry,
	now time.Time,
) bool {
	return !now.Before(entry.acceptedAt) &&
		now.Sub(entry.acceptedAt) >= c.persistGrace
}

func (c *Catalog) removeRoomIndexLocked(room *room, index int) {
	entry := room.order[index]
	delete(room.messages, entry.ID)
	copy(room.order[index:], room.order[index+1:])
	room.order = room.order[:len(room.order)-1]
	c.globalOrder.Remove(entry.globalElement)
	c.size--
}

func (c *Catalog) removeOldestUnpinnedRoomLocked(
	room *room,
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

func (c *Catalog) removeOldestUnpinnedGlobalLocked() bool {
	for element := c.globalOrder.Front(); element != nil; element = element.Next() {
		entry := element.Value.(*entry)
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

func (c *Catalog) removeOldestGlobalLocked() bool {
	element := c.globalOrder.Front()
	if element == nil {
		return false
	}
	entry := element.Value.(*entry)
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

func (c *Catalog) messageExists(roomID, messageID string) bool {
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

func (c *Catalog) shard(roomID string) *shard {
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(roomID))
	return &c.shards[hasher.Sum32()%shardCount]
}

func (s *shard) room(roomID string) *room {
	if s.rooms == nil {
		s.rooms = make(map[string]*room)
	}
	currentRoom := s.rooms[roomID]
	if currentRoom == nil {
		currentRoom = &room{messages: make(map[string]*entry)}
		s.rooms[roomID] = currentRoom
	}
	return currentRoom
}

func snapshot(entry *entry) Message {
	reactions := make(map[string][]string, len(entry.reactions))
	for emoji, accounts := range entry.reactions {
		users := make([]string, 0, len(accounts))
		for account := range accounts {
			users = append(users, account)
		}
		sort.Strings(users)
		reactions[emoji] = users
	}
	return Message{
		Candidate:     entry.Candidate,
		AcceptedAt:    entry.acceptedAt,
		Edited:        entry.edited,
		Deleted:       entry.deleted,
		Pinned:        entry.pinned,
		Reactions:     reactions,
		ThreadReplies: entry.threadReplies,
	}
}

func key(roomID, messageID string) string {
	return roomID + "\x00" + messageID
}
