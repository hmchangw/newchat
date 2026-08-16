package main

import (
	"fmt"
	"math/rand"
	"slices"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/hmchangw/chat/pkg/model"
)

// soakRoomMaxNameRunes mirrors room-service's rename guard. A longer name is
// rejected there, which would score every rename as a business failure.
const soakRoomMaxNameRunes = 100

// soakRoomCandidateRingSize bounds how many reusable accounts each room keeps.
// The lane runs a handful of mutations per second, so a deeper ring buys no
// coverage while every extra account costs memory across thousands of rooms.
const soakRoomCandidateRingSize = 64

const (
	soakRoomPoolReasonNoCandidate    = "no_candidate"
	soakRoomPoolReasonQuarantineFull = "quarantine_full"
)

type soakMemberCandidateState string

const (
	soakMemberCandidateAvailable      soakMemberCandidateState = "available"
	soakMemberCandidateAddInflight    soakMemberCandidateState = "add_inflight"
	soakMemberCandidateMember         soakMemberCandidateState = "member"
	soakMemberCandidateRemoveInflight soakMemberCandidateState = "remove_inflight"
	soakMemberCandidateQuarantined    soakMemberCandidateState = "quarantined"
)

var soakMemberCandidateStates = []soakMemberCandidateState{
	soakMemberCandidateAvailable, soakMemberCandidateAddInflight,
	soakMemberCandidateMember, soakMemberCandidateRemoveInflight,
	soakMemberCandidateQuarantined,
}

// soakMemberIntent is one half of an add/remove cycle against a room the pool
// owns. The requester is always the room owner, so room-service's permission
// checks pass for reasons the workload controls.
type soakMemberIntent struct {
	RoomID    string
	Account   string
	Requester string
	Add       bool
}

func (i soakMemberIntent) OperationType() failureOperationType {
	if i.Add {
		return failureOperationMemberAdd
	}
	return failureOperationMemberRemove
}

type soakRenameIntent struct {
	RoomID    string
	Requester string
	NewName   string
}

type soakMuteIntent struct {
	RoomID      string
	Account     string
	TargetMuted bool
}

// soakReadIntent marks one subscription read. The baseline is the last
// server-written lastSeenAt this pool confirmed, so the expectation compares two
// values the server itself stamped and never depends on loadgen's clock.
type soakReadIntent struct {
	RoomID   string
	Account  string
	Baseline time.Time
	Known    bool
}

// soakRoomProbe re-reads state the pool stopped trusting. A mutation that ended
// unverified leaves the real state unknown, and acting on a guess would either
// re-add an existing member or toggle a mute twice.
type soakRoomProbe struct {
	RoomID  string
	Account string
	Mute    bool
}

type soakMuteState struct {
	muted bool
	known bool
}

type soakReadState struct {
	lastSeenAt time.Time
	known      bool
}

type soakRoomState struct {
	id string
	// baseName is the seeded name and never changes: rename suffixes are built
	// from a monotonic counter on top of it, so a name can never repeat once the
	// length cap starts truncating.
	baseName      string
	confirmedName string
	nameKnown     bool
	owner         string
	renameSeq     int

	available []string
	members   []string
	states    map[string]soakMemberCandidateState

	muteAccounts []string
	mute         map[string]soakMuteState
	muteCursor   int
	muteLeases   map[string]struct{}

	read       map[string]soakReadState
	readCursor int
	readLeases map[string]struct{}

	memberLease bool
	renameLease bool
}

// soakRoomStatePool owns which room/member mutation may be issued next. It
// enforces two invariants the ledger cannot: a candidate is only ever added
// when it is known absent (and removed when known present), and no two
// conflicting operations target the same room or account at once.
type soakRoomStatePool struct {
	mu            sync.Mutex
	rooms         []*soakRoomState
	byID          map[string]*soakRoomState
	memberCursor  int
	renameCursor  int
	muteCursor    int
	readCursor    int
	quarantine    []soakRoomProbe
	inProbe       int
	quarantineMax int
	metrics       *Metrics
	rng           *rand.Rand
}

func newSoakRoomStatePool(
	topology *soakTopology,
	quarantineMax int,
	metrics *Metrics,
	rng *rand.Rand,
) (*soakRoomStatePool, error) {
	if topology == nil {
		return nil, fmt.Errorf("soak room state pool requires a topology")
	}
	if quarantineMax <= 0 {
		return nil, fmt.Errorf("soak room state quarantine capacity must be greater than zero")
	}
	if rng == nil {
		return nil, fmt.Errorf("soak room state pool requires a random source")
	}

	channels := make(map[string]*soakRoomState)
	order := make([]string, 0, len(topology.Rooms))
	for i := range topology.Rooms {
		room := &topology.Rooms[i]
		if room.Type != model.RoomTypeChannel || room.ID == "" {
			continue
		}
		channels[room.ID] = &soakRoomState{
			id: room.ID, baseName: room.Name,
			confirmedName: room.Name, nameKnown: true,
			states: make(map[string]soakMemberCandidateState),
			mute:   make(map[string]soakMuteState), muteLeases: make(map[string]struct{}),
			read: make(map[string]soakReadState), readLeases: make(map[string]struct{}),
		}
		order = append(order, room.ID)
	}
	if len(channels) == 0 {
		return nil, fmt.Errorf("soak room state pool requires at least one channel room")
	}

	subscribed := make(map[string]map[string]struct{}, len(channels))
	for i := range topology.Subscriptions {
		subscription := &topology.Subscriptions[i]
		state, ok := channels[subscription.RoomID]
		if !ok || !subscription.IsSubscribed || subscription.User.Account == "" {
			continue
		}
		if subscribed[state.id] == nil {
			subscribed[state.id] = make(map[string]struct{})
		}
		subscribed[state.id][subscription.User.Account] = struct{}{}
		if slices.Contains(subscription.Roles, model.RoleOwner) && state.owner == "" {
			state.owner = subscription.User.Account
		}
		// Mute targets are seeded members, never lane-managed candidates, so a
		// mute toggle can never race the membership cycle for one account.
		state.muteAccounts = append(state.muteAccounts, subscription.User.Account)
		state.mute[subscription.User.Account] = soakMuteState{muted: subscription.Muted, known: true}
		readState := soakReadState{}
		if subscription.LastSeenAt != nil && !subscription.LastSeenAt.IsZero() {
			readState = soakReadState{lastSeenAt: subscription.LastSeenAt.UTC(), known: true}
		}
		state.read[subscription.User.Account] = readState
	}

	pool := &soakRoomStatePool{
		byID: make(map[string]*soakRoomState, len(channels)),
		rng:  rng, quarantineMax: quarantineMax, metrics: metrics,
	}
	for _, roomID := range order {
		state := channels[roomID]
		if state.owner == "" {
			continue
		}
		for i := range topology.BorrowedUsers {
			user := &topology.BorrowedUsers[i]
			if len(state.available) >= soakRoomCandidateRingSize {
				break
			}
			if user.Account == "" || model.IsBot(user.Account) ||
				model.IsPlatformAdminAccount(user.Account) {
				continue
			}
			if _, member := subscribed[state.id][user.Account]; member {
				continue
			}
			state.available = append(state.available, user.Account)
			state.states[user.Account] = soakMemberCandidateAvailable
		}
		if len(state.available) == 0 {
			continue
		}
		pool.rooms = append(pool.rooms, state)
		pool.byID[state.id] = state
	}
	if len(pool.rooms) == 0 {
		return nil, fmt.Errorf(
			"soak room state pool requires a channel room with an owner and at least one candidate",
		)
	}
	pool.refreshGaugesLocked()
	return pool, nil
}

// ReserveInFlight re-takes the lease an operation held before the process was
// replaced. Without it a replacement process can issue a second mutation
// against a target whose first one is still unresolved, and that second
// mutation can supply the very effect the first one lost — scoring real data
// loss as good.
//
// It deliberately does not touch candidate state: the pool rebuilds that from
// the reloaded subscriptions, which already reflect whatever actually landed.
func (p *soakRoomStatePool) ReserveInFlight(
	kind failureOperationType,
	roomID, account string,
) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	room := p.byID[roomID]
	if room == nil {
		return false
	}
	switch kind {
	case failureOperationMemberAdd, failureOperationMemberRemove:
		if room.memberLease {
			return false
		}
		room.memberLease = true
		return true
	case failureOperationRoomRename:
		if room.renameLease {
			return false
		}
		room.renameLease = true
		return true
	case failureOperationMuteToggle:
		if account == "" {
			return false
		}
		if _, leased := room.muteLeases[account]; leased {
			return false
		}
		room.muteLeases[account] = struct{}{}
		return true
	case failureOperationMessageRead:
		if account == "" {
			return false
		}
		if _, leased := room.readLeases[account]; leased {
			return false
		}
		room.readLeases[account] = struct{}{}
		return true
	default:
		return false
	}
}

func (p *soakRoomStatePool) RoomIDs() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	ids := make([]string, 0, len(p.rooms))
	for _, room := range p.rooms {
		ids = append(ids, room.id)
	}
	return ids
}

// Owner returns the account the pool addresses a room as. Readers and the
// state observer reuse it so every request comes from an identity room-service
// already accepts as a member.
func (p *soakRoomStatePool) Owner(roomID string) (string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	room, ok := p.byID[roomID]
	if !ok {
		return "", false
	}
	return room.owner, true
}

// NextMemberIntent prefers removing a candidate the pool already added, so the
// add/remove cycle stays paired and room size oscillates by one instead of
// growing until the room hits room-service's capacity ceiling.
func (p *soakRoomStatePool) NextMemberIntent() (soakMemberIntent, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for offset := range p.rooms {
		room := p.rooms[(p.memberCursor+offset)%len(p.rooms)]
		if room.memberLease {
			continue
		}
		var (
			account string
			add     bool
		)
		switch {
		case len(room.members) > 0:
			account, room.members = room.members[0], room.members[1:]
			room.states[account] = soakMemberCandidateRemoveInflight
		case len(room.available) > 0:
			account, room.available = room.available[0], room.available[1:]
			room.states[account] = soakMemberCandidateAddInflight
			add = true
		default:
			continue
		}
		room.memberLease = true
		p.memberCursor = (p.memberCursor + offset + 1) % len(p.rooms)
		p.refreshGaugesLocked()
		return soakMemberIntent{
			RoomID: room.id, Account: account, Requester: room.owner, Add: add,
		}, true
	}
	p.countExhausted(soakRoomPoolReasonNoCandidate)
	return soakMemberIntent{}, false
}

func (p *soakRoomStatePool) SettleMember(intent soakMemberIntent, result failureResult) {
	p.mu.Lock()
	defer p.mu.Unlock()
	room, ok := p.byID[intent.RoomID]
	if !ok {
		return
	}
	room.memberLease = false
	switch result {
	case failureResultGood:
		if intent.Add {
			room.members = append(room.members, intent.Account)
			room.states[intent.Account] = soakMemberCandidateMember
		} else {
			room.available = append(room.available, intent.Account)
			room.states[intent.Account] = soakMemberCandidateAvailable
		}
	case failureResultBad, failureResultNotSent:
		// Rejected or never sent: the room is exactly as it was before.
		if intent.Add {
			room.available = append(room.available, intent.Account)
			room.states[intent.Account] = soakMemberCandidateAvailable
		} else {
			room.members = append(room.members, intent.Account)
			room.states[intent.Account] = soakMemberCandidateMember
		}
	default:
		p.quarantineLocked(soakRoomProbe{RoomID: room.id, Account: intent.Account}, room)
	}
	p.refreshGaugesLocked()
}

func (p *soakRoomStatePool) NextRenameIntent() (soakRenameIntent, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for offset := range p.rooms {
		room := p.rooms[(p.renameCursor+offset)%len(p.rooms)]
		if room.renameLease {
			continue
		}
		room.renameLease = true
		room.renameSeq++
		p.renameCursor = (p.renameCursor + offset + 1) % len(p.rooms)
		return soakRenameIntent{
			RoomID: room.id, Requester: room.owner,
			NewName: soakRoomRenameName(room.baseName, room.renameSeq),
		}, true
	}
	p.countExhausted(soakRoomPoolReasonNoCandidate)
	return soakRenameIntent{}, false
}

func (p *soakRoomStatePool) SettleRename(intent soakRenameIntent, result failureResult) {
	p.mu.Lock()
	defer p.mu.Unlock()
	room, ok := p.byID[intent.RoomID]
	if !ok {
		return
	}
	room.renameLease = false
	switch result {
	case failureResultGood:
		room.confirmedName, room.nameKnown = intent.NewName, true
	case failureResultBad, failureResultNotSent:
		// Refused or never sent, so the stored name still holds.
	default:
		room.nameKnown = false
	}
}

func (p *soakRoomStatePool) NextMuteIntent() (soakMuteIntent, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	// The room cursor is this lane's own: sharing memberCursor would leave the
	// scan pinned to one room whenever the member lane is idle, so every toggle
	// would land on the first room that still has a free account.
	for roomOffset := range p.rooms {
		room := p.rooms[(p.muteCursor+roomOffset)%len(p.rooms)]
		for accountOffset := range room.muteAccounts {
			account := room.muteAccounts[(room.muteCursor+accountOffset)%len(room.muteAccounts)]
			state := room.mute[account]
			if !state.known {
				continue
			}
			if _, leased := room.muteLeases[account]; leased {
				continue
			}
			room.muteLeases[account] = struct{}{}
			room.muteCursor = (room.muteCursor + accountOffset + 1) % len(room.muteAccounts)
			p.muteCursor = (p.muteCursor + roomOffset + 1) % len(p.rooms)
			return soakMuteIntent{
				RoomID: room.id, Account: account, TargetMuted: !state.muted,
			}, true
		}
	}
	p.countExhausted(soakRoomPoolReasonNoCandidate)
	return soakMuteIntent{}, false
}

func (p *soakRoomStatePool) SettleMute(intent soakMuteIntent, result failureResult) {
	p.mu.Lock()
	defer p.mu.Unlock()
	room, ok := p.byID[intent.RoomID]
	if !ok {
		return
	}
	delete(room.muteLeases, intent.Account)
	switch result {
	case failureResultGood:
		room.mute[intent.Account] = soakMuteState{muted: intent.TargetMuted, known: true}
	case failureResultBad, failureResultNotSent:
		// The toggle was refused or never sent, so the stored state still holds.
	default:
		// Mute has no idempotent repeat: an unresolved toggle leaves the real
		// state unknown, and toggling again could silently restore it. The pair
		// is parked until a read proves which side it landed on.
		state := room.mute[intent.Account]
		state.known = false
		room.mute[intent.Account] = state
		p.quarantineLocked(soakRoomProbe{RoomID: room.id, Account: intent.Account, Mute: true}, room)
	}
	p.refreshGaugesLocked()
}

// NextReadIntent picks a subscription to mark read. Mark-read only moves
// lastSeenAt forward, so unlike mute it needs no known baseline to be safe to
// issue — an unknown baseline simply weakens the expectation to "a timestamp
// appeared" for that one operation.
func (p *soakRoomStatePool) NextReadIntent() (soakReadIntent, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for roomOffset := range p.rooms {
		room := p.rooms[(p.readCursor+roomOffset)%len(p.rooms)]
		for accountOffset := range room.muteAccounts {
			account := room.muteAccounts[(room.readCursor+accountOffset)%len(room.muteAccounts)]
			if _, leased := room.readLeases[account]; leased {
				continue
			}
			room.readLeases[account] = struct{}{}
			room.readCursor = (room.readCursor + accountOffset + 1) % len(room.muteAccounts)
			p.readCursor = (p.readCursor + roomOffset + 1) % len(p.rooms)
			state := room.read[account]
			return soakReadIntent{
				RoomID: room.id, Account: account,
				Baseline: state.lastSeenAt, Known: state.known,
			}, true
		}
	}
	p.countExhausted(soakRoomPoolReasonNoCandidate)
	return soakReadIntent{}, false
}

// SettleRead records the timestamp the observer confirmed, which becomes the
// baseline the next mark-read must strictly beat.
func (p *soakRoomStatePool) SettleRead(intent soakReadIntent, result failureResult, observed time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	room, ok := p.byID[intent.RoomID]
	if !ok {
		return
	}
	delete(room.readLeases, intent.Account)
	if result == failureResultGood && !observed.IsZero() {
		room.read[intent.Account] = soakReadState{lastSeenAt: observed.UTC(), known: true}
		return
	}
	if result != failureResultBad && result != failureResultNotSent {
		// The write may or may not have landed, so the stored baseline can no
		// longer be trusted as the value the next operation must beat.
		state := room.read[intent.Account]
		state.known = false
		room.read[intent.Account] = state
	}
}

func (p *soakRoomStatePool) NextProbe() (soakRoomProbe, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.quarantine) == 0 {
		return soakRoomProbe{}, false
	}
	probe := p.quarantine[0]
	p.quarantine = p.quarantine[1:]
	p.inProbe++
	return probe, true
}

func (p *soakRoomStatePool) ReleaseProbe(probe soakRoomProbe) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.inProbe = max(0, p.inProbe-1)
	p.quarantine = append(p.quarantine, probe)
}

func (p *soakRoomStatePool) ResolveMemberProbe(roomID, account string, isMember bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.inProbe = max(0, p.inProbe-1)
	room, ok := p.byID[roomID]
	if !ok {
		return
	}
	if isMember {
		room.members = append(room.members, account)
		room.states[account] = soakMemberCandidateMember
	} else {
		room.available = append(room.available, account)
		room.states[account] = soakMemberCandidateAvailable
	}
	p.countProbe("resolved")
	p.refreshGaugesLocked()
}

func (p *soakRoomStatePool) ResolveMuteProbe(roomID, account string, muted bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.inProbe = max(0, p.inProbe-1)
	room, ok := p.byID[roomID]
	if !ok {
		return
	}
	room.mute[account] = soakMuteState{muted: muted, known: true}
	p.countProbe("resolved")
	p.refreshGaugesLocked()
}

func (p *soakRoomStatePool) quarantineLocked(probe soakRoomProbe, room *soakRoomState) {
	if len(p.quarantine)+p.inProbe >= p.quarantineMax {
		// Dropping the candidate is a traffic degradation, not an evidence
		// problem: nothing was sent without being journaled. The reversible
		// gauge below is the signal, never a ledger invalidation.
		delete(room.states, probe.Account)
		p.countExhausted(soakRoomPoolReasonQuarantineFull)
		return
	}
	p.quarantine = append(p.quarantine, probe)
	if !probe.Mute {
		room.states[probe.Account] = soakMemberCandidateQuarantined
	}
}

func (p *soakRoomStatePool) countExhausted(reason string) {
	if p.metrics == nil {
		return
	}
	p.metrics.SoakRoomPoolExhausted.WithLabelValues(reason).Inc()
}

func (p *soakRoomStatePool) countProbe(result string) {
	if p.metrics == nil {
		return
	}
	p.metrics.SoakRoomQuarantineProbes.WithLabelValues(result).Inc()
}

func (p *soakRoomStatePool) refreshGaugesLocked() {
	if p.metrics == nil {
		return
	}
	counts := make(map[soakMemberCandidateState]int, len(soakMemberCandidateStates))
	degraded := len(p.quarantine)+p.inProbe >= p.quarantineMax
	for _, room := range p.rooms {
		for _, state := range room.states {
			counts[state]++
		}
		if len(room.available) == 0 && len(room.members) == 0 {
			degraded = true
		}
	}
	for _, state := range soakMemberCandidateStates {
		p.metrics.SoakRoomCandidates.WithLabelValues(string(state)).Set(float64(counts[state]))
	}
	if degraded {
		p.metrics.SoakRoomPoolDegraded.Set(1)
		return
	}
	p.metrics.SoakRoomPoolDegraded.Set(0)
}

func soakRoomRenameName(base string, sequence int) string {
	suffix := fmt.Sprintf("-r%d", sequence)
	budget := soakRoomMaxNameRunes - utf8.RuneCountInString(suffix)
	if utf8.RuneCountInString(base) > budget {
		base = string([]rune(base)[:budget])
	}
	return base + suffix
}

// RoomName returns the last name the pool saw confirmed, and whether it is
// still trustworthy. After an unresolved rename the real name is unknown, and
// claiming a stale one would turn a merely lost rename into a name nobody
// asked for — a reported corruption that never happened.
func (p *soakRoomStatePool) RoomName(roomID string) (string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	room, ok := p.byID[roomID]
	if !ok || !room.nameKnown {
		return "", false
	}
	return room.confirmedName, true
}

// AnyOwner returns one owner account, used to address rooms the create lane
// made rather than the seed.
func (p *soakRoomStatePool) AnyOwner() (string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.rooms) == 0 {
		return "", false
	}
	return p.rooms[0].owner, true
}

// CreateRoomMembers returns a requester and the members a new room should start
// with. Members come from candidate rings rather than the reserved cycle, so a
// created room never disturbs the add/remove pairing of an existing one.
func (p *soakRoomStatePool) CreateRoomMembers(size int) (string, []string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.rooms) == 0 || size < 1 {
		return "", nil, false
	}
	room := p.rooms[p.rng.Intn(len(p.rooms))]
	if room.owner == "" || len(room.available) == 0 {
		return "", nil, false
	}
	members := make([]string, 0, size)
	for i := 0; i < len(room.available) && len(members) < size; i++ {
		if room.available[i] == room.owner {
			continue
		}
		members = append(members, room.available[i])
	}
	if len(members) == 0 {
		return "", nil, false
	}
	return room.owner, members, true
}
