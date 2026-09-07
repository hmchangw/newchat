package userread

import (
	"context"
	"fmt"
	"math/rand" // #nosec G404 -- load generator randomness, never used for secrets // nosemgrep: math-random-used
	"sync"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/rpc"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/topology"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/wire"
)

const defaultRequestTimeout = 5 * time.Second

type Sample struct {
	Action      rpc.Action
	Latency     time.Duration
	Messages    int
	RowsCounted bool
	ReplyBytes  int
	ErrorClass  rpc.ErrorClass
	ErrorReason rpc.ErrorReason
	Retries     int
	Skipped     bool
}

func (s *Sample) countRows(n int) {
	s.Messages, s.RowsCounted = n, true
}

type Recorder interface {
	Record(*Sample)
}

type Config struct {
	SiteID         string
	PageLimit      int
	RequestTimeout time.Duration
}

// Reader drives user-service's read surface. Every call is read-only,
// so the lane carries no evidence ledger: a read has no expected side effect to
// reconcile, only latency and an outcome.
//
// The dispatch is uniform across the reads rather than weighted like a real
// client. A fault window needs each of these paths exercised often enough to be
// interpretable, and skewing toward the popular ones would leave the rest with
// too few samples to say anything about.
type Reader struct {
	cfg      Config
	rpc      *rpc.Client
	recorder Recorder
	now      func() time.Time

	mu       sync.Mutex
	rng      *rand.Rand
	accounts []string
	rooms    []string
	// dmPairs and channelPairs hold ordered (requester, peer) pairs taken from
	// the topology's own rooms, so the DM and channel reads always name a
	// counterpart the requester actually shares that kind of room with.
	dmPairs      []accountPair
	channelPairs []accountPair
	reads        []read
}

// accountPair is one direction of two accounts that share a room.
type accountPair struct {
	Requester string
	Peer      string
}

// read binds a bounded action label to the call that produces it.
type read struct {
	Action rpc.Action
	Call   func(*Reader, context.Context) error
}

func New(
	cfg Config,
	topology *topology.Topology,
	rpcClient *rpc.Client,
	recorder Recorder,
	rng *rand.Rand,
	now func() time.Time,
) (*Reader, error) {
	if topology == nil {
		return nil, fmt.Errorf("soak user reader requires a topology")
	}
	if rng == nil {
		rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	if now == nil {
		now = time.Now
	}
	if cfg.PageLimit <= 0 {
		cfg.PageLimit = 20
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = defaultRequestTimeout
	}

	reader := &Reader{
		cfg: cfg, rpc: rpcClient, recorder: recorder, now: now, rng: rng,
	}
	for i := range topology.ActiveUsers {
		if account := topology.ActiveUsers[i].Account; account != "" {
			reader.accounts = append(reader.accounts, account)
		}
	}
	if len(reader.accounts) == 0 {
		return nil, fmt.Errorf("soak user reader requires at least one active account")
	}
	for i := range topology.Rooms {
		reader.rooms = append(reader.rooms, topology.Rooms[i].ID)
	}
	reader.dmPairs = roomPairs(topology, reader.accounts, model.RoomTypeDM)
	reader.channelPairs = roomPairs(topology, reader.accounts, model.RoomTypeChannel)
	reader.reads = reads()
	return reader, nil
}

// pairsPerRoom bounds how many (requester, peer) directions one room
// contributes, keeping the index linear in the number of subscriptions.
const pairsPerRoom = 2

// peerFor returns a member of the room other than requester. Any
// co-member serves the purpose — the read asserts that a shared room is
// visible, not which one of several co-members it names.
func peerFor(participants []string, requester string) (string, bool) {
	for _, participant := range participants {
		if participant != requester {
			return participant, true
		}
	}
	return "", false
}

// roomPairs indexes rooms of one type as the (requester, peer)
// directions whose requester is one of the lane's active accounts. Both reads
// that name another account depend on it: a pair drawn at random shares a DM
// almost never and a channel seldom, and in either case an empty answer becomes
// the lane's normal result — indistinguishable from a query that is simply
// broken.
//
// Restricting the requester keeps these reads addressing the same ~2k accounts
// as every other user read. DM rooms are seeded active↔active and
// active↔borrowed, so at least one side always qualifies and no room is lost;
// indexing both sides would have these reads issuing traffic as borrowed
// accounts nothing else touches, quietly changing what the lane measures.
//
// A channel contributes at most pairsPerRoom pairs. The point is a
// co-member that genuinely shares a room, not coverage of the membership
// matrix, and a full cross-product would be quadratic in channelMembers. The
// cap is applied after the active filter, never before it: which members a room
// lists first is an ordering accident of how the topology was loaded, so
// truncating first would silently drop every room whose only active member
// happens to be seeded further down.
//
// The index is built once and never refreshed, which is safe only because the
// member-mutation lane cannot touch an account it holds. That lane draws its
// targets from room.available, which soak_roomstate.go builds from
// BorrowedUsers while skipping every account already subscribed to the room;
// this index sees only subscribed rows. The two sets are disjoint by
// construction, so a pair here can never name someone the lane removes. A
// change that lets either side cross into the other's accounts makes these
// pairs go stale silently, and would need this index invalidated on successful
// membership mutations.
//
// Ordering follows topology.Rooms so a given seed draws the same sequence for a
// given topology. It is not stable across the seed and restart paths: those
// build Rooms in different orders, so a replacement process draws a different
// sequence than the process it replaced. That is acceptable for read lanes with
// no evidence to reconcile — it is recorded here so nobody relies on more.
func roomPairs(
	topologyData *topology.Topology,
	accounts []string,
	roomType model.RoomType,
) []accountPair {
	active := make(map[string]struct{}, len(accounts))
	for _, account := range accounts {
		active[account] = struct{}{}
	}
	members := make(map[string][]string)
	for i := range topologyData.Subscriptions {
		subscription := &topologyData.Subscriptions[i]
		if subscription.RoomType != roomType || !topology.IsRoomMember(subscription) ||
			subscription.User.Account == "" {
			continue
		}
		members[subscription.RoomID] = append(
			members[subscription.RoomID], subscription.User.Account,
		)
	}
	pairs := make([]accountPair, 0, len(members)*2)
	for i := range topologyData.Rooms {
		participants := members[topologyData.Rooms[i].ID]
		// A DM room is always exactly two accounts. Fewer than two cannot name a
		// counterpart at all, whatever the room type.
		if roomType == model.RoomTypeDM && len(participants) != 2 {
			continue
		}
		if len(participants) < 2 {
			continue
		}
		paired := 0
		for _, requester := range participants {
			if paired == pairsPerRoom {
				break
			}
			if _, ok := active[requester]; !ok {
				continue
			}
			peer, ok := peerFor(participants, requester)
			if !ok {
				// Every row in this room names the same account, so there is no
				// counterpart to ask about.
				continue
			}
			pairs = append(pairs, accountPair{Requester: requester, Peer: peer})
			paired++
		}
	}
	return pairs
}

// reads is the dispatch table. Its test keeps it equal to
// rpc.UserReadActions so a new allowlisted action needs a call to send it.
func reads() []read {
	return []read{
		{rpc.ActionUserMe, (*Reader).Me},
		{rpc.ActionUserProfileGet, (*Reader).ProfileByName},
		{rpc.ActionUserStatusGet, (*Reader).StatusByName},
		{rpc.ActionUserSettingsGet, (*Reader).Settings},
		{rpc.ActionUserChatlistGet, (*Reader).Chatlist},
		{rpc.ActionUserPriorityContacts, (*Reader).PriorityContacts},
		{rpc.ActionUserAppsList, (*Reader).AppsList},
		{rpc.ActionUserAppsCategories, (*Reader).AppsCategories},
		{rpc.ActionUserSubscriptionCount, (*Reader).SubscriptionCount},
		{rpc.ActionUserSubscriptionByRoom, (*Reader).SubscriptionByRoom},
		{rpc.ActionUserSubscriptionChannel, (*Reader).SubscriptionChannels},
		{rpc.ActionUserSubscriptionDM, (*Reader).SubscriptionDM},
		{rpc.ActionUserThreadList, (*Reader).ThreadList},
		{rpc.ActionUserThreadUnread, (*Reader).ThreadUnread},
	}
}

func (r *Reader) ReadMixed(ctx context.Context) error {
	r.mu.Lock()
	read := r.reads[r.rng.Intn(len(r.reads))]
	r.mu.Unlock()
	return read.Call(r, ctx)
}

func (r *Reader) Me(ctx context.Context) error {
	account := r.pickAccount()
	var response wire.UserMeResponse
	return r.call(ctx, rpc.Request{
		Action:  rpc.ActionUserMe,
		Subject: subject.UserMe(account, r.cfg.SiteID),
		Account: account,
		Timeout: r.cfg.RequestTimeout, RetryMode: rpc.RetrySafe,
	}, &response, nil)
}

func (r *Reader) ProfileByName(ctx context.Context) error {
	account, target := r.pickAccountPair()
	var response wire.UserStatusResponse
	return r.call(ctx, rpc.Request{
		Action:  rpc.ActionUserProfileGet,
		Subject: subject.UserProfileGetByName(account, r.cfg.SiteID),
		Account: account,
		Body:    wire.UserNameRequest{Name: target},
		Timeout: r.cfg.RequestTimeout, RetryMode: rpc.RetrySafe,
	}, &response, nil)
}

func (r *Reader) StatusByName(ctx context.Context) error {
	account, target := r.pickAccountPair()
	var response wire.UserStatusResponse
	return r.call(ctx, rpc.Request{
		Action:  rpc.ActionUserStatusGet,
		Subject: subject.UserStatusGetByName(account, r.cfg.SiteID),
		Account: account,
		Body:    wire.UserNameRequest{Name: target},
		Timeout: r.cfg.RequestTimeout, RetryMode: rpc.RetrySafe,
	}, &response, nil)
}

func (r *Reader) Settings(ctx context.Context) error {
	account := r.pickAccount()
	var response wire.UserSettingsResponse
	return r.call(ctx, rpc.Request{
		Action:  rpc.ActionUserSettingsGet,
		Subject: subject.UserSettingsGet(account, r.cfg.SiteID),
		Account: account,
		Timeout: r.cfg.RequestTimeout, RetryMode: rpc.RetrySafe,
	}, &response, nil)
}

func (r *Reader) Chatlist(ctx context.Context) error {
	account := r.pickAccount()
	var response wire.UserChatlistResponse
	return r.call(ctx, rpc.Request{
		Action:  rpc.ActionUserChatlistGet,
		Subject: subject.UserChatlistGet(account, r.cfg.SiteID),
		Account: account,
		Timeout: r.cfg.RequestTimeout, RetryMode: rpc.RetrySafe,
	}, &response, func(sample *Sample) {
		sample.countRows(len(response.Sections))
	})
}

func (r *Reader) PriorityContacts(ctx context.Context) error {
	account := r.pickAccount()
	var response wire.UserPriorityContactsResponse
	return r.call(ctx, rpc.Request{
		Action:  rpc.ActionUserPriorityContacts,
		Subject: subject.UserPriorityContactsGet(account, r.cfg.SiteID),
		Account: account,
		Timeout: r.cfg.RequestTimeout, RetryMode: rpc.RetrySafe,
	}, &response, func(sample *Sample) {
		sample.countRows(len(response.Contacts))
	})
}

func (r *Reader) AppsList(ctx context.Context) error {
	account := r.pickAccount()
	var response wire.UserAppsResponse
	return r.call(ctx, rpc.Request{
		Action:  rpc.ActionUserAppsList,
		Subject: subject.UserAppsList(account, r.cfg.SiteID),
		Account: account,
		Body:    wire.UserPageRequest{Limit: r.cfg.PageLimit},
		Timeout: r.cfg.RequestTimeout, RetryMode: rpc.RetrySafe,
	}, &response, func(sample *Sample) {
		sample.countRows(len(response.Apps))
	})
}

func (r *Reader) AppsCategories(ctx context.Context) error {
	account := r.pickAccount()
	var response wire.UserAppCategoriesResponse
	return r.call(ctx, rpc.Request{
		Action:  rpc.ActionUserAppsCategories,
		Subject: subject.UserAppsCategories(account, r.cfg.SiteID),
		Account: account,
		Timeout: r.cfg.RequestTimeout, RetryMode: rpc.RetrySafe,
	}, &response, func(sample *Sample) {
		sample.countRows(len(response.Categories))
	})
}

func (r *Reader) SubscriptionCount(ctx context.Context) error {
	account := r.pickAccount()
	var response wire.UserCountResponse
	return r.call(ctx, rpc.Request{
		Action:  rpc.ActionUserSubscriptionCount,
		Subject: subject.UserSubscriptionCount(account, r.cfg.SiteID),
		Account: account,
		Body:    wire.UserCountRequest{},
		Timeout: r.cfg.RequestTimeout, RetryMode: rpc.RetrySafe,
	}, &response, func(sample *Sample) {
		sample.Messages = response.Count
	})
}

func (r *Reader) SubscriptionByRoom(ctx context.Context) error {
	account := r.pickAccount()
	roomID, ok := r.pickRoom()
	if !ok {
		r.recordSkip(rpc.ActionUserSubscriptionByRoom)
		return nil
	}
	var response wire.SubscriptionListResponse
	return r.call(ctx, rpc.Request{
		Action:  rpc.ActionUserSubscriptionByRoom,
		Subject: subject.UserSubscriptionGetByRoomID(account, r.cfg.SiteID),
		Account: account, RoomID: roomID,
		Body:    wire.UserRoomRequest{RoomID: roomID},
		Timeout: r.cfg.RequestTimeout, RetryMode: rpc.RetrySafe,
	}, &response, func(sample *Sample) {
		// A 0-or-1 answer for the room asked about, not a page.
		sample.Messages = len(response.Subscriptions)
	})
}

// SubscriptionChannels asks which of the requester's channels also contain
// another named account. The co-member comes from a channel they actually
// share, for two reasons. user-service dedupes membersContain with the
// requester before matching, so naming self collapses the intersection to one
// account and every channel matches; and an account that shares no channel
// makes an empty page the lane's normal answer, which cannot be told apart from
// a query that is simply broken. Without a shared channel the lane skips.
func (r *Reader) SubscriptionChannels(ctx context.Context) error {
	account, coMember, ok := r.pickChannelPair()
	if !ok {
		r.recordSkip(rpc.ActionUserSubscriptionChannel)
		return nil
	}
	var response wire.SubscriptionListResponse
	return r.call(ctx, rpc.Request{
		Action:  rpc.ActionUserSubscriptionChannel,
		Subject: subject.UserSubscriptionGetChannels(account, r.cfg.SiteID),
		Account: account,
		Body: wire.UserChannelsRequest{
			MembersContain: coMember, Limit: r.cfg.PageLimit,
		},
		Timeout: r.cfg.RequestTimeout, RetryMode: rpc.RetrySafe,
	}, &response, func(sample *Sample) {
		sample.countRows(len(response.Subscriptions))
	})
}

// SubscriptionDM asks for the DM with another account. The peer comes from the
// topology's own DM rooms: an arbitrary pair almost never shares one, so
// drawing at random would make a guaranteed not-found the lane's normal result
// and hide a real regression behind it. Without a DM room the lane skips.
func (r *Reader) SubscriptionDM(ctx context.Context) error {
	account, target, ok := r.pickDMPair()
	if !ok {
		r.recordSkip(rpc.ActionUserSubscriptionDM)
		return nil
	}
	var response wire.UserDMResponse
	return r.call(ctx, rpc.Request{
		Action:  rpc.ActionUserSubscriptionDM,
		Subject: subject.UserSubscriptionGetDM(account, r.cfg.SiteID),
		Account: account,
		Body:    wire.UserAccountNameRequest{AccountName: target},
		Timeout: r.cfg.RequestTimeout, RetryMode: rpc.RetrySafe,
	}, &response, nil)
}

func (r *Reader) ThreadList(ctx context.Context) error {
	account := r.pickAccount()
	var response wire.UserThreadListResponse
	return r.call(ctx, rpc.Request{
		Action:  rpc.ActionUserThreadList,
		Subject: subject.UserThreadList(account, r.cfg.SiteID),
		Account: account,
		Body:    wire.UserPageRequest{Limit: r.cfg.PageLimit},
		Timeout: r.cfg.RequestTimeout, RetryMode: rpc.RetrySafe,
	}, &response, func(sample *Sample) {
		sample.countRows(len(response.Items))
	})
}

func (r *Reader) ThreadUnread(ctx context.Context) error {
	account := r.pickAccount()
	var response wire.UserThreadUnreadResponse
	return r.call(ctx, rpc.Request{
		Action:  rpc.ActionUserThreadUnread,
		Subject: subject.UserThreadUnreadSummary(account, r.cfg.SiteID),
		Account: account,
		Body:    wire.UserEmptyRequest{},
		Timeout: r.cfg.RequestTimeout, RetryMode: rpc.RetrySafe,
	}, &response, nil)
}

func (r *Reader) pickAccount() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.accounts[r.rng.Intn(len(r.accounts))]
}

// pickAccountPair returns a requester and a distinct target. With a single
// account the pair collapses to itself, which the read handlers still answer.
//
// The retries are bounded rather than looped until distinct: a topology whose
// accounts are all the same string would otherwise spin forever holding the
// pool mutex, and a pair that collapses is a weaker sample, not a broken one.
func (r *Reader) pickAccountPair() (string, string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	requester := r.accounts[r.rng.Intn(len(r.accounts))]
	for range 8 {
		target := r.accounts[r.rng.Intn(len(r.accounts))]
		if target != requester {
			return requester, target
		}
	}
	return requester, requester
}

func (r *Reader) pickDMPair() (string, string, bool) {
	return r.pickPair(func() []accountPair { return r.dmPairs })
}

func (r *Reader) pickChannelPair() (string, string, bool) {
	return r.pickPair(func() []accountPair { return r.channelPairs })
}

// pickPair reads the index under the same lock as rng, which is what makes the
// draw safe from the lane's concurrent goroutines.
func (r *Reader) pickPair(
	index func() []accountPair,
) (string, string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	pairs := index()
	if len(pairs) == 0 {
		return "", "", false
	}
	pair := pairs[r.rng.Intn(len(pairs))]
	return pair.Requester, pair.Peer, true
}

func (r *Reader) pickRoom() (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.rooms) == 0 {
		return "", false
	}
	return r.rooms[r.rng.Intn(len(r.rooms))], true
}

//nolint:gocritic // hugeParam: the request carries the failure identity; the copy is nothing beside the round trip.
func (r *Reader) call(
	ctx context.Context,
	request rpc.Request,
	response any,
	apply func(*Sample),
) error {
	if r.rpc == nil {
		return fmt.Errorf("soak user reader requires an RPC client")
	}
	startedAt := r.now()
	result, err := r.rpc.Call(ctx, request, response)
	sample := Sample{
		Action: request.Action, Latency: r.now().Sub(startedAt),
		ReplyBytes: result.ReplyBytes, Retries: result.Retries,
	}
	if err != nil {
		sample.ErrorClass = result.ErrorClass
		sample.ErrorReason = result.ErrorReason
		r.record(&sample)
		return fmt.Errorf("user read lane: %w", err)
	}
	if apply != nil {
		apply(&sample)
	}
	r.record(&sample)
	return nil
}

func (r *Reader) recordSkip(action rpc.Action) {
	r.record(&Sample{Action: action, Skipped: true})
}

func (r *Reader) record(sample *Sample) {
	if r.recorder != nil {
		r.recorder.Record(sample)
	}
}
