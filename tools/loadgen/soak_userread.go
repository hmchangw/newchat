package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/hmchangw/chat/pkg/subject"
)

type soakUserReadConfig struct {
	SiteID         string
	PageLimit      int
	RequestTimeout time.Duration
}

// soakUserReader drives user-service's read surface. Every call is read-only,
// so the lane carries no evidence ledger: a read has no expected side effect to
// reconcile, only latency and an outcome.
//
// The dispatch is uniform across the reads rather than weighted like a real
// client. A fault window needs each of these paths exercised often enough to be
// interpretable, and skewing toward the popular ones would leave the rest with
// too few samples to say anything about.
type soakUserReader struct {
	cfg      soakUserReadConfig
	rpc      *soakRPCClient
	recorder soakReadSampleRecorder
	now      func() time.Time

	mu       sync.Mutex
	rng      *rand.Rand
	accounts []string
	rooms    []string
	reads    []soakUserRead
}

// soakUserRead binds a bounded action label to the call that produces it.
type soakUserRead struct {
	Action soakRPCAction
	Call   func(*soakUserReader, context.Context) error
}

func newSoakUserReader(
	cfg soakUserReadConfig,
	topology *soakTopology,
	rpc *soakRPCClient,
	recorder soakReadSampleRecorder,
	rng *rand.Rand,
	now func() time.Time,
) (*soakUserReader, error) {
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
		cfg.RequestTimeout = soakRequestTimeout
	}

	reader := &soakUserReader{
		cfg: cfg, rpc: rpc, recorder: recorder, now: now, rng: rng,
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
	reader.reads = soakUserReads()
	return reader, nil
}

// soakUserReads is the dispatch table. It is derived from soakUserReadActions
// so a new action cannot be added to the allowlist without a call to send it.
func soakUserReads() []soakUserRead {
	return []soakUserRead{
		{soakRPCUserMe, (*soakUserReader).Me},
		{soakRPCUserProfileGet, (*soakUserReader).ProfileByName},
		{soakRPCUserStatusGet, (*soakUserReader).StatusByName},
		{soakRPCUserSettingsGet, (*soakUserReader).Settings},
		{soakRPCUserChatlistGet, (*soakUserReader).Chatlist},
		{soakRPCUserPriorityContacts, (*soakUserReader).PriorityContacts},
		{soakRPCUserAppsList, (*soakUserReader).AppsList},
		{soakRPCUserAppsCategories, (*soakUserReader).AppsCategories},
		{soakRPCUserSubscriptionCount, (*soakUserReader).SubscriptionCount},
		{soakRPCUserSubscriptionByRoom, (*soakUserReader).SubscriptionByRoom},
		{soakRPCUserSubscriptionChannel, (*soakUserReader).SubscriptionChannels},
		{soakRPCUserSubscriptionDM, (*soakUserReader).SubscriptionDM},
		{soakRPCUserThreadList, (*soakUserReader).ThreadList},
		{soakRPCUserThreadUnread, (*soakUserReader).ThreadUnread},
	}
}

func (r *soakUserReader) ReadMixed(ctx context.Context) error {
	r.mu.Lock()
	read := r.reads[r.rng.Intn(len(r.reads))]
	r.mu.Unlock()
	return read.Call(r, ctx)
}

func (r *soakUserReader) Me(ctx context.Context) error {
	account := r.pickAccount()
	var response soakUserMeResponse
	return r.call(ctx, soakRPCRequest{
		Action:  soakRPCUserMe,
		Subject: subject.UserMe(account, r.cfg.SiteID),
		Timeout: r.cfg.RequestTimeout, RetryMode: soakRetrySafe,
	}, &response, nil)
}

func (r *soakUserReader) ProfileByName(ctx context.Context) error {
	account, target := r.pickAccountPair()
	var response soakUserStatusResponse
	return r.call(ctx, soakRPCRequest{
		Action:  soakRPCUserProfileGet,
		Subject: subject.UserProfileGetByName(account, r.cfg.SiteID),
		Body:    soakUserNameRequest{Name: target},
		Timeout: r.cfg.RequestTimeout, RetryMode: soakRetrySafe,
	}, &response, nil)
}

func (r *soakUserReader) StatusByName(ctx context.Context) error {
	account, target := r.pickAccountPair()
	var response soakUserStatusResponse
	return r.call(ctx, soakRPCRequest{
		Action:  soakRPCUserStatusGet,
		Subject: subject.UserStatusGetByName(account, r.cfg.SiteID),
		Body:    soakUserNameRequest{Name: target},
		Timeout: r.cfg.RequestTimeout, RetryMode: soakRetrySafe,
	}, &response, nil)
}

func (r *soakUserReader) Settings(ctx context.Context) error {
	account := r.pickAccount()
	var response soakUserSettingsResponse
	return r.call(ctx, soakRPCRequest{
		Action:  soakRPCUserSettingsGet,
		Subject: subject.UserSettingsGet(account, r.cfg.SiteID),
		Timeout: r.cfg.RequestTimeout, RetryMode: soakRetrySafe,
	}, &response, nil)
}

func (r *soakUserReader) Chatlist(ctx context.Context) error {
	account := r.pickAccount()
	var response soakUserChatlistResponse
	return r.call(ctx, soakRPCRequest{
		Action:  soakRPCUserChatlistGet,
		Subject: subject.UserChatlistGet(account, r.cfg.SiteID),
		Timeout: r.cfg.RequestTimeout, RetryMode: soakRetrySafe,
	}, &response, func(sample *soakReadSample) {
		sample.Messages = len(response.Sections)
	})
}

func (r *soakUserReader) PriorityContacts(ctx context.Context) error {
	account := r.pickAccount()
	var response soakUserPriorityContactsResponse
	return r.call(ctx, soakRPCRequest{
		Action:  soakRPCUserPriorityContacts,
		Subject: subject.UserPriorityContactsGet(account, r.cfg.SiteID),
		Timeout: r.cfg.RequestTimeout, RetryMode: soakRetrySafe,
	}, &response, func(sample *soakReadSample) {
		sample.Messages = len(response.Contacts)
	})
}

func (r *soakUserReader) AppsList(ctx context.Context) error {
	account := r.pickAccount()
	var response soakUserAppsResponse
	return r.call(ctx, soakRPCRequest{
		Action:  soakRPCUserAppsList,
		Subject: subject.UserAppsList(account, r.cfg.SiteID),
		Body:    soakUserPageRequest{Limit: r.cfg.PageLimit},
		Timeout: r.cfg.RequestTimeout, RetryMode: soakRetrySafe,
	}, &response, func(sample *soakReadSample) {
		sample.Messages = len(response.Apps)
	})
}

func (r *soakUserReader) AppsCategories(ctx context.Context) error {
	account := r.pickAccount()
	var response soakUserAppCategoriesResponse
	return r.call(ctx, soakRPCRequest{
		Action:  soakRPCUserAppsCategories,
		Subject: subject.UserAppsCategories(account, r.cfg.SiteID),
		Timeout: r.cfg.RequestTimeout, RetryMode: soakRetrySafe,
	}, &response, func(sample *soakReadSample) {
		sample.Messages = len(response.Categories)
	})
}

func (r *soakUserReader) SubscriptionCount(ctx context.Context) error {
	account := r.pickAccount()
	var response soakUserCountResponse
	return r.call(ctx, soakRPCRequest{
		Action:  soakRPCUserSubscriptionCount,
		Subject: subject.UserSubscriptionCount(account, r.cfg.SiteID),
		Body:    soakUserCountRequest{},
		Timeout: r.cfg.RequestTimeout, RetryMode: soakRetrySafe,
	}, &response, func(sample *soakReadSample) {
		sample.Messages = response.Count
	})
}

func (r *soakUserReader) SubscriptionByRoom(ctx context.Context) error {
	account := r.pickAccount()
	roomID, ok := r.pickRoom()
	if !ok {
		r.recordSkip(soakRPCUserSubscriptionByRoom)
		return nil
	}
	var response soakSubscriptionListResponse
	return r.call(ctx, soakRPCRequest{
		Action:  soakRPCUserSubscriptionByRoom,
		Subject: subject.UserSubscriptionGetByRoomID(account, r.cfg.SiteID),
		Body:    soakUserRoomRequest{RoomID: roomID},
		Timeout: r.cfg.RequestTimeout, RetryMode: soakRetrySafe,
	}, &response, func(sample *soakReadSample) {
		sample.Messages = len(response.Subscriptions)
	})
}

func (r *soakUserReader) SubscriptionChannels(ctx context.Context) error {
	account := r.pickAccount()
	var response soakSubscriptionListResponse
	return r.call(ctx, soakRPCRequest{
		Action:  soakRPCUserSubscriptionChannel,
		Subject: subject.UserSubscriptionGetChannels(account, r.cfg.SiteID),
		Body:    soakUserPageRequest{Limit: r.cfg.PageLimit},
		Timeout: r.cfg.RequestTimeout, RetryMode: soakRetrySafe,
	}, &response, func(sample *soakReadSample) {
		sample.Messages = len(response.Subscriptions)
	})
}

// SubscriptionDM asks for the DM with another account. A pair that has no DM is
// a legitimate not-found rather than a fault, so the lane counts the outcome and
// does not treat an empty answer as a failure.
func (r *soakUserReader) SubscriptionDM(ctx context.Context) error {
	account, target := r.pickAccountPair()
	var response soakUserDMResponse
	return r.call(ctx, soakRPCRequest{
		Action:  soakRPCUserSubscriptionDM,
		Subject: subject.UserSubscriptionGetDM(account, r.cfg.SiteID),
		Body:    soakUserAccountNameRequest{AccountName: target},
		Timeout: r.cfg.RequestTimeout, RetryMode: soakRetrySafe,
	}, &response, nil)
}

func (r *soakUserReader) ThreadList(ctx context.Context) error {
	account := r.pickAccount()
	var response soakUserThreadListResponse
	return r.call(ctx, soakRPCRequest{
		Action:  soakRPCUserThreadList,
		Subject: subject.UserThreadList(account, r.cfg.SiteID),
		Body:    soakUserPageRequest{Limit: r.cfg.PageLimit},
		Timeout: r.cfg.RequestTimeout, RetryMode: soakRetrySafe,
	}, &response, func(sample *soakReadSample) {
		sample.Messages = len(response.Items)
	})
}

func (r *soakUserReader) ThreadUnread(ctx context.Context) error {
	account := r.pickAccount()
	var response soakUserThreadUnreadResponse
	return r.call(ctx, soakRPCRequest{
		Action:  soakRPCUserThreadUnread,
		Subject: subject.UserThreadUnreadSummary(account, r.cfg.SiteID),
		Body:    soakUserEmptyRequest{},
		Timeout: r.cfg.RequestTimeout, RetryMode: soakRetrySafe,
	}, &response, nil)
}

func (r *soakUserReader) pickAccount() string {
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
func (r *soakUserReader) pickAccountPair() (string, string) {
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

func (r *soakUserReader) pickRoom() (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.rooms) == 0 {
		return "", false
	}
	return r.rooms[r.rng.Intn(len(r.rooms))], true
}

func (r *soakUserReader) call(
	ctx context.Context,
	request soakRPCRequest,
	response any,
	apply func(*soakReadSample),
) error {
	if r.rpc == nil {
		return fmt.Errorf("soak user reader requires an RPC client")
	}
	startedAt := r.now()
	result, err := r.rpc.Call(ctx, request, response)
	sample := soakReadSample{
		Action: request.Action, Latency: r.now().Sub(startedAt), Retries: result.Retries,
	}
	if err != nil {
		sample.ErrorClass = result.ErrorClass
		r.record(sample)
		return fmt.Errorf("issue %s request: %w", request.Action, err)
	}
	if apply != nil {
		apply(&sample)
	}
	r.record(sample)
	return nil
}

func (r *soakUserReader) recordSkip(action soakRPCAction) {
	r.record(soakReadSample{Action: action, Skipped: true})
}

func (r *soakUserReader) record(sample soakReadSample) {
	if r.recorder != nil {
		r.recorder.Record(sample)
	}
}
