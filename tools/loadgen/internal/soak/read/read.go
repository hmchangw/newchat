package read

import (
	"context"
	"math"
	"math/rand" // #nosec G404 -- load generator randomness, never used for secrets // nosemgrep: math-random-used
	"sync"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/catalog"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/rpc"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/topology"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/wire"
)

type Kind string

const (
	KindHistory Kind = "load_history"
	KindThread  Kind = "get_thread_messages"
	KindMessage Kind = "get_message_by_id"
)

func PickKind(rng *rand.Rand) Kind {
	roll := rng.Float64()
	switch {
	case roll < 0.75:
		return KindHistory
	case roll < 0.90:
		return KindThread
	default:
		return KindMessage
	}
}

type Config struct {
	SiteID         string
	PageLimit      int
	MaxPages       int
	RequestTimeout time.Duration
}

type Sample struct {
	Action  rpc.Action
	Latency time.Duration
	// Messages is what the read came back with; ReplyBytes is what it weighed
	// on the wire. Latency alone cannot tell a slow page from a large one.
	//
	// RowsCounted separates a real row count from the two things that share
	// this field but are not one: a constant (get_message_by_id always returns
	// one) and a server-side total (subscription.count reports the user's whole
	// count, not rows in the reply). Set it through CountRows, never by hand.
	Messages    int
	RowsCounted bool
	ReplyBytes  int
	ErrorClass  rpc.ErrorClass
	ErrorReason rpc.ErrorReason
	Retries     int
	Skipped     bool
}

// CountRows records how many rows the reply carried and marks the sample as
// one loadgen_soak_rows may observe.
func (s *Sample) CountRows(n int) {
	s.Messages, s.RowsCounted = n, true
}

type SampleRecorder interface {
	Record(*Sample)
}

type Outcome struct {
	Action    rpc.Action
	Pages     int
	Messages  int
	MessageID string
	Skipped   bool
}

type Reader struct {
	cfg      Config
	catalog  *catalog.Catalog
	rpc      *rpc.Client
	recorder SampleRecorder
	rng      *rand.Rand
	now      func() time.Time
	members  map[string][]model.SubscriptionUser
	rngMu    sync.Mutex
}

func NewReader(
	cfg Config,
	roomTopology *topology.Topology,
	messageCatalog *catalog.Catalog,
	rpcClient *rpc.Client,
	recorder SampleRecorder,
	rng *rand.Rand,
	now func() time.Time,
) *Reader {
	if cfg.PageLimit <= 0 {
		cfg.PageLimit = 50
	}
	if cfg.MaxPages <= 0 {
		cfg.MaxPages = 100
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 5 * time.Second
	}
	if rng == nil {
		rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	if now == nil {
		now = time.Now
	}
	if messageCatalog == nil {
		messageCatalog = catalog.New(1, 1, 0, nil)
	}
	members := make(map[string][]model.SubscriptionUser)
	if roomTopology != nil {
		active := topology.ActiveUserIDs(roomTopology)
		for i := range roomTopology.Subscriptions {
			subscription := &roomTopology.Subscriptions[i]
			if !topology.IsActiveSubscription(subscription, active) ||
				subscription.RoomID == "" ||
				subscription.User.Account == "" {
				continue
			}
			members[subscription.RoomID] = append(
				members[subscription.RoomID],
				subscription.User,
			)
		}
	}
	return &Reader{
		cfg: cfg, catalog: messageCatalog, rpc: rpcClient, recorder: recorder,
		rng: rng, now: now, members: members,
	}
}

func (r *Reader) ReadMixed(
	ctx context.Context,
	roomID string,
) (Outcome, error) {
	r.rngMu.Lock()
	kind := PickKind(r.rng)
	r.rngMu.Unlock()
	switch kind {
	case KindThread:
		return r.GetThreadMessages(ctx, roomID)
	case KindMessage:
		return r.GetMessageByID(ctx, roomID)
	default:
		return r.LoadHistory(ctx, roomID)
	}
}

func (r *Reader) LoadHistory(
	ctx context.Context,
	roomID string,
) (Outcome, error) {
	outcome := Outcome{Action: rpc.ActionLoadHistory}
	account, ok := r.pickAccount(roomID)
	if !ok {
		return r.skip(outcome), nil
	}

	var before *int64
	lastMsgAt := r.now().UTC().UnixMilli()
	meta := &wire.RoomMeta{LastMsgAt: &lastMsgAt}
	for range r.cfg.MaxPages {
		request := wire.LoadHistoryRequest{
			Before: before,
			Limit:  r.cfg.PageLimit,
			Meta:   meta,
		}
		var response wire.LoadHistoryResponse
		result, latency, err := r.call(ctx, rpc.Request{
			Action: rpc.ActionLoadHistory,
			Subject: subject.MsgHistory(
				account,
				roomID,
				r.cfg.SiteID,
			),
			Account: account, RoomID: roomID,
			Body: request, Timeout: r.cfg.RequestTimeout,
			RetryMode: rpc.RetrySafe,
		}, &response)
		if err != nil {
			r.record(&Sample{
				Action: rpc.ActionLoadHistory, Latency: latency,
				ErrorClass: result.ErrorClass, ErrorReason: result.ErrorReason,
				Retries: result.Retries,
			})
			return outcome, err
		}

		outcome.Pages++
		outcome.Messages += len(response.Messages)
		sample := Sample{
			Action: rpc.ActionLoadHistory, Latency: latency,
			Messages: len(response.Messages), RowsCounted: true, ReplyBytes: result.ReplyBytes,
			Retries: result.Retries,
		}
		if len(response.Messages) == 0 {
			r.record(&sample)
			return outcome, nil
		}

		oldest := oldestMessageMillis(response.Messages)
		if before != nil && oldest >= *before {
			sample.ErrorClass = rpc.ErrorAssertion
			r.record(&sample)
			return outcome, rpc.NewAssertionError(
				"LoadHistory page did not make timestamp progress",
			)
		}
		if oldest == math.MinInt64 {
			sample.ErrorClass = rpc.ErrorAssertion
			r.record(&sample)
			return outcome, rpc.NewAssertionError(
				"LoadHistory oldest timestamp cannot advance",
			)
		}
		nextBefore := oldest - 1
		before = &nextBefore
		r.record(&sample)
	}
	return outcome, nil
}

func (r *Reader) GetThreadMessages(
	ctx context.Context,
	roomID string,
) (Outcome, error) {
	outcome := Outcome{Action: rpc.ActionGetThread}
	account, ok := r.pickAccount(roomID)
	if !ok {
		return r.skip(outcome), nil
	}
	parent, ok := r.catalog.PickEligible(
		roomID,
		account,
		catalog.ActionThreadRead,
	)
	if !ok {
		return r.skip(outcome), nil
	}

	cursor := ""
	seen := make(map[string]struct{})
	for range r.cfg.MaxPages {
		var response wire.GetThreadMessagesResponse
		result, latency, err := r.call(ctx, rpc.Request{
			Action:  rpc.ActionGetThread,
			Subject: subject.MsgThread(account, roomID, r.cfg.SiteID),
			Account: account, RoomID: roomID,
			Body: wire.GetThreadMessagesRequest{
				ThreadMessageID: parent.ID,
				Cursor:          cursor,
				Limit:           r.cfg.PageLimit,
			},
			Timeout: r.cfg.RequestTimeout, RetryMode: rpc.RetrySafe,
		}, &response)
		if err != nil {
			r.record(&Sample{
				Action: rpc.ActionGetThread, Latency: latency,
				ErrorClass: result.ErrorClass, ErrorReason: result.ErrorReason,
				Retries: result.Retries,
			})
			return outcome, err
		}
		outcome.Pages++
		outcome.Messages += len(response.Messages)
		sample := Sample{
			Action: rpc.ActionGetThread, Latency: latency,
			Messages: len(response.Messages), RowsCounted: true, ReplyBytes: result.ReplyBytes,
			Retries: result.Retries,
		}
		if !response.HasNext {
			r.record(&sample)
			return outcome, nil
		}
		if !advanceCursor(cursor, response.NextCursor, seen) {
			sample.ErrorClass = rpc.ErrorAssertion
			r.record(&sample)
			return outcome, rpc.NewAssertionError(
				"GetThreadMessages cursor did not make progress",
			)
		}
		seen[response.NextCursor] = struct{}{}
		cursor = response.NextCursor
		r.record(&sample)
	}
	return outcome, nil
}

func (r *Reader) GetMessageByID(
	ctx context.Context,
	roomID string,
) (Outcome, error) {
	outcome := Outcome{Action: rpc.ActionGetMessage}
	account, ok := r.pickAccount(roomID)
	if !ok {
		return r.skip(outcome), nil
	}
	message, ok := r.catalog.PickEligible(
		roomID,
		account,
		catalog.ActionReaction,
	)
	if !ok {
		return r.skip(outcome), nil
	}

	var response wire.Message
	result, latency, err := r.call(ctx, rpc.Request{
		Action:  rpc.ActionGetMessage,
		Subject: subject.MsgGet(account, roomID, r.cfg.SiteID),
		Account: account, RoomID: roomID,
		Body:    wire.GetMessageByIDRequest{MessageID: message.ID},
		Timeout: r.cfg.RequestTimeout, RetryMode: rpc.RetrySafe,
	}, &response)
	if err != nil {
		r.record(&Sample{
			Action: rpc.ActionGetMessage, Latency: latency,
			ErrorClass: result.ErrorClass, ErrorReason: result.ErrorReason,
			Retries: result.Retries,
		})
		return outcome, err
	}
	sample := Sample{
		Action: rpc.ActionGetMessage, Latency: latency,
		Messages: 1, ReplyBytes: result.ReplyBytes, Retries: result.Retries,
	}
	if response.MessageID != message.ID {
		sample.ErrorClass = rpc.ErrorAssertion
		r.record(&sample)
		return outcome, rpc.NewAssertionError(
			"GetMessageByID returned a different message ID",
		)
	}
	outcome.Pages = 1
	outcome.Messages = 1
	outcome.MessageID = response.MessageID
	r.record(&sample)
	return outcome, nil
}

func (r *Reader) ListPinnedMessages(
	ctx context.Context,
	roomID string,
) (Outcome, error) {
	outcome := Outcome{Action: rpc.ActionPinnedList}
	account, ok := r.pickAccount(roomID)
	if !ok {
		return r.skip(outcome), nil
	}

	cursor := ""
	seen := make(map[string]struct{})
	for range r.cfg.MaxPages {
		var response wire.ListPinnedMessagesResponse
		result, latency, err := r.call(ctx, rpc.Request{
			Action:  rpc.ActionPinnedList,
			Subject: subject.MsgPinnedList(account, roomID, r.cfg.SiteID),
			Account: account, RoomID: roomID,
			Body: wire.ListPinnedMessagesRequest{
				Cursor: cursor,
				Limit:  r.cfg.PageLimit,
			},
			Timeout: r.cfg.RequestTimeout, RetryMode: rpc.RetrySafe,
		}, &response)
		if err != nil {
			r.record(&Sample{
				Action: rpc.ActionPinnedList, Latency: latency,
				ErrorClass: result.ErrorClass, ErrorReason: result.ErrorReason,
				Retries: result.Retries,
			})
			return outcome, err
		}
		outcome.Pages++
		outcome.Messages += len(response.Messages)
		if r.catalog != nil {
			for i := range response.Messages {
				r.catalog.ObservePinned(&response.Messages[i])
			}
		}
		sample := Sample{
			Action: rpc.ActionPinnedList, Latency: latency,
			Messages: len(response.Messages), RowsCounted: true, ReplyBytes: result.ReplyBytes,
			Retries: result.Retries,
		}
		if !response.HasNext {
			r.record(&sample)
			return outcome, nil
		}
		if !advanceCursor(cursor, response.NextCursor, seen) {
			sample.ErrorClass = rpc.ErrorAssertion
			r.record(&sample)
			return outcome, rpc.NewAssertionError(
				"ListPinnedMessages cursor did not make progress",
			)
		}
		seen[response.NextCursor] = struct{}{}
		cursor = response.NextCursor
		r.record(&sample)
	}
	return outcome, nil
}

//nolint:gocritic // hugeParam: the request carries the failure identity; the copy is nothing beside the round trip.
func (r *Reader) call(
	ctx context.Context,
	request rpc.Request,
	response any,
) (rpc.Result, time.Duration, error) {
	startedAt := r.now()
	result, err := r.rpc.Call(ctx, request, response)
	return result, r.now().Sub(startedAt), err
}

func (r *Reader) skip(outcome Outcome) Outcome {
	outcome.Skipped = true
	r.record(&Sample{Action: outcome.Action, Skipped: true})
	return outcome
}

func (r *Reader) record(sample *Sample) {
	if r.recorder != nil {
		r.recorder.Record(sample)
	}
}

func (r *Reader) pickAccount(roomID string) (string, bool) {
	members := r.members[roomID]
	if len(members) == 0 {
		return "", false
	}
	r.rngMu.Lock()
	member := members[r.rng.Intn(len(members))]
	r.rngMu.Unlock()
	return member.Account, true
}

func oldestMessageMillis(messages []wire.Message) int64 {
	oldest := messages[0].CreatedAt.UTC().UnixMilli()
	for i := 1; i < len(messages); i++ {
		createdAt := messages[i].CreatedAt.UTC().UnixMilli()
		if createdAt < oldest {
			oldest = createdAt
		}
	}
	return oldest
}

func advanceCursor(
	current string,
	next string,
	seen map[string]struct{},
) bool {
	if next == "" || next == current {
		return false
	}
	_, duplicate := seen[next]
	return !duplicate
}
