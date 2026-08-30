// Package main fans out MESSAGES-CANONICAL room events with NAK-on-failure;
// handleReacted also publishes the reaction author-notification with log-and-swallow.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bytedance/sonic"
	"go.opentelemetry.io/otel"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/logctx"
	"github.com/hmchangw/chat/pkg/mention"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/natsmetrics"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/obs"
	"github.com/hmchangw/chat/pkg/outbox"
	"github.com/hmchangw/chat/pkg/roomcrypto"
	"github.com/hmchangw/chat/pkg/roomkeystore"
	"github.com/hmchangw/chat/pkg/roommetacache"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/userstore"
)

// maxSiteFanout bounds concurrent per-destination OUTBOX publishes, matching the
// budget user-service and search-service use for their own per-site fan-outs.
const maxSiteFanout = 8

// mentionFanoutTimeout caps the whole per-destination fan-out. The handler waits
// for it before acking, so it must stay well under the 30s AckWait default: a
// stalled OUTBOX would otherwise hold the canonical message past redelivery and
// re-broadcast it to every client in the room. A badge is best-effort; the
// message is not.
const mentionFanoutTimeout = 5 * time.Second

// errNoCurrentKey is returned when a room has no encryption key in its room document.
var errNoCurrentKey = errors.New("no current key")

// Publisher abstracts NATS publishing so the handler is testable.
type Publisher interface {
	Publish(ctx context.Context, subject string, data []byte) error
}

// RoomKeyProvider fetches the current encryption key for a room.
// Defined here (not imported from pkg/roomkeystore directly) to keep the
// handler's dependency contract narrow — only Get is used.
type RoomKeyProvider interface {
	Get(ctx context.Context, roomID string) (*roomkeystore.VersionedKeyPair, error)
}

// ParentMessageInfo is the subset of a thread's parent message the channel fan-out
// needs: the author (always a recipient) and the creation time (feeds the mention
// visibility gate).
type ParentMessageInfo struct {
	SenderAccount string
	CreatedAt     time.Time
}

// ParentFetcher resolves a thread's parent message from history-service. The parent
// pre-exists (a reply targets it), so this is race-free — unlike thread_rooms, which
// message-worker may not have created yet on the first reply.
type ParentFetcher interface {
	FetchParent(ctx context.Context, account, roomID, siteID, messageID string) (*ParentMessageInfo, error)
}

// PublishFunc publishes to JetStream with msgID as the Nats-Msg-Id.
type PublishFunc func(ctx context.Context, subj string, data []byte, msgID string) error

// Handler processes MESSAGES-CANONICAL messages and broadcasts room events.
type Handler struct {
	store         Store
	userStore     userstore.UserStore
	pub           Publisher
	keyStore      RoomKeyProvider
	parentFetcher ParentFetcher
	encrypt       bool
	encoder       *roomcrypto.Encoder
	routeMode     subject.RoomRouteMode
	metrics       *broadcastMetrics
	// threadViewSubject gates the thread-scoped view lane; see publishThreadViewEvent.
	threadViewSubject bool
	siteID            string
	// publish relays onto the OUTBOX; nil disables the cross-site mention fan-out.
	publish PublishFunc
	// activity announces a room's position to remote sites; nil disables it.
	activity *roomActivityRefresher
	// sealer seals the room-doc preview; nil means previews are not persisted.
	sealer *previewSealer
	// previews buffers sealed previews for the room doc; nil disables the write.
	previews *previewWriter
}

type handlerOption func(*handlerOptions)

type handlerOptions struct {
	metrics *broadcastMetrics
	// metricsSet separates "caller passed nil to disable metrics" from "caller
	// passed no option at all". Without it the two are the same value and the
	// constructor rebuilds the instruments over an explicit disable.
	metricsSet        bool
	threadViewSubject bool
	siteID            string
	publish           PublishFunc
	activity          *roomActivityRefresher
	sealer            *previewSealer
	previews          *previewWriter
}

func withBroadcastMetrics(metrics *broadcastMetrics) handlerOption {
	return func(opts *handlerOptions) { opts.metrics, opts.metricsSet = metrics, true }
}

func withThreadViewSubject(enabled bool) handlerOption {
	return func(opts *handlerOptions) { opts.threadViewSubject = enabled }
}

// withOutboxFederation enables the cross-site mention fan-out from siteID.
func withOutboxFederation(siteID string, publish PublishFunc) handlerOption {
	return func(opts *handlerOptions) {
		opts.siteID = siteID
		opts.publish = publish
	}
}

// withRoomActivityRefresh enables the cross-site room-position announce.
func withRoomActivityRefresh(r *roomActivityRefresher) handlerOption {
	return func(opts *handlerOptions) { opts.activity = r }
}

// withPreviewSealer supplies the room-preview sealer and the buffered writer that
// stores what it seals; absent (or nil) disables preview persistence, which is what
// ATREST_ENABLED=false yields.
func withPreviewSealer(sealer *previewSealer, w *previewWriter) handlerOption {
	return func(opts *handlerOptions) { opts.sealer, opts.previews = sealer, w }
}

func NewHandler(store Store, userStore userstore.UserStore, pub Publisher, keyStore RoomKeyProvider, parentFetcher ParentFetcher, encrypt bool, routeMode subject.RoomRouteMode, options ...handlerOption) *Handler {
	var opts handlerOptions
	for _, option := range options {
		option(&opts)
	}
	if !opts.metricsSet {
		opts.metrics = newBroadcastMetrics(otel.Meter("broadcast-worker"))
	}
	// Nil metrics means the toggle is off, so leave the publisher unwrapped.
	// The recorder reads the context for its labels before Delivery's nil guard
	// returns, and that read happens once per recipient publish.
	if opts.metrics != nil {
		pub = &broadcastMetricPublisher{next: pub, metrics: opts.metrics}
	}
	return &Handler{
		store:             store,
		userStore:         userStore,
		pub:               pub,
		keyStore:          keyStore,
		parentFetcher:     parentFetcher,
		encrypt:           encrypt,
		encoder:           roomcrypto.NewEncoder(),
		routeMode:         routeMode,
		metrics:           opts.metrics,
		threadViewSubject: opts.threadViewSubject,
		siteID:            opts.siteID,
		publish:           opts.publish,
		activity:          opts.activity,
		sealer:            opts.sealer,
		previews:          opts.previews,
	}
}

// HandleMessage processes a single MESSAGES-CANONICAL message payload.
func (h *Handler) HandleMessage(ctx context.Context, data []byte) error {
	var evt model.MessageEvent
	if err := sonic.Unmarshal(data, &evt); err != nil {
		natsmetrics.MarkTerminalFromContext(ctx, natsmetrics.TerminalInvalidPayload)
		// Malformed payload — it will never parse on redelivery. Mark permanent
		// so the caller Acks (drops) it instead of retrying until MaxDeliver.
		return errcode.Permanent(errcode.BadRequest("malformed message event"))
	}
	ctx = obs.ContextWithIdentity(ctx, evt.Message.UserAccount, evt.Message.RoomID, evt.SiteID)
	ctx = withBroadcastMetricLabels(ctx, roomUnknown, natsmetrics.EventType(evt.Event))

	switch evt.Event {
	case model.EventCreated:
		return h.handleCreated(ctx, &evt)
	case model.EventUpdated:
		return h.handleUpdated(ctx, &evt)
	case model.EventDeleted:
		return h.handleDeleted(ctx, &evt)
	case model.EventPinned:
		return h.handlePinned(ctx, &evt)
	case model.EventUnpinned:
		return h.handleUnpinned(ctx, &evt)
	case model.EventReacted:
		return h.handleReacted(ctx, &evt)
	default:
		slog.WarnContext(ctx, "unknown message event type, skipping",
			"event", evt.Event,
			"messageID", evt.Message.ID,
			"request_id", natsutil.RequestIDFromContext(ctx))
		return nil
	}
}

// HandleServerBroadcast processes a single server-broadcast core-NATS message
// (chat.server.broadcast.{siteID}.>). Currently handles EventThreadReplyAdded
// badge events published by message-worker.
func (h *Handler) HandleServerBroadcast(ctx context.Context, data []byte) {
	var evt model.MessageEvent
	if err := sonic.Unmarshal(data, &evt); err != nil {
		slog.ErrorContext(ctx, "unmarshal server-broadcast event failed; dropping",
			"error", err,
			"request_id", natsutil.RequestIDFromContext(ctx))
		return
	}
	ctx = obs.ContextWithIdentity(ctx, evt.Message.UserAccount, evt.Message.RoomID, evt.SiteID)
	switch evt.Event {
	case model.EventThreadReplyAdded:
		if err := h.handleThreadTCountUpdated(ctx, &evt); err != nil {
			slog.ErrorContext(ctx, "handle thread tcount update failed",
				"error", err,
				"messageID", evt.Message.ID,
				"request_id", natsutil.RequestIDFromContext(ctx))
		}
	default:
		slog.WarnContext(ctx, "unknown server-broadcast event type; dropping",
			"event", evt.Event,
			"request_id", natsutil.RequestIDFromContext(ctx))
	}
}

func (h *Handler) handleCreated(ctx context.Context, evt *model.MessageEvent) error {
	msg := evt.Message

	if msg.IsHiddenThreadReply() {
		return h.handleThreadCreated(ctx, evt)
	}

	// One user-store round-trip covers both mention enrichment and sender
	// enrichment: parse mentions, dedupe with the sender, fetch once, then
	// hand the resulting map to ResolveFromParsed (skips a second parse) and
	// to buildClientMessage.
	parsed := mention.Parse(msg.Content)
	lookupAccounts := dedupedAccounts(msg.UserAccount, parsed.Accounts)
	users, lookupErr := h.userStore.FindUsersByAccounts(ctx, lookupAccounts)
	if lookupErr != nil {
		slog.WarnContext(ctx, "user lookup failed, falling back to account",
			"error", lookupErr,
			"request_id", natsutil.RequestIDFromContext(ctx))
	}
	userByAccount := usersByAccount(users)

	resolved := mention.ResolveFromParsed(parsed, userByAccount)

	// The room's own pointer (lastMsgAt/lastMsgId/lastMentionAllAt), the sender's
	// lastSeenAt and the mention badges are roomlist-worker's and are off this path
	// entirely. The preview stays here because sealing one needs the users, mention
	// participants and attachments the fan-out below has already resolved — see
	// previewWriter for why the two halves of the room document can be written apart.
	//
	// Buffered, never awaited, and it cannot fail the handler: the message is going
	// out to the room whatever the room list ends up showing.
	sealed, sealFailed := h.previewForInserted(ctx, &msg, userByAccount, resolved.Participants)
	h.previews.buffer(roomPreview{
		RoomID:        msg.RoomID,
		MsgID:         msg.ID,
		At:            msg.CreatedAt,
		Preview:       sealed,
		PreviewFailed: sealFailed,
	})
	meta, err := h.store.GetRoomMeta(ctx, msg.RoomID)
	if err != nil {
		return fmt.Errorf("get room meta %s: %w", msg.RoomID, err)
	}

	clientMsg := buildClientMessage(&msg, userByAccount)

	// debug: how this message was routed for fan-out (metadata only).
	slog.DebugContext(ctx, "broadcast routing", "request_id", natsutil.RequestIDFromContext(ctx),
		"room_id", meta.ID, "type", meta.Type, "mentions", len(resolved.Accounts), "mention_all", resolved.MentionAll)

	switch meta.Type {
	case model.RoomTypeChannel:
		if err := h.publishChannelEvent(ctx, &meta, clientMsg, evt.Timestamp, resolved.MentionAll, resolved.Participants); err != nil {
			return err
		}
	case model.RoomTypeDM, model.RoomTypeBotDM:
		if err := h.publishDMEvents(ctx, &meta, clientMsg, evt.Timestamp, resolved.Accounts, model.RoomEventNewMessage); err != nil {
			return err
		}
	default:
		slog.WarnContext(ctx, "unknown room type, skipping fan-out",
			"type", meta.Type,
			"room_id", meta.ID,
			"request_id", natsutil.RequestIDFromContext(ctx))
		return nil
	}
	h.federateMentions(ctx, meta.ID, msg.ID, resolved.Participants, msg.CreatedAt)
	// Announce the room's new position to remote sites. Fires from the same
	// place the rooms.lastMsgAt write used to, so coverage is unchanged by that
	// write moving to roomlist-worker.
	h.activity.refresh(ctx, meta.ID, meta.CrossSite, msg.CreatedAt)
	return nil
}

func (h *Handler) handleThreadCreated(ctx context.Context, evt *model.MessageEvent) error {
	msg := evt.Message
	parentMsgID := msg.ThreadParentMessageID

	parsed := mention.Parse(msg.Content)

	// Fetch room type first so DM/BotDM rooms skip the thread-subscription query
	// entirely — their fan-out uses ListRoomMembers, not thread subscribers.
	meta, err := h.store.GetRoomMeta(ctx, msg.RoomID)
	if err != nil {
		return fmt.Errorf("get room meta %s: %w", msg.RoomID, err)
	}

	// Channel rooms: only thread followers and (history-gated) @-mentioned accounts
	// receive the event. channelThreadFanOut applies the visibility gate and builds
	// the recipient set.
	var fanOut []string
	if meta.Type == model.RoomTypeChannel {
		fanOut, err = h.channelThreadFanOut(ctx, msg.RoomID, evt.SiteID, parentMsgID, msg.UserAccount, parsed.Accounts, msg.ThreadParentMessageCreatedAt, evt.ThreadParentSenderAccount)
		if err != nil {
			return fmt.Errorf("channel thread fan-out for parent %s: %w", parentMsgID, err)
		}
	}

	lookupAccounts := dedupedAccounts(msg.UserAccount, parsed.Accounts)
	users, lookupErr := h.userStore.FindUsersByAccounts(ctx, lookupAccounts)
	if lookupErr != nil {
		slog.WarnContext(ctx, "user lookup failed for thread reply, falling back to account",
			"error", lookupErr,
			"parentMessageID", parentMsgID,
			"request_id", natsutil.RequestIDFromContext(ctx))
	}
	userByAccount := usersByAccount(users)

	resolved := mention.ResolveFromParsed(parsed, userByAccount)

	clientMsg := buildClientMessage(&msg, userByAccount)

	switch meta.Type {
	case model.RoomTypeChannel:
		// roomlist-worker (not broadcast-worker) owns the room-level mention badge
		// derived from MESSAGES-CANONICAL, and correctly skips it here: TShow=false
		// replies are invisible in the main channel, so a badge would appear with no
		// visible message to explain it.
		roomEvt := buildRoomEvent(&meta, clientMsg, evt.Timestamp)
		roomEvt.Type = model.RoomEventNewThreadMessage
		roomEvt.MentionAll = resolved.MentionAll
		if len(resolved.Participants) > 0 {
			roomEvt.Mentions = resolved.Participants
		}
		payload, err := sonic.Marshal(roomEvt)
		if err != nil {
			return fmt.Errorf("marshal thread created event for parent %s: %w", parentMsgID, err)
		}
		viewPayload := h.sealThreadViewPayload(ctx, meta.ID, payload, func() (any, error) {
			sealed := roomEvt
			if err := h.encryptRoomEvent(ctx, meta.ID, clientMsg, &sealed); err != nil {
				return nil, err
			}
			return &sealed, nil
		})
		return h.publishChannelThreadEvent(ctx, meta.ID, parentMsgID, meta.CrossSite, meta.CrossSiteAt, viewPayload, payload, fanOut)
	case model.RoomTypeDM, model.RoomTypeBotDM:
		// DM thread replies fan out to all members. The thread-sub mention badge is
		// owned by message-worker (markThreadMentions), so broadcast-worker doesn't
		// touch subscriptions here. lastMsgAt is intentionally NOT updated (would
		// wrongly mark hasUnread for non-participants).
		return h.publishDMEvents(ctx, &meta, clientMsg, evt.Timestamp, resolved.Accounts, model.RoomEventNewThreadMessage)
	default:
		slog.WarnContext(ctx, "unknown room type, skipping thread fan-out",
			"type", meta.Type,
			"room_id", meta.ID,
			"request_id", natsutil.RequestIDFromContext(ctx))
		return nil
	}
}

func (h *Handler) handleUpdated(ctx context.Context, evt *model.MessageEvent) error {
	msg := evt.Message
	if msg.EditedAt == nil || msg.UpdatedAt == nil {
		natsmetrics.MarkTerminalFromContext(ctx, natsmetrics.TerminalInvalidPayload)
		return errcode.Permanent(errcode.BadRequest("updated event missing EditedAt or UpdatedAt"))
	}

	if msg.IsHiddenThreadReply() {
		return h.handleThreadUpdated(ctx, evt)
	}

	room, err := h.store.GetRoom(ctx, msg.RoomID)
	if err != nil {
		return fmt.Errorf("fetch room %s: %w", msg.RoomID, err)
	}

	// Routing input for the cross-site relay only. The local badge write this
	// used to perform is roomlist-worker's now — broadcast-worker makes no
	// MongoDB writes, so a badge failure can no longer suppress the edit
	// reaching clients.
	parsed := mention.Parse(msg.Content)

	// Resolve mentionees once: the same participants render on the edit event
	// and route the cross-site badge, so we avoid a second FindUsersByAccounts.
	participants, mentionAll := h.resolveEditMentions(ctx, parsed)

	edit := buildEditRoomEvent(room, evt)
	edit.Mentions = participants
	edit.MentionAll = mentionAll
	if room.Type == model.RoomTypeChannel && h.encrypt {
		if err := h.encryptEditedContent(ctx, room.ID, &edit); err != nil {
			return fmt.Errorf("encrypt edit content for room %s: %w", room.ID, err)
		}
	}
	if err := h.publishMutation(ctx, room, model.RoomEventMessageEdited, msg.ID, &edit); err != nil {
		return err
	}
	// Routing runs after the client broadcast; an unresolved mentionee has no
	// site, so federateMentions simply relays nothing rather than failing the edit.
	h.federateMentions(ctx, room.ID, msg.ID, participants, *msg.EditedAt)
	return nil
}

// federateMentions relays the badge to each mentionee's home site, one event per
// destination. A failure is logged, never returned, so it can't NAK the message
// and re-broadcast it to clients.
func (h *Handler) federateMentions(ctx context.Context, roomID, msgID string, participants []model.Participant, at time.Time) {
	if h.publish == nil {
		return
	}
	// Lazily allocated: most messages have no remote mentionee. A participant with
	// no site is either unresolved or the synthetic @all entry — neither routes.
	var accountsBySite map[string][]string
	for i := range participants {
		p := &participants[i]
		if p.SiteID == "" || p.SiteID == h.siteID {
			continue
		}
		if accountsBySite == nil {
			accountsBySite = make(map[string][]string)
		}
		accountsBySite[p.SiteID] = append(accountsBySite[p.SiteID], p.Account)
	}
	if accountsBySite == nil {
		return
	}
	now := time.Now().UTC().UnixMilli()
	// One budget for the whole fan-out, not one per destination: with more sites
	// than slots the publishes run in batches, and an unbounded wait per batch
	// would let latency scale with the site count.
	fanoutCtx, cancel := context.WithTimeout(ctx, mentionFanoutTimeout)
	defer cancel()
	// Acquire before spawning so the live goroutine count, not just concurrency,
	// stays within the budget.
	var wg sync.WaitGroup
	var dropped int
	sem := make(chan struct{}, maxSiteFanout)
	for destSiteID, accounts := range accountsBySite {
		payload, err := sonic.Marshal(model.SubscriptionMentionEvent{
			RoomID:      roomID,
			Accounts:    accounts,
			MentionedAt: at.UnixMilli(),
			Timestamp:   now,
		})
		if err != nil {
			slog.ErrorContext(ctx, "marshal subscription_mention failed",
				"error", err, "room_id", roomID, "dest_site", destSiteID,
				"request_id", natsutil.RequestIDFromContext(ctx))
			continue
		}
		// at separates an edit from the send; an edit is its own canonical event
		// carrying its own request ID, so two same-ms edits can't collide.
		seed := fmt.Sprintf("%s:%s:%d", roomID, msgID, at.UnixMilli())
		dedupID := natsutil.InboxDedupID(ctx, destSiteID, seed)
		select {
		case sem <- struct{}{}:
		case <-fanoutCtx.Done():
			// Budget spent, so every remaining publish would fail on arrival.
			// Drop them here instead, and don't park on a slot that a publish
			// ignoring cancellation may never free.
			dropped++
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if err := outbox.Publish(fanoutCtx, h.publish, h.siteID, roomID, destSiteID,
				model.InboxSubscriptionMention, payload, dedupID, now); err != nil {
				slog.ErrorContext(ctx, "federate subscription_mention failed",
					"error", err, "room_id", roomID, "dest_site", destSiteID, "accounts", len(accounts),
					"request_id", natsutil.RequestIDFromContext(ctx))
			}
		}()
	}
	wg.Wait()
	if dropped > 0 {
		slog.ErrorContext(ctx, "subscription_mention fan-out budget exhausted, destinations dropped",
			"room_id", roomID, "dropped_sites", dropped, "timeout", mentionFanoutTimeout,
			"request_id", natsutil.RequestIDFromContext(ctx))
	}
}

// resolveEditMentions resolves parsed @-mentions to participants (account +
// display info) for the edit event, mirroring the create path so an edit-added
// mention renders like a fresh one. The event's mentions[] is best-effort
// enrichment, NOT the durable signal: the unread badge is set separately by
// roomlist-worker (deriveIntents, EventUpdated) and newContent still carries the
// raw @account, so on a
// user-lookup error we drop the mentions[] enrichment entirely (return nil)
// rather than emitting a partial set or failing/retrying the edit. nil when none.
// Returns the resolved participants and MentionAll. MentionAll is parse-derived and
// independent of the per-account lookup, so an edit that only adds @all (no individual
// mentions) — or one whose lookup fails — still carries the flag.
func (h *Handler) resolveEditMentions(ctx context.Context, parsed mention.ParseResult) ([]model.Participant, bool) {
	if len(parsed.Accounts) == 0 {
		return nil, parsed.MentionAll
	}
	users, err := h.userStore.FindUsersByAccounts(ctx, parsed.Accounts)
	if err != nil {
		slog.WarnContext(ctx, "user lookup failed resolving edit mentions, dropping edit mentions",
			"error", err, "request_id", natsutil.RequestIDFromContext(ctx))
		return nil, parsed.MentionAll
	}
	resolved := mention.ResolveFromParsed(parsed, usersByAccount(users))
	return resolved.Participants, resolved.MentionAll
}

func (h *Handler) handleThreadUpdated(ctx context.Context, evt *model.MessageEvent) error {
	msg := evt.Message
	if msg.EditedAt == nil || msg.UpdatedAt == nil {
		natsmetrics.MarkTerminalFromContext(ctx, natsmetrics.TerminalInvalidPayload)
		return errcode.Permanent(errcode.BadRequest("updated event missing EditedAt or UpdatedAt for thread reply"))
	}
	parentMsgID := msg.ThreadParentMessageID

	// GetRoom (not GetRoomMeta) so the DM/BotDM branch has room.Accounts for
	// fan-out. Fetched first so the routing decision is made before any
	// thread-follower lookup.
	room, err := h.store.GetRoom(ctx, msg.RoomID)
	if err != nil {
		return fmt.Errorf("get room %s: %w", msg.RoomID, err)
	}

	parsed := mention.Parse(msg.Content)
	edit := buildEditRoomEvent(room, evt)
	edit.Mentions, edit.MentionAll = h.resolveEditMentions(ctx, parsed)

	switch room.Type {
	case model.RoomTypeChannel:
		fanOut, err := h.channelThreadFanOut(ctx, room.ID, room.SiteID, parentMsgID, msg.UserAccount, parsed.Accounts, msg.ThreadParentMessageCreatedAt, evt.ThreadParentSenderAccount)
		if err != nil {
			return fmt.Errorf("channel thread fan-out for thread update of parent %s: %w", parentMsgID, err)
		}
		payload, err := sonic.Marshal(&edit)
		if err != nil {
			return fmt.Errorf("marshal thread edit event for parent %s: %w", parentMsgID, err)
		}
		viewPayload := h.sealThreadViewPayload(ctx, room.ID, payload, func() (any, error) {
			sealed := edit
			if err := h.encryptEditedContent(ctx, room.ID, &sealed); err != nil {
				return nil, err
			}
			return &sealed, nil
		})
		return h.publishChannelThreadEvent(ctx, room.ID, parentMsgID, room.CrossSite, room.CrossSiteAt, viewPayload, payload, fanOut)
	case model.RoomTypeDM, model.RoomTypeBotDM:
		// DM thread replies are visible to every member, so edits fan out to
		// all members (consistent with handleThreadCreated), not just thread
		// subscribers.
		return h.publishMutation(ctx, room, model.RoomEventMessageEdited, msg.ID, &edit)
	default:
		slog.WarnContext(ctx, "unknown room type, skipping thread update fan-out",
			"type", room.Type,
			"room_id", room.ID,
			"request_id", natsutil.RequestIDFromContext(ctx))
		return nil
	}
}

func (h *Handler) handleThreadDeleted(ctx context.Context, evt *model.MessageEvent) error {
	msg := evt.Message
	parentMsgID := msg.ThreadParentMessageID

	if msg.UpdatedAt == nil {
		natsmetrics.MarkTerminalFromContext(ctx, natsmetrics.TerminalInvalidPayload)
		return errcode.Permanent(errcode.BadRequest("missing UpdatedAt for thread message"))
	}

	// GetRoom first so the routing decision (thread followers vs all DM
	// members) is made from the authoritative room type and Accounts.
	room, err := h.store.GetRoom(ctx, msg.RoomID)
	if err != nil {
		return fmt.Errorf("get room %s: %w", msg.RoomID, err)
	}

	del := buildDeleteRoomEvent(room, evt)

	switch room.Type {
	case model.RoomTypeChannel:
		// Parse @-mentions from the deleted message so that non-follower
		// recipients who received the create event (via mention fan-out) also
		// receive the delete. Only the channel path uses mentions; the DM path
		// fans out to all members.
		parsed := mention.Parse(msg.Content)
		fanOut, err := h.channelThreadFanOut(ctx, room.ID, room.SiteID, parentMsgID, msg.UserAccount, parsed.Accounts, msg.ThreadParentMessageCreatedAt, evt.ThreadParentSenderAccount)
		if err != nil {
			return fmt.Errorf("channel thread fan-out for thread delete of parent %s: %w", parentMsgID, err)
		}
		payload, err := sonic.Marshal(&del)
		if err != nil {
			return fmt.Errorf("marshal thread delete event for parent %s: %w", parentMsgID, err)
		}
		// A delete carries ids and timestamps, no body, so both lanes share it.
		if err := h.publishChannelThreadEvent(ctx, room.ID, parentMsgID, room.CrossSite, room.CrossSiteAt, payload, payload, fanOut); err != nil {
			return fmt.Errorf("publish thread delete event for parent %s: %w", parentMsgID, err)
		}
	case model.RoomTypeDM, model.RoomTypeBotDM:
		// DM thread replies are visible to every member, so deletes fan out to
		// all members (consistent with handleThreadCreated), not just thread
		// subscribers.
		if err := h.publishMutation(ctx, room, model.RoomEventMessageDeleted, msg.ID, &del); err != nil {
			return fmt.Errorf("publish thread delete mutation for room %s message %s: %w", room.ID, msg.ID, err)
		}
	default:
		slog.WarnContext(ctx, "unknown room type, skipping thread delete fan-out",
			"type", room.Type,
			"room_id", room.ID,
			"request_id", natsutil.RequestIDFromContext(ctx))
		// No return: the badge update below is safe for all room types;
		// publishThreadMetadata handles unknown types by logging and skipping.
	}

	// Badge (tcount + tlm) update applies to all room types.
	if evt.NewTCount != nil {
		h.publishThreadBadge(ctx, room, *evt.NewTCount, evt.NewThreadLastMsgAt, parentMsgID, msg.ID, evt.Timestamp)
	}

	return nil
}

func (h *Handler) handleThreadTCountUpdated(ctx context.Context, evt *model.MessageEvent) error {
	if evt.NewTCount == nil {
		slog.WarnContext(ctx, "thread_reply_added event missing NewTCount, skipping",
			"messageID", evt.Message.ID,
			"request_id", natsutil.RequestIDFromContext(ctx))
		return nil
	}
	if evt.Message.ThreadParentMessageID == "" {
		slog.WarnContext(ctx, "thread_reply_added event missing ThreadParentMessageID, skipping",
			"messageID", evt.Message.ID,
			"request_id", natsutil.RequestIDFromContext(ctx))
		return nil
	}
	room, err := h.store.GetRoom(ctx, evt.Message.RoomID)
	if err != nil {
		return fmt.Errorf("get room %s: %w", evt.Message.RoomID, err)
	}
	return h.publishThreadMetadata(ctx, room, *evt.NewTCount, evt.NewThreadLastMsgAt,
		evt.Message.ThreadParentMessageID, evt.Message.ID,
		model.ThreadActionReplyAdded, evt.Timestamp)
}

func (h *Handler) publishThreadMetadata(ctx context.Context, room *model.Room, newTcount int, newTlm *time.Time,
	parentMsgID, replyMsgID string, action model.ThreadAction, eventTimestamp int64) error {
	labels := broadcastLabels(ctx)
	ctx = withBroadcastMetricLabels(ctx, roomKind(room.Type), labels.eventType)
	evt := model.ThreadMetadataUpdatedEvent{
		Type:               model.RoomEventThreadMetadataUpdated,
		RoomID:             room.ID,
		SiteID:             room.SiteID,
		ParentMessageID:    parentMsgID,
		ReplyMessageID:     replyMsgID,
		NewTCount:          newTcount,
		NewThreadLastMsgAt: newTlm,
		Action:             action,
		Timestamp:          time.Now().UTC().UnixMilli(),
		EventTimestamp:     eventTimestamp,
	}
	payload, err := sonic.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal thread metadata event for room %s: %w", room.ID, err)
	}
	switch room.Type {
	case model.RoomTypeChannel:
		if err := h.publishRoomEvent(ctx, room.ID, room.CrossSite, room.CrossSiteAt, payload, "thread metadata"); err != nil {
			return err
		}
	case model.RoomTypeDM, model.RoomTypeBotDM:
		for _, account := range room.Accounts {
			if isBot(account) {
				continue
			}
			if err := h.pub.Publish(ctx, subject.UserRoomEvent(account), payload); err != nil {
				return fmt.Errorf("publish thread metadata to DM member %s in room %s: %w", account, room.ID, err)
			}
		}
	default:
		slog.WarnContext(ctx, "unknown room type for thread metadata, skipping",
			"type", room.Type,
			"room_id", room.ID,
			"request_id", natsutil.RequestIDFromContext(ctx))
	}
	return nil
}

func (h *Handler) handleDeleted(ctx context.Context, evt *model.MessageEvent) error {
	msg := evt.Message
	if msg.UpdatedAt == nil {
		natsmetrics.MarkTerminalFromContext(ctx, natsmetrics.TerminalInvalidPayload)
		return errcode.Permanent(errcode.BadRequest("deleted event missing UpdatedAt"))
	}

	if msg.IsHiddenThreadReply() {
		return h.handleThreadDeleted(ctx, evt)
	}

	room, err := h.store.GetRoom(ctx, msg.RoomID)
	if err != nil {
		return fmt.Errorf("fetch room %s: %w", msg.RoomID, err)
	}

	del := buildDeleteRoomEvent(room, evt)
	if err := h.publishMutation(ctx, room, model.RoomEventMessageDeleted, msg.ID, &del); err != nil {
		return fmt.Errorf("publish delete mutation for room %s message %s: %w", room.ID, msg.ID, err)
	}
	// TShow=true thread replies appear in the main room (handled by publishMutation
	// above) but still count toward the thread's reply-count badge. Since
	// handleThreadDeleted is bypassed for TShow=true, we publish the badge update here.
	if msg.ThreadParentMessageID != "" && evt.NewTCount != nil {
		h.publishThreadBadge(ctx, room, *evt.NewTCount, evt.NewThreadLastMsgAt, msg.ThreadParentMessageID, msg.ID, evt.Timestamp)
	}
	return nil
}

// publishThreadBadge publishes a thread-metadata badge update for a deleted
// reply. Errors are logged but not returned: badge updates are best-effort and
// JetStream will redeliver the parent event on failure.
func (h *Handler) publishThreadBadge(ctx context.Context, room *model.Room, newTCount int, newTlm *time.Time, parentMsgID, replyMsgID string, timestamp int64) {
	if err := h.publishThreadMetadata(ctx, room, newTCount, newTlm, parentMsgID, replyMsgID, model.ThreadActionReplyDeleted, timestamp); err != nil {
		slog.ErrorContext(ctx, "publish thread badge for deleted reply failed",
			"error", err,
			"parentMessageID", parentMsgID,
			"request_id", natsutil.RequestIDFromContext(ctx))
	}
}

func (h *Handler) handlePinned(ctx context.Context, evt *model.MessageEvent) error {
	msg := evt.Message
	if msg.PinnedAt == nil {
		natsmetrics.MarkTerminalFromContext(ctx, natsmetrics.TerminalInvalidPayload)
		return errcode.Permanent(errcode.BadRequest("pinned event missing PinnedAt"))
	}

	room, err := h.store.GetRoom(ctx, msg.RoomID)
	if err != nil {
		return fmt.Errorf("fetch room %s: %w", msg.RoomID, err)
	}

	pin := model.PinStateRoomEvent{
		Type:           model.RoomEventMessagePinned,
		RoomID:         room.ID,
		SiteID:         room.SiteID,
		Timestamp:      time.Now().UTC().UnixMilli(),
		EventTimestamp: evt.Timestamp,
		MessageID:      msg.ID,
		Pinned:         true,
		By:             msg.PinnedBy,
		At:             *msg.PinnedAt,
	}
	return h.publishMutation(ctx, room, model.RoomEventMessagePinned, msg.ID, &pin)
}

func (h *Handler) handleUnpinned(ctx context.Context, evt *model.MessageEvent) error {
	msg := evt.Message
	// At comes from evt.Timestamp (set at publish): the canonical unpin
	// payload from history-service clears PinnedAt, so the message itself
	// carries no unpin timestamp.

	room, err := h.store.GetRoom(ctx, msg.RoomID)
	if err != nil {
		return fmt.Errorf("fetch room %s: %w", msg.RoomID, err)
	}

	unpin := model.PinStateRoomEvent{
		Type:           model.RoomEventMessageUnpinned,
		RoomID:         room.ID,
		SiteID:         room.SiteID,
		Timestamp:      time.Now().UTC().UnixMilli(),
		EventTimestamp: evt.Timestamp,
		MessageID:      msg.ID,
		Pinned:         false,
		By:             msg.PinnedBy,
		At:             time.UnixMilli(evt.Timestamp).UTC(),
	}
	return h.publishMutation(ctx, room, model.RoomEventMessageUnpinned, msg.ID, &unpin)
}

// handleReacted fans out a single-actor reaction delta to clients in the
// room. Reactions carry no content, so the encryption branch is skipped.
func (h *Handler) handleReacted(ctx context.Context, evt *model.MessageEvent) error {
	msg := evt.Message
	// Log-and-drop on malformed payloads: NAK would loop forever on a publisher contract violation.
	if evt.ReactionDelta == nil {
		natsmetrics.MarkTerminalFromContext(ctx, natsmetrics.TerminalInvalidPayload)
		slog.ErrorContext(ctx, "reacted event missing ReactionDelta; dropping",
			"messageID", msg.ID,
			"roomID", msg.RoomID,
			"siteID", evt.SiteID,
			"request_id", natsutil.RequestIDFromContext(ctx),
		)
		return nil
	}
	if msg.UpdatedAt == nil {
		natsmetrics.MarkTerminalFromContext(ctx, natsmetrics.TerminalInvalidPayload)
		slog.ErrorContext(ctx, "reacted event missing UpdatedAt; dropping",
			"messageID", msg.ID,
			"roomID", msg.RoomID,
			"siteID", evt.SiteID,
			"request_id", natsutil.RequestIDFromContext(ctx),
		)
		return nil
	}

	room, err := h.store.GetRoom(ctx, msg.RoomID)
	if err != nil {
		return fmt.Errorf("fetch room %s: %w", msg.RoomID, err)
	}

	react := model.ReactRoomEvent{
		Type:           model.RoomEventMessageReacted,
		RoomID:         room.ID,
		SiteID:         room.SiteID,
		Timestamp:      time.Now().UTC().UnixMilli(),
		EventTimestamp: evt.Timestamp,
		MessageID:      msg.ID,
		Shortcode:      evt.ReactionDelta.Shortcode,
		Action:         evt.ReactionDelta.Action,
		Actor:          evt.ReactionDelta.Actor,
		ReactedAt:      *msg.UpdatedAt,
		UpdatedAt:      *msg.UpdatedAt,
	}
	if err := h.publishMutation(ctx, room, model.RoomEventMessageReacted, msg.ID, &react); err != nil {
		return err
	}

	// Author notification: added + author != actor + non-empty author; publish failure swallowed.
	if evt.ReactionDelta.Action != model.ReactionActionAdded {
		return nil
	}
	authorAccount := msg.UserAccount
	if authorAccount == "" || authorAccount == evt.ReactionDelta.Actor.Account {
		return nil
	}
	notif := model.NotificationEvent{
		Type:          "reaction",
		RoomID:        msg.RoomID,
		RoomType:      room.Type,
		Message:       msg,
		ReactionDelta: evt.ReactionDelta,
		Timestamp:     time.Now().UTC().UnixMilli(),
	}
	data, marshalErr := sonic.Marshal(notif)
	if marshalErr != nil {
		slog.ErrorContext(ctx, "marshal reaction author notification failed",
			"error", marshalErr,
			"messageID", msg.ID,
			"roomID", msg.RoomID,
			"siteID", evt.SiteID,
			"request_id", natsutil.RequestIDFromContext(ctx),
		)
	} else if pubErr := h.pub.Publish(ctx, subject.Notification(authorAccount), data); pubErr != nil {
		natsmetrics.MarkTerminalFromContext(ctx, natsmetrics.TerminalPublishExhausted)
		slog.ErrorContext(ctx, "publish reaction author notification failed",
			"error", pubErr,
			"author", authorAccount,
			"messageID", msg.ID,
			"roomID", msg.RoomID,
			"siteID", evt.SiteID,
			"request_id", natsutil.RequestIDFromContext(ctx),
		)
	}
	return nil
}

// publishMutation marshals a flattened edit/delete event and routes it by room
// type: channel events go to the room stream, DM/botDM events fan out per
// non-bot member. evt must marshal to the wire payload for roomEvtType.
func (h *Handler) publishMutation(ctx context.Context, room *model.Room, roomEvtType model.RoomEventType, messageID string, evt any) error {
	labels := broadcastLabels(ctx)
	ctx = withBroadcastMetricLabels(ctx, roomKind(room.Type), labels.eventType)
	payload, err := sonic.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal %s event: %w", roomEvtType, err)
	}

	switch room.Type {
	case model.RoomTypeChannel:
		// Record the intended audience here too: publishChannelEvent does it for
		// new messages, and a mutation with no fanout sample would leave the
		// channel lane blank for edit/delete/pin/react during a campaign.
		h.metrics.Fanout(ctx, roomChannel, labels.eventType, room.UserCount)
		return h.publishRoomEvent(ctx, room.ID, room.CrossSite, room.CrossSiteAt, payload, fmt.Sprintf("%s event (message %s)", roomEvtType, messageID))

	case model.RoomTypeDM, model.RoomTypeBotDM:
		attempted, failed := 0, 0
		for _, account := range room.Accounts {
			if isBot(account) {
				continue
			}
			attempted++
			if err := h.pub.Publish(ctx, subject.UserRoomEvent(account), payload); err != nil {
				failed++
				slog.ErrorContext(ctx, "publish DM mutation event failed",
					"error", err,
					"type", roomEvtType,
					"account", account,
					"messageID", messageID,
					"room_id", room.ID,
					"request_id", natsutil.RequestIDFromContext(ctx),
				)
			}
		}
		h.metrics.Fanout(ctx, roomKind(room.Type), labels.eventType, attempted)
		if failed > 0 {
			natsmetrics.MarkTerminalFromContext(ctx, natsmetrics.TerminalPublishExhausted)
		}
		return nil

	default:
		slog.WarnContext(ctx, "unknown room type, skipping mutation fan-out",
			"type", room.Type,
			"room_id", room.ID,
			"request_id", natsutil.RequestIDFromContext(ctx))
		return nil
	}
}

func buildEditRoomEvent(room *model.Room, evt *model.MessageEvent) model.EditRoomEvent {
	msg := evt.Message
	return model.EditRoomEvent{
		Type:           model.RoomEventMessageEdited,
		RoomID:         room.ID,
		SiteID:         room.SiteID,
		Timestamp:      time.Now().UTC().UnixMilli(),
		EventTimestamp: evt.Timestamp,
		MessageID:      msg.ID,
		NewContent:     msg.Content,
		EditedBy:       msg.UserAccount,
		EditedAt:       *msg.EditedAt,
		UpdatedAt:      *msg.UpdatedAt,
		// Thread linkage so clients can tell a thread-reply edit from a top-level one.
		ThreadParentMessageID: msg.ThreadParentMessageID,
		TShow:                 msg.TShow,
		// Room's current preview after the edit; nil (thread reply / no eligible message) => omitted.
		PreviewMessage: evt.PreviewMessage,
	}
}

func buildDeleteRoomEvent(room *model.Room, evt *model.MessageEvent) model.DeleteRoomEvent {
	msg := evt.Message
	return model.DeleteRoomEvent{
		Type:           model.RoomEventMessageDeleted,
		RoomID:         room.ID,
		SiteID:         room.SiteID,
		Timestamp:      time.Now().UTC().UnixMilli(),
		EventTimestamp: evt.Timestamp,
		MessageID:      msg.ID,
		DeletedBy:      msg.UserAccount,
		DeletedAt:      *msg.UpdatedAt,
		UpdatedAt:      *msg.UpdatedAt,
		// Thread linkage so clients can tell a thread-reply delete from a top-level one.
		ThreadParentMessageID: msg.ThreadParentMessageID,
		TShow:                 msg.TShow,
		// Room's current preview after the delete; nil (thread reply / no eligible message) => omitted.
		PreviewMessage: evt.PreviewMessage,
	}
}

func (h *Handler) encryptEditedContent(ctx context.Context, roomID string, edited *model.EditRoomEvent) error {
	key, err := h.currentRoomKey(ctx, roomID)
	if err != nil {
		return fmt.Errorf("get encryption key for room %s: %w", roomID, err)
	}
	encrypted, err := h.encoder.Encode(roomID, edited.NewContent, key.KeyPair.PrivateKey, key.Version)
	if err != nil {
		return fmt.Errorf("encrypt edit content for room %s: %w", roomID, err)
	}
	encJSON, err := sonic.Marshal(encrypted)
	if err != nil {
		return fmt.Errorf("marshal encrypted edit content: %w", err)
	}
	edited.EncryptedNewContent = json.RawMessage(encJSON)
	edited.NewContent = ""
	return nil
}

// currentRoomKey fetches the room's encryption key, treating a missing key as
// an error (the room is configured for encryption but no key is provisioned).
func (h *Handler) currentRoomKey(ctx context.Context, roomID string) (*roomkeystore.VersionedKeyPair, error) {
	key, err := h.keyStore.Get(ctx, roomID)
	if err != nil {
		return nil, fmt.Errorf("get room key for room %s: %w", roomID, err)
	}
	if key == nil {
		return nil, fmt.Errorf("get room key for room %s: %w", roomID, errNoCurrentKey)
	}
	return key, nil
}

// encryptRoomEvent applies room encryption to evt if h.encrypt is true,
// replacing evt.Message with an EncryptedMessage envelope built from clientMsg.
func (h *Handler) encryptRoomEvent(ctx context.Context, roomID string, clientMsg *model.ClientMessage, evt *model.RoomEvent) error {
	if !h.encrypt {
		return nil
	}
	msgJSON, err := sonic.Marshal(clientMsg)
	if err != nil {
		return fmt.Errorf("marshal client message for room %s: %w", roomID, err)
	}
	key, err := h.currentRoomKey(ctx, roomID)
	if err != nil {
		return fmt.Errorf("get encryption key for room %s: %w", roomID, err)
	}
	encrypted, err := h.encoder.Encode(roomID, string(msgJSON), key.KeyPair.PrivateKey, key.Version)
	if err != nil {
		return fmt.Errorf("encrypt message for room %s: %w", roomID, err)
	}
	encJSON, err := sonic.Marshal(encrypted)
	if err != nil {
		return fmt.Errorf("marshal encrypted message for room %s: %w", roomID, err)
	}
	evt.EncryptedMessage = json.RawMessage(encJSON)
	evt.Message = nil
	return nil
}

func (h *Handler) publishChannelEvent(ctx context.Context, meta *roommetacache.Meta, clientMsg *model.ClientMessage, timestamp int64, mentionAll bool, mentions []model.Participant) error {
	evt := buildRoomEvent(meta, clientMsg, timestamp)
	evt.MentionAll = mentionAll
	if len(mentions) > 0 {
		evt.Mentions = mentions
	}
	if err := h.encryptRoomEvent(ctx, meta.ID, clientMsg, &evt); err != nil {
		return fmt.Errorf("encrypt channel event for room %s: %w", meta.ID, err)
	}
	payload, err := sonic.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal channel event: %w", err)
	}
	// flow: one room-stream publish; NATS fans out to subscribers downstream, so
	// this reports the room audience, not per-recipient deliveries from here.
	slog.Log(ctx, logctx.LevelFlow, "broadcast fan-out", "phase", "fanout",
		"request_id", natsutil.RequestIDFromContext(ctx), "room_id", meta.ID,
		"type", string(meta.Type), "delivery", "room-stream", "audience", meta.UserCount)
	h.metrics.Fanout(ctx, roomChannel, broadcastLabels(ctx).eventType, meta.UserCount)
	return h.publishRoomEvent(ctx, meta.ID, meta.CrossSite, meta.CrossSiteAt, payload, "channel event")
}

// publishRoomEvent fans a channel room's .event out via subject.RoomEventTargets — the
// sanctioned path enforced by .semgrep room-subject-publish-must-route (never inline a subject).
func (h *Handler) publishRoomEvent(ctx context.Context, roomID string, crossSite *bool, crossSiteAt *time.Time, payload []byte, op string) error {
	labels := broadcastLabels(ctx)
	ctx = withBroadcastMetricLabels(ctx, roomChannel, labels.eventType)
	now := time.Now().UTC()
	var pubErr error
	for _, subj := range subject.RoomEventTargets(roomID, crossSite, crossSiteAt, h.routeMode, now) {
		if err := h.pub.Publish(ctx, subj, payload); err != nil {
			pubErr = fmt.Errorf("publish %s for room %s to %s: %w", op, roomID, subj, err)
		}
	}
	return pubErr
}

// publishChannelThreadEvent delivers a channel thread event on both lanes.
// One call, so a new thread branch cannot serve followers but miss viewers.
// The lanes carry different copies: see sealThreadViewPayload.
func (h *Handler) publishChannelThreadEvent(ctx context.Context, roomID, parentMsgID string, crossSite *bool, crossSiteAt *time.Time, viewPayload, followerPayload []byte, fanOut []string) error {
	h.publishThreadViewEvent(ctx, roomID, parentMsgID, crossSite, crossSiteAt, viewPayload)
	return h.publishToThreadAccounts(ctx, fanOut, followerPayload, parentMsgID)
}

// sealThreadViewPayload returns the thread-subject copy of a thread event. That
// subject lives in the room namespace every authenticated client may subscribe
// to, so its body is sealed with the room key; the per-follower copy stays
// plaintext because chat.user.{account}.> is scoped to a single account.
//
// Returns nil when sealing fails, so the caller drops the view lane — a
// room-namespace subject must never carry a plaintext body.
func (h *Handler) sealThreadViewPayload(ctx context.Context, roomID string, plain []byte, seal func() (any, error)) []byte {
	if !h.encrypt {
		return plain
	}
	sealed, err := seal()
	if err == nil {
		var payload []byte
		if payload, err = sonic.Marshal(sealed); err == nil {
			return payload
		}
	}
	h.metrics.ThreadViewPublishFailed(ctx, broadcastLabels(ctx).eventType)
	slog.ErrorContext(ctx, "seal thread view event failed",
		"error", err,
		"room_id", roomID,
		"request_id", natsutil.RequestIDFromContext(ctx))
	return nil
}

// publishThreadViewEvent mirrors a thread event onto the subject open panels
// subscribe to. Never returns an error: a NAK would re-run the fan-out.
func (h *Handler) publishThreadViewEvent(ctx context.Context, roomID, parentMsgID string, crossSite *bool, crossSiteAt *time.Time, payload []byte) {
	if !h.threadViewSubject || parentMsgID == "" || len(payload) == 0 {
		return
	}
	eventType := broadcastLabels(ctx).eventType
	// Unlabelled, these land in the delivery counter's "unknown" room-kind.
	ctx = withBroadcastMetricLabels(ctx, roomThread, eventType)
	now := time.Now().UTC()
	for _, subj := range subject.RoomThreadEventTargets(roomID, parentMsgID, crossSite, crossSiteAt, h.routeMode, now) {
		if err := h.pub.Publish(ctx, subj, payload); err != nil {
			h.metrics.ThreadViewPublishFailed(ctx, eventType)
			slog.ErrorContext(ctx, "publish thread view event failed",
				"error", err,
				"subject", subj,
				"parentMessageID", parentMsgID,
				"room_id", roomID,
				"request_id", natsutil.RequestIDFromContext(ctx))
		}
	}
}

// debugFlowFanout emits the flow-rung outcome of a per-recipient fan-out:
// recipients = individual deliveries attempted in this hop, failed = how many of
// those errored (delivered = recipients - failed). Metadata only. The room-stream
// (channel) path is NOT per-recipient — it reports `audience` inline instead.
func debugFlowFanout(ctx context.Context, roomID, roomType, delivery string, recipients, failed int) {
	slog.Log(ctx, logctx.LevelFlow, "broadcast fan-out", "phase", "fanout",
		"request_id", natsutil.RequestIDFromContext(ctx), "room_id", roomID,
		"type", roomType, "delivery", delivery, "recipients", recipients, "failed", failed)
}

// debugTraceDelivered emits the trace-rung per-recipient delivery line — the
// "did it reach user X?" detail. Recipient account identifiers are permitted at
// trace (never message content); off unless a request is flagged trace.
func debugTraceDelivered(ctx context.Context, account, roomID string) {
	slog.Log(ctx, logctx.LevelTrace, "broadcast delivered",
		"request_id", natsutil.RequestIDFromContext(ctx), "account", account, "room_id", roomID)
}

func (h *Handler) publishDMEvents(ctx context.Context, meta *roommetacache.Meta, clientMsg *model.ClientMessage, timestamp int64, mentionedAccounts []string, roomEventType model.RoomEventType) error {
	labels := broadcastLabels(ctx)
	ctx = withBroadcastMetricLabels(ctx, roomKind(meta.Type), labels.eventType)
	// Cache-fronted: a DM's membership is fixed at its two participants for the
	// room's lifetime, so TTL staleness cannot misroute here — and a warm entry
	// keeps DMs flowing when Mongo is down.
	subs, err := h.store.ListRoomMembers(ctx, meta.ID)
	if err != nil {
		return fmt.Errorf("list members for DM room %s: %w", meta.ID, err)
	}

	mentionSet := make(map[string]struct{}, len(mentionedAccounts))
	for _, name := range mentionedAccounts {
		mentionSet[name] = struct{}{}
	}

	recipients, failed := 0, 0
	for i := range subs {
		account := subs[i].Account
		// Skip bots: live UI events go to human clients only, consistent with
		// publishMutation and publishThreadMetadata. Bots receive messages via
		// their own server-side integration, not the websocket event channel.
		if isBot(account) {
			continue
		}
		_, hasMention := mentionSet[account]

		evt := buildRoomEvent(meta, clientMsg, timestamp)
		evt.Type = roomEventType
		evt.HasMention = hasMention

		payload, err := sonic.Marshal(evt)
		if err != nil {
			return fmt.Errorf("marshal DM event for user %s: %w", account, err)
		}
		recipients++
		// Publish errors are intentionally swallowed here (log-and-continue). DM thread
		// replies have no JetStream retry guarantee by design — the DM path uses
		// publishDMEvents which is fire-and-forget, consistent with how all DM fan-out
		// works in this service (publishMutation). Channel thread events propagate errors
		// via publishToThreadAccounts so JetStream can redeliver.
		if err := h.pub.Publish(ctx, subject.UserRoomEvent(account), payload); err != nil {
			slog.ErrorContext(ctx, "publish DM event failed",
				"error", err,
				"account", account,
				"room_id", meta.ID,
				"request_id", natsutil.RequestIDFromContext(ctx))
			failed++
			continue // don't emit a "delivered" trace for a failed publish
		}
		debugTraceDelivered(ctx, account, meta.ID)
	}
	debugFlowFanout(ctx, meta.ID, string(meta.Type), "per-member", recipients, failed)
	h.metrics.Fanout(ctx, roomKind(meta.Type), labels.eventType, recipients)
	if failed > 0 {
		natsmetrics.MarkTerminalFromContext(ctx, natsmetrics.TerminalPublishExhausted)
	}
	return nil
}

func buildRoomEvent(meta *roommetacache.Meta, clientMsg *model.ClientMessage, eventTimestamp int64) model.RoomEvent {
	return model.RoomEvent{
		Type:           model.RoomEventNewMessage,
		RoomID:         meta.ID,
		Timestamp:      time.Now().UTC().UnixMilli(),
		EventTimestamp: eventTimestamp,
		RoomName:       meta.Name,
		RoomType:       meta.Type,
		SiteID:         meta.SiteID,
		UserCount:      meta.UserCount,
		LastMsgAt:      clientMsg.CreatedAt,
		LastMsgID:      clientMsg.ID,
		Message:        clientMsg,
	}
}

func buildClientMessage(msg *model.Message, userMap map[string]model.User) *model.ClientMessage {
	sender := model.Participant{
		UserID:  msg.UserID,
		Account: msg.UserAccount,
	}
	if u, ok := userMap[msg.UserAccount]; ok {
		sender.ChineseName = u.ChineseName
		sender.EngName = u.EngName
	} else {
		sender.ChineseName = msg.UserAccount
		sender.EngName = msg.UserAccount
	}
	decoded, _ := cassandra.DecodeAttachments(msg.Attachments)
	cm := &model.ClientMessage{
		Message:     *msg,
		Sender:      &sender,
		Attachments: decoded,
	}
	// The embedded Message.Attachments (raw [][]byte) is an internal transport
	// detail; cm.Attachments (decoded) is the sole client-facing form. Null the
	// raw copy so the object carries one representation. Safe: Message: *msg is a
	// value copy, so reassigning this slice header does not touch the caller's *msg.
	cm.Message.Attachments = nil
	// The quoted parent arrives already-decoded on the canonical wire (the
	// gatekeeper projects its DecodedAttachments into the snapshot), so no decode
	// is needed here — it rides along via the embedded Message: *msg copy.
	return cm
}

// publishToThreadAccounts publishes payload concurrently to every account in
// the list. Only returns an error (triggering JetStream redelivery) when every
// publish fails — partial failure is tolerated to avoid duplicate delivery to
// accounts that already received the event on the first attempt.
func (h *Handler) publishToThreadAccounts(ctx context.Context, accounts []string, payload []byte, parentMsgID string) error {
	labels := broadcastLabels(ctx)
	ctx = withBroadcastMetricLabels(ctx, roomThread, labels.eventType)
	h.metrics.Fanout(ctx, roomThread, labels.eventType, len(accounts))
	if len(accounts) == 0 {
		return nil
	}
	var wg sync.WaitGroup
	var failCount atomic.Int64
	for _, account := range accounts {
		account := account
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := h.pub.Publish(ctx, subject.UserRoomEvent(account), payload); err != nil {
				slog.ErrorContext(ctx, "publish thread event failed",
					"error", err,
					"account", account,
					"parentMessageID", parentMsgID,
					"request_id", natsutil.RequestIDFromContext(ctx))
				failCount.Add(1)
				return
			}
			debugTraceDelivered(ctx, account, parentMsgID)
		}()
	}
	wg.Wait()
	debugFlowFanout(ctx, parentMsgID, "thread", "per-follower", len(accounts), int(failCount.Load()))
	if failCount.Load() == int64(len(accounts)) {
		return fmt.Errorf("all %d thread account publishes failed for parent %s", len(accounts), parentMsgID)
	}
	if failCount.Load() > 0 {
		natsmetrics.MarkTerminalFromContext(ctx, natsmetrics.TerminalPublishExhausted)
	}
	return nil
}

func roomKind(roomType model.RoomType) roomKindLabel {
	switch roomType {
	case model.RoomTypeChannel:
		return roomChannel
	case model.RoomTypeDM:
		return roomDM
	case model.RoomTypeBotDM:
		return roomBotDM
	default:
		return roomUnknown
	}
}

// threadFanOutAccounts builds the deduplicated fan-out recipient list for a
// thread event. The message sender is always included first (unless a bot):
// they authored the reply and are therefore a thread participant, so their own
// devices must receive the event for multi-device sync. The sender is added
// directly here rather than relied upon via replyAccounts — replyAccounts is
// written by message-worker on a separate, unordered MESSAGES-CANONICAL
// consumer, so a fan-out that depended on it would race the sender's own first
// reply and silently drop the echo. followers (thread repliers) and
// extraAccounts (@-mentioned users) are merged after, deduped. Bots are always
// excluded.
func threadFanOutAccounts(senderAccount, parentSenderAccount string, followers map[string]struct{}, extraAccounts []string) []string {
	seen := map[string]struct{}{}
	var fanOut []string
	add := func(acc string) {
		if acc == "" {
			return
		}
		if _, ok := seen[acc]; ok {
			return
		}
		if isBot(acc) {
			return
		}
		seen[acc] = struct{}{}
		fanOut = append(fanOut, acc)
	}
	add(senderAccount)       // reply author — thread participant, include race-free
	add(parentSenderAccount) // parent author — thread owner, always included, race-free
	for acc := range followers {
		add(acc)
	}
	for _, acc := range extraAccounts {
		add(acc)
	}
	return fanOut
}

// allowedThreadMentions filters mentions to room members whose history window admits
// the thread parent (mentionVisible); non-members (absent from the window map) are
// excluded. Returns nil for empty input.
func (h *Handler) allowedThreadMentions(ctx context.Context, roomID string, mentions []string, parentCreatedAt *time.Time) ([]string, error) {
	if len(mentions) == 0 {
		return nil, nil
	}
	windows, err := h.store.GetHistorySharedSince(ctx, roomID, mentions)
	if err != nil {
		return nil, fmt.Errorf("get history windows for room %s: %w", roomID, err)
	}
	allowed := make([]string, 0, len(mentions))
	for _, acc := range mentions {
		hss, isMember := windows[acc]
		// Exclude non-members (no room subscription) outright; keep a member only when
		// their history window admits the thread's parent.
		if !isMember || !mentionVisible(hss, parentCreatedAt) {
			continue
		}
		allowed = append(allowed, acc)
	}
	return allowed, nil
}

// channelThreadFanOut builds the deduplicated channel recipient set: the reply sender
// + the parent author (both included for multi-device sync / thread ownership, race-free)
// + the parent's thread followers + history-gated @-mentions, bots excluded.
//
// The parent's CreatedAt (gate) and author account (recipient) come from the event
// when the gatekeeper resolved them on the send path (eventParentCreatedAt != nil &&
// eventParentSenderAccount != "") — skipping the history-service round-trip. When either
// is absent (edit/delete canonical events bypass the gatekeeper, or a gatekeeper
// soft-fail) both are fetched from history-service; a fetch error is returned so the
// caller NAKs and JetStream redelivers. The gate lives here so no thread handler can
// bypass it.
func (h *Handler) channelThreadFanOut(ctx context.Context, roomID, siteID, parentMsgID, sender string, mentions []string, eventParentCreatedAt *time.Time, eventParentSenderAccount string) ([]string, error) {
	parent := &ParentMessageInfo{}
	if eventParentCreatedAt != nil && eventParentSenderAccount != "" {
		parent.CreatedAt = *eventParentCreatedAt
		parent.SenderAccount = eventParentSenderAccount
	} else {
		fetched, err := h.parentFetcher.FetchParent(ctx, sender, roomID, siteID, parentMsgID)
		if err != nil {
			// historyParentFetcher propagates the typed remote error precisely so it
			// can be classified here. A not_found/forbidden parent is retried once —
			// a bot can post a parent and reply to it before bot-message-worker has
			// written the parent to Cassandra — and then dropped: past that the
			// parent is genuinely absent, and spending the rest of MaxDeliver on it
			// would hold an ack-pending slot for 66s per message, which is what
			// fills the consumer's budget and stops it consuming anything at all.
			if ee, terminal := errcode.Terminal(err); terminal && parentResolveExhausted(ctx) {
				natsmetrics.MarkTerminalFromContext(ctx, natsmetrics.TerminalPermanent)
				return nil, errcode.Permanent(ee)
			}
			return nil, fmt.Errorf("fetch thread parent %s: %w", parentMsgID, err)
		}
		parent = fetched
	}
	allowed, err := h.allowedThreadMentions(ctx, roomID, mentions, &parent.CreatedAt)
	if err != nil {
		return nil, err
	}
	followers, err := h.store.GetThreadFollowers(ctx, parentMsgID)
	if err != nil {
		return nil, fmt.Errorf("get thread followers for parent %s: %w", parentMsgID, err)
	}
	return threadFanOutAccounts(sender, parent.SenderAccount, followers, allowed), nil
}

// usersByAccount indexes a slice of users by their Account for O(1) lookup
// during mention resolution and client-message enrichment.
func usersByAccount(users []model.User) map[string]model.User {
	byAccount := make(map[string]model.User, len(users))
	for i := range users {
		byAccount[users[i].Account] = users[i]
	}
	return byAccount
}

// parentResolveAttempts caps how many deliveries are spent retrying a thread
// parent that history-service reports as absent. The race it covers is
// milliseconds (a bot replying to its own just-posted parent), so the full
// MaxDeliver budget buys nothing and costs a lot: on LowLatencyBackoff each
// waiting reply holds its ack-pending slot for 66s, and ~15 of them per second
// fills the budget and stalls the whole site's fan-out.
const parentResolveAttempts = 2

// parentResolveExhausted reports whether the parent-resolution retry budget is
// spent for this delivery. An untracked context or unreadable metadata reports
// false, so a missing count retries rather than dropping a recoverable event.
func parentResolveExhausted(ctx context.Context) bool {
	attempt, ok := natsmetrics.DeliveryAttemptFromContext(ctx)
	return ok && attempt >= parentResolveAttempts
}
