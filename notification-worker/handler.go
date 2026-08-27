package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/bytedance/sonic"
	"go.opentelemetry.io/otel"
	"golang.org/x/sync/errgroup"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/mention"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsmetrics"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/obs"
	"github.com/hmchangw/chat/pkg/roommetacache"
	"github.com/hmchangw/chat/pkg/roomsubcache"
)

// defaultRecipientBatchSize mirrors PUSH_RECIPIENT_BATCH_SIZE's envDefault so unit tests don't re-declare it.
const defaultRecipientBatchSize = 100

// defaultBadgeBatchSize caps accounts per badge.count.batch RPC. The badge
// audience is the whole membership minus sender/muted/restricted — the
// large-room push throttle does not narrow it — so without a cap a 5k-member
// room sends one 5k-account request per message, and user-service recomputes
// unreadRooms per account on every cache miss behind it.
const defaultBadgeBatchSize = 512

// defaultBadgeConcurrency bounds in-flight badge RPCs across all chunks and sites.
const defaultBadgeConcurrency = 8

// maxMentionLookups caps how many distinct @accounts one message may resolve.
// Not an env knob: it bounds a user-controlled fan-out rather than tuning a
// workload, and no legible push body renders this many names.
const maxMentionLookups = 50

// MemberCache reads the cached member list and supports targeted invalidation.
type MemberCache interface {
	GetMembers(ctx context.Context, roomID string) ([]roomsubcache.Member, error)
	Invalidate(ctx context.Context, roomID string)
}

// RoomMetaGetter returns cached room metadata so push-notification-service doesn't hit Mongo.
type RoomMetaGetter interface {
	Get(ctx context.Context, roomID string) (roommetacache.Meta, error)
}

// HandlerDeps groups the handler's collaborators.
type HandlerDeps struct {
	Members            MemberCache
	Followers          ThreadFollowerLister
	Parent             ParentFetcher // resolves a thread's parent author + createdAt from history-service
	Presence           PresenceSnapshotter
	Settings           UserSettingsSnapshotter // nil → noopUserSettings (pre-enforcement behaviour)
	Hook               Vetoer
	Emitter            Emitter
	RoomMeta           RoomMetaGetter      // nil → title falls back to sender.Account
	MentionNames       MentionNameResolver // nil → only @all/@here are substituted in the body
	BadgeClient        badgeClient         // nil (env-disabled or not wired) → badge phase skipped entirely (Phase A compat)
	BadgeBatchSize     int                 // accounts per badge RPC (≥ 1); 0 → defaultBadgeBatchSize
	BadgeConcurrency   int                 // in-flight badge RPCs (≥ 1); 0 → defaultBadgeConcurrency
	LargeRoomThreshold int
	RecipientBatchSize int                  // per-event cap (≥ 1); 0 → defaultRecipientBatchSize
	Metrics            *notificationMetrics // nil → built on the global meter
}

// Handler runs the per-message fan-out pipeline: exclusion filters, hook veto, EligibleForPush
// routing, then the settings- and presence-gated shouldPush — one Emitter.Emit per surviving recipient.
type Handler struct {
	deps    HandlerDeps
	metrics *notificationMetrics
}

// isNotifiable reports whether a message type produces push notifications.
// Every system type is gated out; the empty regular type and client-set types
// (e.g. MessageTypeImportant) notify like a normal message. New system types are
// safe-by-default as long as they join IsSystemMessageType (the single membership list).
func isNotifiable(msgType string) bool {
	return !model.IsSystemMessageType(msgType)
}

func NewHandler(deps HandlerDeps) *Handler { //nolint:gocritic // hugeParam: one-time constructor arg
	if deps.LargeRoomThreshold <= 0 {
		deps.LargeRoomThreshold = 500
	}
	if deps.RecipientBatchSize <= 0 {
		deps.RecipientBatchSize = defaultRecipientBatchSize
	}
	if deps.BadgeBatchSize <= 0 {
		deps.BadgeBatchSize = defaultBadgeBatchSize
	}
	if deps.BadgeConcurrency <= 0 {
		deps.BadgeConcurrency = defaultBadgeConcurrency
	}
	if deps.Settings == nil {
		deps.Settings = noopUserSettings{}
	}
	if deps.Metrics == nil {
		deps.Metrics = newNotificationMetrics(otel.Meter("notification-worker"))
	}
	return &Handler{deps: deps, metrics: deps.Metrics}
}

func (h *Handler) HandleMessage(ctx context.Context, data []byte) (retErr error) {
	outcome := notifySuppressed
	defer func() {
		if retErr != nil && outcome == notifySuppressed {
			outcome = notifyFailed
		}
		h.metrics.Record(ctx, notifyKindPush, outcome)
	}()
	var evt model.MessageEvent
	if err := sonic.Unmarshal(data, &evt); err != nil {
		natsmetrics.MarkTerminalFromContext(ctx, natsmetrics.TerminalInvalidPayload)
		// Malformed payload — it will never parse on redelivery. Mark permanent so the caller Acks (drops) it instead of retrying until MaxDeliver.
		return errcode.Permanent(errcode.BadRequest("malformed message event"))
	}
	ctx = obs.ContextWithIdentity(ctx, evt.Message.UserAccount, evt.Message.RoomID, evt.SiteID)
	// Non-created events are filtered at the broker; defensive backstop only.
	if evt.Event != model.EventCreated && evt.Event != "" {
		return nil
	}
	msg := evt.Message

	// Phase 1 — side effects: member-change sys-messages invalidate the member cache (Option C; safe because room-worker guards add/remove to channels).
	switch msg.Type {
	case model.MessageTypeMembersAdded, model.MessageTypeMemberLeft, model.MessageTypeMemberRemoved:
		h.deps.Members.Invalidate(ctx, msg.RoomID)
	}

	// Phase 2 — notification gate: only regular types push; every system type (current and future) is non-notifying. See isNotifiable.
	if !isNotifiable(msg.Type) {
		return nil
	}

	members, err := h.deps.Members.GetMembers(ctx, msg.RoomID)
	if err != nil {
		return fmt.Errorf("get members for room %s: %w", msg.RoomID, err)
	}
	if len(members) == 0 {
		return nil
	}

	mentionInfo := mention.Parse(msg.Content)
	mentionedAccounts := mentionedSet(mentionInfo)
	// @here is deliberately NOT a push trigger — the legacy frontend doesn't render it.
	mentionsAll := mentionInfo.MentionAll
	isLargeRoom := len(members) > h.deps.LargeRoomThreshold
	isThreadOnlyReply := msg.ThreadParentMessageID != "" && !msg.TShow

	var followers map[string]struct{}
	// parentCreatedAt/parentSenderAccount feed the suppression gate; use gatekeeper-carried values
	// when present, else fetch from history-service (parent pre-exists, so the fetch is race-free).
	var parentCreatedAt *time.Time
	var parentSenderAccount string
	if isThreadOnlyReply {
		// A clean thread_rooms miss returns empty followers + nil error (first-reply race); an actual
		// Mongo failure must NAK rather than silently ack and drop follower-only recipients.
		info, ferr := h.deps.Followers.Lookup(ctx, msg.ThreadParentMessageID)
		if ferr != nil {
			return fmt.Errorf("lookup thread room for parent %s: %w", msg.ThreadParentMessageID, ferr)
		}
		followers = info.Followers
		if msg.ThreadParentMessageCreatedAt != nil && evt.ThreadParentSenderAccount != "" {
			parentCreatedAt = msg.ThreadParentMessageCreatedAt
			parentSenderAccount = evt.ThreadParentSenderAccount
		} else {
			// The reply sender can always read the parent they replied to; fetch on their behalf.
			parent, perr := h.deps.Parent.FetchParent(ctx, msg.UserAccount, msg.RoomID, evt.SiteID, msg.ThreadParentMessageID)
			if perr != nil {
				return fmt.Errorf("fetch thread parent %s: %w", msg.ThreadParentMessageID, perr)
			}
			pc := parent.CreatedAt
			parentCreatedAt = &pc
			parentSenderAccount = parent.SenderAccount
		}
	}

	roomType := members[0].RoomType

	// Sender display name is composed by message-gatekeeper at write time; no per-message lookup here.
	sender := &model.Participant{
		UserID:      msg.UserID,
		Account:     msg.UserAccount,
		DisplayName: msg.SenderDisplayName(),
	}

	// Two audiences fall out of the member pipeline:
	// - badgeAccounts: everyone whose unread state changes (past the
	//   sender/muted/restricted/thread-scope filters) — bumped even when not
	//   pushed, or cached counts go stale.
	// - candidates → survivors: push recipients, further filtered by hook
	//   veto, EligibleForPush, and presence.
	candidates := make([]roomsubcache.Member, 0, len(members))
	accounts := make([]string, 0, len(members))
	badgeAccounts := make([]string, 0, len(members))
	siteByAccount := make(map[string]string, len(members))
	for i := range members {
		m := members[i]
		if m.ID == msg.UserID {
			continue
		}
		if m.Muted {
			continue
		}
		if isRestricted(m, &msg, isThreadOnlyReply, parentCreatedAt) {
			continue
		}

		mentioned := mentionsAll || mentionedAccounts[m.Account]

		if isThreadOnlyReply {
			_, follows := followers[m.Account]
			// The parent author is always notified of replies to their own thread, even before thread_rooms
			// exists; the restricted-room gate still applies but never excludes them (they authored the parent).
			if m.Account == parentSenderAccount {
				follows = true
			}
			if !follows && !mentioned {
				continue
			}
		}

		badgeAccounts = append(badgeAccounts, m.Account)
		siteByAccount[m.Account] = m.HomeSiteID

		// Push-only filters from here down.
		// Stage 2: hook veto (fail-open on error).
		allow, herr := h.deps.Hook.Allow(ctx, &msg, m)
		if herr != nil {
			slog.Warn("hook errored, allowing", "error", herr, "account", m.Account,
				"request_id", natsutil.RequestIDFromContext(ctx))
			allow = true
		}
		if !allow {
			continue
		}

		if !EligibleForPush(&m, roomType, isLargeRoom, mentioned) {
			continue
		}

		candidates = append(candidates, m)
		accounts = append(accounts, m.Account)
	}
	if len(badgeAccounts) == 0 {
		return nil
	}

	// Settings, presence and the badge fan-out are mutually independent, so they run
	// concurrently: the critical path is the slowest of the three rather than their
	// sum. Settings and presence run over the narrowed candidate set — only accounts
	// that survived the exclusion filters, never every member of a large room
	// (TestHandle_SettingsFetchedOnlyForSurvivingCandidates pins that narrowing) —
	// while the badge phase runs over the FULL badge audience, since bumps must land
	// even for members who won't be pushed.
	var (
		settings     map[string]notifSettings
		snapshot     map[string]model.Presence
		unreadCounts map[string]int
		lookups      errgroup.Group
	)
	lookups.Go(func() error {
		settings, _ = h.deps.Settings.Snapshot(ctx, accounts) // fail-open: error → empty map
		return nil
	})
	lookups.Go(func() error {
		snapshot, _ = h.deps.Presence.Snapshot(ctx, accounts) // fail-open: error → empty map
		return nil
	})
	lookups.Go(func() error {
		unreadCounts = h.fetchUnreadCounts(ctx, msg.RoomID, badgeAccounts, siteByAccount)
		return nil
	})
	_ = lookups.Wait() // every lookup above fails open and returns nil

	// shouldPush combines settings and presence, keyed on the sender's account for the priority pierce.
	// Sort survivors so batch N has a deterministic account set across redeliveries — required for the {messageID}-b{N} Nats-Msg-Id to dedup correctly.
	survivors := make([]string, 0, len(candidates))
	for _, c := range candidates {
		ns := settings[c.Account]
		if !shouldPush(snapshot[c.Account], ns, ns.isPriority(msg.UserAccount)) {
			continue
		}
		survivors = append(survivors, c.Account)
	}
	sort.Strings(survivors)

	// Only survivors ride the push payload; unreadCounts already covers the wider audience.
	if len(survivors) == 0 {
		return nil
	}

	// Title and body are independent cache/Mongo reads, so they overlap too. Both stay
	// behind the survivor gate: a message nobody receives must never pay for them.
	var title, body string
	var render errgroup.Group
	render.Go(func() error {
		title = h.resolveTitle(ctx, msg.RoomID, roomType, sender)
		return nil
	})
	render.Go(func() error {
		body = h.resolveBody(ctx, msg.Content, mentionInfo)
		return nil
	})
	_ = render.Wait() // both resolvers fail open internally and return nil

	now := time.Now().UTC()
	// Template carries fields shared across every batch — only ID and Accounts change per batch.
	pushEvt := model.PushNotificationEvent{
		RoomID: msg.RoomID,
		Title:  title,
		Body:   body,
		Data: model.PushNotificationData{
			RoomID:            msg.RoomID,
			MessageID:         msg.ID,
			Type:              shortRoomType(roomType),
			Sender:            sender,
			ThreadMessageID:   msg.ThreadParentMessageID,
			PushTime:          now.Format(time.RFC3339),
			AlsoSendToChannel: msg.TShow,
		},
		Timestamp: now.UnixMilli(),
	}

	batchSize := h.deps.RecipientBatchSize
	// Aggregate per-batch errors so one bad batch doesn't punish the others; still return an error
	// so the caller naks and redelivers ({messageId}-b{N} dedup protects already-succeeded batches).
	var emitErrs []error
	for i, batchIdx := 0, 0; i < len(survivors); i, batchIdx = i+batchSize, batchIdx+1 {
		end := i + batchSize
		if end > len(survivors) {
			end = len(survivors)
		}
		batchAccounts := make([]string, end-i)
		copy(batchAccounts, survivors[i:end])

		evt := pushEvt
		evt.ID = fmt.Sprintf("%s-b%d", msg.ID, batchIdx)
		evt.Accounts = batchAccounts
		if counts := filterUnreadCounts(unreadCounts, batchAccounts); len(counts) > 0 {
			evt.UnreadCounts = counts
		}
		if err := h.deps.Emitter.Emit(ctx, evt); err != nil {
			outcome = notifyPublishFailed
			slog.Error("emit push batch failed", "error", err, "batch", batchIdx,
				"recipients", len(batchAccounts), "messageId", msg.ID,
				"request_id", natsutil.RequestIDFromContext(ctx))
			emitErrs = append(emitErrs, fmt.Errorf("emit push batch %d: %w", batchIdx, err))
		}
	}
	if len(emitErrs) > 0 {
		return fmt.Errorf("emit push batches for message %s: %w", msg.ID, errors.Join(emitErrs...))
	}
	outcome = notifySent
	return nil
}

// fetchUnreadCounts groups the badge audience by home site (Member.HomeSiteID,
// not the room's site), splits each site's accounts into BadgeBatchSize chunks
// and issues the badge.count.batch RPCs concurrently under a BadgeConcurrency
// bound, merged into one account → count map. Fail-open throughout: nil
// BadgeClient is a no-op, an unknown home site skips the account, and a
// per-chunk RPC failure just leaves that chunk's accounts out of the result.
func (h *Handler) fetchUnreadCounts(ctx context.Context, roomID string, accounts []string, siteByAccount map[string]string) map[string]int {
	if h.deps.BadgeClient == nil {
		return nil
	}

	bySite := make(map[string][]string)
	for _, account := range accounts {
		siteID := siteByAccount[account]
		if siteID == "" {
			continue
		}
		bySite[siteID] = append(bySite[siteID], account)
	}
	if len(bySite) == 0 {
		return nil
	}

	var (
		mu     sync.Mutex
		merged = make(map[string]int, len(accounts))
		g      errgroup.Group
	)
	g.SetLimit(h.deps.BadgeConcurrency)
	for siteID, siteAccounts := range bySite {
		for _, chunk := range chunkStrings(siteAccounts, h.deps.BadgeBatchSize) {
			g.Go(func() error {
				counts, err := h.deps.BadgeClient.Counts(ctx, siteID, roomID, chunk)
				if err != nil {
					slog.WarnContext(ctx, "badge count batch RPC failed, accounts publish without counts",
						"error", err, "siteId", siteID, "roomId", roomID, "accounts", len(chunk),
						"request_id", natsutil.RequestIDFromContext(ctx))
					return nil // never fail the push on a badge-count failure
				}
				mu.Lock()
				for account, n := range counts {
					merged[account] = n
				}
				mu.Unlock()
				return nil
			})
		}
	}
	_ = g.Wait() // every g.Go above always returns nil — errors are logged and absorbed per-chunk
	return merged
}

// filterUnreadCounts returns the subset of unreadCounts whose keys are in batchAccounts,
// so each outgoing batch only carries counts for the accounts it actually addresses.
func filterUnreadCounts(unreadCounts map[string]int, batchAccounts []string) map[string]int {
	if len(unreadCounts) == 0 {
		return nil
	}
	filtered := make(map[string]int, len(batchAccounts))
	for _, account := range batchAccounts {
		if n, ok := unreadCounts[account]; ok {
			filtered[account] = n
		}
	}
	return filtered
}

// mentionedSet returns mentioned accounts as a set for O(1) per-recipient lookup.
// msg.Mentions is not populated by message-gatekeeper, so only Parse output is used.
func mentionedSet(parsed mention.ParseResult) map[string]bool {
	out := make(map[string]bool, len(parsed.Accounts))
	for _, a := range parsed.Accounts {
		out[a] = true
	}
	return out
}

// isRestricted filters members who joined after the message timestamp (the parent's
// createdAt for thread-only replies); a nil parentCreatedAt suppresses, not leaks.
func isRestricted(m roomsubcache.Member, msg *model.Message, isThreadOnlyReply bool, parentCreatedAt *time.Time) bool { //nolint:gocritic // hugeParam: Member stays by value; msg is a pointer to avoid a per-member copy
	if m.HistorySharedSince == nil {
		return false
	}
	if isThreadOnlyReply {
		if parentCreatedAt == nil {
			return true
		}
		return parentCreatedAt.UnixMilli() < *m.HistorySharedSince
	}
	return msg.CreatedAt.UnixMilli() < *m.HistorySharedSince
}

func shortRoomType(t model.RoomType) string {
	switch t {
	case model.RoomTypeDM, model.RoomTypeBotDM:
		return "d"
	case model.RoomTypeDiscussion:
		return "p"
	default:
		return "c"
	}
}

// resolveBody renders the push body: @mentions become display names (@all/@here
// become their literal words) so the lock screen shows a person, not an account.
// Runs after the survivor filter, so a message nobody receives never pays for the
// lookup. Fails open — a resolver error substitutes whatever names came back and
// leaves the rest as raw @tokens rather than dropping the push.
func (h *Handler) resolveBody(ctx context.Context, content string, parsed mention.ParseResult) string {
	if len(parsed.Accounts) == 0 && !parsed.MentionAll {
		return content
	}
	var names map[string]string
	if h.deps.MentionNames != nil {
		if lookup := mention.LookupAccountsFromParsed(parsed); len(lookup) > 0 {
			// Message content is user-controlled, so cap the fan-out: the $in is
			// unbounded otherwise, and userstore's batch path neither dedups
			// concurrent misses nor negatively-caches them, so a message spamming
			// unknown @tokens would re-query Mongo on every redelivery. Tokens past
			// the cap keep their raw @token — a push body can't render 50 names anyway.
			if len(lookup) > maxMentionLookups {
				lookup = lookup[:maxMentionLookups]
			}
			resolved, err := h.deps.MentionNames.Resolve(ctx, lookup)
			names = resolved
			if err != nil {
				// Not counted in notificationMetrics: the warn line is the signal.
				slog.WarnContext(ctx, "mention name lookup failed, body keeps raw mentions",
					"error", err, "mentions", len(lookup), "resolved", len(resolved),
					"request_id", natsutil.RequestIDFromContext(ctx))
			}
		}
	}
	return mention.ReplaceAccounts(content, names)
}

// resolveTitle returns the room name when present, else the sender's account (legacy rule).
// DM/botDM rooms skip the cache lookup (never have names); RoomMeta failures also fall back to sender.
func (h *Handler) resolveTitle(ctx context.Context, roomID string, roomType model.RoomType, sender *model.Participant) string {
	if h.deps.RoomMeta != nil && roomType != model.RoomTypeDM && roomType != model.RoomTypeBotDM {
		meta, err := h.deps.RoomMeta.Get(ctx, roomID)
		switch {
		case err == nil && meta.Name != "":
			return meta.Name
		case err != nil:
			slog.Warn("room meta lookup failed, falling back to sender",
				"error", err, "roomId", roomID, "request_id", natsutil.RequestIDFromContext(ctx))
		}
	}
	if sender != nil {
		return sender.Account
	}
	return ""
}
