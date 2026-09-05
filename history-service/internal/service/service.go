package service

import (
	"context"
	"time"

	"github.com/hmchangw/chat/history-service/internal/cassrepo"
	"github.com/hmchangw/chat/history-service/internal/config"
	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/history-service/internal/mongorepo"
	pkgmodel "github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsmetrics"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/pagefit"
	"github.com/hmchangw/chat/pkg/preview"
	"github.com/hmchangw/chat/pkg/subject"
)

//go:generate mockgen -destination=mocks/mock_repository.go -package=mocks . MessageReader,MessageWriter,MessageRepository,SubscriptionRepository,RoomRepository,EventPublisher,ThreadRoomRepository,ThreadSubscriptionRepository,UserStore,AppStore

type MessageReader interface {
	GetMessagesBefore(ctx context.Context, roomID string, before time.Time, floor time.Time, pageReq cassrepo.PageRequest) (cassrepo.Page[models.Message], error)
	GetMessagesBetweenDesc(ctx context.Context, roomID string, since, before time.Time, pageReq cassrepo.PageRequest) (cassrepo.Page[models.Message], error)
	GetMessagesAfter(ctx context.Context, roomID string, after time.Time, ceiling time.Time, pageReq cassrepo.PageRequest) (cassrepo.Page[models.Message], error)
	GetAllMessagesAsc(ctx context.Context, roomID string, floor, ceiling time.Time, pageReq cassrepo.PageRequest) (cassrepo.Page[models.Message], error)
	GetMessageByID(ctx context.Context, messageID string) (*models.Message, error)
	GetThreadMessages(ctx context.Context, threadRoomID string, before, floor time.Time, pageReq cassrepo.PageRequest) (cassrepo.Page[models.Message], error)
	GetMessagesByIDs(ctx context.Context, messageIDs []string) ([]models.Message, error)
	GetPinnedMessages(ctx context.Context, roomID string, pageReq cassrepo.PageRequest) (cassrepo.Page[models.Message], error)
	GetAllPinnedMessages(ctx context.Context, roomID string) ([]models.Message, error)
}

type MessageWriter interface {
	UpdateMessageContent(ctx context.Context, msg *models.Message, newMsg string, mentions []pkgmodel.Participant, editedAt time.Time) error
	// SoftDeleteMessage performs a Cassandra LWT on messages_by_id and only
	// runs the mirror-table and parent-tcount work when the LWT applies.
	// Returns the updated_at value now persisted (the deletedAt argument when
	// applied; the existing value when a concurrent delete won the race).
	// newTcount is non-nil when the parent's tcount was recomputed via CAS;
	// nil means the CAS was skipped (e.g. parent row not found, or msg is not a thread reply).
	// newThreadLastMsgAt is the newest surviving reply's createdAt (nil when none survive or the
	// CAS was skipped), letting the caller carry it on the canonical event without a second read.
	SoftDeleteMessage(ctx context.Context, msg *models.Message, deletedAt time.Time) (actualDeletedAt time.Time, applied bool, newTcount *int, newThreadLastMsgAt *time.Time, err error)
	PinMessage(ctx context.Context, msg *models.Message, pinnedAt time.Time, pinnedBy models.Participant) error
	UnpinMessage(ctx context.Context, msg *models.Message) error
	// AddReaction writes one (emoji, user_account) map-cell to every mirror; idempotent.
	AddReaction(ctx context.Context, msg *models.Message, key models.ReactionKey, reactor models.ReactorInfo) error
	// RemoveReaction deletes one (emoji, user_account) map-cell from every mirror; idempotent on a miss.
	RemoveReaction(ctx context.Context, msg *models.Message, key models.ReactionKey) error
}

// MessageRepository composes read and write access; satisfied by *cassrepo.Repository.
type MessageRepository interface {
	MessageReader
	MessageWriter
}

type SubscriptionRepository interface {
	GetHistorySharedSince(ctx context.Context, account, roomID string) (*time.Time, bool, error)
	GetSubscription(ctx context.Context, account, roomID string) (*pkgmodel.Subscription, error)
}

// RoomRepository reads room metadata required by history handlers:
// MinUserLastSeenAt as a per-user read-receipt floor surfaced to clients,
// GetRoomTimes (lastMsgAt, createdAt) for bucket-walk bounds, and
// GetRoomTimesByIDs, the batched ($in) form for resolving many rooms at once.
type RoomRepository interface {
	GetMinUserLastSeenAt(ctx context.Context, roomID string) (*time.Time, error)
	GetRoomTimes(ctx context.Context, roomID string) (lastMsgAt, createdAt time.Time, err error)
	GetRoomTimesByIDs(ctx context.Context, ids []string) (map[string]mongorepo.RoomTimes, error)
	GetRoomUserCount(ctx context.Context, roomID string) (int, error)
	// SetPreviewMessage seals and stores a walk-resolved preview, guarded by asOf
	// so it fills a room the eager writer never reached but never regresses a
	// newer write. forMsgID is the freshness key; see previewWalk.NewestObservedID.
	// Best-effort: the caller logs and carries on.
	SetPreviewMessage(ctx context.Context, roomID string, pvw models.PreviewMessage, forMsgID string, asOf int64) error
	// UpdatePreviewBody reseals the body after an edit/delete, leaving the
	// freshness key alone (a mutation does not move lastMsgId) and refusing to
	// create — an insert is the sole creator. forMsgID is the key the walk
	// OBSERVED: the write lands only while the stored key still equals it, so an
	// insert that advanced the key between walk and write makes this a no-op
	// rather than pairing this older body with the newer key.
	//
	// Reports whether the write landed. Losing a guard is not an error, but the
	// caller must still repair: the body it failed to replace goes on reading as
	// current, since a mutation never moves lastMsgId (#226).
	UpdatePreviewBody(ctx context.Context, roomID string, pvw models.PreviewMessage, forMsgID string, asOf int64) (bool, error)
	// ClearPreview removes the stored preview under the same guard, for a
	// mutation that leaves the room with no eligible message. Reports whether the
	// write landed, for the same reason UpdatePreviewBody does.
	ClearPreview(ctx context.Context, roomID string, asOf int64) (bool, error)
	// InvalidatePreviewKey withdraws the freshness key from a stored preview whose
	// body describes msgID, so the reader stops serving it and the next read
	// re-derives it. The repair when neither write above could establish what the
	// room now holds; a no-op once any newer write has replaced the body.
	InvalidatePreviewKey(ctx context.Context, roomID, msgID string, asOf int64) error
}

// EventPublisher publishes events to NATS with a Nats-Msg-Id dedup header.
type EventPublisher interface {
	Publish(ctx context.Context, subject string, data []byte, msgID string) error
	// PublishMigration publishes like Publish but stamps X-Migration: live.
	PublishMigration(ctx context.Context, subject string, data []byte, msgID string) error
}

type ThreadRoomRepository interface {
	GetThreadRooms(ctx context.Context, roomID string, accessSince *time.Time, req mongoutil.OffsetPageRequest) (mongoutil.OffsetPage[pkgmodel.ThreadRoom], error)
	GetFollowingThreadRooms(ctx context.Context, roomID, account string, accessSince *time.Time, req mongoutil.OffsetPageRequest) (mongoutil.OffsetPage[pkgmodel.ThreadRoom], error)
	GetUnreadThreadRooms(ctx context.Context, roomID, account string, accessSince *time.Time, req mongoutil.OffsetPageRequest) (mongoutil.OffsetPage[pkgmodel.ThreadRoom], error)
	// GetMinThreadUserLastSeenAt returns thread_rooms.minUserLastSeenAt for
	// threadRoomID. Returns (nil, nil) when the field is unset or the document
	// is missing — both mean "not everyone has read yet".
	GetMinThreadUserLastSeenAt(ctx context.Context, threadRoomID string) (*time.Time, error)
}

// ThreadSubscriptionRepository lists a user's thread subscriptions on this site,
// the per-site leaf of the cross-site thread inbox.
type ThreadSubscriptionRepository interface {
	ListUserThreadSubscriptions(ctx context.Context, account string, cursorLastMsgAt *time.Time, cursorThreadRoomID string, limit int) ([]mongorepo.ThreadSubRow, bool, error)
}

// UserStore resolves user profiles: one account for ReactorInfo and the Participant
// on the canonical event, and a batch for resolving a whole page of legacy
// system-message accounts in a single query.
type UserStore interface {
	FindUserByAccount(ctx context.Context, account string) (*pkgmodel.User, error)
	// FindUsersByAccounts resolves many accounts in one read. Accounts with no
	// matching user are simply absent from the result — not an error.
	FindUsersByAccounts(ctx context.Context, accounts []string) ([]pkgmodel.User, error)
}

// AppStore resolves a bot account's app display name for reaction Actor rendering.
type AppStore interface {
	// AppNameByAccount returns ("", nil) when no app matches botAccount.
	AppNameByAccount(ctx context.Context, botAccount string) (string, error)
}

// PreviewCache fronts the per-room preview resolve on the rooms.get lazy fallback.
// Positives are cached; not-found and errors pass through. *readcache.PreviewCache
// satisfies it.
type PreviewCache interface {
	Get(ctx context.Context, roomID string, load func(context.Context) (models.PreviewMessage, bool, error)) (models.PreviewMessage, bool, error)
	// Invalidate drops a room's entry after a mutation changed what it previews.
	Invalidate(roomID string)
}

// Option configures optional HistoryService dependencies.
type Option func(*HistoryService)

// WithPreviewCache installs a room-preview cache fronting RoomsGet's lazy fallback.
// Without it, the fallback resolves directly (uncached). Rooms served from a stored
// preview never reach the cache — they never reach the walk it fronts.
func WithPreviewCache(pc PreviewCache) Option {
	return func(s *HistoryService) { s.previewCache = pc }
}

// WithPageBudget caps paginated replies at b so an oversize page is trimmed
// rather than refused by the broker.
func WithPageBudget(b pagefit.Budget) Option {
	return func(s *HistoryService) { s.pageBudget = b }
}

// HistoryService handles message history queries and mutations. Transport-agnostic.
type HistoryService struct {
	msgReader     MessageReader
	msgWriter     MessageWriter
	subscriptions SubscriptionRepository
	rooms         RoomRepository
	publisher     EventPublisher
	threadRooms   ThreadRoomRepository
	threadSubs    ThreadSubscriptionRepository
	users         UserStore
	apps          AppStore
	// appName is apps.AppNameByAccount behind a shared TTL cache, built ONCE here: a
	// per-call wrapper would mint a fresh empty cache each time and never hit (#366).
	// Nil when no app store is wired — BotAwareDisplayName degrades on a nil lookup.
	appName            preview.AppNameLookup
	historyFloor       time.Duration // from MESSAGE_HISTORY_FLOOR_DAYS
	largeRoomThreshold int
	maxPinnedPerRoom   int
	pinEnabled         bool // from PIN_ENABLED env var; false disables pin/unpin globally
	previewCache       PreviewCache
	// warmer stores walk-resolved previews off the request path; Close drains it.
	warmer *previewWarmer
	// pageBudget caps a paginated reply so it is trimmed to fit the broker
	// rather than refused by it. Zero value disables trimming.
	pageBudget pagefit.Budget
	// roomTimes remembers the last room times MongoDB confirmed, so a walk can
	// still be bounded while MongoDB is unreachable. Never nil — a disabled
	// deployment gets a no-op, so the read path needs no nil check.
	roomTimes RoomTimesCache
}

// RoomTimesCache remembers a room's last confirmed lastMsgAt/createdAt so the
// bucket walk can still be bounded when MongoDB cannot answer. Write-on-success
// and read-on-failure, NOT a read-through: a healthy request never consults it,
// so it introduces no staleness on the hot path and needs no invalidation.
type RoomTimesCache interface {
	Store(ctx context.Context, roomID string, createdAt time.Time)
	Fallback(ctx context.Context, roomID string) (createdAt time.Time, found bool)
}

// nopRoomTimesCache is the disabled form: it remembers nothing and offers
// nothing, leaving the fail-open path exactly as wide as it was before the
// tier existed.
type nopRoomTimesCache struct{}

func (nopRoomTimesCache) Store(context.Context, string, time.Time) {}
func (nopRoomTimesCache) Fallback(context.Context, string) (time.Time, bool) {
	return time.Time{}, false
}

// WithRoomTimesCache enables the room-times L2 fallback. A nil cache leaves the
// no-op in place.
func WithRoomTimesCache(c RoomTimesCache) Option {
	return func(s *HistoryService) {
		if c != nil {
			s.roomTimes = c
		}
	}
}

func New(
	msgs MessageRepository,
	subs SubscriptionRepository,
	rooms RoomRepository,
	pub EventPublisher,
	threadRooms ThreadRoomRepository,
	threadSubs ThreadSubscriptionRepository,
	users UserStore,
	apps AppStore,
	cfg *config.Config,
	opts ...Option,
) *HistoryService {
	s := &HistoryService{
		msgReader:          msgs,
		msgWriter:          msgs,
		subscriptions:      subs,
		rooms:              rooms,
		publisher:          pub,
		threadRooms:        threadRooms,
		threadSubs:         threadSubs,
		users:              users,
		apps:               apps,
		historyFloor:       time.Duration(cfg.MessageHistoryFloorDays) * 24 * time.Hour,
		largeRoomThreshold: cfg.LargeRoomThreshold,
		maxPinnedPerRoom:   cfg.MaxPinnedPerRoom,
		pinEnabled:         cfg.PinEnabled,
		roomTimes:          nopRoomTimesCache{},
	}
	s.warmer = newPreviewWarmer(rooms, cfg.PreviewWarmBackWorkers, cfg.PreviewWarmBackQueue, warmBackTimeout)
	// A method value derefs its receiver where written, so this is guarded, not eager.
	if apps != nil {
		s.appName = preview.CachedAppNameLookup(apps.AppNameByAccount)
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Close stops the background preview writer and waits for its queue to drain. Call once
// the router has stopped accepting requests and before the Mongo client closes; ctx bounds
// the drain, and an expired one abandons the remaining writes rather than holding shutdown.
func (s *HistoryService) Close(ctx context.Context) error {
	return s.warmer.Close(ctx)
}

// RegisterHandlers wires all NATS endpoints. Panics on subscription failure (fatal at startup).
func (s *HistoryService) RegisterHandlers(r *natsrouter.Router, siteID string) {
	natsrouter.Register(r, subject.MsgHistoryPattern(siteID), natsmetrics.MethodGetChannelHistory, s.LoadHistory)
	natsrouter.Register(r, subject.MsgNextPattern(siteID), natsmetrics.MethodGetNextMessages, s.LoadNextMessages)
	natsrouter.Register(r, subject.MsgSurroundingPattern(siteID), natsmetrics.MethodGetSurroundingMessages, s.LoadSurroundingMessages)
	natsrouter.Register(r, subject.MsgGetPattern(siteID), natsmetrics.MethodGetMessage, s.GetMessageByID)
	natsrouter.Register(r, subject.MsgGetIDsPattern(siteID), natsmetrics.MethodBatchGetMessages, s.GetMessagesByIDs)
	natsrouter.Register(r, subject.RoomsGet(siteID), natsmetrics.MethodBatchGetRoomPreviews, s.RoomsGet)
	natsrouter.Register(r, subject.MsgEditPattern(siteID), natsmetrics.MethodEditMessage, func(c *natsrouter.Context, req models.EditMessageRequest) (*models.EditMessageResponse, error) {
		return s.EditMessage(c, siteID, req)
	})
	natsrouter.Register(r, subject.MsgDeletePattern(siteID), natsmetrics.MethodDeleteMessage, func(c *natsrouter.Context, req models.DeleteMessageRequest) (*models.DeleteMessageResponse, error) {
		return s.DeleteMessage(c, siteID, req)
	})
	natsrouter.Register(r, subject.MsgPinPattern(siteID), natsmetrics.MethodPinMessage, func(c *natsrouter.Context, req models.PinMessageRequest) (*models.PinMessageResponse, error) {
		return s.PinMessage(c, siteID, req)
	})
	natsrouter.Register(r, subject.MsgUnpinPattern(siteID), natsmetrics.MethodUnpinMessage, func(c *natsrouter.Context, req models.UnpinMessageRequest) (*models.UnpinMessageResponse, error) {
		return s.UnpinMessage(c, siteID, req)
	})
	natsrouter.Register(r, subject.MsgPinnedListPattern(siteID), natsmetrics.MethodListPinnedMessages, s.ListPinnedMessages)
	natsrouter.Register(r, subject.MsgReactPattern(siteID), natsmetrics.MethodToggleMessageReaction, func(c *natsrouter.Context, req models.ReactMessageRequest) (*models.ReactMessageResponse, error) {
		return s.ReactMessage(c, siteID, req)
	})
	natsrouter.Register(r, subject.MsgThreadPattern(siteID), natsmetrics.MethodListThreadMessages, s.GetThreadMessages)
	natsrouter.Register(r, subject.MsgThreadParentPattern(siteID), natsmetrics.MethodListThreadParentMessages, s.GetThreadParentMessages)
	natsrouter.Register(r, subject.MigrationInternalMsgEdit(siteID), natsmetrics.MethodMigrateEditMessage, func(c *natsrouter.Context, req pkgmodel.MigrationEditRequest) (*pkgmodel.MigrationAck, error) {
		return s.MigrationEditMessage(c, siteID, req)
	})
	natsrouter.Register(r, subject.MigrationInternalMsgDelete(siteID), natsmetrics.MethodMigrateDeleteMessage, func(c *natsrouter.Context, req pkgmodel.MigrationDeleteRequest) (*pkgmodel.MigrationAck, error) {
		return s.MigrationDeleteMessage(c, siteID, req)
	})
	natsrouter.Register(r, subject.ThreadSubscriptionList(siteID), natsmetrics.MethodListThreadSubscriptions, s.ListThreadSubscriptions)
}

// Compile-time checks.
var _ MessageRepository = (*cassrepo.Repository)(nil)
var _ SubscriptionRepository = (*mongorepo.SubscriptionRepo)(nil)
var _ RoomRepository = (*mongorepo.RoomRepo)(nil)
var _ ThreadSubscriptionRepository = (*mongorepo.ThreadSubscriptionRepo)(nil)
