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
	"github.com/hmchangw/chat/pkg/natsrouter"
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
	UpdateMessageContent(ctx context.Context, msg *models.Message, newMsg string, editedAt time.Time) error
	// SoftDeleteMessage performs a Cassandra LWT on messages_by_id and only
	// runs the mirror-table and parent-tcount work when the LWT applies.
	// Returns the updated_at value now persisted (the deletedAt argument when
	// applied; the existing value when a concurrent delete won the race).
	// newTcount is non-nil when the parent's tcount was decremented via CAS;
	// nil means the CAS was skipped (e.g. parent row not found, or msg is not a thread reply).
	SoftDeleteMessage(ctx context.Context, msg *models.Message, deletedAt time.Time) (actualDeletedAt time.Time, applied bool, newTcount *int, err error)
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
// MinUserLastSeenAt as a per-user read-receipt floor surfaced to clients, and
// GetRoomTimes (lastMsgAt, createdAt) for bucket-walk bounds.
type RoomRepository interface {
	GetMinUserLastSeenAt(ctx context.Context, roomID string) (*time.Time, error)
	GetRoomTimes(ctx context.Context, roomID string) (lastMsgAt, createdAt time.Time, err error)
	GetRoomUserCount(ctx context.Context, roomID string) (int, error)
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

// UserStore resolves the calling user's full profile for ReactorInfo and the Participant on the canonical event.
type UserStore interface {
	FindUserByAccount(ctx context.Context, account string) (*pkgmodel.User, error)
}

// AppStore resolves a bot account's app display name for reaction Actor rendering.
type AppStore interface {
	// AppNameByAccount returns ("", nil) when no app matches botAccount.
	AppNameByAccount(ctx context.Context, botAccount string) (string, error)
}

// HistoryService handles message history queries and mutations. Transport-agnostic.
type HistoryService struct {
	msgReader          MessageReader
	msgWriter          MessageWriter
	subscriptions      SubscriptionRepository
	rooms              RoomRepository
	publisher          EventPublisher
	threadRooms        ThreadRoomRepository
	threadSubs         ThreadSubscriptionRepository
	users              UserStore
	apps               AppStore
	historyFloor       time.Duration // from MESSAGE_HISTORY_FLOOR_DAYS
	largeRoomThreshold int
	maxPinnedPerRoom   int
	pinEnabled         bool // from PIN_ENABLED env var; false disables pin/unpin globally
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
) *HistoryService {
	return &HistoryService{
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
	}
}

// RegisterHandlers wires all NATS endpoints. Panics on subscription failure (fatal at startup).
func (s *HistoryService) RegisterHandlers(r *natsrouter.Router, siteID string) {
	natsrouter.Register(r, subject.MsgHistoryPattern(siteID), s.LoadHistory)
	natsrouter.Register(r, subject.MsgNextPattern(siteID), s.LoadNextMessages)
	natsrouter.Register(r, subject.MsgSurroundingPattern(siteID), s.LoadSurroundingMessages)
	natsrouter.Register(r, subject.MsgGetPattern(siteID), s.GetMessageByID)
	natsrouter.Register(r, subject.MsgGetIDsPattern(siteID), s.GetMessagesByIDs)
	natsrouter.Register(r, subject.RoomsGetPattern(siteID), s.RoomsGet)
	natsrouter.Register(r, subject.MsgEditPattern(siteID), func(c *natsrouter.Context, req models.EditMessageRequest) (*models.EditMessageResponse, error) {
		return s.EditMessage(c, siteID, req)
	})
	natsrouter.Register(r, subject.MsgDeletePattern(siteID), func(c *natsrouter.Context, req models.DeleteMessageRequest) (*models.DeleteMessageResponse, error) {
		return s.DeleteMessage(c, siteID, req)
	})
	natsrouter.Register(r, subject.MsgPinPattern(siteID), func(c *natsrouter.Context, req models.PinMessageRequest) (*models.PinMessageResponse, error) {
		return s.PinMessage(c, siteID, req)
	})
	natsrouter.Register(r, subject.MsgUnpinPattern(siteID), func(c *natsrouter.Context, req models.UnpinMessageRequest) (*models.UnpinMessageResponse, error) {
		return s.UnpinMessage(c, siteID, req)
	})
	natsrouter.Register(r, subject.MsgPinnedListPattern(siteID), s.ListPinnedMessages)
	natsrouter.Register(r, subject.MsgReactPattern(siteID), func(c *natsrouter.Context, req models.ReactMessageRequest) (*models.ReactMessageResponse, error) {
		return s.ReactMessage(c, siteID, req)
	})
	natsrouter.Register(r, subject.MsgThreadPattern(siteID), s.GetThreadMessages)
	natsrouter.Register(r, subject.MsgThreadParentPattern(siteID), s.GetThreadParentMessages)
	natsrouter.Register(r, subject.MigrationInternalMsgEdit(siteID), func(c *natsrouter.Context, req pkgmodel.MigrationEditRequest) (*pkgmodel.MigrationAck, error) {
		return s.MigrationEditMessage(c, siteID, req)
	})
	natsrouter.Register(r, subject.MigrationInternalMsgDelete(siteID), func(c *natsrouter.Context, req pkgmodel.MigrationDeleteRequest) (*pkgmodel.MigrationAck, error) {
		return s.MigrationDeleteMessage(c, siteID, req)
	})
	natsrouter.Register(r, subject.ThreadSubscriptionList(siteID), s.ListThreadSubscriptions)
}

// Compile-time checks.
var _ MessageRepository = (*cassrepo.Repository)(nil)
var _ SubscriptionRepository = (*mongorepo.SubscriptionRepo)(nil)
var _ RoomRepository = (*mongorepo.RoomRepo)(nil)
var _ ThreadSubscriptionRepository = (*mongorepo.ThreadSubscriptionRepo)(nil)
