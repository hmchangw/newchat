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

//go:generate mockgen -destination=mocks/mock_repository.go -package=mocks . MessageReader,MessageWriter,MessageRepository,SubscriptionRepository,RoomRepository,EventPublisher,ThreadRoomRepository

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
	SoftDeleteMessage(ctx context.Context, msg *models.Message, deletedAt time.Time) (actualDeletedAt time.Time, applied bool, err error)
	PinMessage(ctx context.Context, msg *models.Message, pinnedAt time.Time, pinnedBy models.Participant) error
	UnpinMessage(ctx context.Context, msg *models.Message) error
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

// EventPublisher publishes canonical events to a JetStream-backed NATS
// subject. msgID is sent as the Nats-Msg-Id header so the server collapses
// duplicate publishes within the stream's dedup window.
type EventPublisher interface {
	Publish(ctx context.Context, subject string, data []byte, msgID string) error
}

type ThreadRoomRepository interface {
	GetThreadRooms(ctx context.Context, roomID string, accessSince *time.Time, req mongoutil.OffsetPageRequest) (mongoutil.OffsetPage[pkgmodel.ThreadRoom], error)
	GetFollowingThreadRooms(ctx context.Context, roomID, account string, accessSince *time.Time, req mongoutil.OffsetPageRequest) (mongoutil.OffsetPage[pkgmodel.ThreadRoom], error)
	GetUnreadThreadRooms(ctx context.Context, roomID, account string, accessSince *time.Time, req mongoutil.OffsetPageRequest) (mongoutil.OffsetPage[pkgmodel.ThreadRoom], error)
}

// HistoryService handles message history queries and mutations. Transport-agnostic.
type HistoryService struct {
	msgReader          MessageReader
	msgWriter          MessageWriter
	subscriptions      SubscriptionRepository
	rooms              RoomRepository
	publisher          EventPublisher
	threadRooms        ThreadRoomRepository
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
	cfg *config.Config,
) *HistoryService {
	return &HistoryService{
		msgReader:          msgs,
		msgWriter:          msgs,
		subscriptions:      subs,
		rooms:              rooms,
		publisher:          pub,
		threadRooms:        threadRooms,
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
	natsrouter.Register(r, subject.MsgThreadPattern(siteID), s.GetThreadMessages)
	natsrouter.Register(r, subject.MsgThreadParentPattern(siteID), s.GetThreadParentMessages)
}

// Compile-time checks.
var _ MessageRepository = (*cassrepo.Repository)(nil)
var _ SubscriptionRepository = (*mongorepo.SubscriptionRepo)(nil)
var _ RoomRepository = (*mongorepo.RoomRepo)(nil)
