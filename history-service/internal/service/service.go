package service

import (
	"context"
	"time"

	"github.com/hmchangw/chat/history-service/internal/cassrepo"
	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/subject"
)

//go:generate mockgen -destination=mocks/mock_repository.go -package=mocks . MessageRepository,SubscriptionRepository,EventPublisher

// MessageRepository defines Cassandra-backed message operations.
type MessageRepository interface {
	GetMessagesBefore(ctx context.Context, roomID string, before time.Time, q cassrepo.PageRequest) (cassrepo.Page[models.Message], error)
	GetMessagesBetweenDesc(ctx context.Context, roomID string, since, before time.Time, q cassrepo.PageRequest) (cassrepo.Page[models.Message], error)
	GetMessagesAfter(ctx context.Context, roomID string, after time.Time, q cassrepo.PageRequest) (cassrepo.Page[models.Message], error)
	GetAllMessagesAsc(ctx context.Context, roomID string, q cassrepo.PageRequest) (cassrepo.Page[models.Message], error)
	GetMessageByID(ctx context.Context, messageID string) (*models.Message, error)
	UpdateMessageContent(ctx context.Context, msg *models.Message, newMsg string, editedAt time.Time) error
	// SoftDeleteMessage performs a Cassandra LWT on messages_by_id and only
	// runs the mirror-table and parent-tcount work when the LWT applies.
	// Returns the updated_at value now persisted (the deletedAt argument when
	// applied; the existing value when a concurrent delete won the race).
	SoftDeleteMessage(ctx context.Context, msg *models.Message, deletedAt time.Time) (actualDeletedAt time.Time, applied bool, err error)
}

// SubscriptionRepository defines MongoDB-backed subscription lookups.
type SubscriptionRepository interface {
	GetHistorySharedSince(ctx context.Context, account, roomID string) (*time.Time, bool, error)
}

// EventPublisher publishes live events to a NATS subject. Implemented by a
// thin wrapper around *otelnats.Conn in main.go.
type EventPublisher interface {
	Publish(ctx context.Context, subject string, data []byte) error
}

// HistoryService handles message history queries and (starting with this PR) mutations.
// Transport-agnostic.
type HistoryService struct {
	messages      MessageRepository
	subscriptions SubscriptionRepository
	publisher     EventPublisher
}

// New creates a HistoryService with the given repositories and event publisher.
func New(msgs MessageRepository, subs SubscriptionRepository, pub EventPublisher) *HistoryService {
	return &HistoryService{messages: msgs, subscriptions: subs, publisher: pub}
}

// RegisterHandlers wires all NATS endpoints for the history service.
// Panics if any subscription fails (startup-only, fatal if broken).
func (s *HistoryService) RegisterHandlers(r *natsrouter.Router, siteID string) {
	natsrouter.Register(r, subject.MsgHistoryPattern(siteID), s.LoadHistory)
	natsrouter.Register(r, subject.MsgNextPattern(siteID), s.LoadNextMessages)
	natsrouter.Register(r, subject.MsgSurroundingPattern(siteID), s.LoadSurroundingMessages)
	natsrouter.Register(r, subject.MsgGetPattern(siteID), s.GetMessageByID)
	natsrouter.Register(r, subject.MsgEditPattern(siteID), s.EditMessage)
	natsrouter.Register(r, subject.MsgDeletePattern(siteID), s.DeleteMessage)
}
