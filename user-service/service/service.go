package service

import (
	"context"
	"encoding/json"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/user-service/config"
	"github.com/hmchangw/chat/user-service/models"
)

//go:generate mockgen -destination=mocks/mock_repository.go -package=mocks . SubscriptionRepository,UserRepository,AppRepository,RoomClient,HistoryClient,PresenceClient,EventPublisher,ThreadSubscriptionRepository,SettingsRepository

// SubscriptionRepository is the consumer-defined interface for subscription persistence (botDM app-subscription rows included).
type SubscriptionRepository interface {
	AggregateSubscriptions(ctx context.Context, account, listType string, favorite bool, withinDays *int, page mongoutil.OffsetPageRequest) (mongoutil.OffsetPageHasMore[model.EnrichedSubscription], error)
	FindChannelsByMembers(ctx context.Context, account string, members []string, page mongoutil.OffsetPageRequest) (mongoutil.OffsetPageHasMore[model.EnrichedSubscription], error)
	GetDMSubscription(ctx context.Context, account, target string) (*model.EnrichedDMSubscription, error)
	GetSubscriptionByRoomID(ctx context.Context, account, roomID string) (*model.EnrichedSubscription, error)
	CountActiveSubscriptions(ctx context.Context, account string) (int, error)
	GetActiveSubscriptions(ctx context.Context, account string, limit int) ([]model.EnrichedSubscription, error)
	GetAppSubscription(ctx context.Context, account, botName string) (*model.Subscription, error)
	SetAppSubscribed(ctx context.Context, account, botName string, subscribed, muted bool) error
}

// SettingsRepository is the consumer-defined interface for opaque user-settings persistence.
type SettingsRepository interface {
	GetUserSettings(ctx context.Context, account, siteID string) (*model.UserSettings, error)
	SetUserSettings(ctx context.Context, account, siteID string, data json.RawMessage, ifVersion *int64) (*model.UserSettings, error)
}

// UserRepository is the consumer-defined interface for user status persistence.
type UserRepository interface {
	GetUserStatus(ctx context.Context, account string) (*model.User, error)
	SetUserStatus(ctx context.Context, account, text string, isShow *bool) (*model.User, error)
	GetHRInfoByAccounts(ctx context.Context, accounts []string) (map[string]*model.SubscriptionHRInfo, error)
}

// AppRepository is the consumer-defined interface for app catalog reads.
type AppRepository interface {
	GetApp(ctx context.Context, appID string) (*model.App, error)
	ListApps(ctx context.Context, account string, page mongoutil.OffsetPageRequest) (mongoutil.OffsetPageHasMore[models.AppListItem], error)
	GetAppsByAssistants(ctx context.Context, botAccounts []string) (map[string]*model.App, error)
	ListAppCategories(ctx context.Context) ([]models.AppCategory, error)
}

// RoomClient is the consumer-defined interface for room-service / room-worker RPC calls.
type RoomClient interface {
	GetRoomsInfo(ctx context.Context, siteID string, roomIDs []string) ([]model.RoomInfo, error)
	CreateDMRoom(ctx context.Context, account, otherAccount string, roomType model.RoomType) (model.Subscription, error)
	GetThreadRoomInfoBatch(ctx context.Context, siteID string, threadRoomIDs []string) ([]model.ThreadRoomInfo, error)
}

// ThreadSubscriptionRepository reads the local thread_subscriptions replica for
// the thread-unread badge.
type ThreadSubscriptionRepository interface {
	ListByAccount(ctx context.Context, account string) ([]model.ThreadUnreadRow, error)
}

// HistoryClient is the consumer-defined interface for per-site history-service
// RPCs, fanned out across sites by the thread-inbox aggregator.
type HistoryClient interface {
	GetThreadList(ctx context.Context, siteID string, req model.ThreadSubscriptionListRequest) (model.ThreadSubscriptionListResponse, error)
}

// PresenceClient is the consumer-defined interface for user-presence-service RPC calls.
type PresenceClient interface {
	QueryPresence(ctx context.Context, siteID string, accounts []string) ([]model.PresenceState, error)
}

// EventPublisher is the consumer-defined interface for fire-and-forget
// federation publishing — a JetStream publish directly into the destination
// site's INBOX stream. Status is last-write-wins and idempotent, so no
// msgID/dedup is needed.
type EventPublisher interface {
	Publish(ctx context.Context, subject string, data []byte) error
}

// UserService handles all user-related NATS request/reply endpoints.
type UserService struct {
	subs            SubscriptionRepository
	users           UserRepository
	apps            AppRepository
	threadSubs      ThreadSubscriptionRepository
	rooms           RoomClient
	history         HistoryClient
	presence        PresenceClient
	pub             EventPublisher
	siteID          string
	allSiteIDs      []string
	maxSubs         int
	defaultLimit    int
	maxApps         int
	defaultApps     int
	maxAccountNames int
}

// New constructs a UserService with the given dependencies and configuration.
func New(subs SubscriptionRepository, users UserRepository, apps AppRepository, threadSubs ThreadSubscriptionRepository, rooms RoomClient, history HistoryClient, presence PresenceClient, pub EventPublisher, cfg *config.Config) *UserService {
	return &UserService{
		subs:            subs,
		users:           users,
		apps:            apps,
		threadSubs:      threadSubs,
		rooms:           rooms,
		history:         history,
		presence:        presence,
		pub:             pub,
		siteID:          cfg.SiteID,
		allSiteIDs:      cfg.AllSiteIDs,
		maxSubs:         cfg.MaxSubscriptionLimit,
		defaultLimit:    cfg.DefaultSubscriptionLimit,
		maxApps:         cfg.MaxAppsLimit,
		defaultApps:     cfg.DefaultAppsLimit,
		maxAccountNames: cfg.MaxAccountNames,
	}
}

// RegisterHandlers wires all UserService endpoints onto the router.
// siteID is a literal token in each pattern — this instance only subscribes to its own siteID subjects.
func (s *UserService) RegisterHandlers(r *natsrouter.Router) {
	natsrouter.RegisterNoBody(r, subject.UserMePattern(s.siteID), s.Me)
	natsrouter.Register(r, subject.UserStatusGetByNamePattern(s.siteID), s.GetStatusByName)
	natsrouter.Register(r, subject.UserProfileGetByNamePattern(s.siteID), s.GetProfileByName)
	natsrouter.Register(r, subject.UserStatusSetPattern(s.siteID), s.SetStatus)
	natsrouter.Register(r, subject.UserSubscriptionListPattern(s.siteID), s.ListSubscriptions)
	natsrouter.Register(r, subject.UserThreadListPattern(s.siteID), s.ListUserThreads)
	natsrouter.Register(r, subject.UserThreadUnreadSummaryPattern(s.siteID), s.GetThreadUnreadSummary)
	natsrouter.Register(r, subject.UserSubscriptionGetChannelsPattern(s.siteID), s.GetChannels)
	natsrouter.Register(r, subject.UserSubscriptionGetDMPattern(s.siteID), s.GetDM)
	natsrouter.Register(r, subject.UserSubscriptionGetByRoomIDPattern(s.siteID), s.GetByRoomID)
	natsrouter.Register(r, subject.UserSubscriptionCountPattern(s.siteID), s.CountSubscriptions)
	natsrouter.Register(r, subject.UserSubscriptionSetAppSubscriptionPattern(s.siteID), s.SetAppSubscription)
	natsrouter.Register(r, subject.UserAppsListPattern(s.siteID), s.ListApps)
	natsrouter.RegisterNoBody(r, subject.UserAppsCategoriesPattern(s.siteID), s.ListAppCategories)
}
