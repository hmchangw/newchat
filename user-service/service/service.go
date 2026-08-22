package service

import (
	"context"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/oidc"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/user-service/config"
	"github.com/hmchangw/chat/user-service/models"
)

//go:generate mockgen -destination=mocks/mock_repository.go -package=mocks . SubscriptionRepository,UserRepository,AppRepository,RoomClient,HistoryClient,PresenceClient,EventPublisher,ThreadSubscriptionRepository,SSOTokenRepository,TokenValidator,TokenRefresher

// SubscriptionRepository is the consumer-defined interface for subscription persistence (botDM app-subscription rows included).
type SubscriptionRepository interface {
	AggregateSubscriptions(ctx context.Context, account, listType string, favorite bool, withinDays *int, page mongoutil.OffsetPageRequest) (mongoutil.OffsetPageHasMore[model.EnrichedSubscription], error)
	FindChannelsByMembers(ctx context.Context, account string, members []string, page mongoutil.OffsetPageRequest) (mongoutil.OffsetPageHasMore[model.EnrichedSubscription], error)
	GetDMSubscription(ctx context.Context, account, target string) (*model.EnrichedDMSubscription, error)
	GetSubscriptionByRoomID(ctx context.Context, account, roomID string) (*model.EnrichedSubscription, error)
	CountActiveSubscriptions(ctx context.Context, account string) (int, error)
	// GetActiveSubscriptions returns up to limit active subscriptions. The
	// deleted-room filter runs in the leading $match, ahead of the cap, so the
	// page is exactly limit — nothing downstream of the cap can drop a row.
	// Its only consumer is the unread count; not a general pagination surface.
	GetActiveSubscriptions(ctx context.Context, account string, limit int) ([]model.EnrichedSubscription, error)
	GetAppSubscription(ctx context.Context, account, botName string) (*model.Subscription, error)
	SetAppSubscribed(ctx context.Context, account, botName string, subscribed, muted bool) error
}

// UserRepository is the consumer-defined interface for user status persistence.
type UserRepository interface {
	GetUserStatus(ctx context.Context, account string) (*model.User, error)
	SetUserStatus(ctx context.Context, account, text string, isShow *bool) (*model.User, error)
	GetHRInfoByAccounts(ctx context.Context, accounts []string) (map[string]*model.SubscriptionHRInfo, error)
	GetUserSettings(ctx context.Context, account string) (*model.User, error)
	UpdateUserSettings(ctx context.Context, account string, set *model.UserSettings, at time.Time) (*model.User, error)
	GetUserChatlist(ctx context.Context, account string) (*model.User, error)
	UpdateUserChatlist(ctx context.Context, account string, state *model.ChatlistState) (*model.User, error)
	GetUserPriorityContacts(ctx context.Context, account string) (*model.User, error)
	GetPriorityContactUsers(ctx context.Context, accounts []string) (map[string]*models.PriorityContactUser, error)
	UserExists(ctx context.Context, account string) (bool, error)
	AddPriorityContact(ctx context.Context, account, contact string, limit int, at time.Time) (*model.User, error)
	RemovePriorityContact(ctx context.Context, account, contact string, at time.Time) (*model.User, error)
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
	// GetRoomsMeta is the keyless (skipKeys) variant of GetRoomsInfo, for
	// metadata-only callers.
	GetRoomsMeta(ctx context.Context, siteID string, roomIDs []string) ([]model.RoomInfo, error)
	CreateDMRoom(ctx context.Context, account, otherAccount string, roomType model.RoomType) (model.Subscription, error)
	GetThreadRoomInfoBatch(ctx context.Context, siteID string, threadRoomIDs []string) ([]model.ThreadRoomInfo, error)
	ClearAllThreadUnread(ctx context.Context, siteID, account string) error
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
	RoomsGet(ctx context.Context, siteID string, roomIDs []string, hints map[string]model.RoomTimeHint) (map[string]model.PreviewMessage, error)
}

// PresenceClient is the consumer-defined interface for user-presence-service RPC calls.
type PresenceClient interface {
	QueryPresence(ctx context.Context, siteID string, accounts []string) ([]model.PresenceState, error)
}

// badgeCache is the consumer-defined interface for the thread-unread badge's
// Valkey accelerator (pkg/badgecache.Cache satisfies it; a disabled/no-op
// implementation is wired when Valkey is not configured). Only
// BumpBatch/Seed/Reseed/Count are consumed here — ClearRoom/ClearAll belong
// to other event handlers.
type badgeCache interface {
	// BumpBatch pipelines the per-account bump; accounts absent from the result
	// missed (or errored) and must be seeded from the source of truth.
	BumpBatch(ctx context.Context, accounts []string, roomID string) map[string]int
	Seed(ctx context.Context, account string, roomIDs []string, triggerRoomID string) (int, bool)
	Reseed(ctx context.Context, account string, roomIDs []string)
	// Count serves the account's unread-room count from the cache; fresh=false
	// (marker absent or Valkey error) means the caller must recompute from Mongo.
	Count(ctx context.Context, account string) (int, bool)
}

// EventPublisher is the consumer-defined interface for fire-and-forget
// federation publishing — a JetStream publish directly into the destination
// site's INBOX stream. Status is last-write-wins and idempotent, so no
// msgID/dedup is needed.
type EventPublisher interface {
	Publish(ctx context.Context, subject string, data []byte) error
}

// SSOTokenRepository is the consumer-defined interface for the SSO token vault (sso_tokens collection; legacy field names kept).
type SSOTokenRepository interface {
	GetByUsername(ctx context.Context, username string) (*model.SSOToken, error)
	Upsert(ctx context.Context, username, ssoToken string, ssoTokenExpMs int64, refreshToken string) error
}

// TokenValidator verifies an SSO token against the configured OIDC issuer; nil when the SSO feature is not configured (endpoints reply unavailable).
type TokenValidator interface {
	Validate(ctx context.Context, raw string) (oidc.Claims, error)
}

// TokenRefresher exchanges a refresh token at the issuer's token endpoint; nil when the SSO feature is not configured.
type TokenRefresher interface {
	Refresh(ctx context.Context, refreshToken string) (oidc.TokenSet, error)
}

// UserService handles all user-related NATS request/reply endpoints.
type UserService struct {
	subs       SubscriptionRepository
	users      UserRepository
	apps       AppRepository
	threadSubs ThreadSubscriptionRepository
	rooms      RoomClient
	history    HistoryClient
	presence   PresenceClient
	pub        EventPublisher
	// clientPub fans out ephemeral client-facing events (settings.update) over
	// core NATS — same delivery pattern as room-worker's subscription.update.
	clientPub        EventPublisher
	badge            badgeCache
	ssoTokens        SSOTokenRepository
	tokenValidator   TokenValidator
	tokenRefresher   TokenRefresher
	ssoRefreshWindow time.Duration
	siteID           string
	allSiteIDs       []string
	// badgeCap caps badge unread-room counts on the cache-down fallback path
	// (BADGE_COUNT_CAP; pkg/badgecache applies the same cap on cache hits).
	badgeCap int
	// badgeCacheFirst gates serving subscription.count (unread=true) from the
	// badge cache on freshness-marker hit (BADGE_COUNT_CACHE_FIRST).
	badgeCacheFirst bool
	maxSubs         int
	defaultLimit    int
	maxApps         int
	defaultApps     int
	maxAccountNames int
}

// New constructs a UserService with the given dependencies and configuration.
func New(subs SubscriptionRepository, users UserRepository, apps AppRepository, threadSubs ThreadSubscriptionRepository, rooms RoomClient, history HistoryClient, presence PresenceClient, pub, clientPub EventPublisher, badge badgeCache, ssoTokens SSOTokenRepository, tokenValidator TokenValidator, tokenRefresher TokenRefresher, cfg *config.Config) *UserService {
	return &UserService{
		subs:             subs,
		users:            users,
		apps:             apps,
		threadSubs:       threadSubs,
		rooms:            rooms,
		history:          history,
		presence:         presence,
		pub:              pub,
		clientPub:        clientPub,
		badge:            badge,
		ssoTokens:        ssoTokens,
		tokenValidator:   tokenValidator,
		tokenRefresher:   tokenRefresher,
		ssoRefreshWindow: cfg.SSORefreshWindow,
		siteID:           cfg.SiteID,
		allSiteIDs:       cfg.AllSiteIDs,
		badgeCap:         cfg.BadgeCountCap,
		badgeCacheFirst:  cfg.BadgeCountCacheFirst,
		maxSubs:          cfg.MaxSubscriptionLimit,
		defaultLimit:     cfg.DefaultSubscriptionLimit,
		maxApps:          cfg.MaxAppsLimit,
		defaultApps:      cfg.DefaultAppsLimit,
		maxAccountNames:  cfg.MaxAccountNames,
	}
}

// RegisterHandlers wires all UserService endpoints onto the router.
// siteID is a literal token in each pattern — this instance only subscribes to its own siteID subjects.
func (s *UserService) RegisterHandlers(r *natsrouter.Router) {
	natsrouter.RegisterNoBody(r, subject.UserMePattern(s.siteID), s.Me)
	natsrouter.Register(r, subject.UserStatusGetByNamePattern(s.siteID), s.GetStatusByName)
	natsrouter.Register(r, subject.UserProfileGetByNamePattern(s.siteID), s.GetProfileByName)
	natsrouter.Register(r, subject.UserStatusSetPattern(s.siteID), s.SetStatus)
	natsrouter.RegisterNoBody(r, subject.UserSettingsGetPattern(s.siteID), s.GetSettings)
	natsrouter.Register(r, subject.UserSettingsSetPattern(s.siteID), s.SetSettings)
	natsrouter.RegisterNoBody(r, subject.UserPriorityContactsGetPattern(s.siteID), s.GetPriorityContacts)
	natsrouter.Register(r, subject.UserPriorityContactsAddPattern(s.siteID), s.AddPriorityContact)
	natsrouter.Register(r, subject.UserPriorityContactsRemovePattern(s.siteID), s.RemovePriorityContact)
	natsrouter.RegisterNoBody(r, subject.UserChatlistGetPattern(s.siteID), s.GetChatlist)
	natsrouter.Register(r, subject.UserChatlistSectionCreatePattern(s.siteID), s.CreateChatlistSection)
	natsrouter.Register(r, subject.UserChatlistSectionDeletePattern(s.siteID), s.DeleteChatlistSection)
	natsrouter.Register(r, subject.UserChatlistSectionRenamePattern(s.siteID), s.RenameChatlistSection)
	natsrouter.Register(r, subject.UserChatlistSectionReorderPattern(s.siteID), s.ReorderChatlistSections)
	natsrouter.Register(r, subject.UserChatlistSectionSetSortModePattern(s.siteID), s.SetChatlistSectionSortMode)
	natsrouter.Register(r, subject.UserSubscriptionListPattern(s.siteID), s.ListSubscriptions)
	natsrouter.Register(r, subject.UserThreadListPattern(s.siteID), s.ListUserThreads)
	natsrouter.Register(r, subject.UserThreadUnreadSummaryPattern(s.siteID), s.GetThreadUnreadSummary)
	natsrouter.Register(r, subject.UserThreadReadAllPattern(s.siteID), s.ClearAllThreadUnread)
	natsrouter.Register(r, subject.UserSubscriptionGetChannelsPattern(s.siteID), s.GetChannels)
	natsrouter.Register(r, subject.UserSubscriptionGetDMPattern(s.siteID), s.GetDM)
	natsrouter.Register(r, subject.UserSubscriptionGetByRoomIDPattern(s.siteID), s.GetByRoomID)
	natsrouter.Register(r, subject.UserSubscriptionCountPattern(s.siteID), s.CountSubscriptions)
	natsrouter.Register(r, subject.UserSubscriptionSetAppSubscriptionPattern(s.siteID), s.SetAppSubscription)
	natsrouter.Register(r, subject.UserAppsListPattern(s.siteID), s.ListApps)
	natsrouter.RegisterNoBody(r, subject.UserAppsCategoriesPattern(s.siteID), s.ListAppCategories)
	natsrouter.Register(r, subject.UserSSOSetPattern(s.siteID), s.SSOSet)
	natsrouter.RegisterOptionalBody(r, subject.UserSSORefreshPattern(s.siteID), s.SSORefresh)
	natsrouter.Register(r, subject.BadgeCountBatchPattern(s.siteID), s.BadgeCountBatch)
}
