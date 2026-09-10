package service

import (
	"context"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/oidc"
	"github.com/hmchangw/chat/pkg/pagefit"
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
	// GetActiveSubscriptions returns the active set, capped at limit when limit > 0 and
	// projected to the unread count's fields; no stage after the cap drops a row.
	GetActiveSubscriptions(ctx context.Context, account string, limit int) ([]models.ActiveSubscription, error)
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
	// roomBatchChunk caps room ids per enrichment RPC (history-service hard-rejects
	// over 100, and each reply must fit the 128 KB NATS payload).
	roomBatchChunk int
	// previewChars truncates each preview body at read time (PREVIEW_CONTENT_CHARS).
	// Room count cannot bound preview memory on its own: the gatekeeper caps a body
	// at 20 KB, but not a 400-row page at 400 of them.
	previewChars int
	// maxFanout bounds concurrent enrichment RPCs per request (MAX_SITE_FANOUT).
	maxFanout       int
	maxApps         int
	defaultApps     int
	maxAccountNames int
	// pageBudget caps a paginated reply so it is trimmed to fit the broker
	// rather than refused by it. Zero value disables trimming.
	pageBudget pagefit.Budget
}

// Option customises a UserService after construction.
type Option func(*UserService)

// WithPageBudget caps paginated replies at b.
func WithPageBudget(b pagefit.Budget) Option {
	return func(s *UserService) { s.pageBudget = b }
}

// New constructs a UserService with the given dependencies and configuration.
func New(subs SubscriptionRepository, users UserRepository, apps AppRepository, threadSubs ThreadSubscriptionRepository, rooms RoomClient, history HistoryClient, presence PresenceClient, pub, clientPub EventPublisher, badge badgeCache, ssoTokens SSOTokenRepository, tokenValidator TokenValidator, tokenRefresher TokenRefresher, cfg *config.Config, opts ...Option) *UserService {
	s := &UserService{
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
		roomBatchChunk:   cfg.RoomBatchChunk,
		previewChars:     cfg.PreviewContentChars,
		maxFanout:        cfg.MaxSiteFanout,
		defaultLimit:     cfg.DefaultSubscriptionLimit,
		maxApps:          cfg.MaxAppsLimit,
		defaultApps:      cfg.DefaultAppsLimit,
		maxAccountNames:  cfg.MaxAccountNames,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// defaultSiteFanout is the fallback when maxFanout is unset.
const defaultSiteFanout = 8

// fanout is the per-request enrichment RPC bound. It normalises a non-positive
// value because the semaphores sized by it send before spawning their receiver:
// a capacity of zero would be an unbuffered channel, and the send would block
// forever. Config validation keeps production above zero; this covers a
// directly-constructed UserService.
func (s *UserService) fanout() int {
	if s.maxFanout < 1 {
		return defaultSiteFanout
	}
	return s.maxFanout
}

// RegisterHandlers wires all UserService endpoints onto the router.
// siteID is a literal token in each pattern — this instance only subscribes to its own siteID subjects.
func (s *UserService) RegisterHandlers(r *natsrouter.Router) {
	r.RegisterRoutes(
		natsrouter.Route{Pattern: subject.UserMePattern(s.siteID), Method: "get_current_user", Bind: natsrouter.HandleNoBody(s.Me)},
		natsrouter.Route{Pattern: subject.UserStatusGetByNamePattern(s.siteID), Method: "get_user_status", Bind: natsrouter.Handle(s.GetStatusByName)},
		natsrouter.Route{Pattern: subject.UserProfileGetByNamePattern(s.siteID), Method: "get_user_profile", Bind: natsrouter.Handle(s.GetProfileByName)},
		natsrouter.Route{Pattern: subject.UserStatusSetPattern(s.siteID), Method: "set_user_status", Bind: natsrouter.Handle(s.SetStatus)},
		natsrouter.Route{Pattern: subject.UserSettingsGetPattern(s.siteID), Method: "get_settings", Bind: natsrouter.HandleNoBody(s.GetSettings)},
		natsrouter.Route{Pattern: subject.UserSettingsSetPattern(s.siteID), Method: "set_settings", Bind: natsrouter.Handle(s.SetSettings)},
		natsrouter.Route{Pattern: subject.UserPriorityContactsGetPattern(s.siteID), Method: "list_priority_contacts", Bind: natsrouter.HandleNoBody(s.GetPriorityContacts)},
		natsrouter.Route{Pattern: subject.UserPriorityContactsAddPattern(s.siteID), Method: "add_priority_contact", Bind: natsrouter.Handle(s.AddPriorityContact)},
		natsrouter.Route{Pattern: subject.UserPriorityContactsRemovePattern(s.siteID), Method: "remove_priority_contact", Bind: natsrouter.Handle(s.RemovePriorityContact)},
		natsrouter.Route{Pattern: subject.UserChatlistGetPattern(s.siteID), Method: "get_chatlist", Bind: natsrouter.HandleNoBody(s.GetChatlist)},
		natsrouter.Route{Pattern: subject.UserChatlistSectionCreatePattern(s.siteID), Method: "create_chatlist_section", Bind: natsrouter.Handle(s.CreateChatlistSection)},
		natsrouter.Route{Pattern: subject.UserChatlistSectionDeletePattern(s.siteID), Method: "delete_chatlist_section", Bind: natsrouter.Handle(s.DeleteChatlistSection)},
		natsrouter.Route{Pattern: subject.UserChatlistSectionRenamePattern(s.siteID), Method: "rename_chatlist_section", Bind: natsrouter.Handle(s.RenameChatlistSection)},
		natsrouter.Route{Pattern: subject.UserChatlistSectionReorderPattern(s.siteID), Method: "reorder_chatlist_sections", Bind: natsrouter.Handle(s.ReorderChatlistSections)},
		natsrouter.Route{Pattern: subject.UserChatlistSectionSetSortModePattern(s.siteID), Method: "set_chatlist_section_sort_mode", Bind: natsrouter.Handle(s.SetChatlistSectionSortMode)},
		natsrouter.Route{Pattern: subject.UserSubscriptionListPattern(s.siteID), Method: "list_subscriptions", Bind: natsrouter.Handle(s.ListSubscriptions)},
		natsrouter.Route{Pattern: subject.UserThreadListPattern(s.siteID), Method: "list_user_threads", Bind: natsrouter.Handle(s.ListUserThreads)},
		natsrouter.Route{Pattern: subject.UserThreadUnreadSummaryPattern(s.siteID), Method: "get_thread_unread_summary", Bind: natsrouter.Handle(s.GetThreadUnreadSummary)},
		natsrouter.Route{Pattern: subject.UserThreadReadAllPattern(s.siteID), Method: "mark_all_threads_read", Bind: natsrouter.Handle(s.ClearAllThreadUnread)},
		natsrouter.Route{Pattern: subject.UserSubscriptionGetChannelsPattern(s.siteID), Method: "list_channel_subscriptions", Bind: natsrouter.Handle(s.GetChannels)},
		natsrouter.Route{Pattern: subject.UserSubscriptionGetDMPattern(s.siteID), Method: "get_dm_subscription", Bind: natsrouter.Handle(s.GetDM)},
		natsrouter.Route{Pattern: subject.UserSubscriptionGetByRoomIDPattern(s.siteID), Method: "get_subscription_by_room", Bind: natsrouter.Handle(s.GetByRoomID)},
		natsrouter.Route{Pattern: subject.UserSubscriptionCountPattern(s.siteID), Method: "count_subscriptions", Bind: natsrouter.Handle(s.CountSubscriptions)},
		natsrouter.Route{Pattern: subject.UserSubscriptionSetAppSubscriptionPattern(s.siteID), Method: "set_app_subscription", Bind: natsrouter.Handle(s.SetAppSubscription)},
		natsrouter.Route{Pattern: subject.UserAppsListPattern(s.siteID), Method: "list_apps", Bind: natsrouter.Handle(s.ListApps)},
		natsrouter.Route{Pattern: subject.UserAppsCategoriesPattern(s.siteID), Method: "list_app_categories", Bind: natsrouter.HandleNoBody(s.ListAppCategories)},
		natsrouter.Route{Pattern: subject.UserSSOSetPattern(s.siteID), Method: "set_sso_token", Bind: natsrouter.Handle(s.SSOSet)},
		natsrouter.Route{Pattern: subject.UserSSORefreshPattern(s.siteID), Method: "refresh_sso_token", Bind: natsrouter.HandleOptionalBody(s.SSORefresh)},
		natsrouter.Route{Pattern: subject.BadgeCountBatchPattern(s.siteID), Method: "batch_get_badge_counts", Bind: natsrouter.Handle(s.BadgeCountBatch)},
	)
}
