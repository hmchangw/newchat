package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/subject"
)

// defaultUsersLimit is the page size forwarded to the HR endpoint when the
// search-users request omits limit (or sends 0).
const defaultUsersLimit = 25

// handlerConfig carries knobs read per request. `RequestTimeout` of zero
// disables the context deadline so tests can skip wiring it.
type handlerConfig struct {
	SiteID                  string
	DocCounts               int
	MaxDocCounts            int
	RestrictedRoomsCacheTTL time.Duration
	RecentWindow            time.Duration
	RequestTimeout          time.Duration
	UserRoomIndex           string
	SpotlightReadPattern    string
	SpotlightOrgReadPattern string
}

type handler struct {
	store SearchStore
	mongo MongoStore
	users SearchUsersClient
	cache RestrictedRoomCache
	cfg   handlerConfig
}

func newHandler(store SearchStore, mongo MongoStore, users SearchUsersClient, cache RestrictedRoomCache, cfg *handlerConfig) *handler {
	if cfg.DocCounts <= 0 {
		cfg.DocCounts = 25
	}
	if cfg.MaxDocCounts <= 0 {
		cfg.MaxDocCounts = 100
	}
	if cfg.RestrictedRoomsCacheTTL <= 0 {
		cfg.RestrictedRoomsCacheTTL = 5 * time.Minute
	}
	if cfg.RecentWindow <= 0 {
		cfg.RecentWindow = 365 * 24 * time.Hour
	}
	return &handler{store: store, mongo: mongo, users: users, cache: cache, cfg: *cfg}
}

func (h *handler) Register(r *natsrouter.Router) {
	natsrouter.Register(r, subject.SearchMessagesPattern(h.cfg.SiteID), h.searchMessages)
	natsrouter.Register(r, subject.SearchRoomsPattern(h.cfg.SiteID), h.searchRooms)
	natsrouter.Register(r, subject.SearchAppsPattern(h.cfg.SiteID), h.searchApps)
	natsrouter.Register(r, subject.SearchUsersPattern(h.cfg.SiteID), h.searchUsers)
	natsrouter.Register(r, subject.SearchOrgsPattern(h.cfg.SiteID), h.searchOrgs)
}

func (h *handler) withRequestTimeout(parent context.Context) (context.Context, context.CancelFunc) {
	if h.cfg.RequestTimeout <= 0 {
		return parent, func() {}
	}
	return context.WithTimeout(parent, h.cfg.RequestTimeout)
}

func (h *handler) searchMessages(c *natsrouter.Context, req model.SearchMessagesRequest) (resp *model.SearchMessagesResponse, err error) {
	defer observeRequest(c, metricKindMessages, &err)()

	account, rerr := c.Params.Require("account")
	if rerr != nil {
		return nil, rerr
	}
	c.WithLogValues("request_id", natsutil.RequestIDFromContext(c), "account", account)

	if err := h.normalizePagination(&req.Size, &req.Offset); err != nil {
		return nil, err
	}
	req.Query = strings.TrimSpace(req.Query)
	if req.Query == "" {
		return nil, errcode.BadRequest("query is required")
	}

	ctx, cancel := h.withRequestTimeout(c)
	defer cancel()

	restricted, err := h.loadRestricted(ctx, account)
	if err != nil {
		return nil, err
	}

	// `restricted` is the caller's full restrictedRooms map sourced from the
	// ES user-room-mv index (cached in Valkey by loadRestricted). It is the
	// single source of truth for restricted vs unrestricted classification.
	// When req.RoomIDs is set, buildMessageQuery -> scopedAccessClauses
	// iterates req.RoomIDs and classifies each ID against this map directly,
	// so no handler-level pre-classification is needed.
	body, err := buildMessageQuery(req, account, restricted, h.cfg.RecentWindow, h.cfg.UserRoomIndex)
	if err != nil {
		return nil, fmt.Errorf("building search query: %w", err)
	}

	observeESDone := observeES(ctx)
	raw, err := h.store.Search(ctx, MessageIndexPattern, body)
	observeESDone()
	if err != nil {
		return nil, fmt.Errorf("message search backend: %w", err)
	}

	hits, total, err := parseMessagesResponse(raw)
	if err != nil {
		return nil, fmt.Errorf("parsing search response: %w", err)
	}

	messages := make([]model.SearchMessage, 0, len(hits))
	for i := range hits {
		messages = append(messages, toSearchMessage(&hits[i]))
	}
	return &model.SearchMessagesResponse{Messages: messages, Total: total}, nil
}

func (h *handler) searchRooms(c *natsrouter.Context, req model.SearchRoomsRequest) (resp *model.SearchRoomsResponse, err error) {
	defer observeRequest(c, metricKindRooms, &err)()

	account, rerr := c.Params.Require("account")
	if rerr != nil {
		return nil, rerr
	}
	c.WithLogValues("request_id", natsutil.RequestIDFromContext(c), "account", account)

	if err := h.normalizePagination(&req.Size, &req.Offset); err != nil {
		return nil, err
	}

	query := strings.TrimSpace(req.Query)
	if query == "" {
		return nil, errcode.BadRequest("query is required")
	}
	req.Query = query

	ctx, cancel := h.withRequestTimeout(c)
	defer cancel()

	body, err := buildRoomQuery(req, account)
	if err != nil {
		// A typed errcode error (invalid roomType) passes through;
		// anything else (marshal failure — unreachable) gets sanitized.
		var ee *errcode.Error
		if errors.As(err, &ee) {
			return nil, err
		}
		return nil, fmt.Errorf("building search query: %w", err)
	}

	observeESDone := observeES(ctx)
	raw, err := h.store.Search(ctx, []string{h.cfg.SpotlightReadPattern}, body)
	observeESDone()
	if err != nil {
		return nil, fmt.Errorf("subscription search backend: %w", err)
	}

	rooms, err := parseRooms(raw)
	if err != nil {
		return nil, fmt.Errorf("parsing spotlight rooms: %w", err)
	}
	return &model.SearchRoomsResponse{Rooms: rooms}, nil
}

// searchOrgs runs a prefix search over the company-wide spotlight-org ES
// index (one document per section, maintained by search-sync-worker from HR
// employee events). Unlike searchRooms it applies no per-account filter — the
// org directory is not user-scoped. The account from the subject is used for
// logging and metrics only.
func (h *handler) searchOrgs(c *natsrouter.Context, req model.SearchOrgsRequest) (resp *model.SearchOrgsResponse, err error) {
	defer observeRequest(c, metricKindOrgs, &err)()

	account, rerr := c.Params.Require("account")
	if rerr != nil {
		return nil, rerr
	}
	c.WithLogValues("request_id", natsutil.RequestIDFromContext(c), "account", account)

	if err := h.normalizePagination(&req.Size, &req.Offset); err != nil {
		return nil, err
	}

	query := strings.TrimSpace(req.Query)
	if query == "" {
		return nil, errcode.BadRequest("query is required")
	}
	req.Query = query

	ctx, cancel := h.withRequestTimeout(c)
	defer cancel()

	body, err := buildOrgQuery(req)
	if err != nil {
		return nil, fmt.Errorf("building org search query: %w", err)
	}

	observeESDone := observeES(ctx)
	raw, err := h.store.Search(ctx, []string{h.cfg.SpotlightOrgReadPattern}, body)
	observeESDone()
	if err != nil {
		return nil, fmt.Errorf("org search backend: %w", err)
	}

	orgs, err := parseOrgs(raw)
	if err != nil {
		return nil, fmt.Errorf("parsing spotlight orgs: %w", err)
	}
	return &model.SearchOrgsResponse{Orgs: orgs}, nil
}

// loadRestricted implements the 2-tier Valkey → ES read. Cache errors
// alone never fail the request — log-and-fall-through. Only when both
// cache AND ES prefetch fail do we collapse to errcode.Internal at the boundary.
func (h *handler) loadRestricted(ctx context.Context, account string) (map[string]int64, error) {
	cached, hit, cerr := h.cache.GetRestricted(ctx, account)
	if cerr != nil {
		slog.Warn("valkey read failed; falling through to ES", "account", account, "error", cerr)
	}
	if hit {
		return cached, nil
	}
	doc, _, err := h.store.GetUserRoomDoc(ctx, account)
	if err != nil {
		// Classify (via errnats.Reply at the handler boundary) logs the wrapped
		// chain exactly once at ERROR; do not slog.Error here or every failure
		// double-logs. cache_err is the only detail we'd add — fold it into the
		// wrap so it survives in the centralized cause field.
		if cerr != nil {
			return nil, fmt.Errorf("resolving room access (cache_err=%v): %w", cerr, err)
		}
		return nil, fmt.Errorf("resolving room access: %w", err)
	}

	restricted := doc.RestrictedRooms
	if restricted == nil {
		// Covers both "user has no subs" (found=false) and "doc exists
		// but has no restricted rooms" — cache an empty map to prevent
		// miss-storms.
		restricted = map[string]int64{}
	}

	// Skip the SET when the GET already errored — the transport is
	// almost certainly still down and a second warning adds noise
	// without new signal.
	if cerr == nil {
		if err := h.cache.SetRestricted(ctx, account, restricted, h.cfg.RestrictedRoomsCacheTTL); err != nil {
			slog.Warn("valkey set failed; continuing without cache", "account", account, "error", err)
		}
	}
	return restricted, nil
}

func (h *handler) searchApps(c *natsrouter.Context, req model.SearchAppsRequest) (resp *model.SearchAppsResponse, err error) {
	defer observeRequest(c, metricKindApps, &err)()

	account, rerr := c.Params.Require("account")
	if rerr != nil {
		return nil, rerr
	}
	c.WithLogValues("request_id", natsutil.RequestIDFromContext(c), "account", account)

	if err := h.normalizePagination(&req.Size, &req.Offset); err != nil {
		return nil, err
	}

	query := strings.TrimSpace(req.Query)
	if query == "" {
		return nil, errcode.BadRequest("query is required")
	}

	ctx, cancel := h.withRequestTimeout(c)
	defer cancel()

	apps, err := h.mongo.SearchAppsByName(ctx, query, account, req.AssistantEnabled, req.Offset, req.Size)
	if err != nil {
		return nil, fmt.Errorf("app search backend: %w", err)
	}

	if apps == nil {
		apps = []model.App{}
	}
	return &model.SearchAppsResponse{Apps: apps}, nil
}

// searchUsers proxies the query to the third-party HR endpoint via
// SearchUsersClient and returns a raw []model.SearchUser. The account
// from the subject is used for logging and metrics only; scoping is
// enforced entirely by the third-party endpoint.
func (h *handler) searchUsers(c *natsrouter.Context, req model.SearchUsersRequest) (resp *[]model.SearchUser, err error) {
	defer observeRequest(c, metricKindUsers, &err)()

	account, rerr := c.Params.Require("account")
	if rerr != nil {
		return nil, rerr
	}
	c.WithLogValues("request_id", natsutil.RequestIDFromContext(c), "account", account)

	query := strings.TrimSpace(req.Query)
	if query == "" {
		return nil, errcode.BadRequest("query is required")
	}
	if req.Offset < 0 || req.Limit < 0 {
		return nil, errcode.BadRequest("offset and limit must be non-negative")
	}
	limit := req.Limit
	if limit == 0 {
		limit = defaultUsersLimit
	}
	if limit > h.cfg.MaxDocCounts {
		limit = h.cfg.MaxDocCounts
	}

	ctx, cancel := h.withRequestTimeout(c)
	defer cancel()

	users, err := h.users.SearchUsers(ctx, query, req.Offset, limit)
	if err != nil {
		return nil, fmt.Errorf("user search backend: %w", err)
	}

	if users == nil {
		users = []model.SearchUser{}
	}
	return &users, nil
}

// normalizePagination validates and clamps size/offset in place. size=0
// falls back to DocCounts; size>MaxDocCounts is capped. Negative size
// or offset is a client bug, not a defaultable value, so it returns
// errcode.BadRequest.
func (h *handler) normalizePagination(size, offset *int) error {
	if *size < 0 || *offset < 0 {
		return errcode.BadRequest("size and offset must be non-negative")
	}
	if *size == 0 {
		*size = h.cfg.DocCounts
	}
	if *size > h.cfg.MaxDocCounts {
		*size = h.cfg.MaxDocCounts
	}
	return nil
}
