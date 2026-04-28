package main

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/subject"
)

// handlerConfig carries knobs read per request. `RequestTimeout` of zero
// disables the context deadline so tests can skip wiring it.
type handlerConfig struct {
	DocCounts               int
	MaxDocCounts            int
	RestrictedRoomsCacheTTL time.Duration
	RecentWindow            time.Duration
	RequestTimeout          time.Duration
	UserRoomIndex           string
}

type handler struct {
	store SearchStore
	cache RestrictedRoomCache
	cfg   handlerConfig
}

func newHandler(store SearchStore, cache RestrictedRoomCache, cfg handlerConfig) *handler {
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
	return &handler{store: store, cache: cache, cfg: cfg}
}

func (h *handler) Register(r *natsrouter.Router) {
	natsrouter.Register(r, subject.SearchMessagesPattern(), h.searchMessages)
	natsrouter.Register(r, subject.SearchRoomsPattern(), h.searchRooms)
}

func (h *handler) withRequestTimeout(parent context.Context) (context.Context, context.CancelFunc) {
	if h.cfg.RequestTimeout <= 0 {
		return parent, func() {}
	}
	return context.WithTimeout(parent, h.cfg.RequestTimeout)
}

func (h *handler) searchMessages(c *natsrouter.Context, req model.SearchMessagesRequest) (resp *model.SearchMessagesResponse, err error) {
	defer observeRequest(metricKindMessages, &err)()

	account, rerr := c.Params.Require("account")
	if rerr != nil {
		return nil, rerr
	}

	if err := h.normalizePagination(&req.Size, &req.Offset); err != nil {
		return nil, err
	}
	if req.SearchText == "" {
		return nil, natsrouter.ErrBadRequest("searchText is required")
	}

	ctx, cancel := h.withRequestTimeout(c)
	defer cancel()

	restricted, err := h.loadRestricted(ctx, account)
	if err != nil {
		return nil, err
	}

	body, err := buildMessageQuery(req, account, restricted, h.cfg.RecentWindow, h.cfg.UserRoomIndex)
	if err != nil {
		// Only reachable via json.Marshal failure on well-typed maps —
		// effectively unreachable — but sanitize anyway so no raw
		// internal error ever leaves the service boundary.
		slog.Error("build message query failed", "account", account, "error", err)
		return nil, natsrouter.ErrInternal("unable to build search query")
	}

	observeESDone := observeES()
	raw, err := h.store.Search(ctx, MessageIndexPattern, body)
	observeESDone()
	if err != nil {
		slog.Error("message search backend failed", "account", account, "error", err)
		return nil, natsrouter.ErrInternal("search backend unavailable")
	}

	resp, err = parseMessagesResponse(raw)
	if err != nil {
		slog.Error("parse messages response failed", "account", account, "error", err)
		return nil, natsrouter.ErrInternal("unexpected search response")
	}
	return resp, nil
}

func (h *handler) searchRooms(c *natsrouter.Context, req model.SearchRoomsRequest) (resp *model.SearchRoomsResponse, err error) {
	defer observeRequest(metricKindRooms, &err)()

	account, rerr := c.Params.Require("account")
	if rerr != nil {
		return nil, rerr
	}

	if err := h.normalizePagination(&req.Size, &req.Offset); err != nil {
		return nil, err
	}
	if req.SearchText == "" {
		return nil, natsrouter.ErrBadRequest("searchText is required")
	}

	ctx, cancel := h.withRequestTimeout(c)
	defer cancel()

	body, err := buildRoomQuery(req, account)
	if err != nil {
		// RouteError (bad scope, scope=app, unknown) passes through;
		// anything else (marshal failure — unreachable) gets sanitized
		// to ErrInternal. Mirrors searchMessages's buildMessageQuery branch.
		var rerr *natsrouter.RouteError
		if errors.As(err, &rerr) {
			return nil, err
		}
		slog.Error("build room query failed", "account", account, "error", err)
		return nil, natsrouter.ErrInternal("unable to build search query")
	}

	observeESDone := observeES()
	raw, err := h.store.Search(ctx, []string{SpotlightIndex}, body)
	observeESDone()
	if err != nil {
		slog.Error("room search backend failed", "account", account, "error", err)
		return nil, natsrouter.ErrInternal("search backend unavailable")
	}

	resp, err = parseRoomsResponse(raw)
	if err != nil {
		slog.Error("parse rooms response failed", "account", account, "error", err)
		return nil, natsrouter.ErrInternal("unexpected search response")
	}
	return resp, nil
}

// loadRestricted implements the 2-tier Valkey → ES read. Cache errors
// alone never fail the request — log-and-fall-through. Only when both
// cache AND ES prefetch fail do we surface ErrInternal.
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
		// Always log the store error, even if the cache GET also failed
		// — it's the actionable signal when both fail. Include cache_err
		// so operators can correlate, but don't let the cache warning
		// mask the ES root cause.
		slog.Error("user-room doc fetch failed", "account", account, "error", err, "cache_err", cerr)
		return nil, natsrouter.ErrInternal("unable to resolve room access")
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

// normalizePagination validates and clamps size/offset in place. size=0
// falls back to DocCounts; size>MaxDocCounts is capped; negative
// offset is clamped to 0. Negative size or offset in the request is a
// client bug, not a defaultable value, so it returns ErrBadRequest.
func (h *handler) normalizePagination(size, offset *int) error {
	if *size < 0 || *offset < 0 {
		return natsrouter.ErrBadRequest("size and offset must be non-negative")
	}
	if *size == 0 {
		*size = h.cfg.DocCounts
	}
	if *size > h.cfg.MaxDocCounts {
		*size = h.cfg.MaxDocCounts
	}
	return nil
}
