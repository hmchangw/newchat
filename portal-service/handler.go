package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/go-resty/resty/v2"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/subject"
)

// siteURL holds a site's externally reachable coordinates from the
// PORTAL_SITE_URLS registry — explicit per site since sites may live on
// different domains and can't share one URL template. botplatform is reached
// via a single cluster-internal DNS name (BOTPLATFORM_URL), not per site.
type siteURL struct {
	BaseURL string `json:"baseUrl"`
	NATSURL string `json:"natsUrl"`
}

// parseSiteURLs decodes PORTAL_SITE_URLS and requires both URLs per site, so
// misconfiguration fails at startup, not at a user's login.
func parseSiteURLs(raw string) (map[string]siteURL, error) {
	var sites map[string]siteURL
	if err := json.Unmarshal([]byte(raw), &sites); err != nil {
		return nil, fmt.Errorf("decode site URL registry: %w", err)
	}
	if len(sites) == 0 {
		return nil, fmt.Errorf("site URL registry is empty")
	}
	for id, s := range sites {
		if s.BaseURL == "" || s.NATSURL == "" {
			return nil, fmt.Errorf("site %q: baseUrl and natsUrl are both required", id)
		}
	}
	return sites, nil
}

// settingsResponse is the deployment config served to the frontend: the
// backend API generation, the OTEL base URL (client appends /trace, /log),
// and whether bot-role accounts may log in through this client.
type settingsResponse struct {
	APIVersion      string `json:"apiVersion"`
	OTELBaseURL     string `json:"otelBaseUrl"`
	BotLoginEnabled bool   `json:"botLoginEnabled"`
}

// parseOTELBaseURL fails startup unless the value is an absolute http(s) URL
// safe for the client to append /trace and /log to: no credentials, query, or
// fragment, and no trailing slash (so the append can't produce "//trace").
func parseOTELBaseURL(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		// url.Parse's error embeds the raw input; unwrap to the underlying
		// reason so a mistyped credential can't reach the startup log.
		var uerr *url.Error
		if errors.As(err, &uerr) {
			err = uerr.Err
		}
		return "", fmt.Errorf("decode OTEL base URL: %w", err)
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", fmt.Errorf("OTEL base URL %q must be an absolute http(s) URL", u.Redacted())
	}
	if u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return "", fmt.Errorf("OTEL base URL %q must not carry credentials, query, or fragment", u.Redacted())
	}
	u.Path = strings.TrimRight(u.Path, "/")
	return u.String(), nil
}

type userInfoResponse struct {
	Account    string `json:"account"`
	EmployeeID string `json:"employeeId"`
	BaseURL    string `json:"baseUrl"`
	NATSURL    string `json:"natsUrl"`
	SiteID     string `json:"siteId"`
}

// userInfoBotResponse is the minimal shape /api/userInfo returns for bot/admin
// roles — connection URLs only, no employee directory fields.
type userInfoBotResponse struct {
	Account string `json:"account"`
	BaseURL string `json:"baseUrl"`
	NATSURL string `json:"natsUrl"`
	SiteID  string `json:"siteId"`
}

// PortalHandler resolves a user's home-site coordinates from the in-memory
// directory cache — a cache hit means a provisioned account, bot/admin
// included. /api/userInfo is discovery only (no token); /api/v1/login
// forwards bot/admin password logins to the home-site botplatform.
type PortalHandler struct {
	cache              *directoryCache
	devMode            bool
	devFallbackSiteID  string
	devFallbackNatsURL string
	sites              map[string]siteURL
	settings           settingsResponse

	// rest forwards /api/v1/login to the home-site botplatform; nil-safe (502 if unconfigured).
	rest *resty.Client

	// store is the optional live directory lookup used as the /api/v1/login
	// role-gate fallback on a cache miss; nil-safe (miss stays a miss).
	store DirectoryStore
}

// PortalHandlerOption configures optional PortalHandler dependencies.
type PortalHandlerOption func(*PortalHandler)

// WithRestyClient injects the Resty client HandleLogin forwards logins through.
func WithRestyClient(c *resty.Client) PortalHandlerOption {
	return func(h *PortalHandler) { h.rest = c }
}

// WithDirectoryStore injects the live directory store HandleLogin falls back to
// when the in-memory cache misses, so accounts created since the last refresh
// can log in immediately.
func WithDirectoryStore(s DirectoryStore) PortalHandlerOption {
	return func(h *PortalHandler) { h.store = s }
}

// NewPortalHandler creates a PortalHandler. devMode synthesizes a dev-site
// entry for accounts absent from the directory so local logins need no seeding.
// sites is the siteId → URL registry used to resolve each account's home-site
// base URL; settings is the deployment config served at /api/settings.
func NewPortalHandler(cache *directoryCache, devMode bool, devFallbackSiteID, devFallbackNatsURL string, sites map[string]siteURL, settings settingsResponse, opts ...PortalHandlerOption) *PortalHandler {
	h := &PortalHandler{
		cache:              cache,
		devMode:            devMode,
		devFallbackSiteID:  devFallbackSiteID,
		devFallbackNatsURL: devFallbackNatsURL,
		sites:              sites,
		settings:           settings,
	}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

// HandleUserInfo resolves home-site coordinates for the `account` query param.
// Discovery only — no token is validated here.
func (h *PortalHandler) HandleUserInfo(c *gin.Context) {
	ctx := errcode.WithLogValues(c.Request.Context(), "request_id", c.GetString("request_id"))

	account := c.Query("account")
	if account == "" {
		errhttp.Write(ctx, c, errcode.BadRequest("account is required",
			errcode.WithReason(errcode.AuthMissingFields)))
		return
	}
	h.resolve(ctx, c, account)
}

// resolve answers from the directory cache — a single in-memory lookup. In
// devMode, an account absent from the directory is synthesized onto the dev site.
func (h *PortalHandler) resolve(ctx context.Context, c *gin.Context, account string) {
	if !subject.IsValidAccountToken(account) {
		errhttp.Write(ctx, c, errcode.BadRequest("account must be a single NATS subject token (no '.', '*', '>' or whitespace)"))
		return
	}
	ctx = errcode.WithLogValues(ctx, "account", account)

	e, ok := h.cache.Get(account)
	devFallback := false
	if !ok {
		if !h.devMode {
			errhttp.Write(ctx, c, errcode.Forbidden("account not ready for chat",
				errcode.WithReason(errcode.PortalAccountNotReady)))
			return
		}
		e = employee{Account: account, SiteID: h.devFallbackSiteID}
		devFallback = true
	}

	site, siteOK := h.sites[e.SiteID]
	if !siteOK && !devFallback {
		// A directory entry homed on a site missing from the registry is an ops
		// misconfiguration, not a client error — surface it as internal.
		errhttp.Write(ctx, c, fmt.Errorf("no URLs configured for siteId %q", e.SiteID))
		return
	}
	natsURL := site.NATSURL
	if !siteOK {
		// The dev-fallback site itself isn't in the registry — fall back to
		// the legacy PORTAL_DEV_FALLBACK_NATS_URL so local logins keep working.
		natsURL = h.devFallbackNatsURL
	}

	// Bot/admin accounts get the minimal URL bundle; everyone else the rich employee shape.
	if model.HasLoginRole(e.Roles) {
		c.JSON(http.StatusOK, userInfoBotResponse{
			Account: e.Account,
			BaseURL: site.BaseURL,
			NATSURL: natsURL,
			SiteID:  e.SiteID,
		})
		return
	}

	c.JSON(http.StatusOK, userInfoResponse{
		Account:    e.Account,
		EmployeeID: e.EmployeeID,
		BaseURL:    site.BaseURL,
		NATSURL:    natsURL,
		SiteID:     e.SiteID,
	})
}

// ----- POST /api/v1/login (forwarder) ------------------------------------

// loginRequest is what the client sends to portal /api/v1/login.
type loginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// portalLoginResponse is what portal returns to the client: identity fields
// come from the upstream botplatform response (see upstreamLogin), URL
// fields from PORTAL_SITE_URLS[siteId].
type portalLoginResponse struct {
	UserID                string `json:"userId"`
	AuthToken             string `json:"authToken"`
	Account               string `json:"account"`
	SiteID                string `json:"siteId"`
	BaseURL               string `json:"baseUrl"`
	NATSURL               string `json:"natsUrl"`
	RequirePasswordChange bool   `json:"requirePasswordChange"`
}

// upstreamLogin is the botplatform login response; requirePasswordChange is
// read fresh from here rather than the local cache, which can lag right after a password change.
type upstreamLogin struct {
	Status string `json:"status"`
	Data   struct {
		UserID    string `json:"userId"`
		AuthToken string `json:"authToken"`
		Me        struct {
			RequirePasswordChange bool `json:"requirePasswordChange"`
		} `json:"me"`
	} `json:"data"`
}

// HandleLogin forwards bot/admin password login to botplatform (the
// authoritative checker); the local role gate fail-fasts SSO users.
func (h *PortalHandler) HandleLogin(c *gin.Context) {
	ctx := errcode.WithLogValues(c.Request.Context(), "request_id", c.GetString("request_id"))

	var req loginRequest
	if err := c.ShouldBindJSON(&req); err != nil || req.Username == "" || req.Password == "" {
		errhttp.Write(ctx, c, errcode.BadRequest("username and password are required",
			errcode.WithReason(errcode.AuthMissingFields)))
		return
	}
	ctx = errcode.WithLogValues(ctx, "account", req.Username)

	e, ok := h.cache.Get(req.Username)
	if !ok && h.store != nil {
		// Cache miss: the account may have been provisioned since the last
		// periodic refresh (e.g. just created in the admin console). Fall back
		// to a live directory read so it can log in without waiting for the
		// next refresh. A store error is logged and treated as a miss — the
		// gate below then denies, same as an unknown account.
		if fetched, found, err := h.store.GetByAccount(ctx, req.Username); err != nil {
			slog.WarnContext(ctx, "login directory fallback failed", "account", req.Username, "error", err)
		} else if found {
			e, ok = fetched, true
		}
	}
	if !ok || !model.HasLoginRole(e.Roles) {
		slog.WarnContext(ctx, "login denied", "account", req.Username, "reason", "invalid_credentials")
		errhttp.Write(ctx, c, errcode.Unauthenticated("invalid credentials",
			errcode.WithReason(errcode.BotplatformInvalidCredentials)))
		return
	}

	if !h.settings.BotLoginEnabled && model.ContainsBotRole(e.Roles) {
		slog.WarnContext(ctx, "bot login denied by feature flag", "account", req.Username)
		errhttp.Write(ctx, c, errcode.Forbidden("bot accounts cannot log in through this client",
			errcode.WithReason(errcode.PortalBotLoginDisabled)))
		return
	}

	site, ok := h.sites[e.SiteID]
	if !ok {
		errhttp.Write(ctx, c, fmt.Errorf("no URLs configured for siteId %q", e.SiteID))
		return
	}

	if h.rest == nil {
		errhttp.Write(ctx, c, errcode.Unavailable("login upstream not configured",
			errcode.WithReason(errcode.BotplatformUpstreamUnavailable)))
		return
	}

	// Forward to home-site botplatform /api/v1/login. Propagate the request ID
	// so the same correlation key threads through portal → botplatform.
	var upstream upstreamLogin
	resp, err := h.rest.R().
		SetContext(ctx).
		SetHeader(natsutil.RequestIDHeader, c.GetString("request_id")).
		SetBody(req).
		SetResult(&upstream).
		Post("/api/v1/login")
	if err != nil {
		slog.WarnContext(ctx, "login upstream error", "error", err)
		errhttp.Write(ctx, c, errcode.Unavailable("upstream unavailable",
			errcode.WithReason(errcode.BotplatformUpstreamUnavailable)))
		return
	}
	if resp.StatusCode() != http.StatusOK {
		// Propagate the upstream envelope verbatim; preserves reason +
		// status. The body is already an errcode envelope.
		c.Data(resp.StatusCode(), "application/json", resp.Body())
		return
	}

	slog.InfoContext(ctx, "login ok", "account", req.Username, "userId", upstream.Data.UserID)
	c.JSON(http.StatusOK, portalLoginResponse{
		UserID:                upstream.Data.UserID,
		AuthToken:             upstream.Data.AuthToken,
		Account:               e.Account,
		SiteID:                e.SiteID,
		BaseURL:               site.BaseURL,
		NATSURL:               site.NATSURL,
		RequirePasswordChange: upstream.Data.Me.RequirePasswordChange,
	})
}

// HandleSettings serves the startup-validated frontend deployment config.
func (h *PortalHandler) HandleSettings(c *gin.Context) {
	// Deployment config must stay fresh across apiVersion bumps — force
	// caches to revalidate on every fetch.
	c.Header("Cache-Control", "no-cache")
	c.JSON(http.StatusOK, h.settings)
}

// HandleHealth is the liveness probe: the process is up and serving HTTP.
func (h *PortalHandler) HandleHealth(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// HandleReady is the readiness probe: fails until the directory cache holds data.
func (h *PortalHandler) HandleReady(c *gin.Context) {
	if !h.cache.Ready() {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "unavailable"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}
