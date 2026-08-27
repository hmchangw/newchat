package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"golang.org/x/sync/errgroup"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/pwhash"
	"github.com/hmchangw/chat/pkg/session"
	"github.com/hmchangw/chat/pkg/sessioncache"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// Handler wires the AdminStore, session.Store, Config, room-service RPC
// client, and cross-site inbox publisher into HTTP handler methods.
type Handler struct {
	store    AdminStore
	sessions session.Store
	cfg      Config
	roomRPC  roomRequester
	// publish sends one cross-site event; encoding is the Nats-Encoding header
	// value ("" for an uncompressed body).
	publish func(ctx context.Context, subj string, data []byte, encoding string) error
	// remoteDests is remoteSites(cfg.AllSiteIDs, cfg.SiteID), computed once — the
	// config never changes after startup, so no request needs to re-derive it.
	remoteDests []string
	// valkey invalidates cached sessions on revoke. Write-only from here, and
	// nil disables it. Set post-construction, mirroring room-service,
	// room-worker, inbox-worker and bot-room-service.
	valkey valkeyutil.Client
}

// newHandler constructs a Handler with the given stores, config, room RPC, and
// inbox publisher. A nil rpc is tolerated so tests that never touch a room
// route need not build one; the duty handler answers 503 rather than
// dereferencing it. A nil publish is tolerated the same way — fanout
// no-ops in tests that don't exercise it.
func newHandler(store AdminStore, sessions session.Store, cfg Config, rpc roomRequester, publish func(ctx context.Context, subj string, data []byte, encoding string) error) *Handler { //nolint:gocritic // hugeParam: Config is a startup value copied once at construction
	return &Handler{
		store: store, sessions: sessions, cfg: cfg, roomRPC: rpc, publish: publish,
		remoteDests: remoteSites(cfg.AllSiteIDs, cfg.SiteID),
	}
}

// nowMillis returns the current UTC time in unix milliseconds. Injected as a
// package-level indirection so tests can stub without a shim on Handler.
var nowMillis = func() int64 { return time.Now().UTC().UnixMilli() }

// userView is the projection returned to callers — it deliberately excludes
// the Services/bcrypt field so credential material never reaches the wire.
type userView struct {
	ID                    string           `json:"id"`
	Account               string           `json:"account"`
	SiteID                string           `json:"siteId"`
	SectID                string           `json:"sectId,omitempty"`
	SectName              string           `json:"sectName,omitempty"`
	SectTCName            string           `json:"sectTCName,omitempty"`
	SectDescription       string           `json:"sectDescription,omitempty"`
	DeptID                string           `json:"deptId,omitempty"`
	DeptName              string           `json:"deptName,omitempty"`
	DeptTCName            string           `json:"deptTCName,omitempty"`
	DeptDescription       string           `json:"deptDescription,omitempty"`
	EngName               string           `json:"engName,omitempty"`
	ChineseName           string           `json:"chineseName,omitempty"`
	EmployeeID            string           `json:"employeeId,omitempty"`
	StatusIsShow          bool             `json:"statusIsShow"`
	StatusText            string           `json:"statusText,omitempty"`
	Roles                 []model.UserRole `json:"roles,omitempty"`
	RequirePasswordChange bool             `json:"requirePasswordChange,omitempty"`
	Active                bool             `json:"active"`
}

// toView converts a model.User to the projected userView (no Services/bcrypt).
func toView(u *model.User) userView {
	return userView{
		ID:                    u.ID,
		Account:               u.Account,
		SiteID:                u.SiteID,
		SectID:                u.SectID,
		SectName:              u.SectName,
		SectTCName:            u.SectTCName,
		SectDescription:       u.SectDescription,
		DeptID:                u.DeptID,
		DeptName:              u.DeptName,
		DeptTCName:            u.DeptTCName,
		DeptDescription:       u.DeptDescription,
		EngName:               u.EngName,
		ChineseName:           u.ChineseName,
		EmployeeID:            u.EmployeeID,
		StatusIsShow:          u.StatusIsShow,
		StatusText:            u.StatusText,
		Roles:                 u.Roles,
		RequirePasswordChange: u.RequirePasswordChange,
		Active:                u.IsActive(),
	}
}

// createUserResponse is the 201 body: the projected user plus whichever fanout
// lane did not land. Both failure fields are omitempty, so a fully replicated
// create is byte-identical to the pre-fanout response.
type createUserResponse struct {
	userView
	SyncFailures []string `json:"syncFailures,omitempty"`
	HRSyncFailed bool     `json:"hrSyncFailed,omitempty"`
}

// updateUserResponse is the 200 body for a patch; SyncFailures names the remote
// sites whose snapshot publish did not land.
type updateUserResponse struct {
	Status       string   `json:"status"`
	SyncFailures []string `json:"syncFailures,omitempty"`
}

// resyncUserResponse is the 200 body for a resync — the create pair of failure
// fields, since resync re-fires both lanes.
type resyncUserResponse struct {
	Status       string   `json:"status"`
	SyncFailures []string `json:"syncFailures,omitempty"`
	HRSyncFailed bool     `json:"hrSyncFailed,omitempty"`
}

// fanoutUserAccount replicates the admin-owned account state after a committed
// local write: the user_account_updated snapshot to every remote INBOX, plus —
// when withHRBootstrap is set (create and resync) — the durable HR identity
// bootstrap. Failures never change
// the HTTP status — they come back as response fields and are WARN-logged once
// here; heal via the per-user resync endpoint or the next edit (both re-send
// the whole snapshot).
// Takes the request ctx and derives its own — see below.
func (h *Handler) fanoutUserAccount(ctx context.Context, u *model.User, withHRBootstrap bool) (hrFailed bool, syncFailures []string) {
	if h.publish == nil || u == nil {
		return false, nil
	}
	ctx, cancel := h.fanoutCtx(ctx)
	defer cancel()

	// The HR bootstrap shares the INBOX lanes' errgroup rather than running ahead of
	// them: serialized, a stalled local publish would spend the whole budget and every
	// healthy peer would then be reported as failed. hrLaneFailed is written only in
	// that goroutine and read after g.Wait().
	var g errgroup.Group
	var hrLaneFailed bool
	if withHRBootstrap {
		g.Go(func() error {
			hrLaneFailed = h.publishHRBootstrap(ctx, u)
			return nil
		})
	}
	now := nowMillis()
	roles := u.Roles
	if roles == nil {
		roles = []model.UserRole{}
	}
	// Timestamp doubles as the receiver's watermark, so it rides the payload —
	// not just the envelope inbox-worker unwraps and drops.
	payload, err := json.Marshal(model.UserAccountUpdated{
		ID: u.ID, Account: u.Account, SiteID: u.SiteID,
		EngName: u.EngName, ChineseName: u.ChineseName,
		Roles: roles, Active: u.IsActive(), Timestamp: now,
	})
	if err != nil {
		slog.ErrorContext(ctx, "marshal user account snapshot", "error", err, "account", u.Account)
		_ = g.Wait()
		return hrLaneFailed, slices.Clone(h.remoteDests)
	}
	// One goroutine per destination, results in a position-indexed slice: an
	// unreachable peer burns only its own lane, and the reported order stays the
	// configured destination order. Same shape as publishPermissionFanout.
	dests := h.remoteDests
	failed := make([]bool, len(dests))
	for i, dest := range dests {
		g.Go(func() error {
			data, err := json.Marshal(model.InboxEvent{
				Type: model.InboxUserAccountUpdated, SiteID: h.cfg.SiteID,
				DestSiteID: dest, Payload: payload, Timestamp: now,
			})
			if err != nil {
				slog.ErrorContext(ctx, "marshal user account inbox envelope", "dest", dest, "error", err)
				failed[i] = true
				return nil
			}
			if err := h.publish(ctx, subject.InboxExternal(dest, model.InboxUserAccountUpdated), data, ""); err != nil {
				slog.WarnContext(ctx, "publish user account inbox event", "dest", dest, "account", u.Account, "error", err)
				failed[i] = true
			}
			return nil
		})
	}
	_ = g.Wait() // every goroutine returns nil; per-destination outcomes are in failed
	return hrLaneFailed, failedDests(dests, failed)
}

// publishHRBootstrap parks the identity-only users.upsert on the local HR
// stream (zstd, per the feed's current convention) so the create eventually
// lands at every site even when a peer is unreachable right now.
func (h *Handler) publishHRBootstrap(ctx context.Context, u *model.User) bool {
	users := []model.IUserWithChange{{User: model.User{
		ID: u.ID, Account: u.Account, SiteID: u.SiteID,
		EngName: u.EngName, ChineseName: u.ChineseName,
	}}}
	data, err := json.Marshal(users)
	if err != nil {
		slog.ErrorContext(ctx, "marshal hr identity bootstrap", "error", err, "account", u.Account)
		return true
	}
	if err := h.publish(ctx, subject.OrgSyncUsersUpsert(h.cfg.SiteID), natsutil.EncodeZstd(data), natsutil.EncodingZstd); err != nil {
		slog.WarnContext(ctx, "publish hr identity bootstrap", "error", err, "account", u.Account)
		return true
	}
	return false
}

// auditEntry builds one audit entry stamped with the request's principal and this
// site. Details must carry non-secret context only — never passwords, hashes, or
// tokens. ts is passed in so a batch can share one timestamp with its ledger rows.
func (h *Handler) auditEntry(c *gin.Context, action, targetUserID, targetAccount string, details map[string]string, ts time.Time) *AuditEntry {
	p := principalFrom(c)
	return &AuditEntry{
		ID:            idgen.GenerateUUIDv7(),
		ActorUserID:   p.UserID,
		ActorAccount:  p.Account,
		Action:        action,
		TargetUserID:  targetUserID,
		TargetAccount: targetAccount,
		Details:       details, // non-secret only — NEVER password/hash/token
		SiteID:        h.cfg.SiteID,
		Timestamp:     ts.UnixMilli(),
	}
}

// audit appends an audit entry for a mutating admin action; a write failure
// is logged but not fatal.
func (h *Handler) audit(ctx context.Context, c *gin.Context, action, targetUserID, targetAccount string, details map[string]string) {
	if err := h.store.AppendAudit(ctx, h.auditEntry(c, action, targetUserID, targetAccount, details, time.Now().UTC())); err != nil {
		slog.ErrorContext(ctx, "append audit entry failed", "action", action, "error", err)
	}
}

// maxPageLimit caps the page size an admin can request via ?limit=.
const maxPageLimit = 100

// parsePaging extracts page and limit from query params with defaults,
// clamping limit to [1, maxPageLimit].
func parsePaging(c *gin.Context, defaultPage, defaultLimit int) (page, limit int) {
	page = defaultPage
	limit = defaultLimit
	if p := c.Query("page"); p != "" {
		if v, err := strconv.Atoi(p); err == nil && v >= 1 {
			page = v
		}
	}
	if l := c.Query("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil && v >= 1 {
			limit = v
			if limit > maxPageLimit {
				limit = maxPageLimit
			}
		}
	}
	return page, limit
}

// listUsers handles GET /users — searches users by site, query, and paging params.
func (h *Handler) listUsers(c *gin.Context) {
	ctx := c.Request.Context()
	q := c.Query("q")
	page, limit := parsePaging(c, 1, 20)

	users, total, err := h.store.SearchUsers(ctx, q, page, limit)
	if err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("search users: %w", err))
		return
	}

	views := make([]userView, len(users))
	for i := range users {
		views[i] = toView(&users[i])
	}
	c.JSON(http.StatusOK, gin.H{
		"users": views,
		"total": total,
	})
}

// resyncUser handles POST /users/:account/resync — re-delivers the current
// home-site account state on both lanes (HR identity bootstrap + snapshot to
// every remote INBOX). Like permissions resync it is re-delivery, not
// re-recording: no user write, no audit entry, idempotent on the receivers
// (the snapshot re-stamps the watermark with the same field values). Foreign
// replicas 404: only the home site may re-assert an account.
func (h *Handler) resyncUser(c *gin.Context) {
	ctx, cancel := withRequestBudget(c)
	defer cancel()
	account := c.Param("account")

	u, err := h.store.GetUserByAccount(ctx, h.cfg.SiteID, account)
	if err != nil {
		if errors.Is(err, ErrUserNotFound) {
			errhttp.Write(ctx, c, errcode.NotFound("user not found",
				errcode.WithReason(errcode.AdminUserNotFound)))
			return
		}
		errhttp.Write(ctx, c, fmt.Errorf("get user for resync: %w", err))
		return
	}

	hrFailed, syncFailures := h.fanoutUserAccount(ctx, u, true)
	c.JSON(http.StatusOK, resyncUserResponse{
		Status: "ok", SyncFailures: syncFailures, HRSyncFailed: hrFailed,
	})
}

// getUser handles GET /users/:account — fetches a single user by account.
func (h *Handler) getUser(c *gin.Context) {
	ctx := c.Request.Context()
	account := c.Param("account")

	u, err := h.store.GetUserByAccount(ctx, h.cfg.SiteID, account)
	if err != nil {
		if errors.Is(err, ErrUserNotFound) {
			errhttp.Write(ctx, c, errcode.NotFound("user not found",
				errcode.WithReason(errcode.AdminUserNotFound)))
			return
		}
		errhttp.Write(ctx, c, fmt.Errorf("get user: %w", err))
		return
	}

	c.JSON(http.StatusOK, toView(u))
}

// createUserRequest is the request body for POST /users.
type createUserRequest struct {
	Account               string           `json:"account"`
	EngName               string           `json:"engName"`
	ChineseName           string           `json:"chineseName"`
	Password              string           `json:"password"`
	Roles                 []model.UserRole `json:"roles"`
	SiteID                string           `json:"siteId"`
	RequirePasswordChange *bool            `json:"requirePasswordChange"`
}

// createUser handles POST /users — creates a new user account.
func (h *Handler) createUser(c *gin.Context) {
	// One absolute deadline for the whole request, so the fanout below can only
	// spend what the local write and audit left of net/http's write window.
	ctx, cancel := withRequestBudget(c)
	defer cancel()

	var req createUserRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		errhttp.Write(ctx, c, errcode.BadRequest("invalid request body",
			errcode.WithReason(errcode.AuthMissingFields)))
		return
	}

	if req.Account == "" || req.Password == "" {
		errhttp.Write(ctx, c, errcode.BadRequest("account and password are required",
			errcode.WithReason(errcode.AuthMissingFields)))
		return
	}

	hash, err := pwhash.Hash(req.Password, h.cfg.BcryptCost)
	if err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("hash password: %w", err))
		return
	}

	// Default requirePasswordChange to true when the caller omits it.
	requirePasswordChange := true
	if req.RequirePasswordChange != nil {
		requirePasswordChange = *req.RequirePasswordChange
	}

	u := &model.User{
		ID:                    idgen.GenerateUUIDv7(),
		Account:               req.Account,
		SiteID:                h.cfg.SiteID, // always forced, never taken from body
		EngName:               req.EngName,
		ChineseName:           req.ChineseName,
		Roles:                 req.Roles,
		RequirePasswordChange: requirePasswordChange,
		Services: model.Services{
			Password: model.PasswordCredentials{
				Bcrypt: hash,
			},
		},
	}

	if err := h.store.CreateUser(ctx, u); err != nil {
		if errors.Is(err, ErrAccountExists) {
			errhttp.Write(ctx, c, errcode.Conflict("account already exists",
				errcode.WithReason(errcode.AdminAccountExists)))
			return
		}
		errhttp.Write(ctx, c, fmt.Errorf("create user: %w", err))
		return
	}

	h.audit(ctx, c, "user.create", "", u.Account, map[string]string{
		"account": u.Account,
	})

	hrFailed, failures := h.fanoutUserAccount(ctx, u, true)
	c.JSON(http.StatusCreated, createUserResponse{userView: toView(u), SyncFailures: failures, HRSyncFailed: hrFailed})
}

// updateUserRequest is the request body for PATCH /v1/admin/users/:id.
type updateUserRequest struct {
	EngName     *string           `json:"engName"`
	ChineseName *string           `json:"chineseName"`
	Roles       *[]model.UserRole `json:"roles"`
	Active      *bool             `json:"active"`
}

// updateUser handles PATCH /v1/admin/users/:account — applies partial updates to a user.
func (h *Handler) updateUser(c *gin.Context) {
	// One absolute deadline for the whole request — see createUser.
	ctx, cancel := withRequestBudget(c)
	defer cancel()
	account := c.Param("account")

	var req updateUserRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		errhttp.Write(ctx, c, errcode.BadRequest("invalid request body",
			errcode.WithReason(errcode.AuthMissingFields)))
		return
	}

	// Deactivation atomically flips the user and revokes every live session
	// for the account in one Mongo transaction — a partial failure must not
	// leave a disabled account with a still-valid session. Every other patch
	// (name/roles) stays a plain, non-transactional update.
	//
	// Mixing active=false with other field edits in the same PATCH is
	// rejected: the deactivate branch would silently drop the other fields,
	// and the client (admin console) sends deactivation as a distinct action,
	// so mixed patches indicate a client bug. Reactivation (active=true)
	// combined with other fields goes through the normal UpdateUser branch
	// below — no session revoke needed.
	// updated is the post-write doc the fanout replicates; nil means the patch was
	// empty and nothing was written.
	var updated *model.User
	if req.Active != nil && !*req.Active {
		if req.EngName != nil || req.ChineseName != nil || req.Roles != nil {
			errhttp.Write(ctx, c, errcode.BadRequest(
				"active=false cannot be combined with other field updates in a single PATCH",
				errcode.WithReason(errcode.AdminMixedDeactivatePatch)))
			return
		}
		u, err := h.store.DeactivateAndRevoke(ctx, h.cfg.SiteID, account)
		if err != nil {
			if errors.Is(err, ErrUserNotFound) {
				errhttp.Write(ctx, c, errcode.NotFound("user not found",
					errcode.WithReason(errcode.AdminUserNotFound)))
				return
			}
			errhttp.Write(ctx, c, fmt.Errorf("deactivate user and revoke sessions: %w", err))
			return
		}
		updated = u
	} else {
		// UserUpdate and updateUserRequest share an identical field layout, so the conversion is safe (staticcheck S1016).
		u, err := h.store.UpdateUser(ctx, h.cfg.SiteID, account, UserUpdate(req))
		if err != nil {
			if errors.Is(err, ErrUserNotFound) {
				errhttp.Write(ctx, c, errcode.NotFound("user not found",
					errcode.WithReason(errcode.AdminUserNotFound)))
				return
			}
			errhttp.Write(ctx, c, fmt.Errorf("update user: %w", err))
			return
		}
		updated = u
	}

	details := map[string]string{}
	if req.Active != nil {
		details["active"] = strconv.FormatBool(*req.Active)
	}
	h.audit(ctx, c, "user.update", "", account, details)

	var failures []string
	if updated != nil {
		_, failures = h.fanoutUserAccount(ctx, updated, false)
	}
	c.JSON(http.StatusOK, updateUserResponse{Status: "ok", SyncFailures: failures})
}

// setPasswordRequest is the request body for POST /v1/admin/users/:id/password.
type setPasswordRequest struct {
	Password              string `json:"password"`
	RequirePasswordChange *bool  `json:"requirePasswordChange"`
}

// sessionView is the safe projection of Session returned over the wire —
// deliberately omits Roles.
type sessionView struct {
	ID       string `json:"id"`
	UserID   string `json:"userId"`
	Account  string `json:"account"`
	SiteID   string `json:"siteId"`
	IssuedAt int64  `json:"issuedAt"`
}

func toSessionView(s *session.Session) sessionView {
	return sessionView{
		ID:       s.ID,
		UserID:   s.UserID,
		Account:  s.Account,
		SiteID:   s.SiteID,
		IssuedAt: s.IssuedAt,
	}
}

// requireAccountQuery reads the `account` query parameter, writing a 400 and
// returning ok=false when it is absent.
func requireAccountQuery(c *gin.Context) (account string, ok bool) {
	account = c.Query("account")
	if account == "" {
		errhttp.Write(c.Request.Context(), c, errcode.BadRequest("account query parameter is required",
			errcode.WithReason(errcode.AuthMissingFields)))
		return "", false
	}
	return account, true
}

// listSessions handles GET /v1/admin/sessions?account=<account> — lists all
// active sessions for an account, returning only the safe projected fields.
func (h *Handler) listSessions(c *gin.Context) {
	ctx := c.Request.Context()
	account, ok := requireAccountQuery(c)
	if !ok {
		return
	}

	sessions, err := h.sessions.ListForAccount(ctx, h.cfg.SiteID, account)
	if err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("list sessions: %w", err))
		return
	}

	views := make([]sessionView, len(sessions))
	for i := range sessions {
		views[i] = toSessionView(&sessions[i])
	}
	c.JSON(http.StatusOK, gin.H{"sessions": views})
}

// revokeAllSessions handles DELETE /v1/admin/sessions?account=<account> —
// revokes all sessions for an account and appends a session.revoke_all audit entry.
func (h *Handler) revokeAllSessions(c *gin.Context) {
	ctx := c.Request.Context()
	account, ok := requireAccountQuery(c)
	if !ok {
		return
	}

	revoked, err := h.sessions.DeleteForAccount(ctx, h.cfg.SiteID, account)
	if err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("delete sessions: %w", err))
		return
	}
	// The Mongo row is gone, but every bot/admin request resolves through the
	// session cache — without this the revoked tokens keep authenticating until
	// their refresh window elapses, and never at all while Mongo is down.
	sessioncache.BustMany(ctx, h.valkey, revoked)

	h.audit(ctx, c, "session.revoke_all", "", account, nil)
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// revokeSession handles DELETE /v1/admin/sessions/:sessionId?account=<account> —
// revokes a single session and appends a session.revoke audit entry.
func (h *Handler) revokeSession(c *gin.Context) {
	ctx := c.Request.Context()
	account, ok := requireAccountQuery(c)
	if !ok {
		return
	}
	sessionID := c.Param("sessionId")

	if _, err := h.sessions.DeleteByID(ctx, h.cfg.SiteID, account, sessionID); err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("delete session: %w", err))
		return
	}
	// A session's _id IS its token hash, so the path parameter is the cache key.
	sessioncache.Bust(ctx, h.valkey, sessionID)

	h.audit(ctx, c, "session.revoke", "", account, map[string]string{"sessionId": sessionID})
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// listAudit handles GET /v1/admin/audit — lists audit entries filtered by
// query params, scoped to cfg.SiteID, returned newest-first.
func (h *Handler) listAudit(c *gin.Context) {
	ctx := c.Request.Context()

	filter := AuditFilter{
		TargetAccount: c.Query("targetAccount"),
		Actor:         c.Query("actor"),
		Action:        c.Query("action"),
	}
	page, limit := parsePaging(c, 1, 20)

	entries, total, err := h.store.ListAudit(ctx, h.cfg.SiteID, filter, page, limit)
	if err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("list audit: %w", err))
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"entries": entries,
		"total":   total,
	})
}

// healthz handles GET /healthz — always returns 200 {"status":"ok"}.
func (h *Handler) healthz(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// readyz handles GET /readyz — returns 200 when the store is reachable,
// 503 otherwise.
func (h *Handler) readyz(c *gin.Context) {
	ctx := c.Request.Context()
	if err := h.store.Ping(ctx); err != nil {
		slog.WarnContext(ctx, "readyz ping failed", "error", err)
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "down"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// setPassword handles POST /v1/admin/users/:account/password — replaces a user's
// password and forces re-login by revoking all active sessions.
func (h *Handler) setPassword(c *gin.Context) {
	ctx := c.Request.Context()
	account := c.Param("account")

	var req setPasswordRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		errhttp.Write(ctx, c, errcode.BadRequest("invalid request body",
			errcode.WithReason(errcode.AuthMissingFields)))
		return
	}

	if req.Password == "" {
		errhttp.Write(ctx, c, errcode.BadRequest("password is required",
			errcode.WithReason(errcode.AuthMissingFields)))
		return
	}

	hash, err := pwhash.Hash(req.Password, h.cfg.BcryptCost)
	if err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("hash password: %w", err))
		return
	}

	requireChange := true
	if req.RequirePasswordChange != nil {
		requireChange = *req.RequirePasswordChange
	}

	// exceptSessionID="" revokes every session for the target account — an
	// admin-forced reset, unlike self-service change-password, has no caller
	// session to preserve. The password write and the revoke run in one
	// Mongo transaction, so a failure leaves neither applied.
	if err := h.store.UpdateUserPasswordAndRevoke(ctx, h.cfg.SiteID, account, hash, requireChange, ""); err != nil {
		if errors.Is(err, ErrUserNotFound) {
			errhttp.Write(ctx, c, errcode.NotFound("user not found",
				errcode.WithReason(errcode.AdminUserNotFound)))
			return
		}
		errhttp.Write(ctx, c, fmt.Errorf("update user password and revoke sessions: %w", err))
		return
	}

	h.audit(ctx, c, "user.password.set", "", account, map[string]string{
		"requirePasswordChange": strconv.FormatBool(requireChange),
	})

	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}
