package main

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/user-presence-service/presencestore"
)

// Handler holds presence dependencies.
type Handler struct {
	store    PresenceStore
	userDir  UserDirectory
	peer     PeerPresenceClient
	publish  presencestore.PublishFunc
	siteID   string
	batchMax int
	now      func() time.Time
}

// NewHandler builds a Handler.
func NewHandler(store PresenceStore, userDir UserDirectory, peer PeerPresenceClient, publish presencestore.PublishFunc, siteID string, batchMax int) *Handler {
	return &Handler{store: store, userDir: userDir, peer: peer, publish: publish, siteID: siteID, batchMax: batchMax, now: time.Now}
}

// publishState publishes a user's effective status to its state subject.
func (h *Handler) publishState(ctx context.Context, account string, status model.PresenceStatus) {
	presencestore.PublishState(ctx, h.publish, h.siteID, account, status, h.now())
}

// Hello handles a fire-and-forget connection-init message. It registers the
// connection as active (a fresh connection is always active; idle is reported
// later via activity).
func (h *Handler) Hello(c *natsrouter.Context, req model.Hello) error {
	account := c.Param("account")
	if account == "" || req.ConnID == "" {
		return errcode.BadRequest("missing account or connId")
	}
	changed, eff, err := h.store.SetActivity(c, account, req.ConnID, false)
	if err != nil {
		return fmt.Errorf("record hello: %w", err)
	}
	if changed {
		h.publishState(c, account, eff)
	}
	return nil
}

// Ping handles a fire-and-forget liveness refresh. It only flips status when it
// is the first time we have seen this connection (offline->online edge); a
// refresh of a known connection publishes nothing.
func (h *Handler) Ping(c *natsrouter.Context, req model.Ping) error {
	account := c.Param("account")
	if account == "" || req.ConnID == "" {
		return errcode.BadRequest("missing account or connId")
	}
	changed, eff, err := h.store.Ping(c, account, req.ConnID)
	if err != nil {
		return fmt.Errorf("record ping: %w", err)
	}
	if changed {
		h.publishState(c, account, eff)
	}
	return nil
}

// Activity handles a fire-and-forget active/inactive update for one connection.
func (h *Handler) Activity(c *natsrouter.Context, req model.Activity) error {
	account := c.Param("account")
	if account == "" || req.ConnID == "" {
		return errcode.BadRequest("missing account or connId")
	}
	changed, eff, err := h.store.SetActivity(c, account, req.ConnID, req.Away)
	if err != nil {
		return fmt.Errorf("record activity: %w", err)
	}
	if changed {
		h.publishState(c, account, eff)
	}
	return nil
}

// Bye handles a best-effort disconnect.
func (h *Handler) Bye(c *natsrouter.Context, req model.ByeRequest) error {
	account := c.Param("account")
	if account == "" || req.ConnID == "" {
		return errcode.BadRequest("missing account or connId")
	}
	changed, eff, err := h.store.RemoveConnection(c, account, req.ConnID)
	if err != nil {
		return fmt.Errorf("remove connection: %w", err)
	}
	if changed {
		h.publishState(c, account, eff)
	}
	return nil
}

// SetManual sets or clears the manual override and returns the new state.
func (h *Handler) SetManual(c *natsrouter.Context, req model.ManualStatusRequest) (*model.ManualStatusResponse, error) {
	account := c.Param("account")
	if account == "" {
		return nil, errcode.BadRequest("missing account")
	}
	switch req.Status {
	case model.StatusNone, model.StatusOnline, model.StatusAway, model.StatusBusy, model.StatusAppearOffline:
	default:
		return nil, errcode.BadRequest("invalid manual status")
	}
	setAt := h.now().UTC().UnixMilli()
	changed, eff, err := h.store.SetManual(c, account, req.Status)
	if err != nil {
		return nil, fmt.Errorf("set manual: %w", err)
	}
	if changed {
		h.publishState(c, account, eff)
	}
	return &model.ManualStatusResponse{Account: account, Status: req.Status, SetAt: setAt, Effective: eff}, nil
}

// QueryBatchPeer is the server-to-server leaf: it returns the effective status
// for up to batchMax accounts from THIS site's store only, with no fan-out. It
// serves the PresenceQueryBatchPeer subject that peers' QueryBatch fan out to.
func (h *Handler) QueryBatchPeer(c *natsrouter.Context, req model.PresenceQuery) (*model.PresenceQueryResponse, error) {
	now := h.now().UTC().UnixMilli()
	if len(req.Accounts) == 0 {
		return &model.PresenceQueryResponse{States: []model.PresenceState{}, Timestamp: now}, nil
	}
	if len(req.Accounts) > h.batchMax {
		return nil, errcode.BadRequest(fmt.Sprintf("batch exceeds max of %d accounts", h.batchMax))
	}
	statuses, err := h.store.BatchGet(c, req.Accounts)
	if err != nil {
		return nil, fmt.Errorf("batch get: %w", err)
	}
	states := make([]model.PresenceState, 0, len(req.Accounts))
	for _, account := range req.Accounts {
		status, ok := statuses[account]
		if !ok {
			status = model.StatusOffline
		}
		states = append(states, model.PresenceState{
			Account: account, SiteID: h.siteID, Status: status, Timestamp: now,
		})
	}
	return &model.PresenceQueryResponse{States: states, Timestamp: now}, nil
}

// QueryBatch is the client entry point: it resolves each account's home site,
// serves locally-homed accounts from this site's store, fans out to peer sites
// in parallel for remotely-homed accounts, and aggregates. A peer failure or an
// unknown account degrades to StatusOffline (best-effort display data) rather
// than failing the whole query; only a local store error is fatal.
func (h *Handler) QueryBatch(c *natsrouter.Context, req model.PresenceQuery) (*model.PresenceQueryResponse, error) {
	now := h.now().UTC().UnixMilli()
	if len(req.Accounts) == 0 {
		return &model.PresenceQueryResponse{States: []model.PresenceState{}, Timestamp: now}, nil
	}
	if len(req.Accounts) > h.batchMax {
		return nil, errcode.BadRequest(fmt.Sprintf("batch exceeds max of %d accounts", h.batchMax))
	}

	users, err := h.userDir.FindUsersByAccounts(c, req.Accounts)
	if err != nil {
		return nil, fmt.Errorf("resolve user home sites: %w", err)
	}
	// Resolve home sites and group accounts by site in one pass. Accounts absent
	// from the directory never enter bySite and default to offline during
	// assembly; siteByAccount is reused there to stamp each state's home site.
	siteByAccount := make(map[string]string, len(users))
	bySite := make(map[string][]string)
	for i := range users {
		siteByAccount[users[i].Account] = users[i].SiteID
		bySite[users[i].SiteID] = append(bySite[users[i].SiteID], users[i].Account)
	}

	var mu sync.Mutex
	statusByAccount := make(map[string]model.PresenceStatus, len(req.Accounts))
	g, gctx := errgroup.WithContext(c)
	for site, accounts := range bySite {
		site, accounts := site, accounts
		if site == h.siteID {
			g.Go(func() error {
				statuses, err := h.store.BatchGet(gctx, accounts)
				if err != nil {
					return fmt.Errorf("local batch get: %w", err)
				}
				mu.Lock()
				for account, status := range statuses {
					statusByAccount[account] = status
				}
				mu.Unlock()
				return nil
			})
			continue
		}
		g.Go(func() error {
			states, err := h.peer.QueryPeer(gctx, site, accounts)
			if err != nil {
				// Degrade: the affected accounts fall back to offline at assembly.
				slog.Error("presence peer query failed", "error", err, "site", site, "accounts", len(accounts))
				return nil
			}
			mu.Lock()
			for _, st := range states {
				statusByAccount[st.Account] = st.Status
			}
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	states := make([]model.PresenceState, 0, len(req.Accounts))
	for _, account := range req.Accounts {
		status, ok := statusByAccount[account]
		if !ok || status == model.StatusNone {
			status = model.StatusOffline
		}
		site := siteByAccount[account]
		if site == "" {
			site = h.siteID
		}
		states = append(states, model.PresenceState{
			Account: account, SiteID: site, Status: status, Timestamp: now,
		})
	}
	return &model.PresenceQueryResponse{States: states, Timestamp: now}, nil
}
