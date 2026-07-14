package service

import (
	"fmt"
	"log/slog"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/user-service/models"
)

// Me returns the caller's own status view plus effective presence; no request body.
func (s *UserService) Me(c *natsrouter.Context) (*models.MeResponse, error) {
	account := c.Param("account")
	c.WithLogValues("account", account)
	u, err := s.users.GetUserStatus(c, account)
	if err != nil {
		return nil, fmt.Errorf("get user status: %w", err)
	}
	if u == nil {
		return nil, errcode.NotFound("user not found")
	}
	return &models.MeResponse{
		UserStatusView: models.UserStatusView{
			Account:      u.Account,
			StatusText:   u.StatusText,
			StatusIsShow: u.StatusIsShow,
			ChineseName:  u.ChineseName,
			EngName:      u.EngName,
		},
		Presence: s.lookupPresence(c, account),
	}, nil
}

// lookupPresence resolves the account's effective presence, degrading to
// offline on RPC failure, absent account, or StatusNone (best-effort display).
func (s *UserService) lookupPresence(c *natsrouter.Context, account string) model.PresenceStatus {
	states, err := s.presence.QueryPresence(c, s.siteID, []string{account})
	if err != nil {
		slog.WarnContext(c, "presence lookup degraded", "account", account, "request_id", natsutil.RequestIDFromContext(c), "error", err)
		return model.StatusOffline
	}
	// The presence service only exposes batch RPCs, so we send a 1-element
	// batch and scan the reply for our entry (guards a malformed/reordered reply).
	for _, st := range states {
		if st.Account == account {
			if st.Status == model.StatusNone {
				return model.StatusOffline
			}
			return st.Status
		}
	}
	return model.StatusOffline
}
