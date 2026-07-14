package service

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/user-service/models"
)

const maxStatusBytes = 512

func (s *UserService) GetStatusByName(c *natsrouter.Context, req models.StatusGetByNameRequest) (*models.UserStatusView, error) {
	c.WithLogValues("account", c.Param("account"), "target", req.Name)
	if req.Name == "" {
		return nil, errcode.BadRequest("name required")
	}
	u, err := s.users.GetUserStatus(c, req.Name)
	if err != nil {
		return nil, fmt.Errorf("get status: %w", err)
	}
	if u == nil {
		return nil, errcode.NotFound("user not found")
	}
	return &models.UserStatusView{
		Account:      u.Account,
		StatusText:   u.StatusText,
		StatusIsShow: u.StatusIsShow,
		ChineseName:  u.ChineseName,
		EngName:      u.EngName,
	}, nil
}

// GetProfileByName is the profile lookup. It returns the same shape as
// GetStatusByName and currently shares its logic, but is its own handler so
// the profile path can later diverge (e.g. enrich from an HR directory)
// without touching status.
func (s *UserService) GetProfileByName(c *natsrouter.Context, req models.StatusGetByNameRequest) (*models.UserStatusView, error) {
	c.WithLogValues("account", c.Param("account"), "target", req.Name)
	if req.Name == "" {
		return nil, errcode.BadRequest("name required")
	}
	u, err := s.users.GetUserStatus(c, req.Name)
	if err != nil {
		return nil, fmt.Errorf("get profile: %w", err)
	}
	if u == nil {
		return nil, errcode.NotFound("user not found")
	}
	return &models.UserStatusView{
		Account:      u.Account,
		StatusText:   u.StatusText,
		StatusIsShow: u.StatusIsShow,
		ChineseName:  u.ChineseName,
		EngName:      u.EngName,
	}, nil
}

func (s *UserService) SetStatus(c *natsrouter.Context, req models.StatusSetRequest) (*models.UserStatusView, error) {
	account := c.Param("account")
	c.WithLogValues("account", account)
	if len(req.Text) > maxStatusBytes {
		return nil, errcode.BadRequest("status text too long")
	}
	u, err := s.users.SetUserStatus(c, account, req.Text, req.IsShow)
	if err != nil {
		return nil, fmt.Errorf("set status: %w", err)
	}
	if u == nil {
		// No active user doc matched — don't broadcast a status nobody owns.
		return nil, errcode.NotFound("user not found")
	}
	s.publishStatus(c, account, req.Text, req.IsShow)
	// The FindOneAndUpdate already returned the updated doc — no second read.
	return &models.UserStatusView{
		Account:      u.Account,
		StatusText:   u.StatusText,
		StatusIsShow: u.StatusIsShow,
		ChineseName:  u.ChineseName,
		EngName:      u.EngName,
	}, nil
}

// publishStatus broadcasts a user_status_updated InboxEvent via JetStream to
// every configured site except self, publishing directly into each destination
// site's external INBOX lane; errors are logged, not returned.
func (s *UserService) publishStatus(c *natsrouter.Context, account, text string, isShow *bool) {
	now := time.Now().UTC().UnixMilli()
	payload, _ := json.Marshal(model.UserStatusUpdated{
		Account:      account,
		StatusText:   text,
		StatusIsShow: isShow,
		Timestamp:    now,
	}) // UserStatusUpdated is all primitives — Marshal cannot fail
	for _, dest := range s.allSiteIDs {
		if dest == "" || dest == s.siteID {
			continue
		}
		evt := model.InboxEvent{
			Type:       model.InboxUserStatusUpdated,
			SiteID:     s.siteID,
			DestSiteID: dest,
			Payload:    payload,
			Timestamp:  now,
		}
		data, err := json.Marshal(evt)
		if err != nil {
			slog.WarnContext(c, "marshal status inbox event", "error", err, "site", s.siteID, "dest", dest, "account", account, "request_id", natsutil.RequestIDFromContext(c))
			continue
		}
		if err := s.pub.Publish(c, subject.InboxExternal(dest, model.InboxUserStatusUpdated), data); err != nil {
			// Non-fatal: status is last-write-wins, the next SetStatus re-broadcasts.
			slog.WarnContext(c, "publish status inbox event", "error", err, "site", s.siteID, "dest", dest, "account", account, "request_id", natsutil.RequestIDFromContext(c))
		}
	}
}
