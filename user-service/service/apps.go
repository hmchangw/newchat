package service

import (
	"fmt"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/user-service/models"
)

func (s *UserService) SetAppSubscription(c *natsrouter.Context, req models.SetAppSubscriptionRequest) (*models.OKResponse, error) {
	account := c.Param("account")
	c.WithLogValues("account", account)
	if req.AppID == "" {
		return nil, errcode.BadRequest("appId required")
	}
	app, err := s.apps.GetApp(c, req.AppID)
	if err != nil {
		return nil, fmt.Errorf("set app subscription: %w", err)
	}
	if app == nil {
		return nil, errcode.NotFound("app not found", errcode.WithReason(errcode.UserAppNotFound))
	}
	if app.Assistant == nil {
		return nil, errcode.BadRequest("app has no assistant", errcode.WithReason(errcode.UserAppDisabled))
	}
	botName := app.Assistant.Name

	if !req.Subscribed {
		if err := s.subs.SetAppSubscribed(c, account, botName, false, true); err != nil {
			return nil, fmt.Errorf("unsubscribe app: %w", err)
		}
		return &models.OKResponse{Success: true}, nil
	}
	existing, err := s.subs.GetAppSubscription(c, account, botName)
	if err != nil {
		return nil, fmt.Errorf("get app subscription: %w", err)
	}
	if existing == nil {
		if _, err := s.rooms.CreateDMRoom(c, account, botName, model.RoomTypeBotDM); err != nil {
			return nil, fmt.Errorf("create botDM room: %w", err)
		}
		return &models.OKResponse{Success: true}, nil
	}
	if err := s.subs.SetAppSubscribed(c, account, botName, true, false); err != nil {
		return nil, fmt.Errorf("reactivate app: %w", err)
	}
	return &models.OKResponse{Success: true}, nil
}

func (s *UserService) ListApps(c *natsrouter.Context, req models.AppsListRequest) (*models.AppsListResponse, error) {
	account := c.Param("account")
	c.WithLogValues("account", account)
	page, err := s.apps.ListApps(c, account, normalizePage(req.Offset, req.Limit, s.defaultApps, s.maxApps))
	if err != nil {
		return nil, fmt.Errorf("list apps: %w", err)
	}
	return &models.AppsListResponse{Apps: page.Data, HasMore: page.HasMore}, nil
}

// ListAppCategories returns the fab-domain → app-category mapping sorted by name; no request body.
func (s *UserService) ListAppCategories(c *natsrouter.Context) (*models.AppCategoriesResponse, error) {
	account := c.Param("account")
	c.WithLogValues("account", account)
	cats, err := s.apps.ListAppCategories(c)
	if err != nil {
		return nil, fmt.Errorf("list app categories: %w", err)
	}
	if cats == nil {
		// A nil slice marshals to JSON null; clients expect an array.
		cats = []models.AppCategory{}
	}
	return &models.AppCategoriesResponse{Categories: cats}, nil
}
