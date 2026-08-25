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

	// Both branches need this: the removed event carries the room id, and each keys off existence.
	existing, err := s.subs.GetAppSubscription(c, account, botName)
	if err != nil {
		return nil, fmt.Errorf("get app subscription: %w", err)
	}

	if !req.Subscribed {
		if existing == nil {
			return &models.OKResponse{Success: true}, nil // nothing subscribed: no write, no event
		}
		if err := s.subs.SetAppSubscribed(c, account, botName, false, true); err != nil {
			return nil, fmt.Errorf("unsubscribe app: %w", err)
		}
		s.publishAppSubscriptionRemoved(c, account, existing)
		return &models.OKResponse{Success: true}, nil
	}
	if existing == nil {
		// First subscribe emits via room creation — nothing to publish here.
		if _, err := s.rooms.CreateDMRoom(c, account, botName, model.RoomTypeBotDM); err != nil {
			return nil, fmt.Errorf("create botDM room: %w", err)
		}
		return &models.OKResponse{Success: true}, nil
	}
	if err := s.subs.SetAppSubscribed(c, account, botName, true, false); err != nil {
		return nil, fmt.Errorf("reactivate app: %w", err)
	}
	s.publishAppSubscriptionReactivated(c, account, existing, app)
	return &models.OKResponse{Success: true}, nil
}

// Tell the user's other devices to drop the botDM (mirrors room-worker's removed event; best-effort).
func (s *UserService) publishAppSubscriptionRemoved(c *natsrouter.Context, account string, sub *model.Subscription) {
	evt := model.SubscriptionRemovedEvent{
		UserID: sub.User.ID,
		Subscription: model.RemovedSubscriptionRef{
			RoomID:   sub.RoomID,
			RoomType: model.RoomTypeBotDM,
			U:        model.SubscriptionUser{ID: sub.User.ID, Account: account},
		},
		Action:    "removed",
		Timestamp: time.Now().UTC().UnixMilli(),
	}
	data, _ := json.Marshal(evt) // primitives only; cannot fail
	s.publishSubscriptionUpdate(c, account, data)
}

// Tell the user's other devices to re-add the botDM (mirrors room-worker's added shape; best-effort).
func (s *UserService) publishAppSubscriptionReactivated(c *natsrouter.Context, account string, sub *model.Subscription, app *model.App) {
	subCopy := *sub
	subCopy.IsSubscribed = true
	subCopy.Muted = false
	roomName := app.Name
	if roomName == "" {
		roomName = app.Assistant.Name
	}
	evt := model.SubscriptionUpdateEvent{
		UserID:       sub.User.ID,
		Subscription: subCopy,
		Action:       "added",
		RoomName:     roomName,
		AppInfo:      &model.CounterpartAppInfo{ID: app.ID, Name: app.Name, AssistantName: app.Assistant.Name},
		Timestamp:    time.Now().UTC().UnixMilli(),
	}
	data, _ := json.Marshal(evt) // primitives only; cannot fail
	s.publishSubscriptionUpdate(c, account, data)
}

// Best-effort core-NATS fan-out of a botDM subscription.update to the user's own subject.
func (s *UserService) publishSubscriptionUpdate(c *natsrouter.Context, account string, data []byte) {
	if err := s.clientPub.Publish(c, subject.SubscriptionUpdate(account), data); err != nil {
		slog.WarnContext(c, "publish app subscription.update event", "error", err, "account", account, "request_id", natsutil.RequestIDFromContext(c))
	}
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
