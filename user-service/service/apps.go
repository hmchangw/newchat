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
	s.publishAppResubscribed(c, account, app, existing.RoomID)
	return &models.OKResponse{Success: true}, nil
}

// publishAppResubscribed fans out the subscription.update "added" event after a
// re-subscribe, mirroring room-worker's fresh-subscribe fan-out so the sidebar
// updates without a list refetch. Recipient is the re-subscriber only — the bot
// counterpart's subscription did not change. Best-effort like every publisher of
// this subject: the Mongo write is the source of truth, failures are logged and
// swallowed, and the client reconciles on the next subscription.list.
func (s *UserService) publishAppResubscribed(c *natsrouter.Context, account string, app *model.App, roomID string) {
	enriched, err := s.subs.GetSubscriptionByRoomID(c, account, roomID)
	if err != nil {
		slog.WarnContext(c, "resub event: enrich subscription", "error", err, "account", account, "roomId", roomID, "request_id", natsutil.RequestIDFromContext(c))
		return
	}
	if enriched == nil {
		slog.WarnContext(c, "resub event: subscription row missing after reactivate", "account", account, "roomId", roomID, "request_id", natsutil.RequestIDFromContext(c))
		return
	}
	room := buildLocalRoom(enriched)
	if room == nil {
		// Soft-deleted room: an "added" would resurrect a row subscription.list drops.
		return
	}
	sub := enriched.Subscription
	// The enriched read may lag the reactivate $set (secondary read preference);
	// patch the two written fields so the event never ships the pre-write state.
	sub.IsSubscribed = true
	sub.Muted = false
	sub.Room = room
	roomName := app.Name
	if roomName == "" {
		roomName = app.Assistant.Name
	}
	evt := model.SubscriptionUpdateEvent{
		UserID:       sub.User.ID,
		Subscription: sub,
		Action:       "added",
		RoomName:     roomName,
		AppInfo:      &model.CounterpartAppInfo{ID: app.ID, Name: app.Name, AssistantName: app.Assistant.Name},
		Timestamp:    time.Now().UTC().UnixMilli(),
	}
	data, err := json.Marshal(evt)
	if err != nil {
		slog.ErrorContext(c, "resub event: marshal", "error", err, "account", account, "request_id", natsutil.RequestIDFromContext(c))
		return
	}
	if err := s.clientPub.Publish(c, subject.SubscriptionUpdate(account), data); err != nil {
		slog.WarnContext(c, "resub event: publish", "error", err, "account", account, "request_id", natsutil.RequestIDFromContext(c))
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
