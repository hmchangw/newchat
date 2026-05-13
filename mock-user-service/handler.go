package main

import (
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/subject"
)

// --- mock data constants ---

const (
	mockStatusText   = "available"
	mockStatusIsShow = true
	mockDisplayName  = "Mock User"
	mockEmail        = "mock@example.test"
)

var mockJoinedAt = time.Unix(0, 0).UTC()

// --- request / response types ---

type statusGetByNameReq struct {
	Name string `json:"name"`
}

type statusSetReq struct {
	StatusText   string `json:"statusText"`
	StatusIsShow bool   `json:"statusIsShow"`
}

type statusResp struct {
	Name         string `json:"name"`
	StatusText   string `json:"statusText"`
	StatusIsShow bool   `json:"statusIsShow"`
}

type profileGetByNameReq struct {
	Name string `json:"name"`
}

type profileResp struct {
	Name        string `json:"name"`
	DisplayName string `json:"displayName"`
	Email       string `json:"email"`
}

type getSubsReq struct {
	Favorite       *bool    `json:"favorite,omitempty"`
	MembersContain []string `json:"membersContain,omitempty"`
	AccountNames   []string `json:"accountNames,omitempty"`
}

type getAppSubsReq struct {
	Favorite *bool `json:"favorite,omitempty"`
}

type getDMSubReq struct {
	TargetAccount string `json:"targetAccount"`
}

type appSubscriptionReq struct {
	AppID string `json:"appId"`
}

type subscriptionListResp struct {
	Subscriptions []model.Subscription `json:"subscriptions"`
	Total         int                  `json:"total"`
}

type dmSubscriptionResp struct {
	Subscription model.Subscription `json:"subscription"`
}

type roomSubscriptionResp struct {
	Subscription model.Subscription `json:"subscription"`
}

type appListResp struct {
	Apps  []model.App `json:"apps"`
	Total int         `json:"total"`
}

type okResp struct {
	Success bool `json:"success"`
}

// --- handler ---

type Handler struct {
	siteID string
}

func NewHandler(siteID string) *Handler {
	return &Handler{siteID: siteID}
}

func (h *Handler) checkSite(c *natsrouter.Context) error {
	if c.Param("siteID") != h.siteID {
		return natsrouter.ErrNotFound("unknown site")
	}
	return nil
}

// --- mock data helpers ---

func buildMockSub(account, siteID string) model.Subscription {
	return model.Subscription{
		ID:       "mock-sub-" + account,
		User:     model.SubscriptionUser{ID: "mock-user-" + account, Account: account},
		RoomID:   "mock-room",
		SiteID:   siteID,
		Roles:    []model.Role{model.RoleMember},
		Name:     "Mock Room",
		RoomType: model.RoomTypeChannel,
		JoinedAt: mockJoinedAt,
	}
}

func buildMockApp(id, name string) model.App {
	return model.App{ID: id, Name: name}
}

func (h *Handler) statusGetByName(c *natsrouter.Context, req statusGetByNameReq) (*statusResp, error) {
	if err := h.checkSite(c); err != nil {
		return nil, err
	}
	return &statusResp{
		Name:         req.Name,
		StatusText:   mockStatusText,
		StatusIsShow: mockStatusIsShow,
	}, nil
}

func (h *Handler) statusSet(c *natsrouter.Context, req statusSetReq) (*okResp, error) {
	if err := h.checkSite(c); err != nil {
		return nil, err
	}
	_ = req
	return &okResp{Success: true}, nil
}

func (h *Handler) profileGetByName(c *natsrouter.Context, req profileGetByNameReq) (*profileResp, error) {
	if err := h.checkSite(c); err != nil {
		return nil, err
	}
	return &profileResp{
		Name:        req.Name,
		DisplayName: mockDisplayName,
		Email:       mockEmail,
	}, nil
}

func (h *Handler) mockSubList(account string) []model.Subscription {
	return []model.Subscription{
		buildMockSub(account, h.siteID),
		buildMockSub(account, h.siteID),
	}
}

func (h *Handler) subscriptionGetCurrent(c *natsrouter.Context, req getSubsReq) (*subscriptionListResp, error) {
	if err := h.checkSite(c); err != nil {
		return nil, err
	}
	_ = req
	subs := h.mockSubList(c.Param("account"))
	return &subscriptionListResp{Subscriptions: subs, Total: len(subs)}, nil
}

func (h *Handler) subscriptionGetRooms(c *natsrouter.Context, req getSubsReq) (*subscriptionListResp, error) {
	if err := h.checkSite(c); err != nil {
		return nil, err
	}
	_ = req
	subs := h.mockSubList(c.Param("account"))
	return &subscriptionListResp{Subscriptions: subs, Total: len(subs)}, nil
}

func (h *Handler) subscriptionGetChannels(c *natsrouter.Context, req getSubsReq) (*subscriptionListResp, error) {
	if err := h.checkSite(c); err != nil {
		return nil, err
	}
	_ = req
	subs := h.mockSubList(c.Param("account"))
	return &subscriptionListResp{Subscriptions: subs, Total: len(subs)}, nil
}

func (h *Handler) subscriptionGetApps(c *natsrouter.Context, req getAppSubsReq) (*subscriptionListResp, error) {
	if err := h.checkSite(c); err != nil {
		return nil, err
	}
	_ = req
	subs := h.mockSubList(c.Param("account"))
	return &subscriptionListResp{Subscriptions: subs, Total: len(subs)}, nil
}

func (h *Handler) subscriptionGetDM(c *natsrouter.Context, req getDMSubReq) (*dmSubscriptionResp, error) {
	if err := h.checkSite(c); err != nil {
		return nil, err
	}
	return &dmSubscriptionResp{Subscription: buildMockSub(req.TargetAccount, h.siteID)}, nil
}

func (h *Handler) subscriptionSubscribeApp(c *natsrouter.Context, req appSubscriptionReq) (*okResp, error) {
	if err := h.checkSite(c); err != nil {
		return nil, err
	}
	_ = req
	return &okResp{Success: true}, nil
}

func (h *Handler) subscriptionUnsubscribeApp(c *natsrouter.Context, req appSubscriptionReq) (*okResp, error) {
	if err := h.checkSite(c); err != nil {
		return nil, err
	}
	_ = req
	return &okResp{Success: true}, nil
}

func (h *Handler) roomSubscriptionGet(c *natsrouter.Context) (*roomSubscriptionResp, error) {
	if err := h.checkSite(c); err != nil {
		return nil, err
	}
	sub := buildMockSub(c.Param("account"), h.siteID)
	sub.RoomID = c.Param("roomID")
	return &roomSubscriptionResp{Subscription: sub}, nil
}

func (h *Handler) appsList(c *natsrouter.Context) (*appListResp, error) {
	if err := h.checkSite(c); err != nil {
		return nil, err
	}
	apps := []model.App{
		buildMockApp("app-1", "Mock App One"),
		buildMockApp("app-2", "Mock App Two"),
	}
	return &appListResp{Apps: apps, Total: len(apps)}, nil
}

// Register subscribes every mock user RPC route on the supplied router.
func (h *Handler) Register(r *natsrouter.Router) {
	natsrouter.Register(r, subject.UserStatusGetByNamePattern(h.siteID), h.statusGetByName)
	natsrouter.Register(r, subject.UserStatusSetPattern(h.siteID), h.statusSet)
	natsrouter.Register(r, subject.UserProfileGetByNamePattern(h.siteID), h.profileGetByName)
	natsrouter.Register(r, subject.UserSubscriptionGetCurrentPattern(h.siteID), h.subscriptionGetCurrent)
	natsrouter.Register(r, subject.UserSubscriptionGetRoomsPattern(h.siteID), h.subscriptionGetRooms)
	natsrouter.Register(r, subject.UserSubscriptionGetChannelsPattern(h.siteID), h.subscriptionGetChannels)
	natsrouter.Register(r, subject.UserSubscriptionGetDMPattern(h.siteID), h.subscriptionGetDM)
	natsrouter.Register(r, subject.UserSubscriptionGetAppsPattern(h.siteID), h.subscriptionGetApps)
	natsrouter.Register(r, subject.UserSubscriptionSubscribeAppPattern(h.siteID), h.subscriptionSubscribeApp)
	natsrouter.Register(r, subject.UserSubscriptionUnsubscribeAppPattern(h.siteID), h.subscriptionUnsubscribeApp)
	natsrouter.RegisterNoBody(r, subject.UserRoomSubscriptionGetPattern(h.siteID), h.roomSubscriptionGet)
	natsrouter.RegisterNoBody(r, subject.UserAppsListPattern(h.siteID), h.appsList)
}
