package main

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/natsrouter"
)

func newCtx(params map[string]string) *natsrouter.Context {
	return natsrouter.NewContext(params)
}

func TestHandler_CheckSite(t *testing.T) {
	h := NewHandler("site-local")

	t.Run("match", func(t *testing.T) {
		err := h.checkSite(newCtx(map[string]string{"siteID": "site-local"}))
		assert.NoError(t, err)
	})

	t.Run("mismatch returns ErrNotFound", func(t *testing.T) {
		err := h.checkSite(newCtx(map[string]string{"siteID": "site-other"}))
		require.Error(t, err)
		var routeErr *natsrouter.RouteError
		require.True(t, errors.As(err, &routeErr), "want *natsrouter.RouteError, got %T", err)
		assert.Equal(t, natsrouter.CodeNotFound, routeErr.Code)
	})
}

func TestBuildMockSub(t *testing.T) {
	sub := buildMockSub("alice", "site-local")
	assert.Equal(t, "alice", sub.User.Account)
	assert.Equal(t, "site-local", sub.SiteID)
	assert.NotEmpty(t, sub.ID)
	assert.NotEmpty(t, sub.RoomID)
}

func TestBuildMockApp(t *testing.T) {
	app := buildMockApp("app-1", "Mock One")
	assert.Equal(t, "app-1", app.ID)
	assert.Equal(t, "Mock One", app.Name)
}

func TestHandler_StatusGetByName(t *testing.T) {
	h := NewHandler("site-local")

	t.Run("happy path echoes Name", func(t *testing.T) {
		c := newCtx(map[string]string{"account": "alice", "siteID": "site-local"})
		resp, err := h.statusGetByName(c, statusGetByNameReq{Name: "bob"})
		require.NoError(t, err)
		assert.Equal(t, "bob", resp.Name)
		assert.Equal(t, mockStatusText, resp.StatusText)
		assert.Equal(t, mockStatusIsShow, resp.StatusIsShow)
	})

	t.Run("siteID mismatch", func(t *testing.T) {
		c := newCtx(map[string]string{"account": "alice", "siteID": "site-x"})
		_, err := h.statusGetByName(c, statusGetByNameReq{Name: "bob"})
		require.Error(t, err)
	})
}

func TestHandler_StatusSet(t *testing.T) {
	h := NewHandler("site-local")

	t.Run("happy path returns success", func(t *testing.T) {
		c := newCtx(map[string]string{"account": "alice", "siteID": "site-local"})
		resp, err := h.statusSet(c, statusSetReq{StatusText: "busy", StatusIsShow: false})
		require.NoError(t, err)
		assert.True(t, resp.Success)
	})

	t.Run("siteID mismatch", func(t *testing.T) {
		c := newCtx(map[string]string{"account": "alice", "siteID": "site-x"})
		_, err := h.statusSet(c, statusSetReq{})
		require.Error(t, err)
	})
}

func TestHandler_ProfileGetByName(t *testing.T) {
	h := NewHandler("site-local")

	t.Run("happy path echoes Name", func(t *testing.T) {
		c := newCtx(map[string]string{"account": "alice", "siteID": "site-local"})
		resp, err := h.profileGetByName(c, profileGetByNameReq{Name: "bob"})
		require.NoError(t, err)
		assert.Equal(t, "bob", resp.Name)
		assert.Equal(t, mockDisplayName, resp.DisplayName)
		assert.Equal(t, mockEmail, resp.Email)
	})

	t.Run("siteID mismatch", func(t *testing.T) {
		c := newCtx(map[string]string{"account": "alice", "siteID": "site-x"})
		_, err := h.profileGetByName(c, profileGetByNameReq{Name: "bob"})
		require.Error(t, err)
	})
}

func TestHandler_SubscriptionListHandlers(t *testing.T) {
	h := NewHandler("site-local")
	c := newCtx(map[string]string{"account": "alice", "siteID": "site-local"})

	type listFn func() (*subscriptionListResp, error)
	cases := []struct {
		name string
		fn   listFn
	}{
		{"getCurrent", func() (*subscriptionListResp, error) { return h.subscriptionGetCurrent(c, getSubsReq{}) }},
		{"getRooms", func() (*subscriptionListResp, error) { return h.subscriptionGetRooms(c, getSubsReq{}) }},
		{"getChannels", func() (*subscriptionListResp, error) { return h.subscriptionGetChannels(c, getSubsReq{}) }},
		{"getApps", func() (*subscriptionListResp, error) { return h.subscriptionGetApps(c, getAppSubsReq{}) }},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := tt.fn()
			require.NoError(t, err)
			assert.Equal(t, 2, resp.Total)
			require.Len(t, resp.Subscriptions, 2)
			for _, sub := range resp.Subscriptions {
				assert.Equal(t, "alice", sub.User.Account)
				assert.Equal(t, "site-local", sub.SiteID)
			}
		})
	}
}

func TestHandler_SubscriptionListHandlers_SiteMismatch(t *testing.T) {
	h := NewHandler("site-local")
	c := newCtx(map[string]string{"account": "alice", "siteID": "site-x"})

	_, err := h.subscriptionGetCurrent(c, getSubsReq{})
	require.Error(t, err)
	_, err = h.subscriptionGetRooms(c, getSubsReq{})
	require.Error(t, err)
	_, err = h.subscriptionGetChannels(c, getSubsReq{})
	require.Error(t, err)
	_, err = h.subscriptionGetApps(c, getAppSubsReq{})
	require.Error(t, err)
}

func TestHandler_SubscriptionGetDM(t *testing.T) {
	h := NewHandler("site-local")

	t.Run("returns single sub for target", func(t *testing.T) {
		c := newCtx(map[string]string{"account": "alice", "siteID": "site-local"})
		resp, err := h.subscriptionGetDM(c, getDMSubReq{TargetAccount: "bob"})
		require.NoError(t, err)
		assert.Equal(t, "bob", resp.Subscription.User.Account)
		assert.Equal(t, "site-local", resp.Subscription.SiteID)
	})

	t.Run("siteID mismatch", func(t *testing.T) {
		c := newCtx(map[string]string{"account": "alice", "siteID": "site-x"})
		_, err := h.subscriptionGetDM(c, getDMSubReq{TargetAccount: "bob"})
		require.Error(t, err)
	})
}

func TestHandler_SubscriptionAppOps(t *testing.T) {
	h := NewHandler("site-local")
	c := newCtx(map[string]string{"account": "alice", "siteID": "site-local"})

	t.Run("subscribeApp returns success", func(t *testing.T) {
		resp, err := h.subscriptionSubscribeApp(c, appSubscriptionReq{AppID: "app-1"})
		require.NoError(t, err)
		assert.True(t, resp.Success)
	})

	t.Run("unsubscribeApp returns success", func(t *testing.T) {
		resp, err := h.subscriptionUnsubscribeApp(c, appSubscriptionReq{AppID: "app-1"})
		require.NoError(t, err)
		assert.True(t, resp.Success)
	})

	t.Run("subscribeApp siteID mismatch", func(t *testing.T) {
		badC := newCtx(map[string]string{"account": "alice", "siteID": "site-x"})
		_, err := h.subscriptionSubscribeApp(badC, appSubscriptionReq{})
		require.Error(t, err)
	})

	t.Run("unsubscribeApp siteID mismatch", func(t *testing.T) {
		badC := newCtx(map[string]string{"account": "alice", "siteID": "site-x"})
		_, err := h.subscriptionUnsubscribeApp(badC, appSubscriptionReq{})
		require.Error(t, err)
	})
}

func TestHandler_RoomSubscriptionGet(t *testing.T) {
	h := NewHandler("site-local")

	t.Run("echoes roomID from param", func(t *testing.T) {
		c := newCtx(map[string]string{"account": "alice", "siteID": "site-local", "roomID": "r-42"})
		resp, err := h.roomSubscriptionGet(c)
		require.NoError(t, err)
		assert.Equal(t, "r-42", resp.Subscription.RoomID)
		assert.Equal(t, "alice", resp.Subscription.User.Account)
		assert.Equal(t, "site-local", resp.Subscription.SiteID)
	})

	t.Run("siteID mismatch", func(t *testing.T) {
		c := newCtx(map[string]string{"account": "alice", "siteID": "site-x", "roomID": "r-42"})
		_, err := h.roomSubscriptionGet(c)
		require.Error(t, err)
	})
}

func TestHandler_AppsList(t *testing.T) {
	h := NewHandler("site-local")

	t.Run("returns two mock apps", func(t *testing.T) {
		c := newCtx(map[string]string{"account": "alice", "siteID": "site-local"})
		resp, err := h.appsList(c)
		require.NoError(t, err)
		assert.Equal(t, 2, resp.Total)
		require.Len(t, resp.Apps, 2)
		assert.NotEmpty(t, resp.Apps[0].ID)
		assert.NotEmpty(t, resp.Apps[0].Name)
	})

	t.Run("siteID mismatch", func(t *testing.T) {
		c := newCtx(map[string]string{"account": "alice", "siteID": "site-x"})
		_, err := h.appsList(c)
		require.Error(t, err)
	})
}
