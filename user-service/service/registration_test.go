package service

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/stretchr/testify/require"

	"github.com/Marz32onE/instrumentation-go/otel-nats/otelnats"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/user-service/models"
)

func startServiceTestNATS(t *testing.T) *otelnats.Conn {
	t.Helper()
	ns, err := natsserver.NewServer(&natsserver.Options{Port: -1})
	require.NoError(t, err)
	ns.Start()
	require.True(t, ns.ReadyForConnections(5*time.Second))
	t.Cleanup(ns.Shutdown)

	nc, err := otelnats.Connect(ns.ClientURL())
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	return nc
}

// AC-4.1: user.settings.get is registered as a no-body request/reply route.
func TestRegisterHandlers_AC_4_1_SettingsGet(t *testing.T) {
	nc := startServiceTestNATS(t)
	repo := &settingsRepositoryFake{getResult: &model.UserSettings{
		Account: "alice",
		SiteID:  "site-a",
		Data:    json.RawMessage(`{"theme":"dark"}`),
		Version: 3,
	}}
	svc := &UserService{settings: repo, siteID: "site-a"}
	router := natsrouter.New(nc, "user-service-test")
	svc.RegisterHandlers(router)
	t.Cleanup(func() { require.NoError(t, router.Shutdown(context.Background())) })

	resp, err := nc.Request(context.Background(), "chat.user.alice.request.user.site-a.settings.get", nil, 2*time.Second)

	require.NoError(t, err)
	var got models.UserSettingsView
	require.NoError(t, json.Unmarshal(resp.Data, &got))
	require.Equal(t, int64(3), got.Version)
	require.Equal(t, json.RawMessage(`{"theme":"dark"}`), got.Data)
	require.Equal(t, 1, repo.getCalls)
}

// AC-4.2: user.settings.set is registered as a body-bearing request/reply route.
func TestRegisterHandlers_AC_4_2_SettingsSet(t *testing.T) {
	nc := startServiceTestNATS(t)
	data := json.RawMessage(`{"theme":"light"}`)
	repo := &settingsRepositoryFake{setResult: &model.UserSettings{
		Account: "alice",
		SiteID:  "site-a",
		Data:    data,
		Version: 4,
	}}
	svc := &UserService{settings: repo, siteID: "site-a"}
	router := natsrouter.New(nc, "user-service-test")
	svc.RegisterHandlers(router)
	t.Cleanup(func() { require.NoError(t, router.Shutdown(context.Background())) })

	payload, err := json.Marshal(models.SetUserSettingsRequest{Data: data})
	require.NoError(t, err)
	resp, err := nc.Request(context.Background(), "chat.user.alice.request.user.site-a.settings.set", payload, 2*time.Second)

	require.NoError(t, err)
	var got models.UserSettingsView
	require.NoError(t, json.Unmarshal(resp.Data, &got))
	require.Equal(t, int64(4), got.Version)
	require.Equal(t, data, got.Data)
	require.Equal(t, 1, repo.setCalls)
	require.Equal(t, data, repo.data)
}
