package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

func TestPresenceUser_Account(t *testing.T) {
	assert.Equal(t, "u-000000", presenceAccount(0))
	assert.Equal(t, "u-000042", presenceAccount(42))
}

func TestPresenceUser_HelloOnline(t *testing.T) {
	u := newPresenceUser(1, "site-local")
	tr := u.hello(1000)
	assert.Equal(t, "chat.user.u-000001.event.presence.site-local.hello", tr.subject)
	assert.Equal(t, model.StatusOnline, tr.expect)

	var h model.Hello
	require.NoError(t, json.Unmarshal(tr.payload, &h))
	assert.Equal(t, u.connID, h.ConnID)
	assert.Equal(t, int64(1000), h.Timestamp)
	assert.Equal(t, model.StatusOnline, u.status)
}

func TestPresenceUser_PingIsNoOp(t *testing.T) {
	u := newPresenceUser(1, "site-local")
	u.hello(1000)
	tr := u.ping(2000)
	assert.Equal(t, "chat.user.u-000001.event.presence.site-local.ping", tr.subject)
	assert.Equal(t, model.StatusNone, tr.expect, "steady-state ping must expect no publish")
}

func TestPresenceUser_ActivityFlip(t *testing.T) {
	u := newPresenceUser(1, "site-local")
	u.hello(1000)

	away := u.setAway(true, 2000)
	assert.Equal(t, model.StatusAway, away.expect)
	var a model.Activity
	require.NoError(t, json.Unmarshal(away.payload, &a))
	assert.True(t, a.Away)
	assert.Equal(t, model.StatusAway, u.status)

	back := u.setAway(false, 3000)
	assert.Equal(t, model.StatusOnline, back.expect)
}

func TestPresenceUser_ActivityNoChangeNoPublish(t *testing.T) {
	u := newPresenceUser(1, "site-local")
	u.hello(1000)
	tr := u.setAway(false, 2000) // already online/active
	assert.Equal(t, model.StatusNone, tr.expect)
}

func TestPresenceUser_ByeOffline(t *testing.T) {
	u := newPresenceUser(1, "site-local")
	u.hello(1000)
	tr := u.bye(4000)
	assert.Equal(t, "chat.user.u-000001.event.presence.site-local.bye", tr.subject)
	assert.Equal(t, model.StatusOffline, tr.expect)
	assert.Equal(t, model.StatusOffline, u.status)
}

func TestNewPresenceUserForAccount(t *testing.T) {
	u := newPresenceUserForAccount("user-42", "site-x")
	assert.Equal(t, "user-42", u.account)
	assert.Equal(t, "c-user-42", u.connID)
	assert.Equal(t, "site-x", u.siteID)
	assert.Equal(t, model.StatusOffline, u.status)
	assert.Equal(t, -1, u.idx)
}

func TestNewPresenceUser_DelegatesAndKeepsIndex(t *testing.T) {
	u := newPresenceUser(7, "site-y")
	assert.Equal(t, presenceAccount(7), u.account)
	assert.Equal(t, "c-"+presenceAccount(7), u.connID)
	assert.Equal(t, 7, u.idx)
	assert.Equal(t, model.StatusOffline, u.status)
}
