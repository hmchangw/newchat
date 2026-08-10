package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

func TestNotifSettings_ZeroValueIsPreEnforcementBehaviour(t *testing.T) {
	var ns notifSettings
	assert.False(t, ns.muteAll, "zero value must not mute")
	assert.False(t, ns.allowPriority, "zero value must not pierce")
	assert.False(t, ns.showInCall, "zero value must keep in-call suppressed")
	assert.False(t, ns.isPriority("alice"), "zero value has no priority contacts")
}

func TestNotifSettings_IsPriority(t *testing.T) {
	tests := []struct {
		name     string
		contacts map[string]struct{}
		account  string
		want     bool
	}{
		{"listed user", map[string]struct{}{"alice": {}}, "alice", true},
		{"listed bot", map[string]struct{}{"helper.bot": {}}, "helper.bot", true},
		{"not listed", map[string]struct{}{"alice": {}}, "bob", false},
		{"nil map", nil, "alice", false},
		{"empty map", map[string]struct{}{}, "alice", false},
		{"empty account never matches", map[string]struct{}{"": {}}, "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ns := notifSettings{priorityContacts: tt.contacts}
			assert.Equal(t, tt.want, ns.isPriority(tt.account))
		})
	}
}

func TestNoopUserSettings_EmptySnapshot(t *testing.T) {
	var s UserSettingsSnapshotter = noopUserSettings{}
	got, err := s.Snapshot(context.Background(), []string{"alice", "bob"})
	require.NoError(t, err)
	assert.Empty(t, got, "kill switch yields the zero notifSettings for every account")
}

func TestResolveNotifSettings(t *testing.T) {
	boolPtr := func(b bool) *bool { return &b }

	t.Run("nil settings yields zero value", func(t *testing.T) {
		assert.Equal(t, notifSettings{}, resolveNotifSettings(nil))
	})

	t.Run("empty settings yields zero value", func(t *testing.T) {
		assert.Equal(t, notifSettings{}, resolveNotifSettings(&model.UserSettings{}))
	})

	t.Run("all three set", func(t *testing.T) {
		got := resolveNotifSettings(&model.UserSettings{
			MuteAllNotifications:             boolPtr(true),
			AlwaysAllowPriorityNotifications: boolPtr(true),
			ShowNotificationsInCall:          boolPtr(true),
		})
		assert.True(t, got.muteAll)
		assert.True(t, got.allowPriority)
		assert.True(t, got.showInCall)
	})

	t.Run("explicit false is false, not unset", func(t *testing.T) {
		got := resolveNotifSettings(&model.UserSettings{
			MuteAllNotifications: boolPtr(false),
		})
		assert.False(t, got.muteAll)
	})

	t.Run("priority contacts become a set, empties dropped", func(t *testing.T) {
		got := resolveNotifSettings(&model.UserSettings{
			PriorityContacts: []string{"alice", "helper.bot", ""},
		})
		assert.True(t, got.isPriority("alice"))
		assert.True(t, got.isPriority("helper.bot"))
		assert.Len(t, got.priorityContacts, 2, "empty account must not enter the set")
	})

	t.Run("no priority contacts leaves a nil set", func(t *testing.T) {
		got := resolveNotifSettings(&model.UserSettings{})
		assert.Nil(t, got.priorityContacts)
		assert.False(t, got.isPriority("alice"))
	})
}

func TestMongoUserSettings_EmptyAccountsSkipsQuery(t *testing.T) {
	// A nil collection proves no query is attempted on the empty-accounts path.
	s := newMongoUserSettings(nil, 512, time.Second)
	got, err := s.Snapshot(context.Background(), nil)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestNewMongoUserSettings_Defaults(t *testing.T) {
	s := newMongoUserSettings(nil, 0, 0)
	assert.Equal(t, 512, s.batchSize)
	assert.Equal(t, 2*time.Second, s.timeout)
}
