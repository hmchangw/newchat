package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
