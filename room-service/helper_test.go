package main

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/model"
)

func TestHasRole(t *testing.T) {
	tests := []struct {
		name   string
		roles  []model.Role
		target model.Role
		want   bool
	}{
		{"owner in [owner]", []model.Role{model.RoleOwner}, model.RoleOwner, true},
		{"member in [member]", []model.Role{model.RoleMember}, model.RoleMember, true},
		{"owner not in [member]", []model.Role{model.RoleMember}, model.RoleOwner, false},
		{"member not in [owner]", []model.Role{model.RoleOwner}, model.RoleMember, false},
		{"empty roles", []model.Role{}, model.RoleOwner, false},
		{"nil roles", nil, model.RoleOwner, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := hasRole(tt.roles, tt.target)
			if got != tt.want {
				t.Errorf("hasRole(%v, %q) = %v, want %v", tt.roles, tt.target, got, tt.want)
			}
		})
	}
}

func TestSanitizeError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"sentinel: invalid role", errInvalidRole, "invalid role: must be owner or member"},
		{"sentinel: only owners", errOnlyOwners, "only owners can update roles"},
		{"sentinel: cannot demote", errCannotDemoteLast, "cannot demote the last owner"},
		{"sentinel: already owner", errAlreadyOwner, "user is already an owner"},
		{"sentinel: not owner", errNotOwner, "user is not an owner"},
		{"sentinel: room type", errRoomTypeGuard, "role update is only allowed in channel rooms"},
		{"sentinel: target not member", errTargetNotMember, "target user is not a member of this room"},
		{"sentinel: not room member", errNotRoomMember, "only room members can list members"},
		{"sentinel: invalid org", errInvalidOrg, "invalid org"},
		{"sentinel: promote requires individual", errPromoteRequiresIndividual, "only individual members can be promoted to owner"},
		{"wrapped sentinel passes through", fmt.Errorf("get room: %w", errRoomTypeGuard), "get room: role update is only allowed in channel rooms"},
		{"safe owner message", errors.New("only owners can add members"), "only owners can add members"},
		{"safe cannot add", errors.New("cannot add members to a DM room"), "cannot add members to a DM room"},
		{"safe capacity", errors.New("room is at maximum capacity (1000)"), "room is at maximum capacity (1000)"},
		{"safe requester", errors.New("requester not in room: not found"), "requester not in room: not found"},
		{"safe invalid", errors.New("invalid request: bad json"), "invalid request: bad json"},
		{"internal db error", fmt.Errorf("mongo timeout"), "internal error"},
		{"generic error", fmt.Errorf("unexpected failure"), "internal error"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sanitizeError(tt.err)
			if got != tt.want {
				t.Errorf("sanitizeError(%v) = %q, want %q", tt.err, got, tt.want)
			}
		})
	}
}

func TestIsBot(t *testing.T) {
	tests := []struct {
		name    string
		account string
		want    bool
	}{
		{"bot suffix", "helper.bot", true},
		{"bot prefix", "p_scheduler", true},
		{"normal user", "alice", false},
		{"contains bot but not suffix", "botmaster", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isBot(tt.account))
		})
	}
}

func TestFilterBots(t *testing.T) {
	input := []string{"alice", "helper.bot", "bob", "p_scheduler"}
	got := filterBots(input)
	assert.Equal(t, []string{"alice", "bob"}, got)
}

func TestFilterBots_AllBots(t *testing.T) {
	input := []string{"helper.bot", "p_scheduler"}
	got := filterBots(input)
	assert.Nil(t, got)
}

func TestDedup(t *testing.T) {
	input := []string{"a", "b", "a", "c", "b"}
	got := dedup(input)
	assert.Equal(t, []string{"a", "b", "c"}, got)
}

func TestDedup_Empty(t *testing.T) {
	got := dedup([]string{})
	assert.Nil(t, got)
}

func TestSanitizeError_NotRoomMember_WhenWrapped(t *testing.T) {
	// Guards the errors.Is whitelist — wrapping (e.g. by add-member's
	// "expand channels: %w") must not lose the user-safe message.
	wrapped := fmt.Errorf("expand channels: %w", errNotRoomMember)
	assert.Equal(t, "only room members can list members", sanitizeError(wrapped))
}

func TestSanitizeError_RemoteMemberListPrefix(t *testing.T) {
	remote := errors.New("remote member.list: only room members can list members")
	assert.Equal(t, "remote member.list: only room members can list members", sanitizeError(remote))
}

func TestSanitizeError_RemoteMemberListWithContext(t *testing.T) {
	// Error from cross-site RPC includes site context; preserve user-safe message.
	remote := errors.New("expand channels: remote member.list: room not found")
	msg := sanitizeError(remote)
	assert.Contains(t, msg, "remote member.list:")
	assert.Contains(t, msg, "room not found")
}

func TestSanitizeError_TransportFailureStillOpaque(t *testing.T) {
	// Generic transport failure from the client — no user-safe substring — must still be "internal error".
	assert.Equal(t, "internal error", sanitizeError(errors.New("member.list request to site-eu: nats: timeout")))
}
