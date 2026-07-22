package model

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestUser_DisplayName(t *testing.T) {
	tests := []struct {
		name string
		user *User
		want string
	}{
		{"nil user", nil, ""},
		{"both names empty -> account", &User{Account: "alice", EngName: "", ChineseName: ""}, "alice"},
		{"eng empty -> account", &User{Account: "alice", EngName: "", ChineseName: "陳"}, "alice"},
		{"chinese empty -> account", &User{Account: "alice", EngName: "Alice", ChineseName: ""}, "alice"},
		{"equal names -> eng", &User{Account: "alice", EngName: "Same", ChineseName: "Same"}, "Same"},
		{"distinct names -> joined", &User{Account: "alice", EngName: "Alice", ChineseName: "陳"}, "Alice 陳"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.user.DisplayName())
		})
	}
}

// Bcrypt hash material at users.services.password.bcrypt must NEVER serialize
// to JSON (would leak into any outbound payload that embeds a *User), but MUST
// round-trip through BSON so botplatform-service can read it from Mongo.
func TestUser_PasswordBcrypt_JSONHiddenBSONVisible(t *testing.T) {
	u := User{
		ID:      "u1",
		Account: "alice",
		Services: Services{
			Password: PasswordCredentials{Bcrypt: "$2a$10$abcdefg"},
		},
	}

	out, err := json.Marshal(u)
	require.NoError(t, err)
	assert.NotContains(t, string(out), "bcrypt", "bcrypt field must not appear in JSON")
	assert.NotContains(t, string(out), "$2a$10$abcdefg", "bcrypt value must not appear in JSON")

	bdata, err := bson.Marshal(u)
	require.NoError(t, err)
	var back User
	require.NoError(t, bson.Unmarshal(bdata, &back))
	assert.Equal(t, "$2a$10$abcdefg", back.Services.Password.Bcrypt, "bcrypt must round-trip via BSON")
}

// String() must mask the bcrypt hash so a stray %v / %+v / structured log
// never carries the credential to disk.
func TestUser_String_MasksBcrypt(t *testing.T) {
	u := User{
		ID:      "u1",
		Account: "alice",
		Services: Services{
			Password: PasswordCredentials{Bcrypt: "$2a$10$ShouldNeverAppear"},
		},
	}
	s := u.String()
	assert.NotContains(t, s, "ShouldNeverAppear", "String() must not leak bcrypt material")
	assert.Contains(t, s, "alice", "String() should still show account")
}

// UserRoleBot constant exists so callers can compare against a typed value
// instead of a string literal.
func TestUserRoleBot_Value(t *testing.T) {
	assert.Equal(t, "bot", string(UserRoleBot))
}

func TestHasLoginRole(t *testing.T) {
	tests := []struct {
		name  string
		roles []UserRole
		want  bool
	}{
		{"nil roles", nil, false},
		{"empty roles", []UserRole{}, false},
		{"user only", []UserRole{UserRoleUser}, false},
		{"unknown role", []UserRole{"contributor"}, false},
		{"bot only", []UserRole{UserRoleBot}, true},
		{"admin only", []UserRole{UserRoleAdmin}, true},
		{"user + bot", []UserRole{UserRoleUser, UserRoleBot}, true},
		{"user + admin", []UserRole{UserRoleUser, UserRoleAdmin}, true},
		{"admin + bot", []UserRole{UserRoleAdmin, UserRoleBot}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, HasLoginRole(tc.roles))
		})
	}
}

func TestContainsBotRole(t *testing.T) {
	tests := []struct {
		name  string
		roles []UserRole
		want  bool
	}{
		{"nil", nil, false},
		{"empty", []UserRole{}, false},
		{"user only", []UserRole{UserRoleUser}, false},
		{"admin only", []UserRole{UserRoleAdmin}, false},
		{"bot only", []UserRole{UserRoleBot}, true},
		{"user + bot", []UserRole{UserRoleUser, UserRoleBot}, true},
		{"admin + bot", []UserRole{UserRoleAdmin, UserRoleBot}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, ContainsBotRole(tc.roles))
		})
	}
}
