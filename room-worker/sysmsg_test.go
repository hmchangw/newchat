package main

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/model"
)

func TestFormatAddedSingle(t *testing.T) {
	got := formatAddedSingle(
		&model.User{EngName: "Alice", ChineseName: "愛麗絲"},
		&model.User{EngName: "Bob", ChineseName: "鮑勃"},
	)
	assert.Equal(t, `"Alice 愛麗絲" added "Bob 鮑勃" to the channel`, got)
}

func TestFormatAddedMulti(t *testing.T) {
	got := formatAddedMulti(&model.User{EngName: "Alice", ChineseName: "愛麗絲"})
	assert.Equal(t, `"Alice 愛麗絲" added members to the channel`, got)
}

func TestFormatRemovedUser(t *testing.T) {
	got := formatRemovedUser(&model.User{EngName: "Bob", ChineseName: "鮑勃"})
	assert.Equal(t, `"Bob 鮑勃" has been removed from the channel`, got)
}

func TestFormatRemovedOrg(t *testing.T) {
	got := formatRemovedOrg("Engineering")
	assert.Equal(t, `"Engineering" has been removed from the channel`, got)
}

func TestFormatLeft(t *testing.T) {
	got := formatLeft(&model.User{EngName: "Bob", ChineseName: "鮑勃"})
	assert.Equal(t, `"Bob 鮑勃" left the channel`, got)
}

func TestDisplayName(t *testing.T) {
	cases := []struct {
		name string
		user model.User
		want string
	}{
		{
			name: "both names set — concatenated",
			user: model.User{Account: "alice", EngName: "Alice", ChineseName: "愛麗絲"},
			want: "Alice 愛麗絲",
		},
		{
			name: "only EngName — use it",
			user: model.User{Account: "alice", EngName: "Alice"},
			want: "Alice",
		},
		{
			name: "only ChineseName — use it",
			user: model.User{Account: "alice", ChineseName: "愛麗絲"},
			want: "愛麗絲",
		},
		{
			name: "EngName equals ChineseName — render once",
			user: model.User{Account: "alice", EngName: "Bob", ChineseName: "Bob"},
			want: "Bob",
		},
		{
			name: "both empty — fall back to Account",
			user: model.User{Account: "alice"},
			want: "alice",
		},
		{
			name: "whitespace-only names — fall back to Account",
			user: model.User{Account: "alice", EngName: "  ", ChineseName: "\t"},
			want: "alice",
		},
		{
			name: "leading/trailing whitespace trimmed on each side",
			user: model.User{Account: "alice", EngName: "  Alice  ", ChineseName: " 愛 "},
			want: "Alice 愛",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, displayName(&tc.user))
		})
	}
}

func TestFormatLeft_FallsBackToAccount(t *testing.T) {
	got := formatLeft(&model.User{Account: "alice"})
	assert.Equal(t, `"alice" left the channel`, got)
}

func TestFormatAddedSingle_SingleNameSide(t *testing.T) {
	got := formatAddedSingle(
		&model.User{EngName: "Alice"},
		&model.User{ChineseName: "鮑勃"},
	)
	assert.Equal(t, `"Alice" added "鮑勃" to the channel`, got)
}
