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
	assert.Equal(t, `"Alice 愛麗絲" added "Bob 鮑勃" to the chatroom`, got)
}

func TestFormatAddedCounts(t *testing.T) {
	alice := &model.User{EngName: "Alice", ChineseName: "愛麗絲"}
	cases := []struct {
		name         string
		people, orgs int
		want         string
	}{
		{"people plural", 3, 0, `"Alice 愛麗絲" added 3 people to the chatroom`},
		{"people singular", 1, 0, `"Alice 愛麗絲" added 1 person to the chatroom`},
		{"orgs plural", 0, 2, `"Alice 愛麗絲" added 2 organizations to the chatroom`},
		{"orgs singular", 0, 1, `"Alice 愛麗絲" added 1 organization to the chatroom`},
		{"both plural", 2, 3, `"Alice 愛麗絲" added 2 people and 3 organizations to the chatroom`},
		{"both singular", 1, 1, `"Alice 愛麗絲" added 1 person and 1 organization to the chatroom`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, formatAddedCounts(alice, tc.people, tc.orgs))
		})
	}
}

func TestAddedContent(t *testing.T) {
	alice := &model.User{EngName: "Alice", ChineseName: "愛"}
	bob := &model.User{EngName: "Bob", ChineseName: "鮑"}
	lookup := func(a string) *model.User {
		if a == "bob" {
			return bob
		}
		return nil
	}
	assert.Equal(t, `"Alice 愛" added "Bob 鮑" to the chatroom`,
		addedContent(alice, []string{"bob"}, nil, lookup))
	assert.Equal(t, `"Alice 愛" added 1 person to the chatroom`,
		addedContent(alice, []string{"x"}, nil, lookup))
	assert.Equal(t, `"Alice 愛" added 1 organization to the chatroom`,
		addedContent(alice, nil, []string{"o1"}, lookup))
	assert.Equal(t, `"Alice 愛" added 1 person and 1 organization to the chatroom`,
		addedContent(alice, []string{"bob"}, []string{"o1"}, lookup))
	assert.Equal(t, `"Alice 愛" added 2 people and 1 organization to the chatroom`,
		addedContent(alice, []string{"bob", "carol"}, []string{"o1"}, lookup))
}

func TestWithoutAccount(t *testing.T) {
	cases := []struct {
		name     string
		accounts []string
		account  string
		want     []string
	}{
		{"present", []string{"alice", "bob", "carol"}, "alice", []string{"bob", "carol"}},
		{"absent", []string{"bob", "carol"}, "alice", []string{"bob", "carol"}},
		{"only element", []string{"alice"}, "alice", []string{}},
		{"nil input", nil, "alice", []string{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, withoutAccount(tc.accounts, tc.account))
		})
	}
}

func TestFormatRemovedUser(t *testing.T) {
	got := formatRemovedUser(
		&model.User{EngName: "Alice", ChineseName: "愛"},
		&model.User{EngName: "Bob", ChineseName: "鮑勃"},
	)
	assert.Equal(t, `"Alice 愛" removed "Bob 鮑勃" from the chatroom`, got)
}

func TestFormatRemovedOrg(t *testing.T) {
	got := formatRemovedOrg(&model.User{EngName: "Alice", ChineseName: "愛"}, "Engineering", "", "orgX")
	assert.Equal(t, `"Alice 愛" removed "Engineering" from the chatroom`, got)
}

func TestFormatLeft(t *testing.T) {
	got := formatLeft(&model.User{EngName: "Bob", ChineseName: "鮑勃"})
	assert.Equal(t, `"Bob 鮑勃" left the chatroom`, got)
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
	assert.Equal(t, `"alice" left the chatroom`, got)
}

func TestFormatAddedSingle_SingleNameSide(t *testing.T) {
	got := formatAddedSingle(
		&model.User{EngName: "Alice"},
		&model.User{ChineseName: "鮑勃"},
	)
	assert.Equal(t, `"Alice" added "鮑勃" to the chatroom`, got)
}

func TestDisplayName_DelegatesToCombineWithFallback(t *testing.T) {
	u := &model.User{Account: "alice", EngName: "Alice", ChineseName: "爱丽丝"}
	assert.Equal(t, "Alice 爱丽丝", displayName(u))

	u2 := &model.User{Account: "bob"}
	assert.Equal(t, "bob", displayName(u2), "both names empty → fallback to Account")
}

func TestDisplayOrg(t *testing.T) {
	assert.Equal(t, "Eng 工程部", displayOrg("Eng", "工程部", "orgX"))
	assert.Equal(t, "Eng", displayOrg("Eng", "", "orgX"))
	assert.Equal(t, "工程部", displayOrg("", "工程部", "orgX"))
	assert.Equal(t, "orgX", displayOrg("", "", "orgX"))
}

func TestFormatRemovedOrg_Fallbacks(t *testing.T) {
	alice := &model.User{EngName: "Alice", ChineseName: "愛"}
	assert.Equal(t, `"Alice 愛" removed "Eng 工程部" from the chatroom`, formatRemovedOrg(alice, "Eng", "工程部", "orgX"))
	assert.Equal(t, `"Alice 愛" removed "orgX" from the chatroom`, formatRemovedOrg(alice, "", "", "orgX"))
}
