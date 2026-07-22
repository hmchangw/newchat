package main

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/msgraph"
	"github.com/hmchangw/chat/teams-hr-sync/transform"
)

// fakeGroupReader serves canned groups/members for collectEmployees tests.
type fakeGroupReader struct {
	groups  map[string]*msgraph.GroupProfile
	members map[string][]msgraph.GraphUser
	err     error
}

func (f *fakeGroupReader) GetGroup(_ context.Context, id string) (*msgraph.GroupProfile, error) {
	if f.err != nil {
		return nil, f.err
	}
	g, ok := f.groups[id]
	if !ok {
		return nil, errors.New("unknown group")
	}
	return g, nil
}

func (f *fakeGroupReader) ListGroupMembers(_ context.Context, id string, _ int, fn func([]msgraph.GraphUser) error) (int, error) {
	return 0, fn(f.members[id])
}

func TestCollectEmployees_PerGroupSiteAndDedup(t *testing.T) {
	graph := &fakeGroupReader{
		groups: map[string]*msgraph.GroupProfile{
			"g1": {ID: "g1", DisplayName: "Engineering"},
			"g2": {ID: "g2", DisplayName: "Sales"},
		},
		members: map[string][]msgraph.GraphUser{
			"g1": {
				{ID: "u1", UserPrincipalName: "alice@corp.com"},
				{ID: "u3", UserPrincipalName: "bad-upn"},
			},
			"g2": {
				{ID: "u1", UserPrincipalName: "alice@corp.com"}, // dup: first group wins
				{ID: "u2", UserPrincipalName: "bob@corp.com"},
			},
		},
	}
	groups := []syncGroup{{GroupID: "g1", SiteID: "site-a"}, {GroupID: "g2", SiteID: "site-b"}}

	got, stats, err := collectEmployees(context.Background(), graph, transform.DefaultMapper{}, groups, nil, 100)
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, "alice", got[0].Account)
	assert.Equal(t, "site-a", got[0].SiteID, "dup account keeps the first group's site")
	assert.Equal(t, "g1", got[0].SectID, "group maps to section")
	assert.Equal(t, "bob", got[1].Account)
	assert.Equal(t, "site-b", got[1].SiteID)
	assert.Equal(t, collectStats{Groups: 2, Members: 4, InvalidUPN: 1, DupAccount: 1}, stats)
}

func TestCollectEmployees_SiteOverride(t *testing.T) {
	graph := &fakeGroupReader{
		groups: map[string]*msgraph.GroupProfile{"g1": {ID: "g1", DisplayName: "Engineering"}},
		members: map[string][]msgraph.GraphUser{"g1": {
			{ID: "u1", UserPrincipalName: "alice@corp.com"},
			{ID: "u2", UserPrincipalName: "bob@corp.com"},
		}},
	}
	groups := []syncGroup{{GroupID: "g1", SiteID: "site-a"}}
	// alice overridden; carol's override is for an account not in any group (unused, no error)
	overrides := map[string]string{"alice": "site-x", "carol": "site-z"}

	got, stats, err := collectEmployees(context.Background(), graph, transform.DefaultMapper{}, groups, overrides, 100)
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, "alice", got[0].Account)
	assert.Equal(t, "site-x", got[0].SiteID, "override wins over group default")
	assert.Equal(t, "bob", got[1].Account)
	assert.Equal(t, "site-a", got[1].SiteID, "no override → group default")
	assert.Equal(t, 1, stats.Overridden)
}

func TestCollectEmployees_GroupErrorAborts(t *testing.T) {
	boom := errors.New("graph down")
	_, _, err := collectEmployees(context.Background(), &fakeGroupReader{err: boom},
		transform.DefaultMapper{}, []syncGroup{{GroupID: "g1", SiteID: "site-a"}}, nil, 100)
	require.ErrorIs(t, err, boom)
}
