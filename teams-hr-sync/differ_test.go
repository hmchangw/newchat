package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

func teamsEmployee(account, siteID string) model.IEmployee {
	return model.IEmployee{
		Account: account, SiteID: siteID,
		EngName: "Name " + account,
		IOrg:    model.IOrg{SectID: "g1", SectName: "Engineering"},
	}
}

func TestDiffEmployees_Matrix(t *testing.T) {
	unchanged := teamsEmployee("carol", "site-a")
	updatedStored := teamsEmployee("dave", "site-a")
	updatedCurrent := updatedStored
	updatedCurrent.SectName = "Engineering v2" // org change counts as updated

	current := []model.IEmployee{
		teamsEmployee("alice", "site-a"), // absent in store -> created
		unchanged,                        // equal -> omitted
		updatedCurrent,                   // differs -> updated
	}
	stored := []model.IEmployee{
		unchanged,
		updatedStored,
		teamsEmployee("eve", "site-a"),   // absent in graph -> quit
		teamsEmployee("frank", "site-b"), // quit grouped under its own site
	}

	got := diffEmployees(current, stored)
	require.Len(t, got.Upserts, 2)
	assert.Equal(t, "alice", got.Upserts[0].Account)
	assert.Equal(t, model.IChangeTypeNewHire, got.Upserts[0].ChangeType)
	assert.Equal(t, "dave", got.Upserts[1].Account)
	assert.Equal(t, model.IChangeTypeUpdate, got.Upserts[1].ChangeType)
	assert.Equal(t, map[string][]string{"site-a": {"eve"}, "site-b": {"frank"}}, got.Quits)
}

func TestDiffEmployees_EmptyStoreFirstRun(t *testing.T) {
	current := []model.IEmployee{teamsEmployee("alice", "site-a"), teamsEmployee("bob", "site-a")}
	got := diffEmployees(current, nil)
	require.Len(t, got.Upserts, 2)
	for _, u := range got.Upserts {
		assert.Equal(t, model.IChangeTypeNewHire, u.ChangeType)
	}
	assert.Empty(t, got.Quits)
}

func TestDiffEmployees_AllQuitWhenGraphEmpty(t *testing.T) {
	got := diffEmployees(nil, []model.IEmployee{teamsEmployee("alice", "site-a"), teamsEmployee("bob", "site-a")})
	assert.Empty(t, got.Upserts)
	assert.Equal(t, map[string][]string{"site-a": {"alice", "bob"}}, got.Quits)
}
