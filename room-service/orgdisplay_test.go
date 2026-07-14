package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildOrgDisplay reproduces the prior server-side $lookup/$group rollup for org
// members. These tests pin the semantics that the integration tests verify
// end-to-end against Mongo, but here as a pure unit so the dept/sect tiebreak,
// $max name selection, and member counting are exercised without a database.
func TestBuildOrgDisplay(t *testing.T) {
	t.Run("sect-only org aggregates name and count", func(t *testing.T) {
		users := []orgDisplayUser{
			{SectID: "X", SectName: "Engineering"},
			{SectID: "X", SectName: "Engineering"},
			{SectID: "X", SectName: "Engineering"},
		}
		got := buildOrgDisplay([]string{"X"}, users)
		a := got["X"]
		require.NotNil(t, a)
		assert.False(t, a.isDept)
		assert.Equal(t, "Engineering", a.sectName)
		assert.Empty(t, a.deptName)
		assert.Equal(t, 3, a.memberCount)
	})

	t.Run("dept match marks isDept and keeps both branch names", func(t *testing.T) {
		users := []orgDisplayUser{
			{DeptID: "X", DeptName: "Eng Dept", DeptTCName: "工程部"},
			{SectID: "X", SectName: "Eng Sect"},
		}
		got := buildOrgDisplay([]string{"X"}, users)
		a := got["X"]
		require.NotNil(t, a)
		assert.True(t, a.isDept)
		assert.Equal(t, "Eng Dept", a.deptName)
		assert.Equal(t, "工程部", a.deptTCName)
		assert.Equal(t, "Eng Sect", a.sectName)
		assert.Equal(t, 2, a.memberCount)
	})

	t.Run("empty dept name still marks isDept; sect name retained for fallback", func(t *testing.T) {
		users := []orgDisplayUser{
			{DeptID: "X", DeptName: ""},
			{SectID: "X", SectName: "Engineering"},
		}
		got := buildOrgDisplay([]string{"X"}, users)
		a := got["X"]
		require.NotNil(t, a)
		assert.True(t, a.isDept)
		assert.Empty(t, a.deptName)
		assert.Equal(t, "Engineering", a.sectName)
		assert.Equal(t, 2, a.memberCount)
	})

	t.Run("deptId equal to sectId is counted once as a dept match", func(t *testing.T) {
		users := []orgDisplayUser{
			{DeptID: "X", SectID: "X", DeptName: "Dept", SectName: "Sect"},
		}
		got := buildOrgDisplay([]string{"X"}, users)
		a := got["X"]
		require.NotNil(t, a)
		assert.True(t, a.isDept)
		assert.Equal(t, "Dept", a.deptName)
		assert.Empty(t, a.sectName, "sect branch must not double-count the same user")
		assert.Equal(t, 1, a.memberCount)
	})

	t.Run("name uses lexicographic max within a branch (matches $max)", func(t *testing.T) {
		users := []orgDisplayUser{
			{DeptID: "X", DeptName: "Alpha"},
			{DeptID: "X", DeptName: "Zeta"},
		}
		got := buildOrgDisplay([]string{"X"}, users)
		a := got["X"]
		require.NotNil(t, a)
		assert.Equal(t, "Zeta", a.deptName)
		assert.Equal(t, 2, a.memberCount)
	})

	t.Run("multiple orgs in one batch are kept separate", func(t *testing.T) {
		users := []orgDisplayUser{
			{SectID: "X", SectName: "Eng"},
			{DeptID: "Y", DeptName: "Ops"},
			{SectID: "X", SectName: "Eng"},
		}
		got := buildOrgDisplay([]string{"X", "Y"}, users)
		require.NotNil(t, got["X"])
		require.NotNil(t, got["Y"])
		assert.Equal(t, "Eng", got["X"].sectName)
		assert.Equal(t, 2, got["X"].memberCount)
		assert.True(t, got["Y"].isDept)
		assert.Equal(t, "Ops", got["Y"].deptName)
		assert.Equal(t, 1, got["Y"].memberCount)
	})

	t.Run("user matching two requested orgs contributes to both", func(t *testing.T) {
		users := []orgDisplayUser{
			{DeptID: "X", SectID: "Y", DeptName: "DeptX", SectName: "SectY"},
		}
		got := buildOrgDisplay([]string{"X", "Y"}, users)
		require.NotNil(t, got["X"])
		require.NotNil(t, got["Y"])
		assert.True(t, got["X"].isDept)
		assert.Equal(t, 1, got["X"].memberCount)
		assert.False(t, got["Y"].isDept)
		assert.Equal(t, "SectY", got["Y"].sectName)
		assert.Equal(t, 1, got["Y"].memberCount)
	})

	t.Run("org with no matching users is absent from the map", func(t *testing.T) {
		users := []orgDisplayUser{{SectID: "other", SectName: "Other"}}
		got := buildOrgDisplay([]string{"X"}, users)
		assert.Nil(t, got["X"])
	})

	t.Run("empty dept/sect ids on a user match nothing", func(t *testing.T) {
		users := []orgDisplayUser{{DeptID: "", SectID: ""}}
		got := buildOrgDisplay([]string{""}, users)
		assert.Nil(t, got[""], "empty org id must not be matched even if requested")
	})

	t.Run("descriptions roll up alongside names", func(t *testing.T) {
		users := []orgDisplayUser{
			{DeptID: "X", DeptName: "Eng Dept", DeptDescription: "Dept desc"},
			{SectID: "X", SectName: "Eng Sect", SectDescription: "Sect desc"},
		}
		got := buildOrgDisplay([]string{"X"}, users)
		a := got["X"]
		require.NotNil(t, a)
		assert.Equal(t, "Dept desc", a.deptDescription)
		assert.Equal(t, "Sect desc", a.sectDescription)
	})

	t.Run("description uses lexicographic max within a branch", func(t *testing.T) {
		users := []orgDisplayUser{
			{SectID: "X", SectDescription: "Alpha"},
			{SectID: "X", SectDescription: "Zeta"},
		}
		got := buildOrgDisplay([]string{"X"}, users)
		require.NotNil(t, got["X"])
		assert.Equal(t, "Zeta", got["X"].sectDescription)
	})
}

// orgDisplaySectName renders the member.sectName display string from the
// rollup, applying the dept-first-with-fallback tiebreak that must match
// room-worker's sys-message formatter byte-for-byte.
func TestOrgDisplaySectName(t *testing.T) {
	t.Run("nil aggregate falls back to orgID", func(t *testing.T) {
		assert.Equal(t, "org-123", orgDisplaySectName(nil, "org-123"))
	})

	t.Run("dept name wins when present", func(t *testing.T) {
		a := &orgDisplayAgg{isDept: true, deptName: "Engineering", sectName: "ShouldNotShow"}
		assert.Equal(t, "Engineering", orgDisplaySectName(a, "X"))
	})

	t.Run("dept and tc names combine", func(t *testing.T) {
		a := &orgDisplayAgg{isDept: true, deptName: "Engineering", deptTCName: "工程"}
		assert.Equal(t, "Engineering 工程", orgDisplaySectName(a, "X"))
	})

	t.Run("empty dept names fall through to sect names", func(t *testing.T) {
		a := &orgDisplayAgg{isDept: true, deptName: "", deptTCName: "", sectName: "Engineering"}
		assert.Equal(t, "Engineering", orgDisplaySectName(a, "X"))
	})

	t.Run("sect-only org renders sect name", func(t *testing.T) {
		a := &orgDisplayAgg{isDept: false, sectName: "Engineering"}
		assert.Equal(t, "Engineering", orgDisplaySectName(a, "X"))
	})

	t.Run("all names empty falls back to orgID", func(t *testing.T) {
		a := &orgDisplayAgg{isDept: true}
		assert.Equal(t, "X", orgDisplaySectName(a, "X"))
	})
}

func TestOrgDisplayDescription(t *testing.T) {
	t.Run("nil aggregate yields empty", func(t *testing.T) {
		assert.Empty(t, orgDisplayDescription(nil))
	})

	t.Run("dept description wins when the dept supplies the name", func(t *testing.T) {
		a := &orgDisplayAgg{isDept: true, deptName: "Eng", deptDescription: "Dept desc", sectDescription: "Sect desc"}
		assert.Equal(t, "Dept desc", orgDisplayDescription(a))
	})

	t.Run("dept name but empty dept description yields empty, not the sect's", func(t *testing.T) {
		a := &orgDisplayAgg{isDept: true, deptName: "Eng", deptDescription: "", sectDescription: "Sect desc"}
		assert.Empty(t, orgDisplayDescription(a))
	})

	t.Run("dept match without a name falls to the sect branch", func(t *testing.T) {
		a := &orgDisplayAgg{isDept: true, deptName: "", sectName: "Sect", deptDescription: "Dept desc", sectDescription: "Sect desc"}
		assert.Equal(t, "Sect desc", orgDisplayDescription(a))
	})

	t.Run("sect-only org uses sect description", func(t *testing.T) {
		a := &orgDisplayAgg{isDept: false, sectName: "Sect", sectDescription: "Sect desc"}
		assert.Equal(t, "Sect desc", orgDisplayDescription(a))
	})

	t.Run("sect description without a name yields empty (name fell back to orgID)", func(t *testing.T) {
		a := &orgDisplayAgg{isDept: false, sectDescription: "Orphan desc"}
		assert.Empty(t, orgDisplayDescription(a))
	})

	t.Run("all empty yields empty (no orgID fallback)", func(t *testing.T) {
		a := &orgDisplayAgg{isDept: true}
		assert.Empty(t, orgDisplayDescription(a))
	})
}
