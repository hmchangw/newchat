package orgdisplay

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Pins the $lookup/$group-parity semantics (dept/sect tiebreak, $max name selection, counting) as a pure unit.
func TestBuild(t *testing.T) {
	t.Run("sect-only org aggregates name and count", func(t *testing.T) {
		users := []User{
			{SectID: "X", SectName: "Engineering"},
			{SectID: "X", SectName: "Engineering"},
			{SectID: "X", SectName: "Engineering"},
		}
		got := Build([]string{"X"}, users)
		a := got["X"]
		require.NotNil(t, a)
		assert.False(t, a.IsDept)
		assert.Equal(t, "Engineering", a.SectName)
		assert.Empty(t, a.DeptName)
		assert.Equal(t, 3, a.MemberCount)
	})

	t.Run("dept match marks IsDept and keeps both branch names", func(t *testing.T) {
		users := []User{
			{DeptID: "X", DeptName: "Eng Dept", DeptTCName: "工程部"},
			{SectID: "X", SectName: "Eng Sect"},
		}
		got := Build([]string{"X"}, users)
		a := got["X"]
		require.NotNil(t, a)
		assert.True(t, a.IsDept)
		assert.Equal(t, "Eng Dept", a.DeptName)
		assert.Equal(t, "工程部", a.DeptTCName)
		assert.Equal(t, "Eng Sect", a.SectName)
		assert.Equal(t, 2, a.MemberCount)
	})

	t.Run("empty dept name still marks IsDept; sect name retained for fallback", func(t *testing.T) {
		users := []User{
			{DeptID: "X", DeptName: ""},
			{SectID: "X", SectName: "Engineering"},
		}
		got := Build([]string{"X"}, users)
		a := got["X"]
		require.NotNil(t, a)
		assert.True(t, a.IsDept)
		assert.Empty(t, a.DeptName)
		assert.Equal(t, "Engineering", a.SectName)
		assert.Equal(t, 2, a.MemberCount)
	})

	t.Run("deptId equal to sectId is counted once as a dept match", func(t *testing.T) {
		users := []User{
			{DeptID: "X", SectID: "X", DeptName: "Dept", SectName: "Sect"},
		}
		got := Build([]string{"X"}, users)
		a := got["X"]
		require.NotNil(t, a)
		assert.True(t, a.IsDept)
		assert.Equal(t, "Dept", a.DeptName)
		assert.Empty(t, a.SectName, "sect branch must not double-count the same user")
		assert.Equal(t, 1, a.MemberCount)
	})

	t.Run("name uses lexicographic max within a branch (matches $max)", func(t *testing.T) {
		users := []User{
			{DeptID: "X", DeptName: "Alpha"},
			{DeptID: "X", DeptName: "Zeta"},
		}
		got := Build([]string{"X"}, users)
		a := got["X"]
		require.NotNil(t, a)
		assert.Equal(t, "Zeta", a.DeptName)
		assert.Equal(t, 2, a.MemberCount)
	})

	t.Run("multiple orgs in one batch are kept separate", func(t *testing.T) {
		users := []User{
			{SectID: "X", SectName: "Eng"},
			{DeptID: "Y", DeptName: "Ops"},
			{SectID: "X", SectName: "Eng"},
		}
		got := Build([]string{"X", "Y"}, users)
		require.NotNil(t, got["X"])
		require.NotNil(t, got["Y"])
		assert.Equal(t, "Eng", got["X"].SectName)
		assert.Equal(t, 2, got["X"].MemberCount)
		assert.True(t, got["Y"].IsDept)
		assert.Equal(t, "Ops", got["Y"].DeptName)
		assert.Equal(t, 1, got["Y"].MemberCount)
	})

	t.Run("user matching two requested orgs contributes to both", func(t *testing.T) {
		users := []User{
			{DeptID: "X", SectID: "Y", DeptName: "DeptX", SectName: "SectY"},
		}
		got := Build([]string{"X", "Y"}, users)
		require.NotNil(t, got["X"])
		require.NotNil(t, got["Y"])
		assert.True(t, got["X"].IsDept)
		assert.Equal(t, 1, got["X"].MemberCount)
		assert.False(t, got["Y"].IsDept)
		assert.Equal(t, "SectY", got["Y"].SectName)
		assert.Equal(t, 1, got["Y"].MemberCount)
	})

	t.Run("org with no matching users is absent from the map", func(t *testing.T) {
		users := []User{{SectID: "other", SectName: "Other"}}
		got := Build([]string{"X"}, users)
		assert.Nil(t, got["X"])
	})

	t.Run("empty dept/sect ids on a user match nothing", func(t *testing.T) {
		users := []User{{DeptID: "", SectID: ""}}
		got := Build([]string{""}, users)
		assert.Nil(t, got[""], "empty org id must not be matched even if requested")
	})

	t.Run("descriptions roll up alongside names", func(t *testing.T) {
		users := []User{
			{DeptID: "X", DeptName: "Eng Dept", DeptDescription: "Dept desc"},
			{SectID: "X", SectName: "Eng Sect", SectDescription: "Sect desc"},
		}
		got := Build([]string{"X"}, users)
		a := got["X"]
		require.NotNil(t, a)
		assert.Equal(t, "Dept desc", a.DeptDescription)
		assert.Equal(t, "Sect desc", a.SectDescription)
	})

	t.Run("description uses lexicographic max within a branch", func(t *testing.T) {
		users := []User{
			{SectID: "X", SectDescription: "Alpha"},
			{SectID: "X", SectDescription: "Zeta"},
		}
		got := Build([]string{"X"}, users)
		require.NotNil(t, got["X"])
		assert.Equal(t, "Zeta", got["X"].SectDescription)
	})
}

// Name must match room-worker's sys-message formatter byte-for-byte.
func TestName(t *testing.T) {
	t.Run("nil aggregate falls back to orgID", func(t *testing.T) {
		assert.Equal(t, "org-123", Name(nil, "org-123"))
	})

	t.Run("dept name wins when present", func(t *testing.T) {
		a := &Agg{IsDept: true, DeptName: "Engineering", SectName: "ShouldNotShow"}
		assert.Equal(t, "Engineering", Name(a, "X"))
	})

	t.Run("dept and tc names combine", func(t *testing.T) {
		a := &Agg{IsDept: true, DeptName: "Engineering", DeptTCName: "工程"}
		assert.Equal(t, "Engineering 工程", Name(a, "X"))
	})

	t.Run("empty dept names fall through to sect names", func(t *testing.T) {
		a := &Agg{IsDept: true, DeptName: "", DeptTCName: "", SectName: "Engineering"}
		assert.Equal(t, "Engineering", Name(a, "X"))
	})

	t.Run("sect-only org renders sect name", func(t *testing.T) {
		a := &Agg{IsDept: false, SectName: "Engineering"}
		assert.Equal(t, "Engineering", Name(a, "X"))
	})

	t.Run("all names empty falls back to orgID", func(t *testing.T) {
		a := &Agg{IsDept: true}
		assert.Equal(t, "X", Name(a, "X"))
	})
}

func TestDescription(t *testing.T) {
	t.Run("nil aggregate yields empty", func(t *testing.T) {
		assert.Empty(t, Description(nil))
	})

	t.Run("dept description wins when the dept supplies the name", func(t *testing.T) {
		a := &Agg{IsDept: true, DeptName: "Eng", DeptDescription: "Dept desc", SectDescription: "Sect desc"}
		assert.Equal(t, "Dept desc", Description(a))
	})

	t.Run("dept name but empty dept description yields empty, not the sect's", func(t *testing.T) {
		a := &Agg{IsDept: true, DeptName: "Eng", DeptDescription: "", SectDescription: "Sect desc"}
		assert.Empty(t, Description(a))
	})

	t.Run("dept match without a name falls to the sect branch", func(t *testing.T) {
		a := &Agg{IsDept: true, DeptName: "", SectName: "Sect", DeptDescription: "Dept desc", SectDescription: "Sect desc"}
		assert.Equal(t, "Sect desc", Description(a))
	})

	t.Run("sect-only org uses sect description", func(t *testing.T) {
		a := &Agg{IsDept: false, SectName: "Sect", SectDescription: "Sect desc"}
		assert.Equal(t, "Sect desc", Description(a))
	})

	t.Run("sect description without a name yields empty (name fell back to orgID)", func(t *testing.T) {
		a := &Agg{IsDept: false, SectDescription: "Orphan desc"}
		assert.Empty(t, Description(a))
	})

	t.Run("all empty yields empty (no orgID fallback)", func(t *testing.T) {
		a := &Agg{IsDept: true}
		assert.Empty(t, Description(a))
	})
}
