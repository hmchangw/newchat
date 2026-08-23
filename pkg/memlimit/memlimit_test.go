package memlimit

import (
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// noSet fails the test if a limit is applied; used by the no-op cases.
func noSet(t *testing.T) func(int64) int64 {
	t.Helper()
	return func(int64) int64 {
		t.Fatal("no memory limit should have been applied")
		return 0
	}
}

// cgroupFile writes a fake cgroup limit file and returns its path.
func cgroupFile(t *testing.T, name, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	require.NoError(t, os.WriteFile(p, []byte(content), 0o600))
	return p
}

// absent returns a path that does not exist, standing in for the other layout.
func absent(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "absent")
}

// v2 is the layout on any current runtime, so it is tried first.
func TestSetFromFiles_CgroupV2(t *testing.T) {
	var got int64
	limit, err := setFromFiles(
		cgroupFile(t, "memory.max", "1073741824\n"), absent(t), 0.8, "",
		func(n int64) int64 { got = n; return math.MaxInt64 })

	require.NoError(t, err)
	assert.Positive(t, limit)
	assert.Equal(t, int64(858993459), limit, "80% of 1 GiB")
	assert.Equal(t, limit, got, "the reported limit must be the one applied")
}

// "max" means no quota; deriving a limit from it would be meaningless.
func TestSetFromFiles_CgroupV2Unlimited(t *testing.T) {
	limit, err := setFromFiles(
		cgroupFile(t, "memory.max", "max\n"), absent(t), 0.8, "", noSet(t))

	require.NoError(t, err)
	assert.Zero(t, limit)
}

// Older hosts only have v1, and its file lives elsewhere.
func TestSetFromFiles_FallsBackToCgroupV1(t *testing.T) {
	limit, err := setFromFiles(
		absent(t), cgroupFile(t, "memory.limit_in_bytes", "2147483648\n"), 0.5, "",
		func(int64) int64 { return math.MaxInt64 })

	require.NoError(t, err)
	assert.Positive(t, limit)
	assert.Equal(t, int64(1073741824), limit, "50% of 2 GiB")
}

// v1 spells unlimited as a near-int64-max sentinel, not a word.
func TestSetFromFiles_V1SentinelIsUnlimited(t *testing.T) {
	limit, err := setFromFiles(
		absent(t), cgroupFile(t, "memory.limit_in_bytes", "9223372036854771712\n"), 0.8, "", noSet(t))

	require.NoError(t, err)
	assert.Zero(t, limit)
}

// An operator-set GOMEMLIMIT is deliberate; never second-guess it.
func TestSetFromFiles_ExplicitEnvWins(t *testing.T) {
	limit, err := setFromFiles(
		cgroupFile(t, "memory.max", "1073741824\n"), "", 0.8, "500MiB", noSet(t))

	require.NoError(t, err)
	assert.Zero(t, limit, "an operator-set GOMEMLIMIT must never be overridden")
}

// Running outside a container is normal, not a failure.
func TestSetFromFiles_NoCgroupFilesIsNotAnError(t *testing.T) {
	limit, err := setFromFiles(absent(t), absent(t), 0.8, "", noSet(t))

	require.NoError(t, err, "a non-containerized host must still start")
	assert.Zero(t, limit)
}

// A fraction outside (0,1] is a config bug worth failing startup for.
func TestSetFromFiles_RejectsBadFraction(t *testing.T) {
	for _, f := range []float64{0, -1, 1.5, math.NaN()} {
		limit, err := setFromFiles("", "", f, "", noSet(t))
		require.Error(t, err, "fraction %v must be rejected", f)
		assert.Zero(t, limit)
	}
}

// A quota file we cannot read is worth surfacing, unlike a missing one.
func TestSetFromFiles_RejectsUnparseableQuota(t *testing.T) {
	limit, err := setFromFiles(cgroupFile(t, "memory.max", "banana\n"), "", 0.8, "", noSet(t))

	require.Error(t, err)
	assert.Zero(t, limit)
}

// A quota that rounds to zero must be skipped, not applied as zero.
func TestSetFromFiles_QuotaTooSmallToSplit(t *testing.T) {
	// A quota so small that fraction rounds it to zero must be skipped, not applied.
	limit, err := setFromFiles(cgroupFile(t, "memory.max", "1\n"), "", 0.1, "", noSet(t))

	require.NoError(t, err)
	assert.Zero(t, limit)
}

func TestSetFromCgroup_DoesNotPanicOnThisHost(t *testing.T) {
	// Exercises the real cgroup paths; any outcome is valid, a panic is not — an
	// unreadable cgroup file is a property of the host, not a bug in this package.
	assert.NotPanics(t, func() { _, _ = SetFromCgroup(0.8) })
}
