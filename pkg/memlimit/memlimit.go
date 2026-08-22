// Package memlimit derives Go's soft memory limit from the container's cgroup
// quota, so a traffic burst degrades into GC pressure instead of an OOMKill.
package memlimit

import (
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
)

const (
	cgroupV2Path = "/sys/fs/cgroup/memory.max"
	cgroupV1Path = "/sys/fs/cgroup/memory/memory.limit_in_bytes"

	// cgroup v1 reports "no limit" as a near-maxint sentinel rather than a keyword.
	v1UnlimitedFloor = int64(1) << 62
)

// SetFromCgroup sets GOMEMLIMIT to fraction of the container's memory quota and
// returns the limit applied, or 0 when none was. An explicit GOMEMLIMIT, an
// unlimited cgroup, and a host with no cgroup files are all no-ops rather than
// errors.
func SetFromCgroup(fraction float64) (int64, error) {
	return setFromFiles(cgroupV2Path, cgroupV1Path, fraction, os.Getenv("GOMEMLIMIT"), debug.SetMemoryLimit)
}

func setFromFiles(v2Path, v1Path string, fraction float64, envLimit string, set func(int64) int64) (int64, error) {
	if math.IsNaN(fraction) || fraction <= 0 || fraction > 1 {
		return 0, fmt.Errorf("memory limit fraction must be in (0,1], got %v", fraction)
	}
	// An operator-set GOMEMLIMIT is deliberate; never second-guess it.
	if envLimit != "" {
		return 0, nil
	}

	quota, err := readQuota(v2Path, v1Path)
	if err != nil {
		return 0, err
	}
	limit := int64(float64(quota) * fraction)
	if quota <= 0 || limit <= 0 {
		return 0, nil
	}
	set(limit)
	return limit, nil
}

// readQuota returns the cgroup memory quota in bytes, preferring v2; 0 means
// unlimited or absent.
func readQuota(v2Path, v1Path string) (int64, error) {
	v2, err := readLimitFile(v2Path)
	if err != nil || v2 != 0 {
		return v2, err
	}
	return readLimitFile(v1Path)
}

func readLimitFile(path string) (int64, error) {
	// An empty path yields ENOENT, handled as "absent" below like any missing file.
	b, err := os.ReadFile(path) // #nosec G304 -- paths are package constants, never caller input
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return 0, nil
		}
		return 0, fmt.Errorf("read cgroup memory limit %s: %w", path, err)
	}
	s := strings.TrimSpace(string(b))
	if s == "max" || s == "" {
		return 0, nil
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse cgroup memory limit %s: %w", path, err)
	}
	if n <= 0 || n >= v1UnlimitedFloor {
		return 0, nil
	}
	return n, nil
}
