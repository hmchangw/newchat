package obs_test

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// contractDoc is the single place a metric's purpose is written down: who reads
// it, which dashboard or alert consumes it, and whether a platform exporter
// already answers the same question.
const contractDoc = "docs/specs/o11y/nats-metrics-contract.md"

// instrumentDecl matches an OTel instrument construction, capturing its name.
var instrumentDecl = regexp.MustCompile(
	`\b(?:Int64|Float64)(?:Counter|UpDownCounter|Histogram|Gauge)\(\s*"([^"]+)"`)

// TestEveryInstrumentIsDocumented makes "is this metric necessary?" a mechanical
// gate instead of a question that has to be re-litigated in review.
//
// A metric nobody reads still costs: a series in the backend forever, an
// attribute set on a hot path, and a reviewer's time on every PR that touches
// it. Two instruments shipped and were later deleted for exactly this —
// chat.nats.publish.retries never had a producer, and
// chat.nats.consumer.redeliveries duplicated the broker. Both would have been
// caught here, because neither could have been given a consumer in the doc.
//
// Adding an instrument therefore means naming it in the contract next to what
// reads it. If nothing reads it yet, that is the finding.
func TestEveryInstrumentIsDocumented(t *testing.T) {
	root := repoRoot(t)

	contract, err := os.ReadFile(filepath.Join(root, contractDoc))
	require.NoError(t, err, "the metrics contract must exist at %s", contractDoc)
	documented := string(contract)

	declared := map[string][]string{} // instrument name -> files declaring it
	require.NoError(t, filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "node_modules", "testdata", "vendor":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		src, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		rel, _ := filepath.Rel(root, path)
		for _, m := range instrumentDecl.FindAllStringSubmatch(string(src), -1) {
			declared[m[1]] = append(declared[m[1]], rel)
		}
		return nil
	}))
	require.NotEmpty(t, declared, "instrument scan found nothing — the regex is probably wrong")

	var undocumented []string
	for name, files := range declared {
		if skipInstrument(files) {
			continue
		}
		if !strings.Contains(documented, name) {
			undocumented = append(undocumented, name+"  (declared in "+strings.Join(files, ", ")+")")
		}
	}
	sort.Strings(undocumented)

	assert.Empty(t, undocumented,
		"every instrument must appear in %s alongside the dashboard, alert or SLO that reads it, "+
			"and whether a platform exporter already answers the same question. "+
			"If nothing reads it, do not add it.\nUndocumented:\n  %s",
		contractDoc, strings.Join(undocumented, "\n  "))
}

// skipInstrument exempts declarations that are not production telemetry: the
// no-op fallbacks each constructor installs when instrument creation fails, and
// the one-shot data-migration tools, which are supervised for a migration
// window rather than run as always-on services.
func skipInstrument(files []string) bool {
	for _, f := range files {
		if !strings.HasPrefix(f, "data-migration/") {
			return false
		}
	}
	return true
}

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	require.NoError(t, err)
	for {
		if _, statErr := os.Stat(filepath.Join(dir, "go.mod")); statErr == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		require.NotEqual(t, dir, parent, "walked past the filesystem root without finding go.mod")
		dir = parent
	}
}
