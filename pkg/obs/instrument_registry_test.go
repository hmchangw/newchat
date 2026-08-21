package obs_test

import (
	"io/fs"
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

// instrumentDeclAnyArg matches the same constructions with any first argument.
// The two together find the bypass: a name hoisted into a constant is a name
// the scan above cannot read, so the instrument would slip past the registry
// without anyone intending it.
var instrumentDeclAnyArg = regexp.MustCompile(
	`\b(?:Int64|Float64)(?:Counter|UpDownCounter|Histogram|Gauge)\(\s*([^\s,)]+)`)

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
	repo := repoFS(t)

	registered := registeredInstruments(t, repo)
	require.NotEmpty(t, registered, "registry scan found no table rows — the parser is probably wrong")

	declared := map[string][]string{} // instrument name -> files declaring it
	require.NoError(t, walkGoSources(repo, func(path string, src []byte) {
		for _, m := range instrumentDecl.FindAllStringSubmatch(string(src), -1) {
			declared[m[1]] = append(declared[m[1]], path)
		}
	}))
	require.NotEmpty(t, declared, "instrument scan found nothing — the regex is probably wrong")

	var undocumented []string
	for name, files := range declared {
		if skipInstrument(files) {
			continue
		}
		if _, ok := registered[name]; !ok {
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

// TestInstrumentNamesAreLiterals keeps the registry guard above unfalsifiable.
// It reads source, not a running meter, so `meter.Float64Histogram(nameConst)`
// is invisible to it — the instrument ships undocumented and the guard stays
// green. Requiring the literal at the construction site costs nothing (the name
// is written once either way) and means the only way past the registry is to
// add the row.
func TestInstrumentNamesAreLiterals(t *testing.T) {
	repo := repoFS(t)

	var offenders []string
	require.NoError(t, walkGoSources(repo, func(path string, src []byte) {
		for _, m := range instrumentDeclAnyArg.FindAllStringSubmatch(string(src), -1) {
			if strings.HasPrefix(m[1], `"`) {
				continue
			}
			offenders = append(offenders, path+": "+m[0])
		}
	}))
	sort.Strings(offenders)

	assert.Empty(t, offenders,
		"an instrument name must be a string literal at its construction site, so %s can be checked against it:\n  %s",
		contractDoc, strings.Join(offenders, "\n  "))
}

// tableRowFirstCell captures the first cell of a markdown table row, and
// backticked captures each `code` token inside it. Together they read the
// registry's rows without reading its prose.
var (
	tableRowFirstCell = regexp.MustCompile(`(?m)^\|([^|]*)\|`)
	backticked        = regexp.MustCompile("`([^`]+)`")
)

// registeredInstruments is the set of names that have a row in the contract's
// tables.
//
// Matching the whole document instead would let the prose register an
// instrument by accident, which is not hypothetical: the section explaining why
// four families were deleted names all four, so a plain substring search
// accepted every one of them. Substring search also accepted any name that was
// a prefix of a documented one. A registered instrument has a row; a name only
// discussed in prose does not.
func registeredInstruments(t *testing.T, repo fs.FS) map[string]struct{} {
	t.Helper()
	contract, err := fs.ReadFile(repo, contractDoc)
	require.NoError(t, err, "the metrics contract must exist at %s", contractDoc)

	registered := map[string]struct{}{}
	for _, row := range tableRowFirstCell.FindAllStringSubmatch(string(contract), -1) {
		for _, name := range backticked.FindAllStringSubmatch(row[1], -1) {
			registered[name[1]] = struct{}{}
		}
	}
	return registered
}

// walkGoSources hands every non-test .go file in the repo to visit, keyed by its
// repo-relative slash path.
func walkGoSources(repo fs.FS, visit func(path string, src []byte)) error {
	return fs.WalkDir(repo, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "node_modules", "testdata", "vendor":
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		src, readErr := fs.ReadFile(repo, path)
		if readErr != nil {
			return readErr
		}
		visit(path, src)
		return nil
	})
}

// repoFS opens the repository as a root-scoped filesystem.
//
// os.Root rather than a plain path walk: gosec flags os.ReadFile on a
// WalkDir-supplied path (G122/G304) because the path can change identity
// between the walk and the read, and a symlink out of the tree would be
// followed. A root-scoped FS cannot escape the repository, which is also just
// the correct scope for a scan whose whole premise is "every file in this
// repository".
func repoFS(t *testing.T) fs.FS {
	t.Helper()
	root, err := os.OpenRoot(repoRoot(t))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, root.Close()) })
	return root.FS()
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

// TestRegistryMatchesTableRowsNotProse closes the hole the contract's own prose
// opened. The guard used to look for the instrument name anywhere in the
// document, and the section explaining *why* four families were deleted names
// all four — so re-adding `chat.nats.publish.retries` would have passed the
// gate that exists to stop exactly that, without a single doc edit. A name that
// is merely a prefix of a documented one slipped through the same way.
//
// A registered instrument is one that has a row in the registry tables, so that
// is what the guard reads.
func TestRegistryMatchesTableRowsNotProse(t *testing.T) {
	registered := registeredInstruments(t, repoFS(t))
	require.NotEmpty(t, registered, "registry scan found no table rows — the parser is probably wrong")

	for _, deleted := range []string{
		"chat.nats.publish.retries",
		"chat.nats.consumer.redeliveries",
		"chat.nats.requests",
		"chat.nats.request.handled",
	} {
		assert.NotContains(t, registered, deleted,
			"%s is discussed in the contract's prose but has no registry row; "+
				"re-adding it must fail the gate, not pass on the explanation of its removal", deleted)
	}

	// A live instrument still resolves, so the parser has not simply stopped
	// finding things.
	assert.Contains(t, registered, "chat.nats.consumer.loop.up")
	assert.Contains(t, registered, "rpc.server.call.duration")

	// Prefixes are distinct names, not matches.
	assert.NotContains(t, registered, "chat.nats.consumer.loop")
}
