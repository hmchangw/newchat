package obs_test

import (
	"bytes"
	"fmt"
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
	`\b(?:Int64|Float64)(?:Observable)?(?:Counter|UpDownCounter|Histogram|Gauge)\(\s*"([^"]+)"`)

// instrumentDeclAnyArg matches the same constructions with any first argument.
// The two together find the bypass: a name hoisted into a constant is a name
// the scan above cannot read, so the instrument would slip past the registry
// without anyone intending it.
var instrumentDeclAnyArg = regexp.MustCompile(
	`\b(?:Int64|Float64)(?:Observable)?(?:Counter|UpDownCounter|Histogram|Gauge)\(\s*([^\s,)]+)`)

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

// registryHeading opens the one section that registers application instruments,
// tableRowFirstCell captures the first cell of a markdown table row, and
// backticked captures each `code` token inside it. Together they read that
// section's rows and nothing else.
const registryHeading = "## 13. Application instrument registry"

var (
	tableRowFirstCell = regexp.MustCompile(`(?m)^\|([^|]*)\|`)
	backticked        = regexp.MustCompile("`([^`]+)`")
)

// registeredInstruments is the set of names with a row in the registry section.
//
// The scope is deliberately narrow, and each narrowing closed a real hole.
// Searching the whole document let the prose register an instrument by
// accident: the section explaining why four families were deleted names all
// four, so a substring search accepted every one of them, and accepted any name
// that was a prefix of a documented one too. Reading every table was still too
// loose — the infrastructure and loadgen tables in §4 and §5 name plenty of
// series, and a row there says nothing about who reads an application
// instrument. Only §13 carries the "Read by" column that this gate exists to
// require, so only §13 counts.
func registeredInstruments(t *testing.T, repo fs.FS) map[string]struct{} {
	t.Helper()
	contract, err := fs.ReadFile(repo, contractDoc)
	require.NoError(t, err, "the metrics contract must exist at %s", contractDoc)

	start := bytes.Index(contract, []byte(registryHeading))
	require.GreaterOrEqual(t, start, 0, "%s must contain %q", contractDoc, registryHeading)
	registry := string(contract[start:])

	registered := map[string]struct{}{}
	for _, row := range tableRowFirstCell.FindAllStringSubmatch(registry, -1) {
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
			return fmt.Errorf("walk the Go source tree: %w", err)
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
			return fmt.Errorf("read Go source %q: %w", path, readErr)
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

	// Only §13 registers. The infrastructure and loadgen tables in §4/§5 name
	// plenty of series, but a row there carries no "Read by" column, so it
	// cannot stand in for registering an application instrument.
	for _, elsewhere := range []string{
		"chat_nats_server_up",
		"chat_jetstream_stream_up",
		"loadgen_consumer_sample_errors_total",
	} {
		assert.NotContains(t, registered, elsewhere,
			"%s has a row outside §13; only the registry section may register an instrument", elsewhere)
	}
}

// Observable instruments are real OTel constructors and must not bypass either
// gate. Nothing in the repo declares one today, which is exactly why the regexes
// missed them — the first one added would have shipped unregistered.
func TestInstrumentRegexesCoverObservableConstructors(t *testing.T) {
	const src = `
		a, _ := meter.Int64ObservableGauge("chat.example.observable.gauge")
		b, _ := meter.Float64ObservableCounter("chat.example.observable.counter")
		c, _ := meter.Int64ObservableUpDownCounter(nameFromAConstant)
	`

	var names []string
	for _, m := range instrumentDecl.FindAllStringSubmatch(src, -1) {
		names = append(names, m[1])
	}
	assert.Equal(t, []string{"chat.example.observable.gauge", "chat.example.observable.counter"}, names,
		"the documentation gate must see Observable declarations")

	var nonLiteral []string
	for _, m := range instrumentDeclAnyArg.FindAllStringSubmatch(src, -1) {
		if !strings.HasPrefix(m[1], `"`) {
			nonLiteral = append(nonLiteral, m[1])
		}
	}
	assert.Equal(t, []string{"nameFromAConstant"}, nonLiteral,
		"the literal-name gate must see Observable declarations too")
}

// Every NATS subscription subject becomes a metric label value on
// nats_slow_consumer_events_total, via subjectLabel in pkg/natsutil. That label
// carries a nosemgrep whose justification is "a registered subject is a
// per-subscription constant" — true today, but a claim with nothing enforcing
// it, which is precisely how the _INBOX leak survived review once already.
//
// This is what enforces it. A subject built from a room id, an account or any
// other per-entity value would be one series per entity, and the rule that would
// normally catch it is suppressed at that call site by design.
//
// The guard is deliberately shape-based rather than a whitelist of call sites:
// the risk is a subscribe nobody thought about, so a new one has to be spelled
// in a bounded form or fail here.
func TestEverySubscriptionSubjectIsBounded(t *testing.T) {
	repo := repoFS(t)

	var offenders []string
	require.NoError(t, walkGoSources(repo, func(path string, src []byte) {
		// loadgen is a client, not a service: it subscribes to per-user inboxes
		// on purpose and does not emit the slow-consumer counter.
		if strings.HasPrefix(path, "tools/") {
			return
		}
		for _, m := range subscribeCall.FindAllStringSubmatch(string(src), -1) {
			if arg := strings.TrimSpace(m[2]); !boundedSubjectArg(arg) {
				offenders = append(offenders, fmt.Sprintf("%s: %s(%s)", path, m[1], arg))
			}
		}
	}))

	assert.Empty(t, offenders, "a subscription subject reaches nats_slow_consumer_events_total as a "+
		"label value, so it must come from a pkg/subject builder or a stored field, never a string "+
		"built from an id. If one of these is genuinely bounded, widen boundedSubjectArg and say why.\n"+
		"Unbounded:\n  %s", strings.Join(offenders, "\n  "))
}

// subscribeCall captures the method name and first argument of a core-NATS
// subscribe. The o11y wrapper takes a context first, so the subject is the
// second argument there; both spellings are matched.
var subscribeCall = regexp.MustCompile(
	`\.((?:Queue)?Subscribe(?:Sync)?)\((?:[Cc]tx|context\.\w+\(\))?,?\s*([^,)]+)`)

// boundedSubjectArg accepts only shapes whose boundedness something enforces.
//
//   - A pkg/subject builder, provided its arguments name no entity. The prefix
//     alone is not enough: subject.RoomEvents(roomID) is a builder call and is
//     one series per room, so the arguments are checked against the same
//     identity vocabulary the cardinality semgrep rule uses.
//   - routerSubjectExpr, and nothing else shaped like it. A bare identifier
//     could hold anything — subj := fmt.Sprintf("chat.room.%s...", roomID) is
//     the shape that would slip through — so identifiers are rejected in
//     general. This one is admitted because parsePattern replaces every
//     {placeholder} with "*" before the subject reaches Subscribe, and
//     TestParsePattern_NatsSubjectNeverKeepsAPlaceholder is what keeps that
//     true. That is the difference between an exception and a hole.
func boundedSubjectArg(arg string) bool {
	if arg == routerSubjectExpr {
		return true
	}
	if !strings.HasPrefix(arg, "subject.") {
		return false
	}
	return !entityArg.MatchString(arg)
}

// routerSubjectExpr is natsrouter's registered route pattern, bounded by
// parsePattern rather than by its spelling. See boundedSubjectArg.
const routerSubjectExpr = "rt.natsSubject"

// entityArg matches an argument naming a per-entity value. The roots are the
// identity prefixes from the contract's forbidden-label list, so a subject
// builder taking a room, account, user or message id fails here for the same
// reason such a key fails the semgrep rule.
var entityArg = regexp.MustCompile(`(?i)\b(room|account|user|message|msg|thread|device|session|recipient)(id)?\b`)
