package obs_test

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
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
		offenders = append(offenders, unboundedSubscriptions(path, src)...)
	}))
	sort.Strings(offenders)

	assert.Empty(t, offenders, "a subscription subject reaches nats_slow_consumer_events_total as a "+
		"label value, so it must come from a pkg/subject builder or a stored field, never a string "+
		"built from an id. If one of these is genuinely bounded, widen boundedSubjectArg and say why.\n"+
		"Unbounded:\n  %s", strings.Join(offenders, "\n  "))
}

// unboundedSubscriptions reports every subscribe in one file whose subject
// argument is not provably bounded.
//
// This parses rather than pattern-matches, and the reason is a bug that shipped:
// the regex it replaces captured the subject with `[^,)]+`, which stops at the
// first `)`. `Subscribe(ctx, subject.RoomEvents(roomID), handler)` was therefore
// captured as `subject.RoomEvents(roomID` — unbalanced — and builderArgs finds
// no closing parenthesis in that, returns "no arguments", and the call is
// accepted. The guard passed while admitting exactly the per-room subscription
// it exists to reject. A source-level scanner that has to re-derive Go's
// grammar will keep losing to Go's grammar; the parser already knows it.
//
// A file that will not parse is reported rather than skipped. Skipping is how a
// scanner goes quiet without going green.
func unboundedSubscriptions(path string, src []byte) []string {
	file, err := parser.ParseFile(token.NewFileSet(), path, src, 0)
	if err != nil {
		return []string{fmt.Sprintf("%s: could not parse, so its subscriptions went unchecked: %v", path, err)}
	}

	var offenders []string
	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || !subscribeMethods[sel.Sel.Name] {
			return true
		}
		arg, ok := subjectArg(call)
		if !ok {
			return true
		}
		if expr := types.ExprString(arg); !boundedSubjectArg(expr) {
			offenders = append(offenders, fmt.Sprintf("%s: %s(%s)", path, sel.Sel.Name, expr))
		}
		return true
	})
	return offenders
}

// subjectArg picks the subject out of a subscribe call. The o11y wrapper takes a
// context first, so the subject is the second argument there; nats.go's own
// methods take it first.
func subjectArg(call *ast.CallExpr) (ast.Expr, bool) {
	if len(call.Args) == 0 {
		return nil, false
	}
	if isContextArg(call.Args[0]) {
		if len(call.Args) < 2 {
			return nil, false
		}
		return call.Args[1], true
	}
	return call.Args[0], true
}

// isContextArg recognises the leading context of the o11y wrapper — a `ctx`
// identifier or a context.Background()/TODO() call.
func isContextArg(arg ast.Expr) bool {
	switch e := arg.(type) {
	case *ast.Ident:
		return strings.EqualFold(e.Name, "ctx")
	case *ast.CallExpr:
		sel, ok := e.Fun.(*ast.SelectorExpr)
		if !ok {
			return false
		}
		pkg, ok := sel.X.(*ast.Ident)
		return ok && pkg.Name == "context"
	}
	return false
}

// subscribeMethods is every spelling nats.go offers, not only the ones in use
// today. A guard that only knew the shapes already in the repo would pass the
// day someone reached for a different one, which is the day it is supposed to
// earn its place.
var subscribeMethods = map[string]bool{
	"Subscribe":                  true,
	"SubscribeSync":              true,
	"QueueSubscribe":             true,
	"QueueSubscribeSync":         true,
	"QueueSubscribeSyncWithChan": true,
	"ChanSubscribe":              true,
	"ChanQueueSubscribe":         true,
}

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
	for _, a := range strings.Split(builderArgs(arg), ",") {
		if entityArg.MatchString(strings.TrimSpace(a)) {
			return false
		}
	}
	return true
}

// builderArgs returns what a builder call was passed, or "" for a plain
// selector such as subject.SomeConstant.
//
// Splitting the name off the arguments matters in both directions. Matching
// over the whole call text made the builder's own name part of the evidence,
// so subject.RoomEvents(siteID) was rejected for saying "Room" — and matching
// a bare root anywhere in the text was the only thing catching
// subject.RoomEvents(parentRoomID), for the same accidental reason. Neither is
// a judgement about the argument, which is the only thing that decides how
// many subjects the call can produce.
func builderArgs(call string) string {
	open := strings.Index(call, "(")
	closing := strings.LastIndex(call, ")")
	if open < 0 || closing < open {
		return ""
	}
	return call[open+1 : closing]
}

// routerSubjectExpr is natsrouter's registered route pattern, bounded by
// parsePattern rather than by its spelling. See boundedSubjectArg.
const routerSubjectExpr = "rt.natsSubject"

// entityArg matches an argument naming a per-entity value. The roots are the
// identity prefixes from the contract's forbidden-label list, so a subject
// builder taking a room, account, user or message id fails here for the same
// reason such a key fails the semgrep rule.
//
// The root list is the contract's, in full. An earlier version carried only
// the nine roots that came to mind while writing it and omitted request,
// trace, doc, span, tenant, org, run, inbox and pod — so
// subject.SomeBuilder(orgID) was one subject per organisation and passed.
// Keeping the two lists identical is the point: a guard that enforces a
// smaller vocabulary than the rule it backs is a guard with a published list
// of ways around it.
//
// The shape is the semgrep rule's identity branch, transcribed: the argument
// is anchored end to end, so a root has to be the whole identifier bar a
// namespace qualifier and an id tail. That distinction is what separates
// parentRoomID from userAgent — both contain a root, only one of them *is* an
// identity — and getting it from \b instead was wrong twice over. \b sees no
// boundary at a camel hump, so parentRoomID and destinationOrgID passed; and
// it sees one anywhere else, so any text containing a root word matched
// whether or not the argument named an entity.
//
// Both qualifier forms are here for the same reason they are in the semgrep
// rule: destination_room_id and destinationRoomID are the same argument, and
// a guard that knows only one spelling is a guard with a published workaround.
var entityArg = regexp.MustCompile(
	`^(?:[A-Za-z0-9]+[._])*(?:` +
		`(?:room|account|user|message|msg|thread|device|session|recipient|` +
		`request|trace|doc|span|tenant|org|run|inbox|pod)` +
		`|[A-Za-z0-9]*(?:Room|Account|User|Message|Msg|Thread|Device|Session|Recipient|` +
		`Request|Trace|Doc|Span|Tenant|Org|Run|Inbox|Pod)` +
		`)(?:[._]?[uU]?[iI][dD])?$`)

// identityRoots is the vocabulary the forbidden-label rules enforce, and the
// thing four separate lists were each keeping their own copy of.
//
// Two in `.semgrep/metrics.yml` (the literal-key branch's lowercase and camel
// alternations), one more there for semconv key constants, and entityArg above.
// They drifted exactly as you would expect: msg and thread reached the semconv
// list and entityArg and never reached the two literal ones, so
// attribute.String("thread_id", …) passed a gate whose own documentation said
// it would not — in a repo that has thread rooms.
//
// Adding the two missing words fixes today. TestIdentityRootsAgree is what
// fixes tomorrow, because the next word will be added to one list by someone
// who has no reason to know the other three exist.
var identityRoots = []string{
	"room", "account", "user", "message", "msg", "thread", "device", "session",
	"recipient", "request", "trace", "doc", "span", "tenant", "org", "run",
	"inbox", "pod",
}

// semgrepRootLists pulls every identity-root alternation out of the semgrep
// rule file. The three are spelled differently — lowercase, camel, and camel
// again for semconv constants — so they are compared as lowercased sets.
var semgrepRootLists = regexp.MustCompile(`\(\?:([Rr]oom\|[^)]*)\)`)

// TestIdentityRootsAgree fails when any of the lists drifts from the others.
//
// It reads the rule file rather than a copy of it: a test asserting that two
// Go constants match would have said nothing here, because the lists that
// disagreed were in YAML.
func TestIdentityRootsAgree(t *testing.T) {
	repo := repoFS(t)
	rules, err := fs.ReadFile(repo, ".semgrep/metrics.yml")
	require.NoError(t, err)

	want := make(map[string]struct{}, len(identityRoots))
	for _, r := range identityRoots {
		want[r] = struct{}{}
	}

	found := semgrepRootLists.FindAllStringSubmatch(string(rules), -1)
	require.NotEmpty(t, found, "no identity-root alternation found in .semgrep/metrics.yml — "+
		"the rule was restructured and this guard no longer reads it")

	for i, m := range found {
		got := make(map[string]struct{})
		for _, w := range strings.Split(m[1], "|") {
			got[strings.ToLower(w)] = struct{}{}
		}
		assert.Equal(t, want, got,
			"identity-root list %d in .semgrep/metrics.yml disagrees with identityRoots in %s.\n"+
				"Every list of these roots must carry the same words: a rule that enforces a "+
				"smaller vocabulary than its siblings is a gate with a published way around it.",
			i+1, "pkg/obs/instrument_registry_test.go")
	}

	// entityArg is built from a hand-written alternation too, so check it the
	// same way rather than trusting that it was updated alongside.
	for _, root := range identityRoots {
		assert.True(t, entityArg.MatchString(root+"ID"),
			"entityArg does not know the root %q, which identityRoots lists", root)
	}
}

// TestBoundedSubjectArg pins the guard's own decisions.
//
// Without this, TestEverySubscriptionSubjectIsBounded passing proves only that
// today's call sites happen to be spelled acceptably — a regex edit that
// accidentally accepted everything would keep it green, which is the failure
// mode a guard exists to prevent. So the accept and reject sides are asserted
// directly, one case per independently-editable branch.
func TestBoundedSubjectArg(t *testing.T) {
	tests := []struct {
		name    string
		arg     string
		bounded bool
	}{
		{"router stored pattern", routerSubjectExpr, true},
		{"builder with no arguments", "subject.MessagesCanonicalAll()", true},
		{"builder taking a site id", "subject.InboxExternalAll(siteID)", true},
		{"builder taking a wildcard", "subject.RoomEvents(subject.Wildcard)", true},

		{"camel-prefixed entity argument", "subject.RoomEvents(parentRoomID)", false},
		{"camel-prefixed org argument", "subject.SomeBuilder(destinationOrgID)", false},

		{"bare identifier", "subj", false},
		{"struct field", "h.subject", false},
		{"formatted string", `fmt.Sprintf("chat.room.%s.events", roomID)`, false},
		{"string literal", `"chat.room.abc.events"`, false},
		{"builder-shaped but not a builder", "subjects.RoomEvents()", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.bounded, boundedSubjectArg(tt.arg))
		})
	}
}

// TestEntityArg_CoversTheForbiddenIdentityVocabulary asserts that every identity
// root the metrics contract forbids as a label is also rejected as a subject
// builder argument, in every tail spelling.
//
// The two lists had drifted: entityArg knew room, account, user, message,
// msg, thread, device, session and recipient, while the contract — and the
// semgrep cardinality rule that enforces it — also list request, trace, doc,
// span, tenant, org, run, inbox and pod. subject.SomeBuilder(orgID) is one
// subject per organisation whichever list you consult, so the guard has to
// know the whole vocabulary rather than the part that came to mind.
//
// The UID tail is here for the same reason: accountUID is an identifier and
// the earlier `(id)?` tail did not match it, because there is no word boundary
// between "account" and "UID".
func TestEntityArg_CoversTheForbiddenIdentityVocabulary(t *testing.T) {
	roots := []string{
		"room", "account", "user", "message", "msg", "thread", "device",
		"session", "recipient", "request", "trace", "doc", "span", "tenant",
		"org", "run", "inbox", "pod",
	}
	tails := []string{"", "Id", "ID", "id", "UID", "Uid", "_id", "_uid"}
	// Each qualifier spells the same argument. The camel one is the Go-idiomatic
	// form and was the one that got through: \b sees no boundary at the hump.
	qualifiers := []struct {
		name string
		of   func(root string) string
	}{
		{"bare", func(r string) string { return r }},
		{"snake", func(r string) string { return "destination_" + r }},
		{"dotted", func(r string) string { return "destination." + r }},
		{"camel", func(r string) string { return "destination" + strings.ToUpper(r[:1]) + r[1:] }},
	}

	for _, root := range roots {
		for _, tail := range tails {
			for _, q := range qualifiers {
				arg := q.of(root) + tail
				call := "subject.SomeBuilder(" + arg + ")"
				t.Run(q.name+"/"+root+tail, func(t *testing.T) {
					assert.True(t, entityArg.MatchString(arg), "%s names an entity", arg)
					assert.False(t, boundedSubjectArg(call), "%s must not pass the guard", call)
				})
			}
		}
	}
}

// TestEntityArg_AllowsBoundedArguments is the other half, and the one that
// decides whether the anchoring was worth it. Every argument here contains an
// identity root and none of them *is* an identity — the root is followed by
// something other than an id tail. A guard that rejected these would be
// rejecting on the presence of a word rather than on what the argument names,
// and the first person to hit it would delete the guard rather than rename
// their variable.
func TestEntityArg_AllowsBoundedArguments(t *testing.T) {
	bounded := []string{
		"siteID",
		"cfg.SiteID",
		"subject.Wildcard",
		"eventType",
		"userAgent",
		"orgName",
		"errorType",
		"runInfo",
		"messageCount",
		"recipientCount",
		"spanKind",
	}
	for _, arg := range bounded {
		t.Run(arg, func(t *testing.T) {
			assert.False(t, entityArg.MatchString(arg), "%s names no entity", arg)
			assert.True(t, boundedSubjectArg("subject.SomeBuilder("+arg+")"),
				"subject.SomeBuilder(%s) must pass the guard", arg)
		})
	}
}

// TestUnboundedSubscriptions runs real source through the scanner, which is the
// seam the unit tests below never covered.
//
// boundedSubjectArg was tested with hand-written, well-formed strings, so it
// always saw `subject.RoomEvents(roomID)`. What the scanner actually handed it
// was `subject.RoomEvents(roomID` — the capture regex stopped at the first `)`
// — and builderArgs reads an unbalanced expression as having no arguments, so
// the guard accepted it. Every unit test passed while the gate was open. Only a
// test that starts from source rather than from a string can catch that.
func TestUnboundedSubscriptions(t *testing.T) {
	tests := []struct {
		name string
		body string
		want bool // want an offender
	}{
		{"entity builder argument", `nc.Subscribe(ctx, subject.RoomEvents(roomID), h)`, true},
		{"entity builder, no ctx", `nc.Subscribe(subject.RoomEvents(roomID), h)`, true},
		{"entity builder, queue form", `nc.QueueSubscribe(subject.RoomEvents(parentRoomID), "q", h)`, true},
		{"camel-prefixed entity argument", `nc.ChanSubscribe(subject.SomeBuilder(destinationOrgID), ch)`, true},
		{"formatted string", `nc.Subscribe(fmt.Sprintf("chat.room.%s", roomID), h)`, true},
		{"bare identifier", `nc.Subscribe(subj, h)`, true},

		{"bounded builder", `nc.Subscribe(ctx, subject.InboxExternalAll(siteID), h)`, false},
		{"builder named for a room, bounded argument", `nc.QueueSubscribe(subject.RoomEvents(siteID), "q", h)`, false},
		{"no arguments", `nc.Subscribe(subject.MessagesCanonicalAll(), h)`, false},
		{"router stored pattern", `nc.QueueSubscribe(rt.natsSubject, queue, h)`, false},
		{"context.Background leading arg", `nc.Subscribe(context.Background(), subject.InboxExternalAll(siteID), h)`, false},
		{"not a subscribe", `nc.Publish(subject.RoomEvents(roomID), data)`, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src := "package p\n\nfunc f() {\n\t" + tt.body + "\n}\n"
			got := unboundedSubscriptions("x.go", []byte(src))
			if tt.want {
				assert.NotEmpty(t, got, "%s must be reported", tt.body)
				return
			}
			assert.Empty(t, got, "%s must not be reported", tt.body)
		})
	}
}

// A file that cannot be parsed must be reported, not skipped — a scanner that
// goes quiet on bad input goes green for the wrong reason.
func TestUnboundedSubscriptions_UnparseableFileIsReported(t *testing.T) {
	got := unboundedSubscriptions("broken.go", []byte("package p\n\nfunc f( {\n"))
	assert.Len(t, got, 1)
	assert.Contains(t, got[0], "could not parse")
}

// TestBuilderArgs pins the name/argument split, because both gates now depend
// on it and a mistake there fails open — an empty argument list names no
// entity, so everything passes.
func TestBuilderArgs(t *testing.T) {
	tests := []struct {
		call string
		want string
	}{
		{"subject.SomeBuilder(roomID)", "roomID"},
		{"subject.SomeBuilder(siteID, roomID)", "siteID, roomID"},
		{"subject.SomeBuilder(subject.Wildcard)", "subject.Wildcard"},
		{"subject.SomeBuilder()", ""},
		{"subject.SomeConstant", ""},
	}
	for _, tt := range tests {
		t.Run(tt.call, func(t *testing.T) {
			assert.Equal(t, tt.want, builderArgs(tt.call))
		})
	}
}

// A builder whose own name contains a root must be judged on its argument.
// Matching over the whole call text rejected these, which is the kind of false
// positive that gets a guard deleted rather than fixed.
func TestBoundedSubjectArg_IgnoresTheBuilderName(t *testing.T) {
	assert.True(t, boundedSubjectArg("subject.RoomEvents(siteID)"))
	assert.True(t, boundedSubjectArg("subject.InboxExternalAll(siteID)"))
	assert.False(t, boundedSubjectArg("subject.RoomEvents(roomID)"))
	// Multiple arguments: any one of them naming an entity is enough.
	assert.False(t, boundedSubjectArg("subject.RoomEvents(siteID, roomID)"))
}
